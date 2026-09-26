"""
EPUB engine – analyse, translate and rebuild EPUB files *in place*.

Instead of re-generating the book with ebooklib (which can drop covers, CSS
or TOC entries), the original ZIP is copied entry-by-entry and only the
XHTML documents (and NCX labels) are replaced.  Everything else – images,
fonts, CSS, metadata, layout – survives byte-for-byte.

Public API
----------
analyse(path)  -> Analysis              (sync, run in a thread)
translate(analysis, lang, translate_batch, on_progress, cancel) -> out_path
"""

from __future__ import annotations

import asyncio
import logging
import os
import posixpath
import re
import shutil
import tempfile
import zipfile
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

import warnings

from bs4 import BeautifulSoup, Comment, NavigableString, XMLParsedAsHTMLWarning
from bs4.element import CData, Declaration, Doctype, ProcessingInstruction

from .config import settings

log = logging.getLogger("epub")
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

SKIP_TAGS = {"script", "style", "code", "pre", "kbd", "samp", "var", "math", "svg", "textarea", "head", "title"}
_NON_TEXT = re.compile(r"^[\W\d_]+$", re.UNICODE)          # digits / punctuation only
_IGNORED_NODE_TYPES = (Comment, CData, Declaration, Doctype, ProcessingInstruction)
_NS_CONTAINER = "{urn:oasis:names:tc:opendocument:xmlns:container}"
_NS_OPF = "{http://www.idpf.org/2007/opf}"
_XHTML_TYPES = {"application/xhtml+xml", "text/html", "application/x-dtbook+xml"}
_XHTML_EXT = (".xhtml", ".html", ".htm", ".xml")


class EpubError(Exception):
    """User-facing, safe to show in Telegram."""


@dataclass
class DocInfo:
    name: str                                  # zip entry name
    soup: BeautifulSoup
    nodes: List[NavigableString]
    is_ncx: bool = False


@dataclass
class Analysis:
    path: str
    title: str
    docs: List[DocInfo] = field(default_factory=list)
    opf_name: Optional[str] = None
    total_nodes: int = 0
    total_chars: int = 0
    language: str = ""

    def texts(self) -> List[str]:
        return [str(n) for d in self.docs for n in d.nodes]


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #
def _collect_nodes(soup: BeautifulSoup) -> List[NavigableString]:
    """All translatable text nodes in document order; structure untouched."""
    nodes: List[NavigableString] = []
    root = soup.body or soup                      # never touch <head> (title, meta, style)
    for node in root.descendants:
        if not isinstance(node, NavigableString) or isinstance(node, _IGNORED_NODE_TYPES):
            continue
        text = str(node).strip()
        if not text or _NON_TEXT.match(text):
            continue
        parent = node.parent
        skip = False
        while parent is not None and getattr(parent, "name", None):
            if parent.name.lower() in SKIP_TAGS:
                skip = True
                break
            parent = parent.parent
        if not skip:
            nodes.append(node)
    return nodes


def _ncx_nodes(soup: BeautifulSoup) -> List[NavigableString]:
    """Only navLabel/text and docTitle/text in an NCX."""
    out: List[NavigableString] = []
    for tag in soup.find_all("text"):
        for child in tag.children:
            if isinstance(child, NavigableString) and not isinstance(child, _IGNORED_NODE_TYPES) \
                    and str(child).strip() and not _NON_TEXT.match(str(child).strip()):
                out.append(child)
    return out


def _find_opf(zf: zipfile.ZipFile) -> Optional[str]:
    try:
        root = ET.fromstring(zf.read("META-INF/container.xml"))
        for rf in root.iter(f"{_NS_CONTAINER}rootfile"):
            full = rf.get("full-path")
            if full and full in zf.namelist():
                return full
    except (KeyError, ET.ParseError):
        pass
    for n in zf.namelist():                      # fallback: first *.opf
        if n.lower().endswith(".opf"):
            return n
    return None


def _manifest(zf: zipfile.ZipFile, opf_name: str) -> Tuple[List[str], Optional[str], str, str]:
    """Return (xhtml entry names in spine order, ncx entry name, title, language)."""
    base = posixpath.dirname(opf_name)
    names = set(zf.namelist())

    def resolve(href: str) -> str:
        href = href.split("#", 1)[0]
        try:
            from urllib.parse import unquote
            href = unquote(href)
        except Exception:  # pragma: no cover
            pass
        return posixpath.normpath(posixpath.join(base, href)) if base else posixpath.normpath(href)

    docs: List[str] = []
    ncx: Optional[str] = None
    title, lang = "", ""
    try:
        root = ET.fromstring(zf.read(opf_name))
    except ET.ParseError as exc:
        raise EpubError("EPUB ka package file (OPF) corrupt hai.") from exc

    items: Dict[str, Tuple[str, str]] = {}
    for it in root.iter(f"{_NS_OPF}item"):
        iid, href, mt = it.get("id"), it.get("href"), (it.get("media-type") or "").lower()
        if iid and href:
            items[iid] = (resolve(href), mt)
    spine_ids = [ref.get("idref") for ref in root.iter(f"{_NS_OPF}itemref") if ref.get("idref")]
    seen = set()
    for iid in spine_ids + list(items.keys()):        # spine order first, then the rest
        if iid in seen or iid not in items:
            continue
        seen.add(iid)
        name, mt = items[iid]
        if name not in names:
            continue
        if mt in _XHTML_TYPES or (not mt and name.lower().endswith(_XHTML_EXT)):
            docs.append(name)
        elif mt == "application/x-dtbncx+xml" or name.lower().endswith(".ncx"):
            ncx = name

    for el in root.iter():
        tag = el.tag.rsplit("}", 1)[-1]
        if tag == "title" and not title and el.text:
            title = el.text.strip()
        elif tag == "language" and not lang and el.text:
            lang = el.text.strip()
    return docs, ncx, title, lang


def _check_drm(zf: zipfile.ZipFile) -> None:
    if "META-INF/encryption.xml" not in zf.namelist():
        return
    try:
        xml = zf.read("META-INF/encryption.xml").decode("utf-8", "ignore")
    except KeyError:
        return
    if re.search(r'URI="[^"]+\.(x?html?|xml)"', xml, re.I):
        raise EpubError("Yeh EPUB DRM-protected hai, isko translate nahi kiya ja sakta.")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def analyse(path: str) -> Analysis:
    """Parse the EPUB and collect every translatable text node (CPU bound – run in a thread)."""
    if not zipfile.is_zipfile(path):
        raise EpubError("File valid EPUB nahi hai (ZIP header missing).")
    try:
        zf = zipfile.ZipFile(path)
    except zipfile.BadZipFile as exc:
        raise EpubError("EPUB file corrupt hai.") from exc

    with zf:
        if zf.testzip() is not None:
            raise EpubError("EPUB ke andar corrupt entries hain.")
        _check_drm(zf)
        opf = _find_opf(zf)
        if not opf:
            raise EpubError("EPUB mein OPF package file nahi mila.")
        doc_names, ncx_name, title, lang = _manifest(zf, opf)
        if not doc_names:
            raise EpubError("EPUB mein koi text document (XHTML) nahi mila.")

        analysis = Analysis(path=path, title=title or os.path.splitext(os.path.basename(path))[0],
                            opf_name=opf, language=lang)
        for name in doc_names:
            try:
                raw = zf.read(name)
            except KeyError:
                continue
            soup = BeautifulSoup(raw, "html.parser")
            nodes = _collect_nodes(soup)
            if nodes:
                analysis.docs.append(DocInfo(name, soup, nodes))
        if ncx_name:
            try:
                # NCX is real XML (case-sensitive tags) – use the XML parser
                soup = BeautifulSoup(zf.read(ncx_name), "xml")
                nodes = _ncx_nodes(soup)
                if nodes:
                    analysis.docs.append(DocInfo(ncx_name, soup, nodes, is_ncx=True))
            except KeyError:
                pass

    analysis.total_nodes = sum(len(d.nodes) for d in analysis.docs)
    analysis.total_chars = sum(len(str(n).strip()) for d in analysis.docs for n in d.nodes)
    if analysis.total_nodes == 0:
        raise EpubError("Is EPUB mein koi translatable text nahi mila (image-only book?).")
    return analysis


def make_batches(texts: List[str], max_items: int, max_chars: int) -> List[List[int]]:
    batches: List[List[int]] = []
    cur: List[int] = []
    chars = 0
    for i, t in enumerate(texts):
        if cur and (len(cur) >= max_items or chars + len(t) > max_chars):
            batches.append(cur)
            cur, chars = [], 0
        cur.append(i)
        chars += len(t)
    if cur:
        batches.append(cur)
    return batches


def _preserve_ws(original: str, translated: str) -> str:
    lead = original[: len(original) - len(original.lstrip())]
    trail = original[len(original.rstrip()):]
    return f"{lead}{translated.strip()}{trail}"


def _apply(analysis: Analysis, translated: List[str]) -> None:
    i = 0
    for doc in analysis.docs:
        for node in doc.nodes:
            t = translated[i]
            i += 1
            if t and t != str(node):
                try:
                    node.replace_with(NavigableString(_preserve_ws(str(node), t)))
                except Exception:  # node detached – ignore
                    pass


def _serialize(doc: DocInfo) -> bytes:
    if doc.is_ncx:
        return str(doc.soup).encode("utf-8")          # lxml-xml keeps the declaration + case
    return doc.soup.decode(formatter="minimal").encode("utf-8")


def _patch_opf(raw: bytes, lang: str) -> bytes:
    """Update <dc:language> so readers pick the right fonts / hyphenation."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw
    new, n = re.subn(r"(<dc:language[^>]*>)[^<]*(</dc:language>)", rf"\g<1>{lang}\g<2>", text, count=1)
    return new.encode("utf-8") if n else raw


def write_translated(analysis: Analysis, lang: str, out_path: str) -> None:
    """Copy the original ZIP, swapping translated documents in.  Atomic on success."""
    replaced: Dict[str, bytes] = {d.name: _serialize(d) for d in analysis.docs}
    tmp = out_path + ".part"
    with zipfile.ZipFile(analysis.path) as src, zipfile.ZipFile(tmp, "w") as dst:
        infos = src.infolist()
        # 'mimetype' must be the first entry and stored uncompressed (EPUB spec)
        dst.writestr(zipfile.ZipInfo("mimetype"), b"application/epub+zip", compress_type=zipfile.ZIP_STORED)
        for info in infos:
            if info.filename == "mimetype" or info.filename.endswith("/"):
                continue
            if info.filename in replaced:
                dst.writestr(info.filename, replaced[info.filename], compress_type=zipfile.ZIP_DEFLATED)
            elif info.filename == analysis.opf_name:
                dst.writestr(info.filename, _patch_opf(src.read(info), lang), compress_type=zipfile.ZIP_DEFLATED)
            else:
                dst.writestr(info, src.read(info), compress_type=info.compress_type)
    os.replace(tmp, out_path)


TranslateFn = Callable[[List[str], str, int], Awaitable[Tuple[List[str], int]]]
ProgressFn = Callable[[int, int], Awaitable[None]]           # (done_chars, failed_segments)


async def translate(
    analysis: Analysis,
    lang: str,
    translate_batch: TranslateFn,
    on_progress: Optional[ProgressFn] = None,
    cancel: Optional[asyncio.Event] = None,
    out_dir: Optional[str] = None,
) -> str:
    """
    Translate every collected node through `translate_batch` and write the
    new EPUB.  Returns the output path.  Raises asyncio.CancelledError when
    `cancel` is set.
    """
    texts = analysis.texts()
    results: List[str] = list(texts)
    batches = make_batches(texts, settings.batch_items, settings.batch_chars)
    sem = asyncio.Semaphore(max(1, settings.parallel_batches))
    done_chars = 0
    failed = 0
    lock = asyncio.Lock()

    async def run(bi: int, idxs: List[int]) -> None:
        nonlocal done_chars, failed
        if cancel and cancel.is_set():
            raise asyncio.CancelledError
        async with sem:
            if cancel and cancel.is_set():
                raise asyncio.CancelledError
            out, nf = await translate_batch([texts[i] for i in idxs], lang, bi)
        if cancel and cancel.is_set():
            raise asyncio.CancelledError
        for i, t in zip(idxs, out):
            results[i] = t
        async with lock:
            done_chars += sum(len(texts[i].strip()) for i in idxs)
            failed += nf
        if on_progress:
            try:
                await on_progress(done_chars, failed)
            except Exception:  # never let UI errors break the job
                pass

    tasks = [asyncio.create_task(run(bi, b)) for bi, b in enumerate(batches)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    if cancel and cancel.is_set():
        raise asyncio.CancelledError

    _apply(analysis, results)
    out_dir = out_dir or os.path.dirname(analysis.path) or "."
    safe_title = re.sub(r"[^\w\s.-]", "", analysis.title, flags=re.UNICODE).strip()[:60] or "book"
    fd, out_path = tempfile.mkstemp(prefix=f"{safe_title}_{lang}_", suffix=".epub", dir=out_dir)
    os.close(fd)
    await asyncio.to_thread(write_translated, analysis, lang, out_path)
    analysis.failed_segments = failed  # type: ignore[attr-defined]
    return out_path


def cleanup(*paths: Optional[str]) -> None:
    for p in paths:
        if not p:
            continue
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.exists(p):
                os.remove(p)
        except OSError:
            pass
