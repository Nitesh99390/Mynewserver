import asyncio
import os
import zipfile

import pytest

from core import epub_engine
from core.epub_engine import EpubError, analyse, make_batches

CONTAINER = """<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles>
</container>"""

OPF = """<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>My Test Book</dc:title><dc:language>en</dc:language><dc:identifier id="id">x</dc:identifier>
  </metadata>
  <manifest>
    <item id="c1" href="text/ch1.xhtml" media-type="application/xhtml+xml"/>
    <item id="c2" href="text/ch2.xhtml" media-type="application/xhtml+xml"/>
    <item id="css" href="style.css" media-type="text/css"/>
    <item id="img" href="cover.png" media-type="image/png"/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx"><itemref idref="c2"/><itemref idref="c1"/></spine>
</package>"""

CH1 = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Chapter 1</title>
<style>p { color: red }</style></head>
<body><h1>Chapter One</h1>
<p>Hello <b>world</b>. This is a test.</p>
<p>  Leading and trailing spaces  </p>
<pre>code stays</pre><script>var x = 1;</script>
<p>12345</p><p>...</p>
<!-- a comment -->
</body></html>"""

CH2 = """<html><body><p>Second chapter text.</p><img src="../cover.png" alt="cover"/></body></html>"""

NCX = """<?xml version="1.0"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
<docTitle><text>My Test Book</text></docTitle>
<navMap><navPoint id="n1"><navLabel><text>Chapter One</text></navLabel><content src="text/ch1.xhtml"/></navPoint></navMap>
</ncx>"""


def build_epub(path: str, drm: bool = False) -> str:
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(zipfile.ZipInfo("mimetype"), b"application/epub+zip", compress_type=zipfile.ZIP_STORED)
        z.writestr("META-INF/container.xml", CONTAINER)
        if drm:
            z.writestr("META-INF/encryption.xml",
                       '<encryption><EncryptedData><CipherReference URI="OEBPS/text/ch1.xhtml"/></EncryptedData></encryption>')
        z.writestr("OEBPS/content.opf", OPF)
        z.writestr("OEBPS/text/ch1.xhtml", CH1)
        z.writestr("OEBPS/text/ch2.xhtml", CH2)
        z.writestr("OEBPS/style.css", "p{}")
        z.writestr("OEBPS/cover.png", b"\x89PNG\r\n\x1a\nfakepng")
        z.writestr("OEBPS/toc.ncx", NCX)
    return path


def test_analyse_collects_only_translatable(tmp_path):
    a = analyse(build_epub(str(tmp_path / "b.epub")))
    texts = [t.strip() for t in a.texts()]
    assert a.title == "My Test Book"
    assert a.language == "en"
    # spine order: ch2 first, then ch1, then ncx
    assert texts[0] == "Second chapter text."
    assert "Chapter One" in texts and "Hello" in texts and "world" in texts
    assert "Leading and trailing spaces" in texts
    # skipped: <title>, <style>, <pre>, <script>, numbers, punctuation, comments
    for bad in ("Chapter 1", "p { color: red }", "code stays", "var x = 1;", "12345", "...", "a comment"):
        assert bad not in texts
    assert a.total_nodes == len(texts) and a.total_chars == sum(len(t) for t in texts)
    # ncx labels included
    assert texts.count("My Test Book") == 1 and texts.count("Chapter One") == 2


def test_analyse_errors(tmp_path):
    bad = tmp_path / "bad.epub"
    bad.write_bytes(b"not a zip")
    with pytest.raises(EpubError):
        analyse(str(bad))
    with pytest.raises(EpubError, match="DRM"):
        analyse(build_epub(str(tmp_path / "drm.epub"), drm=True))
    empty = tmp_path / "empty.epub"
    with zipfile.ZipFile(empty, "w") as z:
        z.writestr("mimetype", "application/epub+zip")
    with pytest.raises(EpubError):
        analyse(str(empty))


def test_make_batches():
    texts = ["a" * 10] * 10
    assert make_batches(texts, 3, 1000) == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    assert make_batches(texts, 100, 25) == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]
    assert make_batches([], 3, 3) == []


def test_translate_end_to_end(tmp_path):
    src = build_epub(str(tmp_path / "book.epub"))
    a = analyse(src)
    calls = []

    async def fake_batch(texts, lang, hint):
        calls.append(len(texts))
        await asyncio.sleep(0)
        return [f"[{lang}]{t.strip()}" for t in texts], 0

    progress = []

    async def on_progress(done, failed):
        progress.append(done)

    out = asyncio.run(epub_engine.translate(a, "hi", fake_batch, on_progress, None, str(tmp_path)))
    assert os.path.exists(out) and out.endswith(".epub")
    assert sum(calls) == a.total_nodes
    assert progress and progress[-1] == a.total_chars

    with zipfile.ZipFile(out) as z:
        names = z.namelist()
        assert names[0] == "mimetype" and z.getinfo("mimetype").compress_type == zipfile.ZIP_STORED
        # untouched assets preserved byte-for-byte
        assert z.read("OEBPS/cover.png") == b"\x89PNG\r\n\x1a\nfakepng"
        assert z.read("OEBPS/style.css") == b"p{}"
        assert z.read("META-INF/container.xml").decode() == CONTAINER
        ch1 = z.read("OEBPS/text/ch1.xhtml").decode()
        assert "[hi]Hello" in ch1 and "<b>[hi]world</b>" in ch1
        assert "<p>  [hi]Leading and trailing spaces  </p>" in ch1       # whitespace preserved
        assert "code stays" in ch1 and "var x = 1;" in ch1 and "<title>Chapter 1</title>" in ch1
        assert "<p>12345</p>" in ch1 and "<!-- a comment -->" in ch1
        assert "[hi]Second chapter text." in z.read("OEBPS/text/ch2.xhtml").decode()
        ncx = z.read("OEBPS/toc.ncx").decode()
        assert "<text>[hi]Chapter One</text>" in ncx and "<navLabel>" in ncx and "<navMap>" in ncx
        assert "<dc:language>hi</dc:language>" in z.read("OEBPS/content.opf").decode()
        assert z.testzip() is None


def test_translate_cancel(tmp_path):
    a = analyse(build_epub(str(tmp_path / "c.epub")))
    cancel = asyncio.Event()

    async def slow_batch(texts, lang, hint):
        cancel.set()
        await asyncio.sleep(0.01)
        return texts, 0

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(epub_engine.translate(a, "hi", slow_batch, None, cancel, str(tmp_path)))
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".part")]
