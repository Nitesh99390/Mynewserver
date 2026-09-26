# 📚 EPUB Translator Bot v3

Telegram bot jo EPUB books ko 30+ bhashaon mein translate karta hai — **images, cover, CSS, TOC aur chapters bilkul same rehte hain**. Do parts hain:

| File | Role | Kahan chalega |
|------|------|---------------|
| `bot.py` (**single file**) | **Master bot** — Telegram, users, plans, payments, queue, EPUB rebuild, worker manager. **Kabhi Google ko request nahi bhejta** — sirf workers ko | Aapka Oracle VPS (12 GB) |
| `app.py` | **Worker** — stateless FastAPI service, text batches translate karta hai | Render / Vercel free tier (jitne chahiye) |

Master bot batches ko sab workers pe **load-balance** karta hai (least-loaded → fastest), fail hone par doosre worker pe **failover**, aur free-tier hosts ko sone se rokne ke liye **keep-alive ping** karta hai.

---

## 💰 Plans (default — `.env` se badal sakte hain)

| Plan | Price | Detail |
|------|-------|--------|
| 🆓 Free | ₹0 | **1,000,000 characters har din**, har user ko (roz reset) |
| ⭐ Unlimited | **₹100 / 30 din** | Koi limit nahi |
| 💎 Credits | **₹2 per 1M characters** | 1M, 2M, 4M, 6M … 500M tak custom; kabhi expire nahi hote |

Kharch pehle free quota se, phir credits se hota hai. Job fail/cancel hone par **auto-refund**.

Payment: **Razorpay** (payment link + one-tap verify) aur/ya **UPI manual** (user UTR bhejta hai → admin ko Approve/Reject button milta hai). Dono optional hain; koi set na ho to "Contact admin" dikhta hai aur admin `/grant` `/credits` se manually de sakta hai.

---

## 🤖 User experience (kam buttons, sirf zaroori)

```
/start  →  👤 My Plan | 🌐 Language | 💳 Buy          (3 persistent buttons)

.epub bhejo →  📚 Title • 700K chars • 1,200 segments
               🌐 Hindi
               💰 Cost: 400K free + 300K credits
               [▶️ Translate]  [🌐 Language] [✖️ Cancel]

Translating →  ████░░░░ 42% • 420K/1M • ETA 1m 30s   [⏹ Stop]
Done        →  translated .epub file (same name + _hi.epub)
```

Balance kam ho to confirm screen pe seedha **"💎 Buy 1M — ₹2"** / **"⭐ Unlimited — ₹100"** button aata hai; payment ke baad **🔄 Re-check** dabao aur translate ho jayega.

Commands: `/plan` `/lang` `/buy` `/cancel` `/help`

### Admin (⚙️ Admin button / `/admin`)
📊 Stats • 🖥 Workers (➕ Add / ➖ Remove / 🔄 Ping) • 💰 Payments (pending UPI approvals) • 📣 Broadcast • 🧹 Maintenance

Commands: `/stats` `/workers` `/grant <uid> [days]` `/credits <uid> <millions>` `/revoke <uid>` `/ban <uid>` `/unban <uid>` `/user <uid>`

---

## 🚀 Deploy

### 1) Workers — Render (free)

1. Repo ko GitHub pe rakhein → Render Dashboard → **New → Blueprint** → yeh repo chunein (`render.yaml` sab set kar dega).
2. Env var `WORKER_SECRET` set karein (koi bhi lamba random string) — **same value bot ke `.env` mein bhi**.
3. Deploy hone par URL milega, e.g. `https://epub-translator-worker.onrender.com`.
4. Aur workers chahiye? Blueprint mein `name` badal ke dobara deploy karein (ya repo fork karein). Har URL ko bot mein **⚙️ Admin → 🖥 Workers → ➕ Add** se jodein (add karne se pehle bot health-check karta hai).

Manual setup: Build `pip install -r requirements.txt`, Start `python app.py`, Health path `/health`.

### 2) Workers — Vercel (free)

```bash
npm i -g vercel
vercel --prod            # vercel.json + api/index.py already hai
vercel env add WORKER_SECRET production
```
> Vercel serverless hai (10 s timeout on hobby) — chhote batches ke liye theek hai; heavy load ke liye Render prefer karein. Dono ko mix kar sakte hain.

### 3) Master bot — Oracle VPS (Ubuntu)

```bash
git clone https://github.com/Nitesh99390/Mynewserver.git ~/epub-translator
cd ~/epub-translator
bash deploy/install.sh          # venv + deps + systemd unit
nano .env                       # API_ID, API_HASH, BOT_TOKEN, OWNER_ID, WORKER_SECRET, (UPI_ID / RAZORPAY_*)
sudo systemctl start epub-bot
journalctl -u epub-bot -f       # logs
```

Update karna ho: `git pull && sudo systemctl restart epub-bot`.
Data `data/` folder mein rehta hai (SQLite `bot.sqlite3`, session, `bot.log`). Backup = is folder ki copy.

Purana `data.json` (v2) mila to pehli baar start pe **auto-migrate** ho jata hai (workers + languages + active premium).

---

## ⚙️ Configuration (`.env.example` dekhein)

| Var | Default | Kya karta hai |
|-----|---------|---------------|
| `API_ID` `API_HASH` `BOT_TOKEN` `OWNER_ID` | — | **Required** |
| `ADMIN_IDS` | | Extra admins, comma-separated |
| `WORKER_SECRET` | | Workers ke saath shared secret (`X-Worker-Secret` header) |
| `WORKERS` | | Pre-seed worker URLs (optional; admin panel se bhi add ho sakte hain) |
| `FREE_DAILY_CHARS` | `1000000` | Daily free characters |
| `UNLIMITED_PRICE_INR` / `UNLIMITED_DAYS` | `100` / `30` | Unlimited plan |
| `CREDIT_PRICE_PER_MILLION_INR` | `2` | Credits rate |
| `MIN/MAX_CREDIT_MILLIONS` | `1` / `500` | Custom credits range |
| `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET` | | Online payment |
| `UPI_ID` / `UPI_NAME` | | Manual UPI payment |
| `SUPPORT_CONTACT` | | Help text mein dikhega |
| `MAX_FILE_MB` | `50` | EPUB size limit |
| `MAX_CONCURRENT_JOBS` | `3` | Ek saath kitni books |
| `PARALLEL_BATCHES` | `10` | Per job parallel worker requests |
| `BATCH_ITEMS` / `BATCH_CHARS` | `40` / `4000` | Batch size |
| `WORKER_TIMEOUT` / `WORKER_COOLDOWN` | `90` / `90` s | Failover tuning |
| `KEEPALIVE_INTERVAL` | `300` s | Worker ping interval |

Worker side (`app.py`): `PORT` (host set karta hai), `WORKER_SECRET`, `MAX_CONCURRENCY=8`, `MAX_INFLIGHT=16`, `UPSTREAM_TIMEOUT=20`.

---

## 🛡️ Reliability

- Har Telegram call `safe_*` wrappers ke through — FloodWait, MessageNotModified, blocked users sab handle.
- Har callback/handler try/except mein; kabhi bhi ek user ki error se bot crash nahi hota.
- Worker: kabhi 500 nahi deta — chunk fail ho to aadha karke retry, phir bhi fail to original text return karta hai aur `failed` count deta hai. Primary + fallback Google endpoint.
- EPUB **ZIP-level in-place rebuild**: sirf XHTML/NCX text nodes badalte hain; `mimetype` first & stored, baaki entries byte-for-byte copy. DRM detect → clear message.
- Cancel (⏹ Stop / `/cancel`) turant, refund ke saath. Bot restart pe stale jobs mark + temp files cleanup.
- SQLite WAL mode, atomic transactions, thread-safe.
- Graceful shutdown on SIGTERM (systemd stop) — chal rahe jobs refund hote hain.

## 🧪 Tests

```bash
pip install -r requirements-bot.txt -r requirements.txt pytest httpx
python -m pytest tests -q        # 31 tests: store/quota/refunds, pricing parser, EPUB engine end-to-end, worker API
```

## 📁 Layout

```
bot.py                MASTER — ek hi file (Oracle VPS pe sirf yeh + requirements-bot.txt + .env chahiye)
                        ├ Configuration (env → Settings, LANGUAGES)
                        ├ Storage (SQLite: users, credits, payments, jobs, workers)
                        ├ Plans & payments (pricing, Razorpay, UPI, fulfil)
                        ├ EPUB engine (analyse → batch → in-place ZIP rebuild)
                        ├ WorkerPool (least-loaded routing, failover, cooldown, keep-alive)
                        ├ JobManager (pending confirm, queue, runners, refunds, janitor)
                        ├ UI (texts + keyboards)
                        └ Telegram handlers + entrypoint
app.py                WORKER — translation service (FastAPI); yahi Google ko call karta hai
api/index.py          Vercel entry • render.yaml • vercel.json
deploy/               systemd unit + install script
tests/                pytest suite
```
