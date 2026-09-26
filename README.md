# EPUB Translator (Telegram Bot + Translation Workers)

Two small services:

| File | Role | Host |
|------|------|------|
| `app.py` | **Worker** – FastAPI micro-service that translates batches of text via Google Translate (free `gtx` endpoint). Stateless, deploy as many as you like. | Render / Vercel / any free tier |
| `bot.py` | **Master** – Pyrogram Telegram bot. Receives EPUBs, extracts text nodes, load-balances batches across workers, rebuilds the EPUB in place (images / CSS / TOC kept). | VPS / Railway / PC |

## Worker (`app.py`)

```bash
pip install -r requirements.txt
PORT=8000 WORKER_SECRET=mysecret python app.py
```

Endpoints: `GET /` (keep-alive), `GET /health`, `POST /translate`
`{"text_list": [...], "lang": "hi", "source": "auto"}` → `{"success": true, "translated": [...], "failed": 0, "took_ms": 57}`

If `WORKER_SECRET` is set, requests must carry header `X-Worker-Secret`.

Render: build `pip install -r requirements.txt`, start `python app.py`.

## Master bot (`bot.py`)

```bash
pip install -r requirements-bot.txt
cp .env.example .env   # fill API_ID, API_HASH, BOT_TOKEN, OWNER_ID, WORKER_SECRET
python bot.py
```

Owner menu: ➕ Add Worker (health-checked before adding) • ➖ Del Worker • 🖥 Worker List (live status) • 🧹 Clear Stuck Tasks
Owner commands: `/stats`, `/premium <user_id> [days]`, `/revoke <user_id>`
User: 🌐 Set Language • 📊 Queue Status • `/pay`

Features: per-user daily free quota, premium expiry, worker failover + cooldown, parallel batching, live progress, atomic `data.json` persistence.
