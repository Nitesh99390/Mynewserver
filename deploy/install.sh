#!/usr/bin/env bash
# One-shot installer for the master bot on Ubuntu (Oracle Cloud free/paid tier).
#   curl -fsSL <raw url>/deploy/install.sh | bash   – or –   bash deploy/install.sh
set -euo pipefail
APP_DIR="${APP_DIR:-$HOME/epub-translator}"
sudo apt-get update -y && sudo apt-get install -y python3 python3-venv python3-pip git
if [ ! -d "$APP_DIR/.git" ]; then
  git clone "${REPO_URL:-https://github.com/Nitesh99390/Mynewserver.git}" "$APP_DIR"
fi
cd "$APP_DIR"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements-bot.txt
[ -f .env ] || { cp .env.example .env; echo ">> Edit $APP_DIR/.env and fill API_ID, API_HASH, BOT_TOKEN, OWNER_ID"; }
sed "s#/home/ubuntu/epub-translator#$APP_DIR#g; s#User=ubuntu#User=$USER#" deploy/epub-bot.service | sudo tee /etc/systemd/system/epub-bot.service >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable epub-bot
echo ">> Done. Start with:  sudo systemctl start epub-bot   |  logs: journalctl -u epub-bot -f"
