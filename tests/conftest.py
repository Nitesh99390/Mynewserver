import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Minimal env so bot.py imports without a real bot token.
os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "x")
os.environ.setdefault("BOT_TOKEN", "x")
os.environ.setdefault("OWNER_ID", "1")
os.environ.setdefault("DATA_DIR", os.path.join(ROOT, "tests", ".data"))
