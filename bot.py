import os
import json
import asyncio
import aiohttp
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import razorpay

from dotenv import load_dotenv
load_dotenv()

# --- Configurations ---
API_ID = int(os.environ.get("API_ID", "36681596"))
API_HASH = os.environ.get("API_HASH", "bece5a5cb8d1abc08b644410b6e85d5e")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID", "6069200310"))
RAZORPAY_KEY_ID = os.environ.get("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET")

rzp_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
app = Client("TranslatorBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- Persistence (Auto-Save Logic) ---
DATA_FILE = "data.json"
WORKERS = []
users_db = {}
admin_state = {}

def load_data():
    global WORKERS, users_db
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                WORKERS = data.get("workers", [])
                users_db = {int(k): v for k, v in data.get("users_db", {}).items()}
        except Exception as e:
            print(f"Data load error: {e}")

def save_data():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({"workers": WORKERS, "users_db": users_db}, f)
    except Exception as e:
        print(f"Data save error: {e}")

# Startup par purana data load karna
load_data()

# --- Queue System ---
translation_queue = asyncio.Queue()
active_tasks = {}

LANGUAGES = {
    "hi": "Hindi", "bn": "Bengali", "ta": "Tamil", "te": "Telugu",
    "mr": "Marathi", "gu": "Gujarati", "ur": "Urdu"
}

# --- Smart Keyboard Menu ---
def get_main_keyboard(user_id):
    buttons = [
        [KeyboardButton("🌐 Set Language"), KeyboardButton("💳 Premium (/pay)")],
        [KeyboardButton("📊 Queue Status"), KeyboardButton("❓ Help")]
    ]
    if user_id == OWNER_ID:
        buttons.append([KeyboardButton("➕ Add Worker"), KeyboardButton("➖ Del Worker")])
        buttons.append([KeyboardButton("🖥 Worker List"), KeyboardButton("🧹 Clear Stuck Tasks")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# --- Core Translation Worker Logic ---
async def keep_workers_alive():
    """Render workers ko sleep mode se bachane ke liye har 10 min mein ping karega"""
    while True:
        if WORKERS:
            async with aiohttp.ClientSession() as session:
                for worker in WORKERS:
                    try:
                        await session.get(f"{worker}/")
                    except:
                        pass
        await asyncio.sleep(600)

async def process_queue():
    """Queue mein aayi files ko ek-ek karke process karega"""
    while True:
        task = await translation_queue.get()
        user_id, file_path, target_lang, original_name, status_msg = task
        try:
            active_tasks[user_id] = "Processing"
            await perform_translation(user_id, file_path, target_lang, original_name, status_msg)
        except Exception as e:
            await status_msg.edit_text(f"❌ Translation failed: {e}")
        finally:
            active_tasks.pop(user_id, None)
            translation_queue.task_done()
            if os.path.exists(file_path):
                os.remove(file_path)

async def translate_batch_req(session, text_list, target_lang, worker_url, retries=3):
    """Worker ko paragraphs ka batch bhej kar super-fast translation laayega"""
    for attempt in range(retries):
        try:
            async with session.post(f"{worker_url}/translate", json={"text_list": text_list, "lang": target_lang}, timeout=45) as response:
                result = await response.json()
                if result.get("success"): 
                    return result.get("translated")
        except:
            if attempt == retries - 1: return text_list
            await asyncio.sleep(2)
    return text_list

async def perform_translation(user_id, file_path, target_lang, original_name, status_msg):
    book = epub.read_epub(file_path)
    new_book = epub.EpubBook()
    new_book.metadata = book.metadata
    new_book.spine = book.spine
    new_book.toc = book.toc

    total_items = len(list(book.get_items()))
    processed_items = 0

    async with aiohttp.ClientSession() as session:
        worker_index = 0
        for item in book.get_items():
            if item.get_type() == ebooklib.ITEM_DOCUMENT:
                soup = BeautifulSoup(item.get_content(), 'html.parser')
                paragraphs = soup.find_all(['p', 'h1', 'h2', 'h3', 'div', 'span'])

                valid_paragraphs = [p for p in paragraphs if p.text.strip()]
                texts_to_translate = [p.text for p in valid_paragraphs]

                batch_size = 50 
                tasks = []

                for i in range(0, len(texts_to_translate), batch_size):
                    batch = texts_to_translate[i:i+batch_size]
                    if not WORKERS:
                        raise Exception("All workers are offline or deleted!")
                    worker_url = WORKERS[worker_index % len(WORKERS)]
                    tasks.append(translate_batch_req(session, batch, target_lang, worker_url))
                    worker_index += 1

                results = await asyncio.gather(*tasks)

                translated_texts = []
                for res in results:
                    translated_texts.extend(res)

                for p, t_text in zip(valid_paragraphs, translated_texts):
                    if t_text:
                        p.string = str(t_text)

                item.content = str(soup).encode('utf-8')
            new_book.add_item(item)
            processed_items += 1

            if processed_items % max(1, (total_items // 10)) == 0:
                progress = int((processed_items / total_items) * 100)
                try:
                    await status_msg.edit_text(f"⏳ Translation in progress: {progress}%\nUsing {len(WORKERS)} worker(s).")
                except:
                    pass

    output_file = f"Translated_{original_name}"
    epub.write_epub(output_file, new_book)
    await status_msg.edit_text("✅ Translation complete! Uploading file...")
    await app.send_document(chat_id=user_id, document=output_file, caption="Here is your translated book!")

    if os.path.exists(output_file): 
        os.remove(output_file)

# --- Bot Message Handlers ---
@app.on_message(filters.command("start"))
async def start(client, message):
    user_id = message.from_user.id
    if user_id not in users_db:
        users_db[user_id] = {"lang": "hi", "has_subscription": False}
        save_data()

    await message.reply_text(
        "Namaste! Main ek Super Fast EPUB Translator Bot hu.\n\nKripya niche diye gaye menu ka use karein ya seedhe ek EPUB file bhejein.",
        reply_markup=get_main_keyboard(user_id)
    )

@app.on_message(filters.document)
async def handle_document(client, message):
    user_id = message.from_user.id
    if not message.document.file_name.lower().endswith('.epub'):
        return await message.reply_text("⚠️ Kripya sirf .epub file bhejein.")

    if not WORKERS:
        return await message.reply_text("⚠️ Koi bhi worker online nahi hai. Menu se pehle worker add karein.")

    if user_id in active_tasks:
         return await message.reply_text("⚠️ Aapki ek file pehle se process ho rahi hai. Kripya wait karein.")

    target_lang = users_db.get(user_id, {}).get("lang", "hi")
    queue_pos = translation_queue.qsize() + 1
    status_msg = await message.reply_text(f"📥 File received. Position in queue: {queue_pos}\nDownloading...")

    file_path = await message.download()
    await status_msg.edit_text(f"✅ Download complete! Waiting in queue (Position: {queue_pos})...")

    await translation_queue.put((user_id, file_path, target_lang, message.document.file_name, status_msg))

@app.on_message(filters.text & ~filters.command(["start", "pay"]))
async def handle_text_buttons(client, message):
    user_id = message.from_user.id
    text = message.text.strip()

    if user_id == OWNER_ID and user_id in admin_state:
        state = admin_state[user_id]
        if text == "❌ Cancel":
            del admin_state[user_id]
            return await message.reply_text("Action cancelled.", reply_markup=get_main_keyboard(user_id))

        if state == "ADDING_WORKER":
            url = text if text.startswith("http") else "https://" + text
            url = url.rstrip("/")
            if url not in WORKERS:
                WORKERS.append(url)
                save_data()
                await message.reply_text(f"✅ Worker added!\nTotal workers: {len(WORKERS)}", reply_markup=get_main_keyboard(user_id))
            else:
                await message.reply_text("⚠️ Worker pehle se majood hai.", reply_markup=get_main_keyboard(user_id))
            del admin_state[user_id]
            return

        elif state == "DELETING_WORKER":
            url = text if text.startswith("http") else "https://" + text
            url = url.rstrip("/")
            if url in WORKERS:
                WORKERS.remove(url)
                save_data()
                await message.reply_text(f"✅ Worker removed!\nTotal workers left: {len(WORKERS)}", reply_markup=get_main_keyboard(user_id))
            else:
                await message.reply_text("⚠️ Worker list mein nahi mila.", reply_markup=get_main_keyboard(user_id))
            del admin_state[user_id]
            return

    if text == "🌐 Set Language":
        buttons = [InlineKeyboardButton(l, callback_data=f"lang_{c}") for c, l in LANGUAGES.items()]
        keyboard = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
        await message.reply_text("Niche apni pasandida bhasha chunein:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif text == "💳 Premium (/pay)":
        await pay_command(client, message)

    elif text == "📊 Queue Status":
        q_size = translation_queue.qsize()
        active = len(active_tasks)
        await message.reply_text(f"📊 **System Status**\n\nWorkers Online: {len(WORKERS)}\nFiles Processing: {active}\nFiles in Queue: {q_size}")

    elif text == "❓ Help":
        await message.reply_text("1. Apni EPUB file upload karein.\n2. Language set karein.\n3. Bot automatically translate karke file de dega.\n\nPremium ke liye 'Premium' button dabayein.")

    elif text == "➕ Add Worker" and user_id == OWNER_ID:
        admin_state[user_id] = "ADDING_WORKER"
        cancel_kb = ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True)
        await message.reply_text("Kripya naye worker ka URL bhejein (eg: https://app.onrender.com):", reply_markup=cancel_kb)

    elif text == "➖ Del Worker" and user_id == OWNER_ID:
        admin_state[user_id] = "DELETING_WORKER"
        cancel_kb = ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True)
        await message.reply_text("Kripya worker ka URL bhejein jise hatana hai:", reply_markup=cancel_kb)

    elif text == "🖥 Worker List" and user_id == OWNER_ID:
        if not WORKERS:
            await message.reply_text("Koi worker list mein nahi hai.")
        else:
            await message.reply_text("Current Workers:\n\n" + "\n".join([f"{i+1}. {w}" for i, w in enumerate(WORKERS)]))

    elif text == "🧹 Clear Stuck Tasks" and user_id == OWNER_ID:
        active_tasks.clear()
        await message.reply_text("✅ Sabhi stuck active tasks clear kar diye gaye hain. Log ab nayi files bhej sakte hain.")

@app.on_callback_query(filters.regex(r"^lang_"))
async def set_language(client, callback_query):
    lang_code = callback_query.data.split("_")[1]
    user_id = callback_query.from_user.id
    if user_id not in users_db:
         users_db[user_id] = {"lang": lang_code, "has_subscription": False}
    else:
        users_db[user_id]["lang"] = lang_code
    save_data()
    await callback_query.answer(f"Language {LANGUAGES[lang_code]} set ho gayi hai.", show_alert=True)
    await callback_query.message.edit_text(f"Selected Language: **{LANGUAGES[lang_code]}**\nAb apni EPUB file bhej sakte hain.")

@app.on_message(filters.command("pay"))
async def pay_command(client, message):
    amount = 10000 
    try:
        payment_link_data = {
            "amount": amount,
            "currency": "INR",
            "accept_partial": False,
            "description": "Unlimited Translation Subscription (1 Month)",
            "notify": {"sms": False, "email": False},
            "reminder_enable": False,
            "notes": {"user_id": message.from_user.id}
        }
        payment_link = rzp_client.payment_link.create(data=payment_link_data)
        await message.reply_text(
            f"Please pay ₹100 for 1 month unlimited access.\nClick here: {payment_link['short_url']}\n\nPayment ke baad owner ko screenshot bhejein activation ke liye."
        )
    except Exception as e:
        await message.reply_text(f"Payment error: {e}")

async def main():
    await app.start()
    asyncio.create_task(keep_workers_alive())
    asyncio.create_task(process_queue()) 
    print("Master Bot (Advanced + Crash-Proof) start ho gaya hai...")
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
