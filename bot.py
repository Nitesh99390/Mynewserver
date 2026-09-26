import os
import asyncio
import aiohttp
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import razorpay

from dotenv import load_dotenv
load_dotenv()

# Environment variables
API_ID = int(os.environ.get("API_ID", "36681596"))
API_HASH = os.environ.get("API_HASH", "bece5a5cb8d1abc08b644410b6e85d5e")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID"))
RAZORPAY_KEY_ID = os.environ.get("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET")

rzp_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
app = Client("TranslatorBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

WORKERS = []
users_db = {}
translation_queue = asyncio.Queue()
active_tasks = {} # Ek user ki ek hi file process hogi

LANGUAGES = {
    "hi": "Hindi", "bn": "Bengali", "ta": "Tamil", "te": "Telugu",
    "mr": "Marathi", "gu": "Gujarati", "ur": "Urdu"
}

# --- Owner Commands ---
@app.on_message(filters.command("addworker") & filters.user(OWNER_ID))
async def add_worker(client, message: Message):
    try:
        url = message.text.split(" ", 1)[1].strip()
        if not url.startswith("http"): url = "https://" + url
        if url.endswith("/"): url = url[:-1]
        
        if url not in WORKERS:
            WORKERS.append(url)
            await message.reply_text(f"✅ Worker add ho gaya: {url}\nTotal workers: {len(WORKERS)}")
        else:
            await message.reply_text("⚠️ Yeh worker pehle se list mein hai.")
    except:
        await message.reply_text("❌ Sahi format: /addworker <worker_url>")

@app.on_message(filters.command("delworker") & filters.user(OWNER_ID))
async def del_worker(client, message: Message):
    try:
        url = message.text.split(" ", 1)[1].strip()
        if url in WORKERS:
            WORKERS.remove(url)
            await message.reply_text(f"✅ Worker remove ho gaya: {url}\nTotal workers: {len(WORKERS)}")
        else:
            await message.reply_text("⚠️ Yeh worker list mein nahi mila.")
    except:
        await message.reply_text("❌ Sahi format: /delworker <worker_url>")

@app.on_message(filters.command("workers") & filters.user(OWNER_ID))
async def list_workers(client, message: Message):
    if not WORKERS:
        return await message.reply_text("Koi worker list mein nahi hai.")
    await message.reply_text("Current Workers:\n" + "\n".join([f"{i+1}. {w}" for i, w in enumerate(WORKERS)]))

# --- Keep Alive pinging function ---
async def keep_workers_alive():
    while True:
        if WORKERS:
            async with aiohttp.ClientSession() as session:
                for worker in WORKERS:
                    try:
                        await session.get(f"{worker}/")
                    except:
                        pass
        await asyncio.sleep(600)

# --- Subscriptions ---
@app.on_message(filters.command("pay"))
async def pay_command(client, message: Message):
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

@app.on_message(filters.command("start"))
async def start(client, message):
    if message.from_user.id not in users_db:
        users_db[message.from_user.id] = {"lang": "hi", "has_subscription": False}
        
    buttons = [InlineKeyboardButton(l, callback_data=f"lang_{c}") for c, l in LANGUAGES.items()]
    keyboard = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    
    await message.reply_text(
        "Namaste! Mujhe koi bhi EPUB file bhejiye, main use translate kar dunga.\n\nUnlimited access ke liye /pay command use karein.\n\nNeeche apni pasandida bhasha chunein (Default: Hindi):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@app.on_callback_query(filters.regex(r"^lang_"))
async def set_language(client, callback_query):
    lang_code = callback_query.data.split("_")[1]
    if callback_query.from_user.id not in users_db:
         users_db[callback_query.from_user.id] = {"lang": lang_code, "has_subscription": False}
    else:
        users_db[callback_query.from_user.id]["lang"] = lang_code
    await callback_query.answer(f"Language {LANGUAGES[lang_code]} set ho gayi hai.", show_alert=True)
    await callback_query.message.edit_text(f"Selected Language: **{LANGUAGES[lang_code]}**\nAb apni EPUB file bhej sakte hain.")

# --- PRODUCTION FEATURE: Queue Worker ---
async def process_queue():
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

# --- SUPER FAST BATCH TRANSLATION LOGIC ---
async def translate_batch_req(session, text_list, target_lang, worker_url, retries=3):
    for attempt in range(retries):
        try:
            # Timeout bada diya hai kyunki 50 lines ek sath jayengi
            async with session.post(f"{worker_url}/translate", json={"text_list": text_list, "lang": target_lang}, timeout=30) as response:
                result = await response.json()
                if result.get("success"): 
                    return result.get("translated")
        except:
            if attempt == retries - 1: return text_list # Failsafe
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
                
                # BATCH PROCESSING: 50 paragraphs in 1 Request
                batch_size = 50 
                tasks = []
                
                for i in range(0, len(texts_to_translate), batch_size):
                    batch = texts_to_translate[i:i+batch_size]
                    worker_url = WORKERS[worker_index % len(WORKERS)]
                    tasks.append(translate_batch_req(session, batch, target_lang, worker_url))
                    worker_index += 1
                
                results = await asyncio.gather(*tasks)
                
                translated_texts = []
                for res in results:
                    translated_texts.extend(res)
                
                # Apply translated text back
                for p, t_text in zip(valid_paragraphs, translated_texts):
                    if t_text:
                        p.string = str(t_text)
                    
                item.content = str(soup).encode('utf-8')
            new_book.add_item(item)
            processed_items += 1
            
            # LIVE PROGRESS BAR
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
    
    # Cleanup
    if os.path.exists(file_path): os.remove(file_path)
    if os.path.exists(output_file): os.remove(output_file)

@app.on_message(filters.document)
async def handle_document(client, message):
    if not message.document.file_name.lower().endswith('.epub'):
        return await message.reply_text("Bhai, filhal main sirf .epub files hi translate karta hu.")

    if not WORKERS:
        return await message.reply_text("⚠️ Koi bhi worker online nahi hai. Owner ko contact karein.")
        
    if message.from_user.id in active_tasks:
         return await message.reply_text("⚠️ Aapki ek file pehle se process ho rahi hai. Kripya wait karein.")

    target_lang = users_db.get(message.from_user.id, {}).get("lang", "hi")
    
    # Queue System
    queue_pos = translation_queue.qsize() + 1
    status_msg = await message.reply_text(f"📥 File received. You are at position {queue_pos} in queue.\nDownloading...")

    file_path = await message.download()
    await status_msg.edit_text(f"✅ Download complete! Waiting in queue (Position: {queue_pos})...")
    
    await translation_queue.put((message.from_user.id, file_path, target_lang, message.document.file_name, status_msg))

async def main():
    await app.start()
    asyncio.create_task(keep_workers_alive())
    asyncio.create_task(process_queue()) 
    print("Master Bot (Super Fast Version) start ho gaya hai...")
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
