import os
import asyncio
import aiohttp
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import razorpay

# Environment variables se configurations uthana
API_ID = int(os.environ.get("API_ID", "36681596"))
API_HASH = os.environ.get("API_HASH", "bece5a5cb8d1abc08b644410b6e85d5e")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID")) # Aapka Telegram ID
RAZORPAY_KEY_ID = os.environ.get("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET")

# Razorpay client setup
rzp_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))

app = Client("TranslatorBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Telegram se update hone wale workers ki list
# Default ek khali list rakhenge, owner command se add/remove karega
WORKERS = []

# Dummy database user subscription aur settings ke liye
# Production mein MySQL/MongoDB ya SQLite use karna behtar hai
users_db = {}
'''
users_db format:
{
    user_id: {
        "lang": "hi",
        "has_subscription": False,
        "subscription_expiry": None # Timestamp aayega
    }
}
'''

LANGUAGES = {
    "hi": "Hindi",
    "bn": "Bengali",
    "ta": "Tamil",
    "te": "Telugu",
    "mr": "Marathi",
    "gu": "Gujarati",
    "ur": "Urdu"
}

# --- Owner Commands for Managing Workers ---

@app.on_message(filters.command("addworker") & filters.user(OWNER_ID))
async def add_worker(client, message: Message):
    try:
        url = message.text.split(" ", 1)[1].strip()
        # Basic URL validation
        if not url.startswith("http"):
            url = "https://" + url

        # Remove trailing slash
        if url.endswith("/"):
            url = url[:-1]

        if url not in WORKERS:
            WORKERS.append(url)
            await message.reply_text(f"✅ Worker add ho gaya: {url}\nTotal workers: {len(WORKERS)}")
        else:
            await message.reply_text("⚠️ Yeh worker pehle se list mein hai.")
    except IndexError:
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
    except IndexError:
        await message.reply_text("❌ Sahi format: /delworker <worker_url>")

@app.on_message(filters.command("workers") & filters.user(OWNER_ID))
async def list_workers(client, message: Message):
    if not WORKERS:
        await message.reply_text("Koi worker list mein nahi hai.")
        return
    text = "Current Workers:\n"
    for i, w in enumerate(WORKERS):
        text += f"{i+1}. {w}\n"
    await message.reply_text(text)

# --- Keep Alive pinging function ---
async def keep_workers_alive():
    while True:
        if WORKERS:
            async with aiohttp.ClientSession() as session:
                for worker in WORKERS:
                    try:
                        async with session.get(f"{worker}/") as response:
                            print(f"Pinged {worker}: {response.status}")
                    except Exception as e:
                        print(f"Ping failed for {worker}: {e}")
        # Har 10 minute (600 seconds) mein ek baar ping
        await asyncio.sleep(600)

# --- Subscriptions ---
@app.on_message(filters.command("pay"))
async def pay_command(client, message: Message):
    # ₹100 per month ka subscription
    amount = 10000 # Paise mein (100 * 100)

    try:
        # Create Razorpay order
        order_data = {
            "amount": amount,
            "currency": "INR",
            "receipt": f"receipt_{message.from_user.id}",
            "notes": {
                "user_id": message.from_user.id
            }
        }
        order = rzp_client.order.create(data=order_data)

        # Payment link banane ke liye payment_link api use kar sakte hain
        payment_link_data = {
            "amount": amount,
            "currency": "INR",
            "accept_partial": False,
            "description": "Unlimited Translation Subscription (1 Month)",
            "customer": {
                "name": message.from_user.first_name,
                "contact": "",
                "email": ""
            },
            "notify": {
                "sms": False,
                "email": False
            },
            "reminder_enable": False,
            "notes": {
                "order_id": order['id'],
                "user_id": message.from_user.id
            }
        }

        payment_link = rzp_client.payment_link.create(data=payment_link_data)
        short_url = payment_link['short_url']

        await message.reply_text(
            f"Please pay ₹100 for 1 month unlimited access.\nClick here: {short_url}\n\nPayment ke baad owner ko screenshot bhejein activation ke liye."
        )
    except Exception as e:
        await message.reply_text(f"Payment link generate karne mein error aayi: {e}")

@app.on_message(filters.command("start"))
async def start(client, message):
    if message.from_user.id not in users_db:
        users_db[message.from_user.id] = {"lang": "hi", "has_subscription": False}

    keyboard = []
    row = []
    for code, lang in LANGUAGES.items():
        row.append(InlineKeyboardButton(lang, callback_data=f"lang_{code}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

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

async def translate_chunk(session, text, target_lang, worker_url):
    try:
        async with session.post(f"{worker_url}/translate", json={"text": text, "lang": target_lang}) as response:
            result = await response.json()
            if result.get("success"):
                return result.get("translated")
            return text
    except:
        return text

@app.on_message(filters.document)
async def handle_document(client, message):
    if not message.document.file_name.lower().endswith('.epub'):
        return await message.reply_text("Bhai, filhal main sirf .epub files hi translate karta hu.")

    # Check for subscription (Optional logic to restrict free users)
    user_data = users_db.get(message.from_user.id, {})
    if not user_data.get("has_subscription"):
        # Yahan aap logic daal sakte ho ki free users sirf choti files hi translate kar payein
        pass

    if not WORKERS:
        return await message.reply_text("⚠️ Koi bhi worker online nahi hai. Owner ko contact karein.")

    target_lang = user_data.get("lang", "hi")
    status_msg = await message.reply_text("File download ho rahi hai... Kripya pratiksha karein.")

    file_path = await message.download()
    await status_msg.edit_text(f"Download complete! {LANGUAGES[target_lang]} mein translation shuru ho raha hai. Isme samay lag sakta hai...")

    try:
        book = epub.read_epub(file_path)
        new_book = epub.EpubBook()
        new_book.metadata = book.metadata
        new_book.spine = book.spine
        new_book.toc = book.toc

        async with aiohttp.ClientSession() as session:
            worker_index = 0
            for item in book.get_items():
                if item.get_type() == ebooklib.ITEM_DOCUMENT:
                    soup = BeautifulSoup(item.get_content(), 'html.parser')
                    paragraphs = soup.find_all(['p', 'h1', 'h2', 'h3', 'div', 'span'])

                    for p in paragraphs:
                        if p.text.strip():
                            worker_url = WORKERS[worker_index % len(WORKERS)]
                            translated_text = await translate_chunk(session, p.text, target_lang, worker_url)
                            p.string = translated_text
                            worker_index += 1

                    item.content = str(soup).encode('utf-8')
                new_book.add_item(item)

        output_file = f"Translated_{message.document.file_name}"
        epub.write_epub(output_file, new_book)

        await status_msg.edit_text("Translation poora ho gaya! File upload kar raha hu...")
        await message.reply_document(document=output_file, caption="Yeh rahi aapki translated book!")

    except Exception as e:
        await status_msg.edit_text(f"File process karne mein error aayi: {e}")

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
        if 'output_file' in locals() and os.path.exists(output_file):
            os.remove(output_file)

async def main():
    await app.start()
    asyncio.create_task(keep_workers_alive())
    print("Master Bot start ho gaya hai...")
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
