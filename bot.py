import os
import asyncio
import aiohttp
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup

# 1749.jpg se liye gaye MTProto credentials
API_ID = 36681596
API_HASH = "bece5a5cb8d1abc08b644410b6e85d5e"
BOT_TOKEN = "APNA_BOT_TOKEN_YAHA_DALO"

app = Client("TranslatorBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Yahan apne sabhi Render/Vercel apps ke link daal do
WORKERS = [
    "https://worker1.onrender.com",
    "https://worker2.onrender.com",
    "https://worker3.vercel.app"
]

# Default Hindi, baki famous Indian languages
LANGUAGES = {
    "hi": "Hindi",
    "bn": "Bengali",
    "ta": "Tamil",
    "te": "Telugu",
    "mr": "Marathi",
    "gu": "Gujarati",
    "ur": "Urdu"
}

user_settings = {}

# Render apps ko sone se rokne ke liye ping function
async def keep_workers_alive():
    while True:
        async with aiohttp.ClientSession() as session:
            for worker in WORKERS:
                try:
                    async with session.get(f"{worker}/") as response:
                        print(f"Pinged {worker}: {response.status}")
                except Exception as e:
                    print(f"Ping failed for {worker}: {e}")
        # Har 10 minute (600 seconds) mein ek baar ping
        await asyncio.sleep(600)

@app.on_message(filters.command("start"))
async def start(client, message):
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
        "Namaste! Mujhe koi bhi EPUB file bhejiye, main use translate kar dunga.\nNeeche apni pasandida bhasha chunein (Default: Hindi):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@app.on_callback_query(filters.regex(r"^lang_"))
async def set_language(client, callback_query):
    lang_code = callback_query.data.split("_")[1]
    user_settings[callback_query.from_user.id] = lang_code
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

    target_lang = user_settings.get(message.from_user.id, "hi")
    status_msg = await message.reply_text("File download ho rahi hai... Kripya pratiksha karein.")

    file_path = await message.download()
    await status_msg.edit_text(f"Download complete! {LANGUAGES[target_lang]} mein translation shuru ho raha hai. Isme samay lag sakta hai...")

    # EPUB parsing aur translation logic
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
                    # Paragraphs, headings div chunks nikalna
                    paragraphs = soup.find_all(['p', 'h1', 'h2', 'h3', 'div', 'span'])

                    for p in paragraphs:
                        if p.text.strip():
                            # Round Robin se workers par load distribute karna
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
    # Har 10 minute mein Render ko ping karne wala task shuru karo
    asyncio.create_task(keep_workers_alive())
    print("Master Bot start ho gaya hai...")
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
