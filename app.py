from fastapi import FastAPI, Request
from deep_translator import GoogleTranslator
import uvicorn

app = FastAPI()

# Har 10 minute mein ping receive karne ke liye route (taki server zinda rahe)
@app.get("/")
def keep_alive():
    return {"status": "Main zinda hu!"}

@app.post("/translate")
async def translate_text(request: Request):
    data = await request.json()
    text = data.get("text", "")
    target_lang = data.get("lang", "hi")

    if not text.strip():
        return {"success": True, "translated": ""}

    try:
        # Google Translate ke through translation
        translated = GoogleTranslator(source='auto', target=target_lang).translate(text)
        return {"success": True, "translated": translated}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
