from fastapi import FastAPI, Request
import uvicorn
import aiohttp
import os

app = FastAPI()

@app.get("/")
def keep_alive():
    return {"status": "Main zinda hu, aur super fast hu!"}

@app.post("/translate")
async def translate_batch(request: Request):
    data = await request.json()
    
    # Master server se aayi text ki list
    text_list = data.get("text_list", [])
    target_lang = data.get("lang", "hi")
    
    if not text_list:
        return {"success": True, "translated": []}

    # Direct Google Translate API logic (Batching)
    url = "https://translate.googleapis.com/translate_a/t"
    params = {"client": "gtx", "sl": "auto", "tl": target_lang}
    payload = [("q", text) for text in text_list]
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params, data=payload) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    # Aayi hui JSON list ko wapas text array mein convert karna
                    translated_texts = [item[0] if isinstance(item, list) else item for item in result]
                    return {"success": True, "translated": translated_texts}
                else:
                    return {"success": False, "error": f"Google API Error: HTTP {resp.status}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
