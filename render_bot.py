import os
import json
import time
import threading
import requests
import telebot
from http.server import HTTPServer, BaseHTTPRequestHandler

# ─────────────────────────────
# CONFIG
# ─────────────────────────────

BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
HF_SPACE_URL = os.getenv("HF_SPACE_URL", "").rstrip("/")
RENDER_URL   = os.getenv("RENDER_URL", "").rstrip("/")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable not set")
if not HF_SPACE_URL:
    raise ValueError("❌ HF_SPACE_URL environment variable not set")

bot = telebot.TeleBot(BOT_TOKEN, threaded=True)

# ─────────────────────────────
# CASUAL MESSAGE DETECTION
# ─────────────────────────────

CASUAL_TRIGGERS = [
    "hi", "hello", "hey", "hii", "helo", "helllo",
    "thanks", "thank you", "thankyou", "thx", "ty",
    "ok", "okay", "k", "fine", "good", "great",
    "bye", "goodbye", "see you", "cya",
    "how are you", "what's up", "whats up", "sup",
    "who are you", "what are you", "what can you do"
]

def is_casual(text: str) -> bool:
    t = text.lower().strip().rstrip("!?.").strip()
    return t in CASUAL_TRIGGERS or any(t.startswith(kw) for kw in CASUAL_TRIGGERS)


# ─────────────────────────────
# NOTIFICATION & HEALTH SERVER
# ─────────────────────────────

class HealthAndNotificationHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Standard UptimeRobot Health Check"""
        if self.path == "/":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        """
        Catches notification triggers from Hugging Face.
        This bypasses the HF SSL Handshake timeout issue.
        """
        if self.path == "/notify-upload":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            try:
                payload = json.loads(post_data.decode('utf-8'))
                chat_id = payload.get("chat_id")
                text = payload.get("text")
                
                if chat_id and text:
                    # Send the message to Telegram from Render (Stable Connection)
                    bot.send_message(chat_id, text, parse_mode="HTML")
                    print(f"🔔 Notification forwarded to Chat ID: {chat_id}")
                
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"status": "delivered"}')
                return
            except Exception as e:
                print(f"❌ Notification relay failed: {e}")
                
        self.send_response(400)
        self.end_headers()

    def log_message(self, *args):
        pass # Suppress standard GET logs


def run_health_server():
    port = int(os.getenv("PORT", 8080))
    # Use the new Handler that supports POST
    server = HTTPServer(("0.0.0.0", port), HealthAndNotificationHandler)
    print(f"✅ Health & Notification server running on port {port}")
    server.serve_forever()


# ─────────────────────────────
# SELF-PING (Keep-alive)
# ─────────────────────────────

def keep_alive():
    if not RENDER_URL:
        print("⚠️ RENDER_URL not set — self-ping disabled")
        return
    while True:
        time.sleep(600)
        try:
            requests.get(f"{RENDER_URL}/", timeout=10)
            print("✅ Self-ping sent")
        except Exception as e:
            print(f"⚠️ Self-ping failed: {e}")


# ─────────────────────────────
# HF API CALLS
# ─────────────────────────────

def call_hf_query(question: str) -> dict:
    try:
        resp = requests.post(
            f"{HF_SPACE_URL}/query",
            json={"session_id": "latest", "question": question},
            timeout=(10, 60)
        )
        return resp.json()
    except requests.exceptions.Timeout:
        return {"error": "HF API timed out. Please try again."}
    except Exception as e:
        return {"error": str(e)}


def call_hf_health() -> dict:
    try:
        resp = requests.get(f"{HF_SPACE_URL}/health", timeout=10)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────
# RESULT FORMATTER
# ─────────────────────────────

def format_result(data: dict) -> str:
    if "error" in data and not data.get("sql"):
        return (
            f"❌ {data['error']}\n\n"
            f"Possible reasons:\n"
            f"• No CSV uploaded yet\n"
            f"• AI rate limit hit\n"
            f"• HF Space is waking up"
        )

    sql     = data.get("sql", "")
    results = data.get("results", [])

    if isinstance(results, list) and results and "error" in results[0]:
        return f"❌ SQL Error:\n{results[0]['error']}\n\nSQL tried:\n{sql}"

    if not results:
        return f"SQL:\n{sql}\n\nNo records found."

    # Format as a clean text table
    cols   = list(results[0].keys())
    rows   = results[:15]
    widths = {c: max(len(str(c)), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}

    header  = " | ".join(str(c).ljust(widths[c]) for c in cols)
    divider = "-+-".join("-" * widths[c] for c in cols)
    body    = "\n".join(" | ".join(str(row.get(c, "")).ljust(widths[c]) for c in cols) for row in rows)

    extra = f"\n... and {len(results)-15} more rows." if len(results) > 15 else ""

    return f"SQL:\n<code>{sql}</code>\n\nResults ({len(results)} rows):\n<pre>{header}\n{divider}\n{body}{extra}</pre>"


# ─────────────────────────────
# TELEGRAM HANDLERS
# ─────────────────────────────

@bot.message_handler(commands=["start", "upload"])
def welcome(message):
    chat_id     = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        "📂 <b>Upload your CSV here:</b>\n"
        f"{upload_link}\n\n"
        "<i>Confirmation will be sent here automatically after upload.</i>",
        parse_mode="HTML"
    )

@bot.message_handler(commands=["status"])
def status(message):
    data = call_hf_health()
    if "error" in data:
        bot.send_message(message.chat.id, f"❌ HF API Offline: {data['error']}")
    else:
        bot.send_message(message.chat.id, f"✅ HF API Online ({data.get('model')})")

@bot.message_handler(func=lambda m: True)
def handle_query(message):
    chat_id = message.chat.id
    question = message.text.strip()

    if is_casual(question):
        welcome(message)
        return

    thinking = bot.send_message(chat_id, "⏳ Thinking...")
    data = call_hf_query(question)
    result = format_result(data)

    try:
        bot.delete_message(chat_id, thinking.message_id)
    except:
        pass
    
    bot.send_message(chat_id, result, parse_mode="HTML")


if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()

    print("✅ render_bot.py active and listening for notifications...")
    bot.infinity_polling(timeout=60)
