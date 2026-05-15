import os
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
RENDER_URL   = os.getenv("RENDER_URL", "").rstrip("/")  # optional self-ping

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable not set")
if not HF_SPACE_URL:
    raise ValueError("❌ HF_SPACE_URL environment variable not set")

bot = telebot.TeleBot(BOT_TOKEN, threaded=True)

# ─────────────────────────────
# HEALTH SERVER (for UptimeRobot)
# ─────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass  # suppress noisy logs


def run_health_server():
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"✅ Health server running on port {port}")
    server.serve_forever()


# ─────────────────────────────
# SELF-PING (backup keep-alive)
# ─────────────────────────────

def keep_alive():
    if not RENDER_URL:
        print("⚠️ RENDER_URL not set — self-ping disabled")
        return
    while True:
        time.sleep(600)  # every 10 minutes
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
            timeout=(10, 60)   # connect=10s, read=60s
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
    if "error" in data:
        return (
            f"❌ Error: {data['error']}\n\n"
            f"⚠️ Possible reasons:\n"
            f"• No CSV uploaded yet — visit the web app first\n"
            f"• AI rate limit hit — wait 60s and retry\n"
            f"• HF Space is waking up — retry in 30s"
        )

    sql     = data.get("sql", "")
    results = data.get("results", [])

    if not results:
        return f"🔍 SQL:\n{sql}\n\n📭 No records found."

    if isinstance(results, list) and results and "error" in results[0]:
        return f"❌ SQL Error:\n{results[0]['error']}\n\n🔍 SQL tried:\n{sql}"

    text = f"🔍 SQL:\n{sql}\n\n📊 Results:\n\n"
    for i, row in enumerate(results[:10], 1):
        text += f"{i}. {', '.join(f'{k}: {v}' for k, v in row.items())}\n"

    if len(results) > 10:
        text += f"\n... and {len(results) - 10} more rows."

    return text


# ─────────────────────────────
# TELEGRAM HANDLERS
# ─────────────────────────────

@bot.message_handler(commands=["start"])
def welcome(message):
    bot.send_message(
        message.chat.id,
        "⚡ Welcome to QueryMind — AI CSV Analyst\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📂 STEP 1 — Upload your CSV here:\n{HF_SPACE_URL}\n\n"
        "💬 STEP 2 — Ask questions below in plain English.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📋 Example questions:\n\n"
        "• Show first 5 rows\n"
        "• Count total records\n"
        "• Show unique values in column_name\n"
        "• Group by column_name and count\n"
        "• Average of column_name\n"
        "• Sort by column_name descending limit 10\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ Upload CSV on the website BEFORE asking questions here."
    )


@bot.message_handler(commands=["status"])
def status(message):
    bot.send_message(message.chat.id, "🔍 Checking HF API status...")
    data = call_hf_health()

    if "error" in data:
        bot.send_message(
            message.chat.id,
            f"❌ HF API unreachable\n\nError: {data['error']}"
        )
    else:
        bot.send_message(
            message.chat.id,
            f"✅ HF API: online\n"
            f"🤖 Model: {data.get('model', 'unknown')}\n"
            f"🔧 Service: {data.get('service', 'unknown')}"
        )


@bot.message_handler(commands=["help"])
def help_cmd(message):
    bot.send_message(
        message.chat.id,
        "🆘 Help — QueryMind Bot\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "/start  — Welcome message + upload link\n"
        "/status — Check if HF API is online\n"
        "/help   — This message\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "How to use:\n"
        "1. Upload CSV at the web app\n"
        "2. Ask any question in plain English\n"
        "3. Bot returns SQL + results\n\n"
        "If bot is slow: HF Space may be waking up.\n"
        "Wait 30s and retry."
    )


@bot.message_handler(func=lambda m: True)
def handle_query(message):
    chat_id  = message.chat.id
    question = message.text.strip()

    # Acknowledge immediately so user knows bot is alive
    thinking_msg = bot.send_message(chat_id, "⏳ Thinking...")

    # Call HF API
    data   = call_hf_query(question)
    result = format_result(data)

    # Delete "Thinking..." and send real result
    try:
        bot.delete_message(chat_id, thinking_msg.message_id)
    except Exception:
        pass  # if delete fails, just send result anyway

    bot.send_message(chat_id, result)


# ─────────────────────────────
# ENTRY POINT
# ─────────────────────────────

if __name__ == "__main__":
    # Start health server in background thread
    threading.Thread(target=run_health_server, daemon=True).start()

    # Start self-ping keep-alive (backup for UptimeRobot)
    threading.Thread(target=keep_alive, daemon=True).start()

    print("✅ render_bot.py started — polling Telegram...")
    print(f"🌐 HF Space: {HF_SPACE_URL}")

    bot.infinity_polling(
        timeout=60,
        long_polling_timeout=30,
        allowed_updates=["message"]
    )
