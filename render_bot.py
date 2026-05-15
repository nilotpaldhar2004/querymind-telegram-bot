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
# HEALTH SERVER (for UptimeRobot)
# ─────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


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
# Copy-paste friendly plain text table
# ─────────────────────────────

def format_result(data: dict) -> str:

    # Top-level error (Gemini 429, HF down, no CSV, etc.)
    if "error" in data and not data.get("sql") and not data.get("results"):
        return (
            f"❌ {data['error']}\n\n"
            f"Possible reasons:\n"
            f"• No CSV uploaded yet — send /upload for the link\n"
            f"• AI rate limit hit — wait 60s and retry\n"
            f"• HF Space is waking up — retry in 30s"
        )

    sql     = data.get("sql", "")
    results = data.get("results", [])

    # SQL execution error
    if isinstance(results, list) and results and "error" in results[0]:
        return (
            f"❌ SQL Error:\n{results[0]['error']}\n\n"
            f"SQL attempted:\n{sql}"
        )

    # No rows returned
    if not results:
        return f"SQL:\n{sql}\n\nNo records found."

    # ── Copy-paste friendly plain table ──────────────────
    cols   = list(results[0].keys())
    rows   = results[:15]

    widths = {
        c: max(len(str(c)), max(len(str(r.get(c, ""))) for r in rows))
        for c in cols
    }

    header  = " | ".join(str(c).ljust(widths[c]) for c in cols)
    divider = "-+-".join("-" * widths[c] for c in cols)
    body    = "\n".join(
        " | ".join(str(row.get(c, "")).ljust(widths[c]) for c in cols)
        for row in rows
    )

    extra = (
        f"\n... and {len(results) - 15} more rows."
        if len(results) > 15 else ""
    )

    return (
        f"SQL:\n{sql}\n\n"
        f"Results ({len(results)} row{'s' if len(results) != 1 else ''}):\n\n"
        f"{header}\n{divider}\n{body}{extra}"
    )


# ─────────────────────────────
# TELEGRAM HANDLERS
# ─────────────────────────────

@bot.message_handler(commands=["start"])
def welcome(message):
    chat_id     = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        "⚡ Welcome to QueryMind — AI CSV Analyst\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📂 STEP 1 — Upload your CSV here:\n{upload_link}\n\n"
        "After upload you will get a confirmation here automatically.\n\n"
        "💬 STEP 2 — Ask questions in plain English.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📋 Example questions:\n\n"
        "• Show first 5 rows\n"
        "• Count total records\n"
        "• Show unique values in column_name\n"
        "• Group by column_name and count\n"
        "• Average of column_name\n"
        "• Sort by column_name descending limit 10\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Commands:\n"
        "/upload — get upload link\n"
        "/status — check API health\n"
        "/help   — all commands"
    )


@bot.message_handler(commands=["upload"])
def send_upload_link(message):
    chat_id     = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        f"📂 Upload your CSV here:\n{upload_link}\n\n"
        f"After upload, you will receive a confirmation here automatically.\n"
        f"Then ask your questions in plain English."
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
    chat_id     = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        "🆘 Help — QueryMind Bot\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "/start  — Welcome message\n"
        "/upload — Get your CSV upload link\n"
        "/status — Check if HF API is online\n"
        "/help   — This message\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "How to use:\n"
        f"1. Upload CSV: {upload_link}\n"
        "2. Ask any question in plain English\n"
        "3. Bot returns SQL + copy-paste table\n\n"
        "If bot is slow — HF Space may be waking up.\n"
        "Wait 30s and retry."
    )


@bot.message_handler(func=lambda m: True)
def handle_query(message):
    chat_id  = message.chat.id
    question = message.text.strip()

    # ── Casual message handler ────────────────────────────
    if is_casual(question):
        upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
        bot.send_message(
            chat_id,
            f"👋 Hello! I am QueryMind — your AI CSV analyst.\n\n"
            f"I answer questions about your uploaded data.\n\n"
            f"📂 Upload your CSV:\n{upload_link}\n\n"
            f"Then ask me anything like:\n"
            f"• Show first 5 rows\n"
            f"• Count total records\n"
            f"• Average of column_name\n\n"
            f"Type /help to see all commands."
        )
        return

    # ── Data query handler ────────────────────────────────
    thinking_msg = bot.send_message(chat_id, "⏳ Thinking...")

    data   = call_hf_query(question)
    result = format_result(data)

    try:
        bot.delete_message(chat_id, thinking_msg.message_id)
    except Exception:
        pass

    bot.send_message(chat_id, result)


# ─────────────────────────────
# ENTRY POINT
# ─────────────────────────────

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()

    print("✅ render_bot.py started — polling Telegram...")
    print(f"🌐 HF Space: {HF_SPACE_URL}")

    bot.infinity_polling(
        timeout=60,
        long_polling_timeout=30,
        allowed_updates=["message"]
    )
