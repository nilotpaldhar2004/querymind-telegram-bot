import os
import json # ✅ Added to handle incoming notification data
import time
import threading
import requests
import telebot
import html # ✅ Added for safe HTML formatting
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
# HEALTH & NOTIFICATION SERVER
# ─────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
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
        ✅ Added: Catches automated upload triggers from Hugging Face app.py
        Bypasses HF's outbound SSL handshake restrictions.
        """
        if self.path == "/notify-upload":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            try:
                payload = json.loads(post_data.decode('utf-8'))
                chat_id = payload.get("chat_id")
                text = payload.get("text")
                
                if chat_id and text:
                    # Render sends the actual message using its stable connection
                    bot.send_message(chat_id, text, parse_mode="HTML")
                    print(f"🔔 Signal from HF: Notification sent to {chat_id}")
                
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"status": "delivered"}')
                return
            except Exception as e:
                print(f"❌ Proxy Notification Error: {e}")
                
        self.send_response(400)
        self.end_headers()

    def log_message(self, *args):
        pass  # suppress noisy logs


def run_health_server():
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"✅ Health & Notification server running on port {port}")
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

def call_hf_query(chat_id: int, question: str) -> dict:
    try:
        resp = requests.post(
            f"{HF_SPACE_URL}/query",
            json={"session_id": str(chat_id), "question": question}, # ✅ Updated: Pass chat_id for user isolation
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
    if "error" in data and not data.get("sql") and not data.get("results"):
        return (
            f"❌ <b>Error:</b> {html.escape(data['error'])}\n\n"
            f"Possible reasons:\n"
            f"• No CSV uploaded yet — visit web app\n"
            f"• AI rate limit hit — wait 60s\n"
            f"• HF Space waking up — retry in 30s"
        )

    sql     = data.get("sql", "")
    results = data.get("results", [])

    # HTML Escape the SQL for safe code display
    safe_sql = html.escape(sql)
    sql_block = f"🔍 <b>Generated SQL:</b>\n<code>{safe_sql}</code>"

    if isinstance(results, list) and results and "error" in results[0]:
        err_sql = html.escape(results[0]['error'])
        return (
            f"❌ <b>SQL Execution Error:</b>\n"
            f"<code>{err_sql}</code>\n\n"
            f"{sql_block}"
        )

    if not results:
        return f"{sql_block}\n\n📭 <b>No records found.</b>"

    # ── Table Construction ──────────────────
    cols   = list(results[0].keys())
    rows_to_show = results[:10]

    # Calculate widths for proper alignment
    widths = {c: max(len(str(c)), max([len(str(r.get(c, ""))) for r in rows_to_show] + [0])) for c in cols}

    # Header and Divider (Upper Case for visual structure)
    header  = " | ".join(html.escape(str(c)).upper().ljust(widths[c]) for c in cols)
    divider = "-+-".join("-" * widths[c] for c in cols)
    
    # Body
    body_lines = []
    for row in rows_to_show:
        line = " | ".join(html.escape(str(row.get(c, ""))).ljust(widths[c]) for c in cols)
        body_lines.append(line)
    body = "\n".join(body_lines)

    footer = f"\n\n<i>Showing {len(rows_to_show)} of {len(results)} rows.</i>" if len(results) > 10 else ""

    return (
        f"{sql_block}\n\n"
        f"📊 <b>Results:</b>\n"
        f"<pre>{header}\n{divider}\n{body}</pre>{footer}"
    )


# ─────────────────────────────
# TELEGRAM HANDLERS
# ─────────────────────────────

@bot.message_handler(commands=["start"])
def welcome(message):
    chat_id      = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        "⚡ <b>Welcome to QueryMind — AI CSV Analyst</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📂 <b>STEP 1</b> — Upload your CSV here:\n{upload_link}\n\n"
        "After upload you will get a confirmation here automatically.\n\n"
        "💬 <b>STEP 2</b> — Ask questions in plain English.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>Example questions:</b>\n\n"
        "• Show first 5 rows\n"
        "• Count total records\n"
        "• Show unique values in column_name\n"
        "• Group by column_name and count\n\n"
        "<b>Commands:</b>\n"
        "/upload — get upload link\n"
        "/status — check API health\n"
        "/help   — all commands",
        parse_mode="HTML"
    )


@bot.message_handler(commands=["upload"])
def send_upload_link(message):
    chat_id      = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        f"📂 <b>Upload your CSV here:</b>\n{upload_link}\n\n"
        f"After upload, you will receive a confirmation here automatically.\n"
        f"Then ask your questions in plain English.",
        parse_mode="HTML"
    )


@bot.message_handler(commands=["status"])
def status(message):
    bot.send_message(message.chat.id, "🔍 Checking HF API status...")
    data = call_hf_health()

    if "error" in data:
        bot.send_message(
            message.chat.id,
            f"❌ <b>HF API unreachable</b>\n\nError: <code>{html.escape(data['error'])}</code>",
            parse_mode="HTML"
        )
    else:
        bot.send_message(
            message.chat.id,
            f"✅ <b>HF API: online</b>\n"
            f"🤖 Model: {data.get('model', 'unknown')}\n"
            f"🔧 Service: {data.get('service', 'unknown')}",
            parse_mode="HTML"
        )


@bot.message_handler(commands=["help"])
def help_cmd(message):
    chat_id      = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        "🆘 <b>Help — QueryMind Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "/start  — Welcome message\n"
        "/upload — Get your CSV upload link\n"
        "/status — Check if HF API is online\n"
        "/help   — This message\n\n"
        "<b>How to use:</b>\n"
        f"1. Upload CSV: {upload_link}\n"
        "2. Ask any question in plain English",
        parse_mode="HTML"
    )


@bot.message_handler(func=lambda m: True)
def handle_query(message):
    chat_id  = message.chat.id
    question = message.text.strip()

    if is_casual(question):
        upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
        bot.send_message(
            chat_id,
            f"👋 <b>Hello!</b> I am QueryMind — your AI CSV analyst.\n\n"
            f"📂 Upload your CSV:\n{upload_link}\n\n"
            f"Then ask me anything like:\n"
            f"• Show first 5 rows\n"
            f"• Count total records",
            parse_mode="HTML"
        )
        return

    thinking_msg = bot.send_message(chat_id, "⏳ <i>Thinking...</i>", parse_mode="HTML")

    data   = call_hf_query(chat_id, question) # ✅ Updated: Pass real chat_id for session context
    result = format_result(data)

    try:
        bot.delete_message(chat_id, thinking_msg.message_id)
    except Exception:
        pass

    bot.send_message(chat_id, result, parse_mode="HTML")


# ─────────────────────────────
# ENTRY POINT
# ─────────────────────────────

if __name__ == "__main__":
    # Start the Health + Notification server
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # Start the Self-Ping task
    threading.Thread(target=keep_alive, daemon=True).start()

    print("✅ render_bot.py started — polling Telegram...")
    print(f"🌐 HF Space: {HF_SPACE_URL}")

    bot.infinity_polling(
        timeout=60,
        long_polling_timeout=30,
        allowed_updates=["message"]
    )
