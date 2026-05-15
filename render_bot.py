import os
import json
import time
import threading
import requests
import telebot
import html
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
        Catches automated upload triggers from Hugging Face app.py
        to bypass HF's outbound SSL handshake restrictions.
        """
        if self.path == "/notify-upload":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            try:
                payload = json.loads(post_data.decode('utf-8'))
                chat_id = payload.get("chat_id")
                text = payload.get("text")
                
                if chat_id and text:
                    # Forward message from HF to Telegram
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
    # Error handling
    if "error" in data and not data.get("sql") and not data.get("results"):
        err_msg = html.escape(data['error'])
        return (
            f"❌ <b>Error:</b> {err_msg}\n\n"
            f"Possible reasons:\n"
            f"• No CSV uploaded yet\n"
            f"• AI rate limit hit\n"
            f"• HF Space waking up"
        )

    sql     = data.get("sql", "")
    results = data.get("results", [])

    # HTML Escape the SQL for safe display
    safe_sql = html.escape(sql)
    sql_block = f"🔍 <b>SQL:</b>\n<code>{safe_sql}</code>"

    # SQL Error handling
    if isinstance(results, list) and results and "error" in results[0]:
        err_sql = html.escape(results[0]['error'])
        return f"❌ <b>SQL Error:</b>\n<code>{err_sql}</code>\n\n{sql_block}"

    if not results:
        return f"{sql_block}\n\n📭 No records found."

    # ── Table Construction ──────────────────
    cols   = list(results[0].keys())
    rows_to_show = results[:10]

    # Calculate widths
    widths = {c: max(len(str(c)), max([len(str(r.get(c, ""))) for r in rows_to_show] + [0])) for c in cols}

    # Header and Divider
    header  = " | ".join(html.escape(str(c)).upper().ljust(widths[c]) for c in cols)
    divider = "-+-".join("-" * widths[c] for c in cols)
    
    # Body
    body_lines = []
    for row in rows_to_show:
        line = " | ".join(html.escape(str(row.get(c, ""))).ljust(widths[c]) for c in cols)
        body_lines.append(line)
    body = "\n".join(body_lines)

    footer = f"\n\n<i>Showing {len(rows_to_show)} of {len(results)} rows.</i>" if len(results) > 10 else ""

    # Return structured message
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
        "⚡ <b>Welcome to QueryMind</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📂 <b>STEP 1</b> — Upload CSV:\n{upload_link}\n\n"
        "💬 <b>STEP 2</b> — Ask questions in English.",
        parse_mode="HTML"
    )


@bot.message_handler(commands=["upload"])
def send_upload_link(message):
    chat_id      = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(chat_id, f"📂 <b>Upload CSV:</b>\n{upload_link}", parse_mode="HTML")


@bot.message_handler(commands=["status"])
def status(message):
    bot.send_message(message.chat.id, "🔍 Checking status...")
    data = call_hf_health()
    if "error" in data:
        bot.send_message(message.chat.id, "❌ HF API Offline")
    else:
        bot.send_message(message.chat.id, "✅ HF API Online", parse_mode="HTML")


@bot.message_handler(commands=["help"])
def help_cmd(message):
    bot.send_message(message.chat.id, "/start, /upload, /status, /help")


@bot.message_handler(func=lambda m: True)
def handle_query(message):
    chat_id  = message.chat.id
    question = message.text.strip()

    if is_casual(question):
        bot.send_message(chat_id, "👋 Hello! Please upload a CSV to begin.")
        return

    thinking_msg = bot.send_message(chat_id, "⏳ <i>Thinking...</i>", parse_mode="HTML")

    data   = call_hf_query(question)
    result = format_result(data)

    try:
        bot.delete_message(chat_id, thinking_msg.message_id)
    except:
        pass

    bot.send_message(chat_id, result, parse_mode="HTML")


# ─────────────────────────────
# ENTRY POINT
# ─────────────────────────────

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()

    print("✅ render_bot.py started...")
    bot.infinity_polling(timeout=60, allowed_updates=["message"])
