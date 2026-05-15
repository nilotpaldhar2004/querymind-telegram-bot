import os
import json
import time
import threading
import requests
import html
from http.server import HTTPServer, BaseHTTPRequestHandler

# ═══════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════

BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
HF_SPACE_URL = os.getenv("HF_SPACE_URL", "").rstrip("/")
RENDER_URL   = os.getenv("RENDER_URL", "").rstrip("/")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable not set")
if not HF_SPACE_URL:
    raise ValueError("❌ HF_SPACE_URL environment variable not set")

import telebot
bot = telebot.TeleBot(BOT_TOKEN, threaded=True)


# ═══════════════════════════════════════════════════════
#  CASUAL MESSAGE DETECTION
# ═══════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════
#  HEALTH & NOTIFICATION SERVER
# ═══════════════════════════════════════════════════════

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        """
        Catches upload triggers from HF app.py to bypass
        HF's outbound SSL handshake restrictions.
        """
        if self.path == "/notify-upload":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            try:
                payload = json.loads(post_data.decode("utf-8"))
                chat_id = payload.get("chat_id")
                text    = payload.get("text")
                if chat_id and text:
                    bot.send_message(chat_id, text, parse_mode="HTML")
                    print(f"🔔 Notification forwarded to {chat_id}")
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"status": "delivered"}')
                return
            except Exception as e:
                print(f"❌ Proxy Notification Error: {e}")
        self.send_response(400)
        self.end_headers()

    def log_message(self, *args):
        pass  # suppress noisy HTTP logs


def run_health_server():
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"✅ Health & Notification server running on port {port}")
    server.serve_forever()


# ═══════════════════════════════════════════════════════
#  SELF-PING  (keep-alive)
# ═══════════════════════════════════════════════════════

def keep_alive():
    if not RENDER_URL:
        print("⚠️  RENDER_URL not set — self-ping disabled")
        return
    while True:
        time.sleep(600)
        try:
            requests.get(f"{RENDER_URL}/", timeout=10)
            print("✅ Self-ping sent")
        except Exception as e:
            print(f"⚠️  Self-ping failed: {e}")


# ═══════════════════════════════════════════════════════
#  HF API CALLS
# ═══════════════════════════════════════════════════════

def call_hf_query(question: str) -> dict:
    try:
        resp = requests.post(
            f"{HF_SPACE_URL}/query",
            json={"session_id": "latest", "question": question},
            timeout=(10, 60),
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


# ═══════════════════════════════════════════════════════
#  RESULT FORMATTER  — Professional & Beautiful
# ═══════════════════════════════════════════════════════

def _divider(char: str = "─", length: int = 32) -> str:
    return char * length


def _infer_value_type(value: str) -> str:
    """Return an emoji hint for the cell value type."""
    v = value.strip()
    if v == "" or v.lower() in ("none", "null", "nan"):
        return "∅"
    try:
        float(v.replace(",", ""))
        return "🔢"
    except ValueError:
        pass
    if len(v) == 10 and v[4] == "-" and v[7] == "-":
        return "📅"
    return "🔤"


def format_result(data: dict) -> str:
    """
    Renders a clean, professional, emoji-rich Telegram HTML message.

    Layout:
        ┌─ SQL QUERY block  (monospace, copyable)
        ├─ RESULTS header   (row count badge)
        └─ DATA TABLE       (structured, readable rows — not a raw grid)
    """

    # ── Error: nothing useful in the payload ──────────────────────────────
    if "error" in data and not data.get("sql") and not data.get("results"):
        err = html.escape(str(data["error"]))
        return (
            f"❌  <b>Something went wrong</b>\n"
            f"{_divider()}\n"
            f"<i>{err}</i>\n\n"
            f"<b>Possible causes:</b>\n"
            f"  • No CSV uploaded yet — send /upload\n"
            f"  • AI rate limit hit — wait 60 s and retry\n"
            f"  • HF Space is waking up — retry in 30 s"
        )

    sql     = data.get("sql", "").strip()
    results = data.get("results", [])

    # ── SQL block ─────────────────────────────────────────────────────────
    sql_section = (
        f"🗄  <b>Generated SQL</b>\n"
        f"{_divider()}\n"
        f"<code>{html.escape(sql)}</code>"
    )

    # ── SQL execution error ───────────────────────────────────────────────
    if isinstance(results, list) and results and "error" in results[0]:
        err = html.escape(str(results[0]["error"]))
        return (
            f"{sql_section}\n\n"
            f"❌  <b>SQL Execution Error</b>\n"
            f"{_divider()}\n"
            f"<code>{err}</code>"
        )

    # ── Empty result ──────────────────────────────────────────────────────
    if not results:
        return (
            f"{sql_section}\n\n"
            f"📭  <b>No records found</b>\n"
            f"<i>The query ran successfully but returned 0 rows.</i>"
        )

    # ── Build structured result table ─────────────────────────────────────
    cols        = list(results[0].keys())
    display_rows = results[:20]          # cap at 20 rows for readability
    total_rows  = len(results)
    hidden_rows = total_rows - len(display_rows)

    # Row count badge
    row_badge = (
        f"📊  <b>Results</b>  ·  "
        f"<b>{total_rows}</b> row{'s' if total_rows != 1 else ''}"
    )
    if hidden_rows > 0:
        row_badge += f"  <i>(showing first {len(display_rows)})</i>"

    # ── Format each row as a mini card ────────────────────────────────────
    # Instead of a raw ASCII grid (which breaks on mobile & wraps badly),
    # we render each row as a named-field block, separated by a thin line.
    # This is the standard approach used by professional bots (e.g. Notion,
    # Linear, GitHub bots).

    row_blocks = []
    for idx, row in enumerate(display_rows, start=1):
        lines = [f"<b>#{idx}</b>"]
        for col in cols:
            raw_val  = str(row.get(col, ""))
            safe_col = html.escape(col)
            safe_val = html.escape(raw_val) if raw_val not in ("", "None", "nan") else "<i>—</i>"
            lines.append(f"  <b>{safe_col}:</b>  {safe_val}")
        row_blocks.append("\n".join(lines))

    rows_section = f"\n{_divider('┄')}\n".join(row_blocks)

    # ── Trailing note for hidden rows ─────────────────────────────────────
    hidden_note = ""
    if hidden_rows > 0:
        hidden_note = (
            f"\n{_divider()}\n"
            f"⚠️  <i>+{hidden_rows} more row{'s' if hidden_rows != 1 else ''} not shown.</i>\n"
            f"<i>Refine your query to narrow the results.</i>"
        )

    # ── Column summary line ───────────────────────────────────────────────
    col_summary = (
        f"🗂  <b>Columns ({len(cols)}):</b>  "
        + "  ·  ".join(html.escape(c) for c in cols)
    )

    # ── Assemble final message ────────────────────────────────────────────
    return (
        f"{sql_section}\n\n"
        f"{_divider('═')}\n"
        f"{row_badge}\n"
        f"{col_summary}\n"
        f"{_divider('═')}\n\n"
        f"{rows_section}"
        f"{hidden_note}"
    )


# ═══════════════════════════════════════════════════════
#  TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════════

@bot.message_handler(commands=["start"])
def welcome(message):
    chat_id     = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        f"⚡  <b>QueryMind — AI CSV Analyst</b>\n"
        f"{_divider('═')}\n\n"
        f"Turn plain English into SQL insights from any CSV file.\n\n"
        f"<b>Step 1 — Upload your CSV</b>\n"
        f"📂  <a href=\"{upload_link}\">Open upload page</a>\n"
        f"<i>You'll get a confirmation here once uploaded.</i>\n\n"
        f"<b>Step 2 — Ask anything</b>\n"
        f"Just type your question in plain English.\n\n"
        f"{_divider()}\n"
        f"<b>Example questions:</b>\n"
        f"  • Show first 5 rows\n"
        f"  • Count total records\n"
        f"  • Group by category and count\n"
        f"  • Average of salary column\n"
        f"  • Top 10 by revenue descending\n\n"
        f"{_divider()}\n"
        f"<b>Commands:</b>\n"
        f"  /upload — Get upload link\n"
        f"  /status — Check API health\n"
        f"  /help   — All commands",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@bot.message_handler(commands=["upload"])
def send_upload_link(message):
    chat_id     = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        f"📂  <b>Upload Your CSV</b>\n"
        f"{_divider()}\n\n"
        f"<a href=\"{upload_link}\">Open upload page →</a>\n\n"
        f"<i>After upload, you'll receive a confirmation here.\n"
        f"Then start asking questions in plain English.</i>",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@bot.message_handler(commands=["status"])
def status(message):
    checking_msg = bot.send_message(
        message.chat.id,
        "🔍  Checking API status...",
    )
    data = call_hf_health()
    try:
        bot.delete_message(message.chat.id, checking_msg.message_id)
    except Exception:
        pass

    if "error" in data:
        bot.send_message(
            message.chat.id,
            f"❌  <b>HF API Unreachable</b>\n"
            f"{_divider()}\n"
            f"<code>{html.escape(str(data['error']))}</code>\n\n"
            f"<i>HF Space may be waking up. Retry in 30 s.</i>",
            parse_mode="HTML",
        )
    else:
        bot.send_message(
            message.chat.id,
            f"✅  <b>HF API Online</b>\n"
            f"{_divider()}\n"
            f"  🤖  <b>Model:</b>   {html.escape(str(data.get('model', 'unknown')))}\n"
            f"  🔧  <b>Service:</b>  {html.escape(str(data.get('service', 'unknown')))}",
            parse_mode="HTML",
        )


@bot.message_handler(commands=["help"])
def help_cmd(message):
    chat_id     = message.chat.id
    upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
    bot.send_message(
        chat_id,
        f"🆘  <b>QueryMind — Help</b>\n"
        f"{_divider('═')}\n\n"
        f"<b>Commands</b>\n"
        f"  /start  — Welcome & quick-start guide\n"
        f"  /upload — Get your personal CSV upload link\n"
        f"  /status — Check if the AI backend is online\n"
        f"  /help   — This help message\n\n"
        f"{_divider()}\n"
        f"<b>How to use</b>\n"
        f"  1️⃣  Upload CSV:  <a href=\"{upload_link}\">upload page</a>\n"
        f"  2️⃣  Ask a question in plain English\n"
        f"  3️⃣  Get SQL + structured results instantly\n\n"
        f"{_divider()}\n"
        f"<b>Tips</b>\n"
        f"  • Be specific: <i>\"Top 5 products by sales\"</i>\n"
        f"  • Name columns exactly as in your CSV\n"
        f"  • If slow: HF Space may be waking up — retry in 30 s",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@bot.message_handler(func=lambda m: True)
def handle_query(message):
    chat_id  = message.chat.id
    question = message.text.strip()

    # ── Casual greeting ───────────────────────────────────────────────────
    if is_casual(question):
        upload_link = f"{HF_SPACE_URL}?chat_id={chat_id}"
        bot.send_message(
            chat_id,
            f"👋  <b>Hey there!</b>  I'm QueryMind — your AI CSV analyst.\n\n"
            f"📂  <a href=\"{upload_link}\">Upload a CSV file</a> to get started.\n\n"
            f"Then ask me anything, for example:\n"
            f"  • <i>Show first 5 rows</i>\n"
            f"  • <i>Count total records</i>\n"
            f"  • <i>Top 10 by revenue</i>",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    # ── Real query ────────────────────────────────────────────────────────
    thinking_msg = bot.send_message(
        chat_id,
        "⏳  <i>Analyzing your question…</i>",
        parse_mode="HTML",
    )

    data   = call_hf_query(question)
    result = format_result(data)

    try:
        bot.delete_message(chat_id, thinking_msg.message_id)
    except Exception:
        pass

    bot.send_message(chat_id, result, parse_mode="HTML")


# ═══════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=keep_alive,        daemon=True).start()

    print("✅ render_bot.py started — polling Telegram...")
    print(f"🌐 HF Space: {HF_SPACE_URL}")

    bot.infinity_polling(
        timeout=60,
        long_polling_timeout=30,
        allowed_updates=["message"],
    )
