import os
import re
import io
import json
import time
import sqlite3
import tempfile
import threading
import pandas as pd
import urllib.request
import urllib.error

try:
    import requests as _requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from fastapi import FastAPI, File, UploadFile, HTTPException, Request, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

# ─────────────────────────────
# CONFIG
# ─────────────────────────────

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")   # needed to notify Telegram

_db_store     = {}
_schema_store = {}

app = FastAPI(title="QueryMind AI Analyst", version="7.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────
# REQUEST MODEL
# ─────────────────────────────

class QueryRequest(BaseModel):
    session_id: str
    question:   str


# ─────────────────────────────
# TELEGRAM NOTIFY (fire-and-forget)
# Called after CSV upload if chat_id is provided
# ─────────────────────────────

def _notify_telegram(chat_id: str, text: str):
    if not BOT_TOKEN or not chat_id:
        return
    try:
        url     = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id,
            "text":    text
        }).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)
        print(f"✅ Telegram notified: chat_id={chat_id}")
    except Exception as e:
        print(f"⚠️ Telegram notify failed: {e}")


# ─────────────────────────────
# GEMINI CALL
# retries=1 → fail fast
# ─────────────────────────────

def _call_gemini(question: str, schema: str, columns: list, table: str,
                 retries: int = 1):

    if not GEMINI_API_KEY:
        return ""

    prompt = f"""
You are an expert SQLite data analyst.
Return ONLY a valid SQL query. No explanation, no markdown, no text before or after.
STRICT RULES:
- Use only the table "{table}"
- Never use DROP or DELETE
- For GROUP BY queries, always include the grouped column in SELECT
- For filtering, use the exact case as in the data
- Return only one SQL statement ending without semicolon
COLUMNS AVAILABLE:
{", ".join(columns)}
SCHEMA:
{schema}
QUESTION:
{question}
"""

    url = (
        "https://generativelanguage.googleapis.com"
        "/v1beta/models/gemini-2.5-flash"
        f":generateContent?key={GEMINI_API_KEY}"
    )

    payload = json.dumps({
        "contents": [{"role": "user", "parts": [{"text": prompt}]}]
    }).encode("utf-8")

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())

            try:
                sql = data["candidates"][0]["content"]["parts"][0]["text"]
            except Exception:
                return ""

            if not sql:
                return ""

            sql = sql.replace("```sql", "").replace("```", "").strip()
            sql = sql.split(";")[0].strip()

            if "drop" in sql.lower() or "delete" in sql.lower():
                return f'SELECT * FROM "{table}" LIMIT 10'

            return sql

        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"⚠️ Gemini 429 — rate limited (attempt {attempt}/{retries})")
                return ""
            else:
                print(f"❌ GEMINI HTTP ERROR {e.code}: {e}")
                return ""

        except Exception as e:
            print(f"❌ GEMINI ERROR: {e}")
            return ""

    return ""


# ─────────────────────────────
# EXECUTE SQL
# ─────────────────────────────

def execute_sql(sql: str, db_bytes: bytes):
    conn      = sqlite3.connect(":memory:")
    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(db_bytes)
            temp_path = f.name

        disk = sqlite3.connect(temp_path)
        disk.backup(conn)
        disk.close()

        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql)

        return [dict(r) for r in cur.fetchall()]

    except Exception as e:
        return [{"error": str(e)}]

    finally:
        conn.close()
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


# ─────────────────────────────
# UPLOAD CSV
# Accepts optional chat_id query param to notify Telegram
# Frontend appends ?chat_id=XXXXX from the upload link
# ─────────────────────────────

@app.post("/upload")
async def upload_csv(
    file:    UploadFile = File(...),
    chat_id: Optional[str] = None   # from query param or form
):
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content)).dropna(how="all")

        session_id = os.urandom(8).hex()

        table_name = re.sub(r"[^a-zA-Z0-9_]", "_", file.filename)
        if table_name and table_name[0].isdigit():
            table_name = "t_" + table_name
        table_name = table_name[:40]

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            conn = sqlite3.connect(tf.name)
            df.to_sql(table_name, conn, index=False, if_exists="replace")

            schema = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]

            conn.close()
            db_bytes = open(tf.name, "rb").read()

        os.remove(tf.name)

        _db_store[session_id] = {
            "bytes": db_bytes,
            "table": table_name,
            "cols":  list(df.columns)
        }
        _schema_store[session_id] = schema

        # ── Notify Telegram if chat_id was provided ──────
        if chat_id:
            col_preview = ", ".join(list(df.columns)[:8])
            more_cols   = f" and {len(df.columns)-8} more" if len(df.columns) > 8 else ""
            threading.Thread(
                target=_notify_telegram,
                args=(
                    chat_id,
                    f"✅ CSV Uploaded Successfully!\n\n"
                    f"📄 File: {file.filename}\n"
                    f"📊 Rows: {len(df)}\n"
                    f"📋 Columns: {col_preview}{more_cols}\n\n"
                    f"You can now ask questions about your data here."
                ),
                daemon=True
            ).start()

        return {
            "session_id": session_id,
            "row_count":  len(df),
            "columns":    list(df.columns),
            "table_name": table_name
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────
# QUERY ENGINE
# ─────────────────────────────

@app.post("/query")
async def query(req: QueryRequest):

    data = _db_store.get(req.session_id)
    if not data and _db_store:
        data = list(_db_store.values())[-1]
    if not data:
        raise HTTPException(status_code=404, detail="No dataset loaded")

    table  = data["table"]
    schema = (
        _schema_store.get(req.session_id)
        or list(_schema_store.values())[-1]
    )

    sql = _call_gemini(req.question, schema, data["cols"], table)

    if not sql:
        return {
            "sql":     "",
            "results": [],
            "error":   "⚠️ AI busy (rate limit). Wait 60s and retry."
        }

    results = execute_sql(sql, data["bytes"])

    return {"sql": sql, "results": results}


# ─────────────────────────────
# HEALTH
# ─────────────────────────────

@app.get("/health")
def health():
    return {
        "status":  "ok",
        "model":   "gemini-2.5-flash",
        "service": "AI Data Analyst"
    }


# ─────────────────────────────
# TELEGRAM WEBHOOK — DISABLED
# Telegram handled by Render bot (polling)
# Do not re-enable — conflicts with Render polling
# ─────────────────────────────

# @app.on_event("startup") — DISABLED
# @app.post("/webhook/{token}") — DISABLED
# @app.get("/set-webhook") — DISABLED


# ─────────────────────────────
# FRONTEND
# ─────────────────────────────

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    return FileResponse("static/webapp.html")
