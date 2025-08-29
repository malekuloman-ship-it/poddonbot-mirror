import asyncio
import os
import json
import random
import logging
import re
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, User,
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, InputMediaPhoto
)
from aiogram.filters import Command
from dotenv import load_dotenv
from PIL import Image, ImageOps
from hashlib import md5

# -------------------- PATHS (нужны раньше для логов) --------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_BOOKINGS    = os.path.join(BASE_DIR, "bookings.csv")
MENU_IMAGES_DIR  = os.path.join(BASE_DIR, "menu_images")  # подпапки big / small
TMP_MENU_DIR     = os.path.join(BASE_DIR, "tmp_menu_cache")
QUIZ_USERS_CSV   = os.path.join(BASE_DIR, "quiz_users.csv")
COUPONS_GEN_CSV  = os.path.join(BASE_DIR, "coupons_generated.csv")
os.makedirs(TMP_MENU_DIR, exist_ok=True)

# Филиалы (единый список с slug'ами)
MENU_BRANCHES = [
    {"name": "Большой ПОДДОН", "slug": "big"},
    {"name": "Малый ПОДДОН",   "slug": "small"},
]

# -------------------- ENV & LOGGING --------------------
load_dotenv(os.path.join(BASE_DIR, ".env"))
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

from logging.handlers import RotatingFileHandler
file_handler = RotatingFileHandler(
    filename=os.path.join(BASE_DIR, "bot.log"),
    maxBytes=10_000_000,
    backupCount=5,
    encoding="utf-8",
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logging.getLogger().addHandler(file_handler)

async def report_error_to_admin(text: str):
    try:
        if ADMIN_CHAT_ID and str(ADMIN_CHAT_ID).strip():
            await bot.send_message(int(ADMIN_CHAT_ID), f"⚠️ Ошибка: {text[:3800]}")
    except Exception:
        pass

if not BOT_TOKEN:
    raise RuntimeError("В .env не найден TELEGRAM_BOT_TOKEN / BOT_TOKEN")

REQUIRED_ENV = ["TELEGRAM_BOT_TOKEN", "ADMIN_CHAT_ID"]
def verify_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"В .env отсутствует(ют): {', '.join(missing)}")
verify_env()

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# -------------------- DATA LOADING --------------------
def load_copy() -> dict:
    path = os.path.join(BASE_DIR, "bot_copy.json")
    default = {
        "greeting": "Привет! Помогу с бронью, меню, адресом и мини-викториной. С чего начнём?",
        "unknown": "Я на связи. Могу помочь с бронью, меню, адресом и викториной. Что интересно?",
        "quiz_intro": "Молниеносная викторина. Готов?",
    }
    if not os.path.exists(path):
        return default
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                logging.warning("bot_copy.json не dict — откатываюсь к дефолту")
                return default
            return {**default, **data}
    except Exception as e:
        logging.exception("Не смог прочитать bot_copy.json — беру дефолт: %s", e)
        return default

def read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        logging.warning("Файл не найден: %s", path)
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8")

COPY    = load_copy()
MENU    = read_csv_safe(os.path.join(BASE_DIR, "menu_template.csv"))
VENUES  = read_csv_safe(os.path.join(BASE_DIR, "venues_template.csv"))
QUIZ    = read_csv_safe(os.path.join(BASE_DIR, "quiz_template.csv"))
FACTS   = read_csv_safe(os.path.join(BASE_DIR, "facts_template.csv"))

QUIZ_STATE: dict[int, str] = {}    # qid -> correct (a/b/c/d)
BOOK_STATE: dict[int, dict] = {}   # user_id -> промежуточные ответы по броням

# -------------------- FILE HELPERS --------------------
def ensure_bookings_file():
    if not os.path.exists(DATA_BOOKINGS):
        cols = ["id","tg_user_id","name","phone","guests","guests_range","date","time","comment","status","venue_id","created_at","updated_at"]
        pd.DataFrame(columns=cols).to_csv(DATA_BOOKINGS, index=False, encoding="utf-8-sig")

def load_bookings() -> pd.DataFrame:
    ensure_bookings_file()
    return pd.read_csv(DATA_BOOKINGS, encoding="utf-8-sig")

def save_bookings(df: pd.DataFrame):
    df.to_csv(DATA_BOOKINGS, index=False, encoding="utf-8-sig")

def next_booking_id(df: pd.DataFrame) -> int:
    if df.empty: return 1
    return int(df["id"].max()) + 1

# --- quiz users & coupons files ---
def ensure_quiz_files():
    if not os.path.exists(QUIZ_USERS_CSV):
        cols = ["user_id","streak","locked_until_iso","awarded","last_played_at","current_qid"]
        pd.DataFrame(columns=cols).to_csv(QUIZ_USERS_CSV, index=False, encoding="utf-8-sig")
    if not os.path.exists(COUPONS_GEN_CSV):
        cols = ["code","user_id","username","full_name","issued_at"]
        pd.DataFrame(columns=cols).to_csv(COUPONS_GEN_CSV, index=False, encoding="utf-8-sig")

def fix_quiz_users_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["locked_until_iso", "last_played_at"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype("object")
    for col in ["user_id", "streak", "awarded", "current_qid"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df

def load_quiz_users() -> pd.DataFrame:
    ensure_quiz_files()
    df = pd.read_csv(QUIZ_USERS_CSV, encoding="utf-8-sig")
    return fix_quiz_users_dtypes(df)

def save_quiz_users(df: pd.DataFrame):
    df.to_csv(QUIZ_USERS_CSV, index=False, encoding="utf-8-sig")

def load_coupons_gen() -> pd.DataFrame:
    ensure_quiz_files()
    return pd.read_csv(COUPONS_GEN_CSV, encoding="utf-8-sig")

def save_coupons_gen(df: pd.DataFrame):
    df.to_csv(COUPONS_GEN_CSV, index=False, encoding="utf-8-sig")

# -------------------- QUIZ CORE --------------------
def pick_quiz_question():
    if QUIZ.empty: return None
    df = QUIZ
    if "active" in df.columns:
        active = df[df["active"] == 1]
        if not active.empty:
            df = active
    return df.sample(1).iloc[0]

def make_qid(row) -> int:
    val = row.get("id")
    if val is not None:
        try:
            return int(str(val).strip())
        except Exception:
            pass
    text = str(row.get("text") or row.get("question") or random.random())
    h = int(md5(text.encode("utf-8")).hexdigest()[:8], 16)
    return (h % 999_999) + 1

def get_question_text(row) -> str:
    if "question" in row and pd.notna(row["question"]) and str(row["question"]).strip():
        return str(row["question"]).strip()
    if "text" in row and pd.notna(row["text"]) and str(row["text"]).strip():
        return str(row["text"]).strip()
    try:
        frags = []
        for k, v in row.items():
            if isinstance(k, str) and k.startswith("Unnamed") and pd.notna(v):
                frags.append(str(v))
        if frags:
            return ", ".join(frags).strip()
    except Exception:
        pass
    return "Вопрос"

def format_time_hhmm(dt: datetime) -> str:
    return dt.strftime("%d.%m %H:%M")

def get_user_quiz_state(user_id: int) -> dict:
    df = load_quiz_users()
    row = df[df["user_id"] == user_id]
    if row.empty:
        return {"user_id": user_id, "streak": 0, "locked_until_iso": "", "awarded": 0, "last_played_at": "", "current_qid": 0}
    r = row.iloc[0].to_dict()
    r["streak"] = int(r.get("streak", 0) or 0)
    r["awarded"] = int(r.get("awarded", 0) or 0)
    r["current_qid"] = int(r.get("current_qid", 0) or 0)
    r["locked_until_iso"] = str(r.get("locked_until_iso", "") or "")
    r["last_played_at"]   = str(r.get("last_played_at", "") or "")
    return r

def set_user_quiz_state(state: dict):
    df = load_quiz_users()
    df = fix_quiz_users_dtypes(df)

    user_id = int(state["user_id"])
    streak = int(state.get("streak", 0) or 0)
    awarded = int(state.get("awarded", 0) or 0)
    current_qid = int(state.get("current_qid", 0) or 0)
    locked_until_iso = str(state.get("locked_until_iso", "") or "")
    last_played_at = str(state.get("last_played_at", "") or "")

    if (df["user_id"] == user_id).any():
        df.loc[df["user_id"] == user_id, "streak"] = streak
        df.loc[df["user_id"] == user_id, "awarded"] = awarded
        df.loc[df["user_id"] == user_id, "current_qid"] = current_qid
        df.loc[df["user_id"] == user_id, "locked_until_iso"] = locked_until_iso
        df.loc[df["user_id"] == user_id, "last_played_at"] = last_played_at
    else:
        df = pd.concat([df, pd.DataFrame([{
            "user_id": user_id,
            "streak": streak,
            "awarded": awarded,
            "current_qid": current_qid,
            "locked_until_iso": locked_until_iso,
            "last_played_at": last_played_at,
        }])], ignore_index=True)

    df = fix_quiz_users_dtypes(df)
    save_quiz_users(df)

def generate_unique_coupon_code() -> str:
    df = load_coupons_gen()
    existing = set(df["code"].astype(str)) if not df.empty else set()
    for _ in range(10000):
        code = f"{random.randint(0, 999999):06d}"
        if code not in existing:
            return code
    return datetime.now().strftime("%H%M%S")

def issue_coupon_to_user(user_id: int, username: Optional[str], full_name: Optional[str]) -> Optional[str]:
    code = generate_unique_coupon_code()
    df = load_coupons_gen()
    new_row = {
        "code": code,
        "user_id": user_id,
        "username": username or "",
        "full_name": full_name or "",
        "issued_at": datetime.now().isoformat(timespec="seconds"),
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    save_coupons_gen(df)
    return code

async def notify_admin_coupon(code: str, user: User):
    if not ADMIN_CHAT_ID or not str(ADMIN_CHAT_ID).strip():
        return
    user_line = f"@{user.username}" if user.username else f"{user.full_name} (id={user.id})"
    text = (
        "🎁 Выигрыш в викторине\n"
        f"Пользователь: {user_line}\n"
        f"Купон: {code}\n"
        "Приз: Бесплатная настойка"
    )
    try:
        await bot.send_message(int(ADMIN_CHAT_ID), text)
    except Exception as e:
        logging.exception("Не удалось отправить администратору: %s", e)

async def start_quiz_for_user(message: Message):
    uid = message.from_user.id
    st = get_user_quiz_state(uid)

    # Полный запрет для победителей
    if int(st.get("awarded", 0)) == 1:
        await message.answer("Вы уже выиграли приз 🎉 Повторная игра недоступна.")
        return

    # Проверка блокировки
    if st.get("locked_until_iso"):
        try:
            until = datetime.fromisoformat(st["locked_until_iso"])
            if datetime.now() < until:
                await message.answer(f"Сегодня без игры 😔 Попробуйте снова после {format_time_hhmm(until)}.")
                return
            else:
                st["locked_until_iso"] = ""
                set_user_quiz_state(st)
        except Exception:
            st["locked_until_iso"] = ""
            set_user_quiz_state(st)

    row = pick_quiz_question()
    if row is None:
        await message.answer("Вопросы викторины пока не добавлены.")
        return

    qid = make_qid(row)
    text = get_question_text(row)

    options_list = []
    for key in ["a", "b", "c", "d"]:
        if key in row and pd.notna(row[key]) and str(row[key]).strip():
            options_list.append((key.upper(), str(row[key]).strip()))
    if not options_list:
        for i, key in enumerate(["option1", "option2", "option3", "option4"], start=1):
            if key in row and pd.notna(row[key]) and str(row[key]).strip():
                options_list.append((chr(64 + i), str(row[key]).strip()))
    if not options_list:
        await message.answer("У этого вопроса нет вариантов ответа. Попробуем другой.")
        await start_quiz_for_user(message)
        return

    correct_raw = str(row.get("correct", "a")).strip()
    if correct_raw.lower() in ["a", "b", "c", "d"]:
        correct_letter = correct_raw.lower()
    else:
        correct_letter = None
        cr = correct_raw.lower()
        for label, val in options_list:
            if val.lower() == cr:
                correct_letter = label.lower()
                break
        if not correct_letter:
            correct_letter = options_list[0][0].lower()

    QUIZ_STATE[qid] = correct_letter
    st["current_qid"] = qid
    st["last_played_at"] = datetime.now().isoformat(timespec="seconds")
    set_user_quiz_state(st)

    buttons = [
        [InlineKeyboardButton(text=f"{label}: {val}", callback_data=f"quiz:{qid}:{label.lower()}")]
        for label, val in options_list
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("quiz:"))
async def cb_quiz_answer(call: CallbackQuery):
    try:
        _, qid_str, opt = call.data.split(":")
        qid = int(qid_str)
        uid = call.from_user.id

        st = get_user_quiz_state(uid)

        # Полный запрет для победителей (клик по старым кнопкам)
        if int(st.get("awarded", 0)) == 1:
            await call.message.answer("Вы уже выиграли приз 🎉 Повторная игра недоступна.")
            await call.answer()
            return

        correct = QUIZ_STATE.get(qid)
        if not correct:
            await call.message.answer("Этот вопрос уже закрыт. Давай новый!")
            await start_quiz_for_user(call.message)
            await call.answer()
            return

        if opt == correct:
            st["streak"] = int(st.get("streak", 0) or 0) + 1
            st["current_qid"] = 0
            set_user_quiz_state(st)

            if st["streak"] >= 3:
                code = None
                if int(st.get("awarded", 0)) == 0:
                    code = issue_coupon_to_user(uid, call.from_user.username, call.from_user.full_name)
                    st["awarded"] = 1
                    set_user_quiz_state(st)

                if code:
                    await call.message.answer(
                        f"🔥 Три подряд! Ваш приз — бесплатная настойка.\n"
                        f"Код купона: {code}\n"
                        f"Покажите его бармену при заказе."
                    )
                    await notify_admin_coupon(code, call.from_user)
                else:
                    await call.message.answer(
                        "🔥 Три подряд! Приз — бесплатная настойка.\n"
                        "Купон уже получали ранее — повторно не выдаётся."
                    )

                # Сбрасываем стрик и закрываем вопрос без выдачи нового
                st["streak"] = 0
                set_user_quiz_state(st)
                QUIZ_STATE.pop(qid, None)
                await call.answer()
                return
            else:
                await call.message.answer(f"Верно! 👏 Осталось правильных подряд: {3 - st['streak']}")
                await start_quiz_for_user(call.message)

        else:
            st["streak"] = 0
            st["current_qid"] = 0
            lock_until = datetime.now() + timedelta(hours=24)
            st["locked_until_iso"] = lock_until.isoformat(timespec="seconds")
            set_user_quiz_state(st)
            await call.message.answer(
                f"Чуть-чуть мимо 😬 Попробуйте снова после {format_time_hhmm(lock_until)}."
            )

        QUIZ_STATE.pop(qid, None)

    except Exception as e:
        logging.exception("Ошибка обработки ответа викторины: %s", e)
        await call.message.answer("Что-то пошло не так. Попробуем ещё раз.")

    await call.answer()

# -------------------- NLU / PARSING (бронь) --------------------
PHONE_RE = re.compile(r"(\+?\d[\d\s\-\(\)]{6,}\d)")

WORDS_TO_NUM = {
    "один":1,"одна":1,"по одному":1,
    "двое":2,"двоих":2,"двух":2,"вдвоем":2,"вдвоём":2,"на двоих":2,
    "трое":3,"троих":3,"трех":3,"трёх":3,"втроем":3,"втроём":3,"на троих":3,
    "четверо":4,"четверых":4,"четырех":4,"четырёх":4,"вчетвером":4,"на четверых":4,
    "пятеро":5,"пятерых":5,"пяти":5,"на пятерых":5,
    "шестеро":6,"шестерых":6,"шести":6,"на шестерых":6,
    "семеро":7,"семерых":7,"семи":7,"на семерых":7,
    "восьмеро":8,"восьмерых":8,"восьми":8,"на восьмерых":8,
    "девятеро":9,"девятерых":9,"девяти":9,"на девятерых":9,
    "десятеро":10,"десятерых":10,"десяти":10,"на десятерых":10,
}
WEEKDAY_FULL = {
    "понедельник": 0, "вторник": 1, "среда": 2, "четверг": 3,
    "пятница": 4, "суббота": 5, "воскресенье": 6
}

def normalize_text(t: str) -> str:
    return (t or "").lower().strip()

def extract_phone(text: str) -> Optional[str]:
    t = normalize_text(text)
    m = PHONE_RE.search(t or "")
    if not m: return None
    raw = m.group(1)
    cleaned = []
    for i, ch in enumerate(raw.strip()):
        if ch.isdigit(): cleaned.append(ch)
        elif ch == "+" and i == 0: cleaned.append(ch)
    phone = "".join(cleaned)
    return phone if sum(c.isdigit() for c in phone) >= 8 else None

def extract_name_from_contact_text(text: str, phone: Optional[str]) -> Optional[str]:
    if not text: return None
    t = text
    if phone:
        for ch in "+-() ": t = t.replace(ch, " ")
        for ch in "0123456789": t = t.replace(ch, " ")
    name = " ".join([w for w in t.split() if w.isalpha()]).strip()
    return name or None

def extract_time(text: str) -> Optional[str]:
    t = normalize_text(text)
    m = re.search(r"\b(\d{1,2})[:.](\d{2})\b", t)
    if m:
        hh = int(m.group(1)); mm = int(m.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59: return f"{hh:02d}:{mm:02d}"
    m = re.search(r"\b(?:в|к)\s*(\d{1,2})(?:\s*(?:час(?:а|ов)?|ч))?\b", t)
    if m:
        hh = int(m.group(1))
        if 0 <= hh <= 23:
            if hh <= 11 and re.search(r"(вечер|ноч)", t): hh += 12
            return f"{hh:02d}:00"
    return None

def extract_date(text: str):
    t = normalize_text(text)
    m = re.search(r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b", t)
    if m:
        d = int(m.group(1)); mth = int(m.group(2)); y = m.group(3)
        y = (int(y) + 2000) if y and int(y) < 100 else (int(y) if y else datetime.now().year)
        try: return datetime(y, mth, d).date()
        except ValueError: pass
    if "сегодня" in t: return datetime.now().date()
    if "завтра" in t:  return (datetime.now() + timedelta(days=1)).date()
    if "послезавтра" in t or "после завтра" in t: return (datetime.now() + timedelta(days=2)).date()
    for w, idx in WEEKDAY_FULL.items():
        if re.search(rf"\b{re.escape(w[:-1])}\w*\b", t):
            today = datetime.now()
            shift = (idx - today.weekday()) % 7 or 7
            return (today + timedelta(days=shift)).date()
    m = re.search(r"\b(\d{1,2})(?:\s*|-)?(?:го|ого)\b", t) or re.search(r"\b(\d{1,2})\s*числа\b", t)
    if m:
        day = int(m.group(1)); today = datetime.now().date()
        def _safe(y, m, d):
            try: return datetime(y, m, d).date()
            except ValueError: return None
        y, mth = today.year, today.month
        cand = _safe(y, mth, day)
        if not cand or cand <= today:
            if mth == 12: y += 1; mth = 1
            else: mth += 1
            cand = _safe(y, mth, day)
        if cand: return cand
    return None

def extract_guests_range(text: str) -> Tuple[Optional[int], Optional[int]]:
    t = normalize_text(text)
    if not t: return None, None
    m = re.search(r"\b(\d{1,2})\s*(?:-|—|–|or|до)\s*(\d{1,2})\b", t)
    if m:
        a, b = int(m.group(1)), int(m.group(2)); lo, hi = sorted((a, b)); return lo, hi
    words_map = WORDS_TO_NUM
    pat_words = r"(" + "|".join(map(re.escape, words_map.keys())) + r")\s*(?:или|до|-|—|–)\s*(" + "|".join(map(re.escape, words_map.keys())) + r")"
    m = re.search(pat_words, t)
    if m:
        a = words_map.get(m.group(1)); b = words_map.get(m.group(2))
        if a and b: return min(a,b), max(a,b)
    m = re.search(r"\bот\s+(\d{1,2})\s+до\s+(\d{1,2})\b", t)
    if m:
        a, b = int(m.group(1)), int(m.group(2)); return min(a,b), max(a,b)
    m = re.search(r"\bдо\s+(\d{1,2})\b", t)
    if m: return None, int(m.group(1))
    for w, n in words_map.items():
        if re.search(rf"\bдо\s+{re.escape(w)}\b", t): return None, n
    for w, n in words_map.items():
        if re.search(rf"\b{re.escape(w)}\b", t): return n, n
    m = re.search(r"(?:\bна|\bдля)?\s*(\d{1,2})\b", t)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 20: return n, n
    return None, None

def parse_booking_phrase(text: str):
    d = extract_date(text); t = extract_time(text); gmin, gmax = extract_guests_range(text)
    return d, t, gmin, gmax

# -------------------- INTENTS --------------------
INTENT_KEYWORDS = {
    "menu": ["меню","посмотреть меню","карта","барная карта","лист"],
    "venue": ["адрес","где вы","как добраться","работаете","часы","до скольки","во сколько","контакты","телефон"],
    "quiz": ["викторина","квиз","приз","розыгрыш"],
    "book": ["бронь","заброни","резерв","столик","стол","посадка"],
}
def detect_intent(text: str, in_booking_flow: bool) -> Optional[str]:
    t = normalize_text(text)
    if in_booking_flow:
        for intent, keys in INTENT_KEYWORDS.items():
            if intent == "book": continue
            if any(k in t for k in keys): return intent
        return "book"
    for intent, keys in INTENT_KEYWORDS.items():
        if any(k in t for k in keys): return intent
    return None

# -------------------- MENU IMAGES --------------------
def list_menu_images_for_slug(slug: str) -> List[str]:
    folder = os.path.join(MENU_IMAGES_DIR, slug)
    if not os.path.isdir(folder): return []
    files = []
    for fn in sorted(os.listdir(folder)):
        if fn.lower().endswith((".jpg",".jpeg",".png",".webp")):
            files.append(os.path.join(folder, fn))
    return files

def preprocess_image_for_telegram(src_path: str, slug: str) -> str:
    try:
        stat = os.stat(src_path)
        base = f"{slug}__{os.path.basename(src_path)}__{int(stat.st_mtime)}.jpg"
    except Exception:
        base = f"{slug}__{os.path.basename(src_path)}.jpg"

    out_path = os.path.join(TMP_MENU_DIR, base)
    if os.path.exists(out_path):
        return out_path

    try:
        with Image.open(src_path) as im:
            im = ImageOps.exif_transpose(im)
            if im.mode not in ("RGB", "L"):
                im = im.convert("RGB")
            elif im.mode == "L":
                im = im.convert("RGB")

            w, h = im.size
            max_side = 4096
            max_pixels = 36_000_000
            scale = 1.0
            if w > max_side or h > max_side or (w * h) > max_pixels:
                from math import sqrt
                scale_side = min(max_side / w, max_side / h)
                scale_pixels = sqrt(max_pixels / (w * h))
                scale = min(scale_side, scale_pixels, 1.0)
            if scale < 1.0:
                new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
                im = im.resize(new_size, Image.LANCZOS)

            im.save(out_path, format="JPEG", quality=88, optimize=True, progressive=True)
            return out_path
    except Exception as e:
        logging.exception("Не удалось обработать изображение %s: %s", src_path, e)
        return src_path

async def show_menu_branch_picker(message: Message):
    buttons = [[InlineKeyboardButton(text=b["name"], callback_data=f"menu_branch:{b['slug']}")] for b in MENU_BRANCHES]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выбери филиал:", reply_markup=kb)

async def send_menu_images_for_branch(message: Message, slug: str, branch_name: str):
    paths = list_menu_images_for_slug(slug)
    if not paths:
        await message.answer(f"Для филиала «{branch_name}» пока нет картинок меню.\n"
                             f"Положи файлы в `{os.path.join('menu_images', slug)}`.")
        return

    await message.answer(f"Меню — {branch_name}")

    batch: List[InputMediaPhoto] = []
    for i, p in enumerate(paths, 1):
        prepped = preprocess_image_for_telegram(p, slug)
        try:
            batch.append(InputMediaPhoto(media=FSInputFile(prepped)))
        except Exception as e:
            logging.exception("Подготовка медиа упала (%s): %s", prepped, e)

        if len(batch) == 10 or i == len(paths):
            try:
                await bot.send_media_group(message.chat.id, media=batch)
            except Exception as e:
                logging.error("send_media_group упал, шлём по одному: %s", e)
                for m in batch:
                    try:
                        await bot.send_photo(message.chat.id, photo=m.media)
                    except Exception as e2:
                        logging.error("send_photo тоже упал для %s: %s", m.media, e2)
            batch = []

def branch_name_by_slug(slug: str) -> str:
    for b in MENU_BRANCHES:
        if b["slug"] == slug:
            return b["name"]
    return slug

# -------------------- VENUE (контакты) --------------------
def venue_today_hours(venue_row: pd.Series):
    if venue_row is None or venue_row.empty: return None
    weekday = datetime.now().weekday()
    hours_col = "hours_weekday" if weekday < 5 else "hours_weekend"
    return venue_row.get(hours_col)

def find_venue_row(slug: str) -> Optional[pd.Series]:
    if VENUES.empty:
        return None
    # 1) Явный столбец slug
    if "slug" in VENUES.columns:
        rows = VENUES[VENUES["slug"].astype(str).str.lower() == slug.lower()]
        if not rows.empty:
            return rows.iloc[0]
    # 2) По имени из MENU_BRANCHES
    name_hint = branch_name_by_slug(slug)
    if "name" in VENUES.columns:
        mask = VENUES["name"].astype(str).str.lower().str.contains(name_hint.lower().replace("поддон", "").strip())
        rows = VENUES[mask]
        if not rows.empty:
            return rows.iloc[0]
    # 3) Фоллбек — первая строка
    return VENUES.iloc[0]

async def show_venue_branch_picker(message: Message):
    buttons = [[InlineKeyboardButton(text=b["name"], callback_data=f"venue_branch:{b['slug']}")] for b in MENU_BRANCHES]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Выбери филиал для контактов:", reply_markup=kb)

async def send_venue_for_branch(message: Message, slug: str):
    if VENUES.empty:
        await message.answer("Адреса и часы не заданы. Заполни venues_template.csv.")
        return
    row = find_venue_row(slug)
    name = row.get("name", branch_name_by_slug(slug))
    address = row.get("address", "—")
    phone = row.get("phone", "—")
    maps = row.get("maps_url", "")
    hours_val = venue_today_hours(row) or "часы не заданы"

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Построить маршрут", url=maps)]]) \
        if isinstance(maps, str) and maps.strip() else None

    await message.answer(f"{name}\nАдрес: {address}\nТелефон: {phone}\nСегодня работаем: {hours_val}", reply_markup=kb)

# -------------------- ADMIN: смена статуса брони --------------------
async def admin_update_status(booking_id: int, new_status: str, chat):
    df = load_bookings()
    if df.empty or booking_id not in set(df["id"].astype(int)):
        await chat.answer("Бронь не найдена (возможно уже изменена)."); return
    df.loc[df["id"].astype(int) == booking_id, "status"] = new_status
    df.loc[df["id"].astype(int) == booking_id, "updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_bookings(df); await chat.answer(f"Статус брони {booking_id} → {new_status}")

@dp.callback_query(F.data.startswith("admin:confirm:"))
async def cb_admin_confirm(call: CallbackQuery):
    if not ADMIN_CHAT_ID or str(call.message.chat.id) != str(ADMIN_CHAT_ID):
        await call.answer("Недостаточно прав", show_alert=True); return
    await admin_update_status(int(call.data.split(":")[-1]), "confirmed", call.message); await call.answer()

@dp.callback_query(F.data.startswith("admin:cancel:"))
async def cb_admin_cancel(call: CallbackQuery):
    if not ADMIN_CHAT_ID or str(call.message.chat.id) != str(ADMIN_CHAT_ID):
        await call.answer("Недостаточно прав", show_alert=True); return
    await admin_update_status(int(call.data.split(":")[-1]), "canceled", call.message); await call.answer()

# -------------------- COMMANDS --------------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="МЕНЮ", callback_data="action:menu"),
         InlineKeyboardButton(text="КОНТАКТЫ", callback_data="action:venue")],
        [InlineKeyboardButton(text="ВИКТОРИНА", callback_data="action:quiz"),
         InlineKeyboardButton(text="БРОНЬ", callback_data="action:book")]
    ])
    await message.answer(COPY.get("greeting", "Привет!"), reply_markup=kb)

@dp.message(Command("health"))
async def cmd_health(message: Message):
    import sys
    import aiogram, pandas, PIL
    info = (
        "OK\n"
        f"python: {sys.version.split()[0]}\n"
        f"aiogram: {aiogram.__version__}\n"
        f"pandas: {pandas.__version__}\n"
        f"Pillow: {PIL.__version__}"
    )
    await message.answer(info)

@dp.message(Command("whoami"))
async def cmd_whoami(message: Message):
    await message.answer(f"Твой user_id: {message.from_user.id}\nЧат id: {message.chat.id}")

@dp.message(Command("bookings_today"))
async def cmd_bookings_today(message: Message):
    if not ADMIN_CHAT_ID or str(message.chat.id) != str(ADMIN_CHAT_ID):
        await message.answer("Команда доступна только в админ-чате."); return
    df = load_bookings()
    if df.empty:
        await message.answer("Сегодня броней нет.")
        return
    today = datetime.now().date().isoformat(); df["date"] = df["date"].astype(str)
    today_df = df[df["date"] == today]
    if today_df.empty:
        await message.answer("Сегодня броней нет.")
        return
    lines = [f"#{int(r['id'])} — {r['time']} — {r.get('name','')} ({r.get('guests_range', r['guests'])}) — {r['status']}" for _, r in today_df.sort_values("time").iterrows()]
    await message.answer("\n".join(lines))

# -------------------- CALLBACKS --------------------
@dp.callback_query(F.data == "action:menu")
async def cb_menu(call: CallbackQuery):
    await show_menu_branch_picker(call.message); await call.answer()

@dp.callback_query(F.data.startswith("menu_branch:"))
async def cb_menu_branch(call: CallbackQuery):
    slug = call.data.split(":", 1)[1]
    await call.message.answer("Секунду, собираю меню…")
    await send_menu_images_for_branch(call.message, slug, branch_name_by_slug(slug))
    await call.answer()

@dp.callback_query(F.data == "action:venue")
async def cb_venue(call: CallbackQuery):
    # Раньше показывали контакты сразу; теперь — выбор филиала
    await show_venue_branch_picker(call.message)
    await call.answer()

@dp.callback_query(F.data.startswith("venue_branch:"))
async def cb_venue_branch(call: CallbackQuery):
    slug = call.data.split(":", 1)[1]
    await send_venue_for_branch(call.message, slug)
    await call.answer()

@dp.callback_query(F.data == "action:book")
async def cb_book(call: CallbackQuery):
    await call.message.answer("Окей, забронируем. Расскажи когда, во сколько и сколько вас будет человек (можно диапазон).")
    await call.answer()

@dp.callback_query(F.data == "action:quiz")
async def cb_quiz(call: CallbackQuery):
    await start_quiz_for_user(call.message)
    await call.answer()

# -------------------- UNIVERSAL HANDLER --------------------
@dp.message()
async def universal_router(message: Message):
    uid = message.from_user.id
    text = message.text or ""
    st = BOOK_STATE.get(uid)

    intent = detect_intent(text, in_booking_flow=bool(st))
    if intent == "menu":  await show_menu_branch_picker(message)
    if intent == "venue": await show_venue_branch_picker(message)  # изменено: сначала выбор филиала
    if intent == "quiz":  await start_quiz_for_user(message)

    d, t, gmin, gmax = parse_booking_phrase(text)
    has_clues = any([d, t, gmin, gmax])

    if intent == "book" or st or has_clues:
        st = st or {}
        if d: st["date"] = d
        if t: st["time"] = t
        if gmin is not None: st["guests_min"] = gmin
        if gmax is not None: st["guests_max"] = gmax

        phone = extract_phone(text)
        if phone:
            st["phone"] = phone
            name = extract_name_from_contact_text(text, phone)
            if name: st["name"] = name

        st.setdefault("name", (message.from_user.full_name or "Гость"))
        BOOK_STATE[uid] = st

        missing = []
        if "date" not in st:   missing.append("дату")
        if "time" not in st:   missing.append("время")
        if "guests_max" not in st and "guests_min" not in st:
            missing.append("кол-во гостей (можно диапазон)")
        if "phone" not in st:  missing.append("телефон (и имя)")

        if missing:
            parts = []
            if st.get("date"):   parts.append(st["date"].strftime("%d.%m.%Y"))
            if st.get("time"):   parts.append(st["time"])
            if st.get("guests_min") or st.get("guests_max"):
                g1 = st.get("guests_min"); g2 = st.get("guests_max", g1)
                parts.append(f"{g1}–{g2} чел." if (g1 and g2 and g1 != g2) else f"{g2 or g1} чел.")
            if st.get("phone"):  parts.append(f"тел. {st['phone']}")
            known_str = " • ".join(parts) if parts else "пока ничего не уточнили"
            await message.answer(
                f"Понял: {known_str}.\n"
                f"Допиши недостающее: {', '.join(missing)}.\n"
                f"Можно свободно — напр.: «в субботу к 18, нас трое» или «3–5 человек» или «+79991234567 Алексей».")
            return

        await finalize_booking(message, st); return

    if not intent and not st:
        await message.answer(COPY.get("unknown", "Я на связи. Могу помочь с бронью, меню, адресом и викториной. Что интересно?"))

async def finalize_booking(message: Message, st: dict):
    uid = message.from_user.id
    name   = st.get("name", (message.from_user.full_name or "Гость"))
    phone  = st.get("phone", "").strip()
    date_o = st.get("date")
    time_s = st.get("time", "20:00")
    gmin   = st.get("guests_min")
    gmax   = st.get("guests_max", gmin)

    if not (date_o and time_s and gmax and phone):
        await message.answer("Давай ещё раз: когда, во сколько, сколько вас (можно диапазон), и телефон с именем.")
        return

    guests_range_str = f"{gmin}-{gmax}" if gmin and gmax and gmin != gmax else str(gmax)
    guests_for_table = gmax

    df = load_bookings()
    bid = next_booking_id(df)
    row = {
        "id": bid,
        "tg_user_id": uid,
        "name": name,
        "phone": phone,
        "guests": guests_for_table,
        "guests_range": guests_range_str,
        "date": date_o.isoformat(),
        "time": time_s,
        "comment": "",
        "status": "new",
        "venue_id": 1,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    save_bookings(df)

    show_guests = f"{gmin}–{gmax} чел." if gmin and gmax and gmin != gmax else f"{gmax} чел."
    await message.answer(
        f"Записал: {date_o.strftime('%d.%m.%Y')} в {time_s}, {show_guests}\n"
        f"Имя: {name}, телефон: {phone}.\n"
        f"Мы свяжемся для подтверждения."
    )

    if ADMIN_CHAT_ID and str(ADMIN_CHAT_ID).strip():
        text = (
            "🆕 Новая бронь\n"
            f"ID: {row['id']}\n"
            f"Дата: {row['date']}  Время: {row['time']}\n"
            f"Гостей: {row.get('guests_range', row['guests'])}\n"
            f"Имя: {row.get('name','')}\n"
            f"Телефон: {row.get('phone','')}\n"
            f"Комментарий: {row.get('comment','')}\n"
            f"Статус: {row['status']}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin:confirm:{row['id']}"),
            InlineKeyboardButton(text="❌ Отменить",    callback_data=f"admin:cancel:{row['id']}")
        ]])
        try:
            await bot.send_message(int(ADMIN_CHAT_ID), text, reply_markup=kb)
        except Exception as e:
            logging.exception("Не удалось отправить сообщение админу: %s", e)

    BOOK_STATE.pop(uid, None)

# -------- Middleware: ловим необработанные исключения и не падаем -----
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.types import Update

class AdminErrorMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Update, data):
        try:
            return await handler(event, data)
        except Exception as e:
            logging.exception("Необработанное исключение: %s", e)
            await report_error_to_admin(repr(e))
            return

dp.message.middleware(AdminErrorMiddleware())
dp.callback_query.middleware(AdminErrorMiddleware())

# -------------------- MAIN --------------------
async def main():
    logging.info("Инициализация бота…")
    me = await bot.get_me()
    logging.info(f"Бот: @{me.username} (id={me.id})")
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Webhook удалён (если был). Запускаю long polling…")
    ensure_bookings_file()
    ensure_quiz_files()
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
