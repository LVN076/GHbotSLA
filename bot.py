import asyncio
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram import F

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, MessageReactionUpdated
from aiogram.filters import Command
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.enums import ParseMode

from config import (
    BOT_TOKEN,
    DB_DSN,
    SLA_MINUTES,
    ESCALATE_TO_OWNER_AFTER_MIN,
    OWNER_USER_ID,
)

from db import get_conn
logging.basicConfig(level=logging.INFO)

# ===== ВРЕМЕННЫЕ НАСТРОЙКИ =====
MSK_TZ = timezone(timedelta(hours=3))

WORKDAY_START_HOUR = 9
WORKDAY_START_MIN = 0

WORKDAY_END_HOUR = 20
WORKDAY_END_MIN = 0


def format_msk(dt: datetime) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MSK_TZ).strftime("%d.%m.%Y %H:%M:%S МСК")


import re

WEEKDAYS = {
    "понедельник": 0, "пон": 0,
    "вторник": 1, "вт": 1,
    "среда": 2, "ср": 2,
    "четверг": 3, "чт": 3,
    "пятница": 4, "пт": 4,
    "суббота": 5, "сб": 5,
    "воскресенье": 6, "вс": 6,
}


def stage_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="🟢 До стройки (prebuild)",
                callback_data="stage:set:prebuild"
            )],
            [InlineKeyboardButton(
                text="🟡 Стройка (build)",
                callback_data="stage:set:build"
            )],
            [InlineKeyboardButton(
                text="🔵 Гарантия (warranty)",
                callback_data="stage:set:warranty"
            )],
        ]
    )


def parse_due_at(text: str):
    """
    Возвращает:
      due_at_utc: datetime | None
      needs_due_at: bool
    """
    if not text:
        return None, True

    t = text.strip().lower()
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc.astimezone(MSK_TZ)

    def dt_msk_on(date_obj, hour, minute):
        return datetime(date_obj.year, date_obj.month, date_obj.day, hour, minute, 0, tzinfo=MSK_TZ)

    # 1) Относительные сроки: "в течение часа", "через 20 минут", "через 2 часа"
    m_rel = re.search(r"(?:в течени[еи]|через)\s*(\d+)?\s*(минут|мин|час|часа|часов)\b", t)
    if m_rel:
        qty = int(m_rel.group(1) or 1)
        unit = m_rel.group(2)
        delta = timedelta(minutes=qty) if "мин" in unit else timedelta(hours=qty)
        due_msk = now_msk + delta
        return due_msk.astimezone(timezone.utc), False

    # 2) Явное время: "до 12:00" / "до 12" / "к 12:30"
    tm = re.search(r"(?:до|к)\s*(\d{1,2})(?:[:.](\d{2}))?", t)
    hh = mm = None
    if tm:
        hh = int(tm.group(1))
        mm = int(tm.group(2) or 0)

    # 3) Части дня: "утром/днем/вечером"
    if hh is None:
        if re.search(r"\bутром\b", t):
            hh, mm = 10, 0
        elif re.search(r"\bднем\b|\bднём\b", t):
            hh, mm = 14, 0
        elif re.search(r"\bвечером\b", t):
            hh, mm = 19, 0

    # "сегодня"
    if "сегодня" in t:
        d = now_msk.date()
        if hh is None:
            due_msk = dt_msk_on(d, WORKDAY_END_HOUR, WORKDAY_END_MIN)
        else:
            due_msk = dt_msk_on(d, hh, mm)
        if due_msk <= now_msk:
            d2 = (now_msk + timedelta(days=1)).date()
            if hh is None:
                due_msk = dt_msk_on(d2, WORKDAY_END_HOUR, WORKDAY_END_MIN)
            else:
                due_msk = dt_msk_on(d2, hh, mm)
        return due_msk.astimezone(timezone.utc), False

    # "завтра"
    if "завтра" in t:
        d = (now_msk + timedelta(days=1)).date()
        if hh is None:
            due_msk = dt_msk_on(d, WORKDAY_START_HOUR, WORKDAY_START_MIN)
        else:
            due_msk = dt_msk_on(d, hh, mm)
        return due_msk.astimezone(timezone.utc), False

    # День недели
    for name, wd in WEEKDAYS.items():
        if re.search(rf"\b{name}\b", t):
            delta = (wd - now_msk.weekday()) % 7
            if delta == 0:
                delta = 7
            d = (now_msk + timedelta(days=delta)).date()
            if hh is None:
                due_msk = dt_msk_on(d, WORKDAY_START_HOUR, WORKDAY_START_MIN)
            else:
                due_msk = dt_msk_on(d, hh, mm)
            return due_msk.astimezone(timezone.utc), False

    # Только время ("до 12:00") без "сегодня/завтра"
    if hh is not None:
        d = now_msk.date()
        due_msk = dt_msk_on(d, hh, mm)
        if due_msk <= now_msk:
            d2 = (now_msk + timedelta(days=1)).date()
            due_msk = dt_msk_on(d2, hh, mm)
        return due_msk.astimezone(timezone.utc), False

    return None, True


def _has_time_context(text: str) -> bool:
    """
    Возвращает True только если в тексте есть явный временной контекст.
    Голые числа (50к, 2 окна, 3 варианта) — НЕ являются временным контекстом.
    """
    if not text:
        return False
    t = text.lower()
    patterns = [
        r"\bсегодня\b",
        r"\bзавтра\b",
        r"\bутром\b",
        r"\bвечером\b",
        r"\bднём\b",
        r"\bднем\b",
        r"(?:до|к)\s*\d{1,2}(?:[:.]\d{2})?",
        r"через\s+\d+\s*(?:минут|мин|час|часа|часов)",
        r"в течени[еи]\s*(?:\d+\s*)?(?:час|минут|мин)",
        r"\b(?:понедельник|пон|вторник|вт|среда|ср|четверг|чт|пятница|пт|суббота|сб|воскресенье|вс)\b",
    ]
    return any(re.search(p, t) for p in patterns)


def sla_policy_by_stage(stage: str):
    """
    SLA-логика по стадиям согласно ТЗ:
    prebuild: ping1=40мин → менеджер, ping2=+15мин → директор
    build/warranty: ping1=90мин, ping2=+30мин, ping3=+30мин
    """
    st = (stage or "").strip().lower()

    if st == "prebuild":
        wait1 = timedelta(minutes=40)
        wait2 = timedelta(minutes=15)
        wait3 = None
        wait4 = None
        label = "⏱ SLA: клиент ждёт ответа > 40 минут."
    else:  # build, warranty
        wait1 = timedelta(minutes=90)
        wait2 = timedelta(minutes=30)
        wait3 = timedelta(minutes=30)
        wait4 = timedelta(minutes=30)
        label = "⏱ SLA: клиент ждёт ответа > 90 минут."

    return {
        "wait1": wait1,
        "wait2": wait2,
        "wait3": wait3,
        "wait4": wait4,
        "label": label,
    }


def silence_policy_by_stage(stage: str) -> dict:
    """
    Правила контроля тишины (нет сообщений от компании direction='out').
    prebuild: >2 суток -> менеджер, +12ч -> директор
    build: >3 суток -> прораб, +12ч -> нач.участка, +24ч -> дир.по стройке, +24ч -> директор
    """
    if stage == "prebuild":
        return {
            "threshold": timedelta(hours=72),
            "wait2": timedelta(hours=12),
            "wait3": None,
            "wait4": None,
        }
    if stage == "build":
        return {
            "threshold": timedelta(days=3),
            "wait2": timedelta(hours=12),
            "wait3": timedelta(hours=24),
            "wait4": timedelta(hours=24),
        }
    return {
        "threshold": timedelta(days=3),
        "wait2": timedelta(hours=12),
        "wait3": timedelta(hours=24),
        "wait4": timedelta(hours=24),
    }


# =========================
# 1) Сотрудники / роли
# =========================

USERS = {
    "owner": {"name": "Малышев Олег", "user_id": 120526283},
    "manager": {"name": "Любовь Черницына", "user_id": 6230098132},
    "foreman": {"name": "Кондратьев Евгений", "user_id": 857676388},
    "site_manager": {"name": "Илья Егоров", "user_id": 1829592679},
    "construction_director": {"name": "Старцев Глеб", "user_id": 364871863},
    "architect": {"name": "Юлия Васильевская", "user_id": 7672313310},
}

ROLE_BY_USER_ID = {v["user_id"]: k for k, v in USERS.items()}

ROLE_NAMES = {
    "owner": "Директор компании",
    "manager": "Менеджер",
    "foreman": "Прораб",
    "architect": "Архитектор",
    "construction_director": "Директор по строительству",
    "site_manager": "Начальник участка",
}


def is_employee(user_id: int) -> bool:
    return user_id in ROLE_BY_USER_ID


def detect_has_question(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return "?" in t


def detect_has_neg(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    if "??" in t or "!!" in t or "?!" in t or "!?" in t:
        return True
    return False


# ── Игнор-фразы клиента (SLA не запускается и не сбрасывается) ──────────────

CLIENT_IGNORED_PHRASES = {
    "спасибо", "ок", "окей", "хорошо", "понял", "поняла", "понятно",
    "мы подумаем", "подумаем", "посмотрим", "хорошо посмотрим",
    "ладно", "договорились", "принято", "благодарю",
}


def is_client_ignored_phrase(text: str) -> bool:
    """
    True если текст клиента — игнор-фраза.
    SLA не запускается и не сбрасывается.
    """
    if not text:
        return False
    return text.strip().lower() in CLIENT_IGNORED_PHRASES


# =========================
# 2) Router
# =========================

router = Router()


@router.message(Command("whoami"))
@router.message(Command("pingme"))
async def cmd_pingme(message: Message):
    await message.reply("✅ Ок! Я могу писать вам в личку. Уведомления будут приходить.")


@router.message(Command(commands=["stage", "стадия"]))
async def cmd_stage(message: Message):
    if not is_employee(message.from_user.id):
        await message.reply("⛔ Команда доступна только сотрудникам.")
        return

    parts = (message.text or "").strip().split(maxsplit=1)

    if len(parts) == 1:
        await message.reply(
            "Выбери стадию чата:",
            reply_markup=stage_keyboard()
        )
        return

    stage = parts[1].strip().lower()
    if stage not in ("prebuild", "build", "warranty"):
        await message.reply("Неверная стадия. Доступно: prebuild, build, warranty")
        return

    chat_id = message.chat.id

    conn = get_conn(DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_stage(chat_id, stage)
                VALUES (%s, %s)
                ON CONFLICT (chat_id)
                DO UPDATE SET stage=EXCLUDED.stage, updated_at=now()
                """,
                (chat_id, stage),
            )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    await message.reply(f"✅ Стадия чата установлена: {stage}")


@router.callback_query(F.data.startswith("stage:set:"))
async def cb_stage_set(callback: CallbackQuery):
    user_id = callback.from_user.id

    if not is_employee(user_id):
        await callback.answer("Команда доступна только сотрудникам.", show_alert=True)
        return

    stage = callback.data.split(":")[-1]

    if stage not in ("prebuild", "build", "warranty"):
        await callback.answer("Неверная стадия.", show_alert=True)
        return

    chat_id = callback.message.chat.id

    conn = get_conn(DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_stage(chat_id, stage)
                VALUES (%s, %s)
                ON CONFLICT (chat_id)
                DO UPDATE SET stage=EXCLUDED.stage, updated_at=now()
                """,
                (chat_id, stage),
            )
        conn.commit()
    finally:
        conn.close()

    await callback.message.edit_text(f"✅ Стадия чата установлена: {stage}")
    await callback.answer("Готово ✅")


async def cmd_whoami(message: Message):
    role = ROLE_BY_USER_ID.get(message.from_user.id)
    if not role:
        await message.reply("⛔ Вы не зарегистрированы как сотрудник.")
        return

    await message.reply(
        f"👤 {USERS[role]['name']}\n"
        f"🏷 Роль: {ROLE_NAMES.get(role, role)}\n"
        f"🆔 {message.from_user.id}"
    )


# ── Пауза чата ───────────────────────────────────────────────────────────────

@router.message(Command(commands=["pause", "пауза"]))
async def cmd_pause(message: Message):
    if not is_employee(message.from_user.id):
        await message.reply("⛔ Команда доступна только сотрудникам.")
        return
    if message.chat.type not in ("group", "supergroup"):
        await message.reply("⛔ Только в групповых чатах.")
        return

    chat_id = message.chat.id
    user_id = message.from_user.id

    conn = get_conn(DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_pause_state(
                    chat_id, is_paused, paused_at, paused_by, updated_at
                )
                VALUES (%s, TRUE, NOW() AT TIME ZONE 'utc', %s, NOW() AT TIME ZONE 'utc')
                ON CONFLICT (chat_id) DO UPDATE
                SET is_paused = TRUE,
                    paused_at = NOW() AT TIME ZONE 'utc',
                    paused_by = EXCLUDED.paused_by,
                    resumed_at = NULL,
                    pause_reminded_at = NULL,
                    updated_at = NOW() AT TIME ZONE 'utc'
                """,
                (chat_id, user_id),
            )
        conn.commit()
    finally:
        conn.close()

    resume_date = datetime.now(timezone.utc) + timedelta(days=7)
    await message.reply(
        f"⏸ Чат поставлен на паузу. Контроль тишины отключён.\n"
        f"Автоматическое снятие: {format_msk(resume_date)}\n"
        f"Снять вручную: /снятьпаузу"
    )


@router.message(Command(commands=["resume", "снятьпаузу"]))
async def cmd_resume(message: Message):
    if not is_employee(message.from_user.id):
        await message.reply("⛔ Команда доступна только сотрудникам.")
        return
    if message.chat.type not in ("group", "supergroup"):
        await message.reply("⛔ Только в групповых чатах.")
        return

    chat_id = message.chat.id

    conn = get_conn(DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_pause_state(
                    chat_id, is_paused, resumed_at, updated_at
                )
                VALUES (%s, FALSE, NOW() AT TIME ZONE 'utc', NOW() AT TIME ZONE 'utc')
                ON CONFLICT (chat_id) DO UPDATE
                SET is_paused = FALSE,
                    resumed_at = NOW() AT TIME ZONE 'utc',
                    updated_at = NOW() AT TIME ZONE 'utc'
                """,
                (chat_id,),
            )
        conn.commit()
    finally:
        conn.close()

    await message.reply("▶️ Пауза снята. SLA-контроль возобновлён.")


def _get_chat_pause(conn, chat_id: int) -> dict | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM chat_pause_state WHERE chat_id=%s", (chat_id,))
        return cur.fetchone()


# ── Promise detection ─────────────────────────────────────────────────────────

PROMISE_KEYWORDS = [
    # уточнение
    "уточню",
    "уточняю",
    "выясню",
    "узнаю",
    "спрошу",
    "проверю",
    # ответ
    "отвечу",
    "дам ответ",
    "дам обратную связь",
    "позже отвечу",
    "отвечу позже",
    "чуть позже отвечу",
    "скоро отвечу",
    "отвечу скоро",
    # написать
    "напишу",
    "напишем",
    "отпишусь",
    "отпишу",
    "позже напишу",
    # вернуться
    "вернусь",
    "перезвоню",
    "свяжусь",
    # прислать
    "пришлю",
    "пришлю вам",
    "скину",
    "скину вам",
    "отправлю",
    "пришлем",
    # сообщить
    "сообщу",
    "сообщу вам",
    "скажу",
    "скажу вам",
    # ориентировать
    "сориентирую",
    "сориентируюсь",
    # знать
    "дам знать",
    "дам вам знать",
    "дам тебе знать",
    # разобраться
    "разберусь",
    "займусь",
    # согласование
    "согласую",
    "передам",
    "подготовлю",
]

# ожидание срока после обещания без срока
PENDING_DUE: dict = {}
PENDING_DUE_TTL = timedelta(minutes=10)


def detect_promise(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in PROMISE_KEYWORDS)


def _close_last_promise(db, chat_id: int, user_id: int, current_message_id: int):
    """
    Закрывает открытое обещание сотрудника если новое обещание заменяет старое (2.5).
    """
    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE promises
            SET is_done = TRUE,
                done_at = NOW() AT TIME ZONE 'utc'
            WHERE id = (
                SELECT id FROM promises
                WHERE chat_id = %s
                  AND user_id = %s
                  AND is_done = FALSE
                  AND message_id < %s
                ORDER BY message_id DESC
                LIMIT 1
            )
            """,
            (chat_id, user_id, current_message_id),
        )


def _close_promise_by_reply(db, chat_id: int, user_id: int, reply_to_message_id: int) -> bool:
    """
    Закрывает обещание когда сотрудник отвечает (reply) именно на своё
    сообщение-обещание. Возвращает True если обещание было найдено и закрыто.
    """
    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE promises
            SET is_done = TRUE,
                done_at = NOW() AT TIME ZONE 'utc'
            WHERE chat_id = %s
              AND user_id = %s
              AND message_id = %s
              AND is_done = FALSE
            """,
            (chat_id, user_id, reply_to_message_id),
        )
        return cur.rowcount > 0


# ── Закрытие SLA по реакции сотрудника ───────────────────────────────────────

@router.message_reaction()
async def on_message_reaction(event: MessageReactionUpdated, db):
    """
    Сотрудник поставил реакцию на сообщение → SLA закрывается (1.7).
    """
    user_id = event.user.id if event.user else None
    if not user_id or not is_employee(user_id):
        return

    chat_id = event.chat.id

    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE sla_state_v1
            SET answered_at = NOW() AT TIME ZONE 'utc',
                answered_by_user_id = %s,
                ping1_at = NULL,
                ping2_at = NULL,
                ping3_at = NULL,
                ping4_at = NULL,
                updated_at = NOW() AT TIME ZONE 'utc'
            WHERE chat_id = %s
            """,
            (user_id, chat_id),
        )
    db.commit()


@router.message()
async def any_message_handler(message: Message, db):

    due_at_utc = None
    needs_due_at = False

    chat_id = message.chat.id
    chat_type = message.chat.type

    title = None
    if chat_type in ("group", "supergroup"):
        title = message.chat.title

    user_id = message.from_user.id
    username = message.from_user.username
    full_name = message.from_user.full_name

    text = (message.text or message.caption or "").strip()
    direction = "out" if is_employee(user_id) else "in"

    # Игнор-фразы клиента: полностью пропускаем, не пишем в БД (1.3)
    if direction == "in" and is_client_ignored_phrase(text):
        return

    has_q = detect_has_question(text)
    has_neg = detect_has_neg(text)

    print("SEEN MESSAGE:", chat_id, user_id, text)

    # 1) ensure chat exists
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO chats(chat_id, chat_type, title)
            VALUES (%s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE
            SET chat_type = EXCLUDED.chat_type,
                title = COALESCE(EXCLUDED.title, chats.title),
                updated_at = now()
            """,
            (chat_id, chat_type, title),
        )

    # 2) ensure user exists
    role = ROLE_BY_USER_ID.get(user_id)
    role_text = ROLE_NAMES.get(role) if role else None

    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users(user_id, username, full_name, role)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET username = EXCLUDED.username,
                full_name = EXCLUDED.full_name,
                role = COALESCE(EXCLUDED.role, users.role),
                updated_at = now()
            """,
            (user_id, username, full_name, role_text),
        )

    # 3) insert message (всегда пишем в messages)
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO messages(chat_id, user_id, message_id, direction, text, has_question, has_neg)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (chat_id, user_id, message.message_id, direction, text, has_q, has_neg),
        )

    db.commit()

    # ── SLA: ответ сотрудника закрывает SLA (1.1) ────────────────────────────
    if direction == "out" and chat_type in ("group", "supergroup"):
        with db.cursor() as cur:
            cur.execute(
                """
                UPDATE sla_state_v1
                SET answered_at = NOW() AT TIME ZONE 'utc',
                    answered_by_user_id = %s,
                    ping1_at = NULL,
                    ping2_at = NULL,
                    ping3_at = NULL,
                    ping4_at = NULL,
                    updated_at = NOW() AT TIME ZONE 'utc'
                WHERE chat_id = %s
                """,
                (user_id, chat_id),
            )
        db.commit()

    # ── Контроль обещаний ─────────────────────────────────────────────────────

    # 0) Ранее попросили срок — сотрудник прислал его отдельным сообщением
    if direction == "out" and user_id in PENDING_DUE:
        pending = PENDING_DUE.get(user_id)
        asked_at = pending.get("asked_at_utc") if pending else None

        if asked_at and (datetime.now(timezone.utc) - asked_at) <= PENDING_DUE_TTL:
            due_at2, needs2 = parse_due_at(text)

            if not needs2 and due_at2 is not None:
                # Закрываем старое обещание (2.5)
                _close_last_promise(db, pending["chat_id"], user_id, pending["message_id"])
                with db.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO promises(
                            chat_id, user_id, message_id,
                            promise_text, due_at, needs_due_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            pending["chat_id"],
                            user_id,
                            pending["message_id"],
                            pending["promise_text"],
                            due_at2,
                            False,
                        ),
                    )
                db.commit()

                await message.reply(
                    f"✅ Принял срок: до {format_msk(due_at2)}. Обещание зафиксировано."
                )

                PENDING_DUE.pop(user_id, None)
                return

    is_new_promise = direction == "out" and detect_promise(text)

    # 1) Закрытие обещания через reply на своё сообщение-обещание
    if direction == "out" and not is_new_promise and message.reply_to_message:
        replied_msg_id = message.reply_to_message.message_id
        closed = _close_promise_by_reply(db, chat_id, user_id, replied_msg_id)
        if closed:
            db.commit()
            await message.reply("✅ Обещание выполнено и закрыто.")

    # 2) Детект нового обещания
    if is_new_promise:
        msg_id = message.message_id

        if _has_time_context(text):
            due_at_utc, needs_due_at = parse_due_at(text)
        else:
            due_at_utc, needs_due_at = None, True

        if needs_due_at:
            mention = f'<a href="tg://user?id={user_id}">{full_name}</a>'
            await message.reply(
                f"🧩 {mention}, ты написал «{text[:60]}», но не указал срок.\n"
                f"Напиши, пожалуйста, срок одним сообщением.\n"
                f"Пример: «до 13:00» или «завтра до 12:00».",
                parse_mode=ParseMode.HTML,
            )

            PENDING_DUE[user_id] = {
                "chat_id": chat_id,
                "message_id": msg_id,
                "promise_text": text,
                "asked_at_utc": datetime.now(timezone.utc),
            }

        else:
            # Закрываем старое открытое обещание (2.5)
            _close_last_promise(db, chat_id, user_id, msg_id)
            with db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO promises(
                        chat_id, user_id, message_id,
                        promise_text, due_at, needs_due_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (chat_id, user_id, msg_id, text, due_at_utc, False),
                )
            db.commit()

            await message.reply(
                f"✅ Обещание зафиксировано. Срок: до {format_msk(due_at_utc)}."
            )


# =========================
# 3) Middleware DB
# =========================

class DbMiddleware(BaseMiddleware):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    async def __call__(self, handler, event, data):
        data["db"] = self.conn
        return await handler(event, data)


# =========================
# 4) SLA watcher
# =========================

from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError


async def safe_send(bot: Bot, user_id: int, text: str, parse_mode: str | None = None) -> bool:
    try:
        await bot.send_message(user_id, text, parse_mode=parse_mode)
        return True
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        print(f"[SEND] FAIL user_id={user_id} forbidden/badrequest: {repr(e)}")
        return False
    except TelegramNetworkError as e:
        print(f"[SEND] FAIL user_id={user_id} network: {repr(e)}")
        return False
    except Exception as e:
        print(f"[SEND] FAIL user_id={user_id} other: {repr(e)}")
        return False


def in_work_hours_msk(dt_utc: datetime) -> bool:
    dt_msk = dt_utc.astimezone(MSK_TZ)
    start = dt_msk.replace(
        hour=WORKDAY_START_HOUR,
        minute=WORKDAY_START_MIN,
        second=0,
        microsecond=0,
    )
    end = dt_msk.replace(
        hour=WORKDAY_END_HOUR,
        minute=WORKDAY_END_MIN,
        second=0,
        microsecond=0,
    )
    return start <= dt_msk <= end


def make_chat_link(chat_id: int, message_id: int | None) -> str:
    cid = str(abs(chat_id))
    if cid.startswith("100"):
        cid = cid[3:]
    if message_id:
        return f"https://t.me/c/{cid}/{message_id}"
    return f"https://t.me/c/{cid}"


async def get_chat_title_safe(bot, chat_id: int, cache: dict[int, str] | None = None) -> str:
    if cache is not None and chat_id in cache:
        return cache[chat_id]

    title = str(chat_id)
    try:
        tg_chat = await bot.get_chat(chat_id)
        title = tg_chat.title or getattr(tg_chat, "full_name", None) or str(chat_id)
    except Exception as e:
        print("[SLA] get_chat title error:", repr(e))

    if cache is not None:
        cache[chat_id] = title
    return title


def escalation_chain_by_stage(stage: str) -> list[int]:
    """
    Статическая цепочка для silence watcher.
    """
    stage = (stage or "prebuild").lower()
    if stage == "build":
        roles = ["foreman", "site_manager", "construction_director", "owner"]
    elif stage == "warranty":
        roles = ["site_manager", "construction_director", "owner"]
    else:
        roles = ["manager", "owner"]
    return [USERS[r]["user_id"] for r in roles]


async def escalation_chain_dynamic(bot: Bot, conn, chat_id: int, stage: str) -> list[int]:
    """
    Динамическая цепочка эскалации для SLA (1.5).
    build: прораб в чате? → [foreman, construction_director, owner]
           иначе          → [site_manager, construction_director, owner]
    warranty:              → [site_manager, construction_director, owner]
    prebuild:              → [manager, owner]
    """
    st = (stage or "prebuild").lower()

    if st == "prebuild":
        return [USERS["manager"]["user_id"], USERS["owner"]["user_id"]]

    if st == "warranty":
        return [
            USERS["site_manager"]["user_id"],
            USERS["construction_director"]["user_id"],
            USERS["owner"]["user_id"],
        ]

    # build — проверяем прораба
    foreman_id = USERS["foreman"]["user_id"]
    foreman_in_chat = False

    # Проверка 1: участник группы
    try:
        member = await bot.get_chat_member(chat_id, foreman_id)
        if member.status in ("member", "administrator", "creator"):
            foreman_in_chat = True
    except Exception:
        pass

    # Проверка 2: когда-либо писал в этот чат
    if not foreman_in_chat:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM messages WHERE chat_id=%s AND user_id=%s LIMIT 1",
                    (chat_id, foreman_id),
                )
                if cur.fetchone():
                    foreman_in_chat = True
        except Exception:
            pass

    if foreman_in_chat:
        return [
            foreman_id,
            USERS["site_manager"]["user_id"],
            USERS["construction_director"]["user_id"],
            USERS["owner"]["user_id"],
        ]
    else:
        return [
            USERS["site_manager"]["user_id"],
            USERS["construction_director"]["user_id"],
            USERS["owner"]["user_id"],
        ]


import html


def stage_to_ru(stage: str) -> str:
    return {
        "prebuild": "До стройки",
        "build": "Стройка",
        "warranty": "Гарантия",
    }.get(stage or "", stage or "—")


def build_sla_message(chat_title: str, stage: str, level: int, level_total: int, last_in_at, last_in_text: str) -> str:
    chat_title = html.escape(chat_title or "—")
    stage_ru = html.escape(stage_to_ru(stage))
    text_preview = html.escape((last_in_text or "").strip())

    if level == 1:
        badge = "⏱️"
        tail = "👉 Пожалуйста, ответьте в чат — клиент ждёт."
    elif level == 2:
        badge = "⚠️"
        tail = "👉 Нужен короткий ответ в чат, чтобы удержать клиента."
    else:
        badge = "🚨"
        tail = "👉 Важно ответить сейчас, чтобы не ушло в негатив."

    return (
        f"{badge} <b>Клиент ждёт ответа</b>\n"
        f"<b>Чат:</b> <i>{chat_title}</i>\n"
        f"<b>Этап:</b> {stage_ru}\n"
        f"<b>Уровень:</b> {level}/{level_total}\n"
        f"<b>Последнее сообщение клиента:</b> {format_msk(last_in_at)} (МСК)\n"
        f"<b>Текст:</b> «{text_preview[:300]}»\n\n"
        f"{tail}"
    )


def _to_utc(dt):
    """Привести datetime к aware UTC. None → None."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dt_equal(a, b, tol_seconds: int = 1) -> bool:
    """Сравнение datetime с допуском (защита от round-trip через TIMESTAMP без tz)."""
    a = _to_utc(a)
    b = _to_utc(b)
    if a is None or b is None:
        return a is b
    return abs((a - b).total_seconds()) <= tol_seconds


def _silence_state_get(conn, chat_id: int):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM silence_state_v1 WHERE chat_id=%s", (chat_id,))
        return cur.fetchone()


def _silence_state_reset(conn, chat_id: int, last_out_at):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO silence_state_v1 (chat_id, last_out_at, ping1_at, ping2_at, ping3_at, ping4_at)
            VALUES (%s, %s, NULL, NULL, NULL, NULL)
            ON CONFLICT (chat_id) DO UPDATE SET
              last_out_at = EXCLUDED.last_out_at,
              ping1_at = NULL, ping2_at = NULL, ping3_at = NULL, ping4_at = NULL
        """, (chat_id, last_out_at))
    conn.commit()


def _silence_state_set_ping(conn, chat_id: int, level: int):
    col = {1: "ping1_at", 2: "ping2_at", 3: "ping3_at", 4: "ping4_at"}[level]
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE silence_state_v1 SET {col} = NOW() AT TIME ZONE 'utc' WHERE chat_id=%s",
            (chat_id,)
        )
    conn.commit()


async def sla_watcher_v1(bot: Bot, conn):
    print("[SLA] watcher started")
    chat_title_cache: dict[int, str] = {}

    while True:
        print("[SLA] tick")
        try:
            now = datetime.now(timezone.utc)
            if not in_work_hours_msk(now):
                await asyncio.sleep(60)
                continue
            print(f"[SLA] tick {now.isoformat()}")

            with conn.cursor() as cur:
                # Последнее входящее по каждому чату
                cur.execute(
                    """
                    SELECT m.chat_id, m.created_at, m.message_id, m.text
                    FROM messages m
                    INNER JOIN (
                        SELECT chat_id, MAX(created_at) AS max_created_at
                        FROM messages
                        WHERE direction = 'in'
                        GROUP BY chat_id
                    ) t ON t.chat_id = m.chat_id AND t.max_created_at = m.created_at
                    WHERE m.direction = 'in'
                    """
                )
                rows = cur.fetchall()

            for r in rows:
                chat_id = r["chat_id"]
                last_in_at = r["created_at"]
                last_in_msg_id = r["message_id"]
                last_in_text = (r.get("text") or "").strip()

                # Игнор-фразы клиента — SLA не тревожим (1.3)
                if is_client_ignored_phrase(last_in_text):
                    continue

                # Пауза чата — SLA отключён (3.2)
                pause = _get_chat_pause(conn, chat_id)
                if pause and pause.get("is_paused"):
                    continue

                # Стадия чата
                stage = "prebuild"
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT stage FROM chat_stage WHERE chat_id=%s",
                        (chat_id,),
                    )
                    s = cur.fetchone()

                if s:
                    stage = (s.get("stage") if isinstance(s, dict) else s[0]) or "prebuild"

                # Динамическая цепочка (1.5)
                chain = await escalation_chain_dynamic(bot, conn, chat_id, stage)

                # Читаем или создаём состояние SLA
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT last_incoming_at,
                               last_incoming_message_id,
                               ping1_at,
                               ping2_at,
                               ping3_at,
                               ping4_at,
                               answered_at
                        FROM sla_state_v1
                        WHERE chat_id=%s
                        """,
                        (chat_id,),
                    )
                    st = cur.fetchone()

                if st is None:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO sla_state_v1(chat_id, last_incoming_at, last_incoming_message_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (chat_id) DO NOTHING
                            """,
                            (chat_id, last_in_at, last_in_msg_id),
                        )
                    conn.commit()
                    saved_last_in_at = last_in_at
                    saved_last_in_msg_id = last_in_msg_id
                    ping1_at = ping2_at = ping3_at = ping4_at = None
                    answered_at = None
                else:
                    if isinstance(st, dict):
                        saved_last_in_at = st.get("last_incoming_at")
                        saved_last_in_msg_id = st.get("last_incoming_message_id")
                        ping1_at = st.get("ping1_at")
                        ping2_at = st.get("ping2_at")
                        ping3_at = st.get("ping3_at")
                        ping4_at = st.get("ping4_at")
                        answered_at = st.get("answered_at")
                    else:
                        saved_last_in_at, saved_last_in_msg_id, ping1_at, ping2_at, ping3_at, ping4_at = st[:6]
                        answered_at = st[6] if len(st) > 6 else None

                # Нормализуем строки → datetime
                if isinstance(saved_last_in_at, str):
                    saved_last_in_at = datetime.fromisoformat(saved_last_in_at.replace("Z", "+00:00"))
                if isinstance(ping1_at, str):
                    ping1_at = datetime.fromisoformat(ping1_at.replace("Z", "+00:00"))
                if isinstance(ping2_at, str):
                    ping2_at = datetime.fromisoformat(ping2_at.replace("Z", "+00:00"))
                if isinstance(ping3_at, str):
                    ping3_at = datetime.fromisoformat(ping3_at.replace("Z", "+00:00"))
                if isinstance(ping4_at, str):
                    ping4_at = datetime.fromisoformat(ping4_at.replace("Z", "+00:00"))
                if isinstance(answered_at, str):
                    answered_at = datetime.fromisoformat(answered_at.replace("Z", "+00:00"))

                # Если уже ответили — SLA закрыт (1.1)
                if answered_at and saved_last_in_at and answered_at >= saved_last_in_at:
                    continue

                # Новое входящее — сбрасываем пинги (1.4)
                if saved_last_in_at != last_in_at or saved_last_in_msg_id != last_in_msg_id:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE sla_state_v1
                            SET last_incoming_at=%s,
                                last_incoming_message_id=%s,
                                ping1_at=NULL, ping1_user_id=NULL,
                                ping2_at=NULL, ping2_user_id=NULL,
                                ping3_at=NULL, ping3_user_id=NULL,
                                ping4_at=NULL, ping4_user_id=NULL,
                                answered_at=NULL, answered_by_user_id=NULL,
                                updated_at=now()
                           WHERE chat_id=%s
                           """,
                           (last_in_at, last_in_msg_id, chat_id),
                        )
                    conn.commit()
                    ping1_at = ping2_at = ping3_at = ping4_at = None
                    answered_at = None

                policy = sla_policy_by_stage(stage)
                wait1 = policy["wait1"]
                wait2 = policy["wait2"]
                wait3 = policy["wait3"]
                esc_total = len(chain)
                stage_ru = stage_to_ru(stage)
                chat_title = await get_chat_title_safe(bot, chat_id, chat_title_cache)

                async def send_with_member_check(target_id: int, text: str, cid: int = chat_id) -> bool:
                    """Проверяет наличие пользователя в чате, затем отправляет (1.6)."""
                    try:
                        member = await bot.get_chat_member(cid, target_id)
                        if member.status not in ("member", "administrator", "creator"):
                            print(f"[SLA] skip: user {target_id} not in chat {cid}")
                            return False
                    except Exception as e:
                        print(f"[SLA] get_chat_member error {target_id}: {repr(e)}")
                        return False
                    return await safe_send(bot, target_id, text, parse_mode="HTML")

                # ping1
                if ping1_at is None and (now - last_in_at) >= wait1:
                    text = (
                        f"⏱ Заказчик ждёт ответа более {int(wait1.total_seconds() // 60)} минут\n\n"
                        f"Чат: {chat_title}\n"
                        f"Этап: {stage_ru}\n"
                        f"Уровень: 1/{esc_total}\n"
                        f"Время сообщения Заказчика: {format_msk(last_in_at)}\n"
                        f"Текст: {last_in_text[:300]}\n"
                        f"👉 Пожалуйста, ответь в чате"
                    )
                    target = chain[0]
                    ok = await send_with_member_check(target, text)
                    if not ok and len(chain) > 1:
                        target = chain[1]
                        ok = await safe_send(bot, target, text, parse_mode="HTML")
                    if ok:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE sla_state_v1 SET ping1_at=%s, ping1_user_id=%s WHERE chat_id=%s",
                                (now, target, chat_id),
                            )
                        conn.commit()
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO sla_ping_events(chat_id, stage, level, target_user_id, last_in_at, ping_at)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """,
                                (chat_id, stage or "", 1, target, last_in_at, now),
                            )
                        conn.commit()

                # ping2
                if ping1_at is not None and ping2_at is None and (now - ping1_at) >= wait2:
                    if len(chain) >= 2:
                        text = (
                            f"⚠️ Заказчик ждёт ответа\n\n"
                            f"Чат: {chat_title}\n"
                            f"Этап: {stage_ru}\n"
                            f"Уровень: 2/{esc_total}\n"
                            f"Время сообщения Заказчика: {format_msk(last_in_at)}\n"
                            f"Текст: {last_in_text[:300]}\n"
                            f"👉 Пожалуйста, ответь в чате."
                        )
                        target = chain[1]
                        ok = await send_with_member_check(target, text)
                        if not ok and len(chain) > 2:
                            target = chain[2]
                            ok = await safe_send(bot, target, text, parse_mode="HTML")
                        if ok:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE sla_state_v1 SET ping2_at=%s, ping2_user_id=%s WHERE chat_id=%s",
                                    (now, target, chat_id),
                                )
                            conn.commit()
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    INSERT INTO sla_ping_events(chat_id, stage, level, target_user_id, last_in_at, ping_at)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    """,
                                    (chat_id, stage or "", 2, target, last_in_at, now),
                                )
                            conn.commit()

                # ping3
                if wait3 is not None and ping2_at is not None and ping3_at is None and (now - ping2_at) >= wait3:
                    if len(chain) >= 3:
                        text = (
                            f"🚨 Заказчик ждёт ответа\n\n"
                            f"Чат: {chat_title}\n"
                            f"Этап: {stage_ru}\n"
                            f"Уровень: 3/{esc_total}\n"
                            f"Время сообщения Заказчика: {format_msk(last_in_at)}\n"
                            f"Текст: {last_in_text[:300]}\n"
                            f"👉 Пожалуйста, подключитесь и дайте ответ в чате."
                        )
                        target = chain[2]
                        ok = await send_with_member_check(target, text)
                        if ok:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE sla_state_v1 SET ping3_at=%s, ping3_user_id=%s WHERE chat_id=%s",
                                    (now, target, chat_id),
                                )
                            conn.commit()
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    INSERT INTO sla_ping_events(chat_id, stage, level, target_user_id, last_in_at, ping_at)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    """,
                                    (chat_id, stage or "", 3, target, last_in_at, now),
                                )
                            conn.commit()

                # ping4
                wait4 = policy.get("wait4")
                if wait4 and ping3_at is not None and ping4_at is None and (now - ping3_at) >= wait4:
                    if len(chain) >= 4:
                        text = (
                            f"🚨 Финальная эскалация\n\n"
                            f"Чат: {chat_title}\n"
                            f"Этап: {stage_ru}\n"
                            f"Уровень: 4/{esc_total}\n"
                            f"Время сообщения Заказчика: {format_msk(last_in_at)}\n"
                            f"Текст: {last_in_text[:300]}\n"
                            f"👉 Пожалуйста, подключитесь и дайте ответ в чате."
                        )
                        target = chain[3]
                        ok = await send_with_member_check(target, text)
                        if ok:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE sla_state_v1 SET ping4_at=%s, ping4_user_id=%s WHERE chat_id=%s",
                                    (now, target, chat_id),
                                )
                            conn.commit()
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    INSERT INTO sla_ping_events(chat_id, stage, level, target_user_id, last_in_at, ping_at)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    """,
                                    (chat_id, stage or "", 4, target, last_in_at, now),
                                )
                            conn.commit()

        except Exception as e:
            print("[SLA] watcher error:", repr(e))

        await asyncio.sleep(20)


async def silence_watcher_v1(bot: Bot, conn):
    """
    Контроль тишины.
    Без паузы: prebuild=2д, build/warranty=3д.
    С паузой: порог 7 дней, SLA-пинги не отправляются.
    """
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            print("[SILENCE] tick", now_utc.isoformat())

            # Не дёргаем сотрудников вне рабочего дня (09:00–20:00 МСК)
            if not in_work_hours_msk(now_utc):
                await asyncio.sleep(600)
                continue

            # Напоминание о паузе через 7 дней (3.3) — только уведомление, пауза НЕ снимается
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT chat_id, paused_by
                    FROM chat_pause_state
                    WHERE is_paused = TRUE
                      AND pause_reminded_at IS NULL
                      AND paused_at IS NOT NULL
                      AND NOW() AT TIME ZONE 'utc' - paused_at > INTERVAL '7 days'
                """)
                pause_reminders = cur.fetchall()

            for pr in pause_reminders:
                chat_id_p = pr["chat_id"]
                paused_by = pr.get("paused_by")
                if paused_by:
                    try:
                        await bot.send_message(
                            paused_by,
                            "⏸ Ты поставил чат на паузу — прошло 7 дней.\n"
                            "Проверь чат, может пора написать клиенту?\n"
                            "Снять паузу: /снятьпаузу"
                        )
                    except Exception:
                        pass
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE chat_pause_state SET pause_reminded_at = NOW() AT TIME ZONE 'utc' WHERE chat_id=%s",
                        (chat_id_p,),
                    )
                conn.commit()

            # Основной контроль тишины
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        c.chat_id,
                        COALESCE(cs.stage, 'prebuild') AS stage,
                        MAX(m.created_at) AS last_out_at
                    FROM chats c
                    LEFT JOIN chat_stage cs ON cs.chat_id = c.chat_id
                    LEFT JOIN messages m
                      ON m.chat_id = c.chat_id AND m.direction = 'out'
                    WHERE c.chat_id < 0
                    GROUP BY c.chat_id, cs.stage
                """)
                chats = cur.fetchall()

            for row in chats:
                chat_id = int(row["chat_id"])
                stage = (row["stage"] or "prebuild").strip().lower()
                last_out_at = row["last_out_at"]
                if not last_out_at:
                    continue

                if last_out_at.tzinfo is None:
                    last_out_at = last_out_at.replace(tzinfo=timezone.utc)
                else:
                    last_out_at = last_out_at.astimezone(timezone.utc)

                # Учитываем паузу (3.2): порог тишины меняется на 7 дней
                pause = _get_chat_pause(conn, chat_id)
                is_paused = bool(pause and pause.get("is_paused"))

                policy = silence_policy_by_stage(stage)
                threshold = timedelta(days=7) if is_paused else policy["threshold"]

                if (now_utc - last_out_at) < threshold:
                    state = _silence_state_get(conn, chat_id)
                    if state and not _dt_equal(state.get("last_out_at"), last_out_at):
                        _silence_state_reset(conn, chat_id, last_out_at)
                    continue

                chain = escalation_chain_by_stage(stage)
                if not chain:
                    continue

                state = _silence_state_get(conn, chat_id)
                if not state:
                    _silence_state_reset(conn, chat_id, last_out_at)
                    state = _silence_state_get(conn, chat_id)

                if not _dt_equal(state.get("last_out_at"), last_out_at):
                    _silence_state_reset(conn, chat_id, last_out_at)
                    state = _silence_state_get(conn, chat_id)

                ping1_at = state.get("ping1_at")
                ping2_at = state.get("ping2_at")
                ping3_at = state.get("ping3_at")
                ping4_at = state.get("ping4_at")

                try:
                    tg_chat = await bot.get_chat(chat_id)
                    chat_title = tg_chat.title or getattr(tg_chat, "full_name", None) or str(chat_id)
                except Exception as e:
                    print("[SILENCE] get_chat title error:", repr(e))
                    chat_title = str(chat_id)

                msg = (
                    "⏳ Контроль тишины\n"
                    f"Чат: {chat_title}\n"
                    f"Стадия: {stage}\n\n"
                    "Нет сообщений от компании уже длительное время.\n"
                    "Рекомендуется отправить заказчику статус/апдейт."
                )

                if ping1_at is None:
                    print("[SILENCE] ping1", chat_id, stage, "to", chain[0], "last_out_at", last_out_at)
                    await safe_send(bot, chain[0], msg)
                    _silence_state_set_ping(conn, chat_id, 1)
                    continue

                wait2 = policy.get("wait2")
                if wait2 and ping2_at is None and now_utc >= (ping1_at + wait2) and len(chain) >= 2:
                    await safe_send(bot, chain[1], msg)
                    _silence_state_set_ping(conn, chat_id, 2)
                    continue

                wait3 = policy.get("wait3")
                if wait3 and ping3_at is None and ping2_at and now_utc >= (ping2_at + wait3) and len(chain) >= 3:
                    await safe_send(bot, chain[2], msg)
                    _silence_state_set_ping(conn, chat_id, 3)
                    continue

                wait4 = policy.get("wait4")
                if wait4 and ping4_at is None and ping3_at and now_utc >= (ping3_at + wait4) and len(chain) >= 4:
                    await safe_send(bot, chain[3], msg)
                    _silence_state_set_ping(conn, chat_id, 4)
                    continue

        except Exception:
            logging.exception("silence_watcher_v1 failed")

        await asyncio.sleep(600)  # каждые 10 минут


async def promise_watcher_v1(bot: Bot, conn):
    while True:
        try:
            now_utc = datetime.now(timezone.utc)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        p.id, p.chat_id, p.user_id, p.promise_text, p.due_at,
                        p.reminded_at, p.ping5_at, p.ping20_at, p.escalated_at,
                        COALESCE(u.full_name, '') as full_name,
                        COALESCE(c.title, '') as chat_title,
                        COALESCE(c.chat_type, '') as chat_type
                    FROM promises p
                    LEFT JOIN users u ON u.user_id = p.user_id
                    LEFT JOIN chats c ON c.chat_id = p.chat_id
                    WHERE
                        p.needs_due_at = FALSE
                        AND p.is_done = FALSE
                        AND p.due_at IS NOT NULL
                        AND (
                            p.reminded_at IS NULL
                            OR p.ping5_at IS NULL
                            OR p.ping20_at IS NULL
                            OR p.escalated_at IS NULL
                        )
                        AND p.created_at > (NOW() AT TIME ZONE 'utc') - INTERVAL '30 days'
                    ORDER BY p.due_at ASC
                    LIMIT 200
                    """
                )
                rows = cur.fetchall()

            for row in rows:
                pid = row["id"]
                chat_id = row["chat_id"]
                author_id = row["user_id"]
                promise_text = row.get("promise_text") or ""
                due_at = row.get("due_at")
                reminded_at = row.get("reminded_at")
                ping5_at = row.get("ping5_at")
                ping20_at = row.get("ping20_at")
                escalated_at = row.get("escalated_at")
                full_name = row.get("full_name") or "сотрудник"
                chat_title = (row.get("chat_title") or "").strip()
                chat_type = (row.get("chat_type") or "").strip()

                if chat_title:
                    chat_display = chat_title
                elif chat_type == "private":
                    chat_display = f"ЛС: {full_name}"
                else:
                    chat_display = str(chat_id)

                if isinstance(due_at, str):
                    s = due_at.strip()
                    if s.lower() == "due_at" or s == "":
                        continue
                    try:
                        due_at = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    except ValueError:
                        continue

                if due_at is None:
                    continue

                if due_at.tzinfo is None:
                    due_at = due_at.replace(tzinfo=timezone.utc)

                due_at_utc = due_at.astimezone(timezone.utc)

                # 1) за 10 минут до срока — личка автору
                if reminded_at is None and (due_at_utc - timedelta(minutes=10)) <= now_utc < (due_at_utc + timedelta(minutes=5)):
                    try:
                        await bot.send_message(
                            author_id,
                            f"⏰ Напоминание: обещание до {format_msk(due_at_utc)}\n"
                            f"Текст: «{promise_text[:180]}»\n"
                            f"Чат: {chat_display}"
                        )
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE promises SET reminded_at = NOW() AT TIME ZONE 'utc' WHERE id=%s AND reminded_at IS NULL",
                                (pid,)
                            )
                        conn.commit()
                    except Exception:
                        logging.exception("promise reminder failed")

                # 2) +5 мин просрочки — пинг автору
                if ping5_at is None and (due_at_utc + timedelta(minutes=5)) <= now_utc < (due_at_utc + timedelta(minutes=20)):
                    try:
                        await bot.send_message(
                            author_id,
                            f"⚠ Просрочка обещания (+5 мин)\n"
                            f"Срок был: {format_msk(due_at_utc)}\n"
                            f"Текст: «{promise_text[:180]}»"
                        )
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE promises SET ping5_at = NOW() AT TIME ZONE 'utc' WHERE id=%s AND ping5_at IS NULL",
                                (pid,)
                            )
                        conn.commit()
                    except Exception:
                        logging.exception("promise +5 ping failed")

                # 3) +20 мин → директору
                if ping20_at is None and now_utc >= (due_at_utc + timedelta(minutes=20)) and now_utc < (due_at_utc + timedelta(minutes=40)):
                    try:
                        await bot.send_message(
                            OWNER_USER_ID,
                            f"🟠 Просрочка обещания (+20 мин)\n"
                            f"Автор: {full_name} ({author_id})\n"
                            f"Срок был: {format_msk(due_at_utc)}\n"
                            f"Текст: «{promise_text[:180]}»\n"
                            f"Чат: {chat_display}",
                            parse_mode=ParseMode.HTML
                        )
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE promises SET ping20_at = NOW() AT TIME ZONE 'utc' WHERE id=%s AND ping20_at IS NULL",
                                (pid,)
                            )
                        conn.commit()
                    except Exception:
                        logging.exception("promise +20 ping failed")

                # 4) +40 мин → директору (финальная эскалация)
                if escalated_at is None and now_utc >= (due_at_utc + timedelta(minutes=40)):
                    try:
                        await bot.send_message(
                            OWNER_USER_ID,
                            f"🔴 Эскалация обещания (+40 мин)\n"
                            f"Автор: {full_name} ({author_id})\n"
                            f"Срок был: {format_msk(due_at_utc)}\n"
                            f"Текст: «{promise_text[:180]}»\n"
                            f"Чат: {chat_display}",
                            parse_mode=ParseMode.HTML
                        )
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE promises SET escalated_at = NOW() AT TIME ZONE 'utc' WHERE id=%s AND escalated_at IS NULL",
                                (pid,)
                            )
                        conn.commit()
                    except Exception:
                        logging.exception("promise +40 escalation failed")

        except Exception as e:
            print("[PROMISE] watcher error:", repr(e))

        await asyncio.sleep(20)


# =========================
# 5) main
# =========================
async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    conn = get_conn(DB_DSN)
    print("[OK] DB connected.")

    dp.update.middleware(DbMiddleware(conn))
    dp.include_router(router)

    asyncio.create_task(sla_watcher_v1(bot, conn))
    asyncio.create_task(silence_watcher_v1(bot, conn))
    asyncio.create_task(promise_watcher_v1(bot, conn))

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
