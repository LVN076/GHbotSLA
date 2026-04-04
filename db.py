import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime


def get_conn(dsn: str):
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    conn.autocommit = True
    return conn


def upsert_user(conn, user_id: int, username: str | None, full_name: str | None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (user_id, username, full_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET username = EXCLUDED.username,
                full_name = EXCLUDED.full_name
            """,
            (user_id, username, full_name),
        )


def upsert_chat(conn, chat_id: int, chat_type: str | None, title: str | None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO chats (chat_id, chat_type, title)
            VALUES (%s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE
            SET chat_type = EXCLUDED.chat_type,
                title = EXCLUDED.title
            """,
            (chat_id, chat_type, title),
        )


def insert_message(conn, chat_id: int, user_id: int, text: str | None, message_id: int,
                   direction: str, created_at):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO messages (chat_id, user_id, message_id, direction, text, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (chat_id, user_id, message_id, direction, text, created_at))


def sla_open_case(conn, chat_id: int, last_in_msg_id: int, last_in_at: datetime):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sla_cases (chat_id, last_in_msg_id, last_in_at, status)
            VALUES (%s, %s, %s, 'open')
            ON CONFLICT (chat_id) DO UPDATE
            SET last_in_msg_id = EXCLUDED.last_in_msg_id,
                last_in_at = EXCLUDED.last_in_at,
                status = 'open',
                escalated_at = NULL
            """,
            (chat_id, last_in_msg_id, last_in_at),
        )


def sla_close_case(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sla_cases
            SET status='closed'
            WHERE chat_id=%s
            """,
            (chat_id,),
        )


def sla_get_overdue_open_cases(conn, minutes: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT chat_id, last_in_msg_id, last_in_at
            FROM sla_cases
            WHERE status='open'
              AND escalated_at IS NULL
              AND now() - last_in_at > (%s || ' minutes')::interval
            ORDER BY last_in_at ASC
            """,
            (minutes,),
        )
        return cur.fetchall()


def sla_mark_escalated(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sla_cases
            SET escalated_at = now()
            WHERE chat_id=%s
            """,
            (chat_id,),
        )
