import os
import asyncio
import logging
import sqlite3
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.types import Message

# =======================
# НАСТРОЙКИ БЕРЁМ ИЗ ПЕРЕМЕННЫХ СРЕДЫ (чтобы не светить токен)
# =======================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GROUP_ID_RAW = os.getenv("GROUP_ID", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN. Добавь его в Variables/Environment.")
if not GROUP_ID_RAW:
    raise RuntimeError("Не задан GROUP_ID. Добавь его в Variables/Environment.")

GROUP_ID = int(GROUP_ID_RAW)

DB_PATH = "links.sqlite"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS links (
            group_message_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def save_link(group_message_id: int, user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO links(group_message_id, user_id, created_at) VALUES (?, ?, ?)",
        (group_message_id, user_id, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()

def get_user_id_by_group_message_id(group_message_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM links WHERE group_message_id = ?", (group_message_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

def client_header(user) -> str:
    name = (user.full_name or "").strip()
    username = f"@{user.username}" if user.username else "нет"
    return (
        f"👤 <b>Клиент</b>: {name}\n"
        f"🔗 <b>Username</b>: {username}\n"
        f"🆔 <b>ID</b>: <code>{user.id}</code>\n"
        f"✍️ <i>Ответьте на это сообщение реплаем — бот отправит ответ клиенту.</i>"
    )

@dp.message(F.text == "/id")
async def cmd_id(message: Message):
    await message.reply(f"chat_id = <code>{message.chat.id}</code>")

# 1) Клиент -> Группа
@dp.message(F.chat.type == ChatType.PRIVATE)
async def from_client_to_group(message: Message):
    header = client_header(message.from_user)

    if message.text:
        sent = await bot.send_message(
            chat_id=GROUP_ID,
            text=f"{header}\n\n💬 <b>Сообщение:</b>\n{message.text}"
        )
        save_link(sent.message_id, message.from_user.id)
        return

    copied = await bot.copy_message(
        chat_id=GROUP_ID,
        from_chat_id=message.chat.id,
        message_id=message.message_id
    )
    save_link(copied.message_id, message.from_user.id)

    await bot.send_message(
        chat_id=GROUP_ID,
        text=header + "\n\n📎 <b>Клиент прислал вложение/медиа.</b>\n"
                    "↩️ <b>Ответьте реплаем НА СКОПИРОВАННОЕ вложение</b>, и бот отправит ответ клиенту."
    )

# 2) Группа -> Клиент (только реплаи)
@dp.message(F.chat.id == GROUP_ID)
async def from_group_to_client(message: Message):
    if not message.reply_to_message:
        return

    replied_id = message.reply_to_message.message_id
    user_id = get_user_id_by_group_message_id(replied_id)
    if not user_id:
        return

    if message.text:
        await bot.send_message(chat_id=user_id, text=f"💬 Ответ оператора:\n{message.text}")
        return

    await bot.copy_message(
        chat_id=user_id,
        from_chat_id=message.chat.id,
        message_id=message.message_id
    )

async def main():
    init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
