# hehe_bot_patched.py
import os
import logging
import sqlite3
import time
import uuid
import random
import re
import threading
from collections import defaultdict

from telebot import TeleBot
from telebot.types import (
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

# ---------- CONFIG ----------
OWNER_ID = 7739179390  # tg://user?id=7739179390 (доступ к отладке/админке)

BOT_TOKEN = os.environ.get("BOT_TOKEN") or "8366664373:AAEiutaqAHmQF4Tbcz0iRV1JBZD2jihY5q0"
if BOT_TOKEN == "PUT_YOUR_TOKEN_HERE":
    raise SystemExit("Set BOT_TOKEN env var (recommended) or put token in code.")

bot = TeleBot(BOT_TOKEN)
ME = bot.get_me()
BOT_USERNAME = ME.username  # without @

PREFIX_LEN = 12

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Paths ----------
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
DB_FILENAME = "hehe_bot.db"
DB = os.path.join(SCRIPT_DIR, DB_FILENAME)

TEXT_RU_FILE = os.path.join(SCRIPT_DIR, "text_ru.txt")
TEXT_EN_FILE = os.path.join(SCRIPT_DIR, "text_en.txt")
CONTEST_RU_FILE = os.path.join(SCRIPT_DIR, "contest_ru.txt")
CONTEST_EN_FILE = os.path.join(SCRIPT_DIR, "contest_en.txt")

# ---------- DB ----------
conn = sqlite3.connect(DB, check_same_thread=False)

conn.execute('PRAGMA journal_mode=WAL;')
conn.execute('PRAGMA synchronous=NORMAL;')
conn.execute('PRAGMA busy_timeout=5000;')
class _ThreadLocalCursor:
    """One sqlite cursor per thread (prevents recursive cursor use)."""
    def __init__(self, connection: sqlite3.Connection):
        self._conn = connection
        self._local = threading.local()

    def _get(self):
        c = getattr(self._local, "cur", None)
        if c is None:
            c = self._conn.cursor()
            self._local.cur = c
        return c

    def execute(self, *args, **kwargs):
        return self._get().execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._get().executemany(*args, **kwargs)

    def fetchone(self):
        return self._get().fetchone()

    def fetchall(self):
        return self._get().fetchall()

    def __getattr__(self, name):
        return getattr(self._get(), name)

cur = _ThreadLocalCursor(conn)

cur.execute("""
CREATE TABLE IF NOT EXISTS registrations (
    id TEXT PRIMARY KEY,
    group_key TEXT,
    inline_message_id TEXT,
    user_id INTEGER,
    username TEXT,
    first_name TEXT,
    ts INTEGER,
    UNIQUE(group_key, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS group_examples (
    group_key TEXT PRIMARY KEY,
    sample_inline_id TEXT,
    observed_chat_id INTEGER,
    last_seen INTEGER
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS stats (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    success_count INTEGER DEFAULT 0,
    total_count INTEGER DEFAULT 0,
    last_update INTEGER
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS stats_chat (
    group_key TEXT,
    user_id INTEGER,
    username TEXT,
    first_name TEXT,
    success_count INTEGER DEFAULT 0,
    total_count INTEGER DEFAULT 0,
    last_update INTEGER,
    PRIMARY KEY (group_key, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS attempts (
    user_id INTEGER PRIMARY KEY,
    count INTEGER DEFAULT 0,
    last_ts INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS settings (
    user_id INTEGER PRIMARY KEY,
    language TEXT DEFAULT 'ru',  -- 'ru' or 'en'
    gender TEXT DEFAULT 'm'      -- 'm' or 'f'
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS callback_logs (
    id TEXT PRIMARY KEY,
    ts INTEGER,
    clicker_id INTEGER,
    owner_id INTEGER,
    base_data TEXT,
    ok INTEGER,
    group_key TEXT,
    inline_message_id TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS bodyguards (
    group_key TEXT,
    user_id INTEGER,
    active INTEGER DEFAULT 1,
    uses_left INTEGER DEFAULT 2,
    since_ts INTEGER,
    PRIMARY KEY (group_key, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS target_stats (
    group_key TEXT,
    user_id INTEGER,
    target_key TEXT,            -- "id:123" or "username:someuser"
    target_user_id INTEGER,     -- nullable (if known)
    target_username TEXT,
    target_first_name TEXT,
    target_last_name TEXT,
    attempts INTEGER DEFAULT 0,
    successes INTEGER DEFAULT 0,
    last_seen INTEGER,
    PRIMARY KEY (group_key, user_id, target_key)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS chat_state (
    group_key TEXT,
    user_id INTEGER,
    last_target_key TEXT,        -- for explicit anti-spam
    last_target_block_rem INTEGER DEFAULT 0,  -- remaining handshake attempts to restrict repeats
    last_rand1 INTEGER DEFAULT NULL,          -- last random target uid
    last_rand2 INTEGER DEFAULT NULL,          -- prev random target uid
    PRIMARY KEY (group_key, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_state (
    group_key TEXT PRIMARY KEY,
    next_reset_ts INTEGER
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_inventory (
    group_key TEXT,
    item_key TEXT,
    qty INTEGER,
    PRIMARY KEY (group_key, item_key)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS user_effects (
    group_key TEXT,
    user_id INTEGER,
    bonus_attempts INTEGER DEFAULT 0,
    wine_state INTEGER DEFAULT 0,
    bouquet INTEGER DEFAULT 0,
    insurance INTEGER DEFAULT 0,
    candy INTEGER DEFAULT 0,
    PRIMARY KEY (group_key, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS contest_state (
    group_key TEXT,
    user_id INTEGER,
    attempts_used INTEGER DEFAULT 0,
    reset_ts INTEGER DEFAULT 0,
    stake INTEGER DEFAULT 0,
    PRIMARY KEY (group_key, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS roulette_stats (
    group_key TEXT,
    user_id INTEGER,
    win2 INTEGER DEFAULT 0,
    win3 INTEGER DEFAULT 0,
    lose INTEGER DEFAULT 0,
    games INTEGER DEFAULT 0,
    spent INTEGER DEFAULT 0,
    PRIMARY KEY (group_key, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_purchases (
    group_key TEXT,
    user_id INTEGER,
    item_key TEXT,
    qty INTEGER DEFAULT 0,
    PRIMARY KEY (group_key, user_id, item_key)
)
""")

conn.commit()

def _ensure_column(table: str, col: str, coldef: str):
    cur.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coldef}")
        conn.commit()

_ensure_column("settings", "configured", "INTEGER DEFAULT 0")
_ensure_column("settings", "setup_msg_id", "INTEGER DEFAULT NULL")
_ensure_column("settings", "setup_stage", "TEXT DEFAULT NULL")
_ensure_column("registrations", "last_name", "TEXT")
_ensure_column("registrations", "last_seen", "INTEGER DEFAULT 0")
_ensure_column("stats", "last_name", "TEXT")
_ensure_column("stats_chat", "last_name", "TEXT")
_ensure_column("stats_chat", "points", "INTEGER DEFAULT 0")
_ensure_column("attempts", "cooldown_until", "INTEGER DEFAULT 0")
_ensure_column("bodyguards", "uses_left", "INTEGER DEFAULT 2")

# ---------- UI localization (NOT from text files) ----------
UI = {
    "ru": {
        "inline_open_settings_title": "⚙️Открыть настройки",
        "inline_open_settings_desc": "Настройте свой пол и язык бота",
        "inline_open_settings_text": "⚙️Настройки профиля\n\nНажми кнопку ниже, чтобы перейти к редактированию",
        "inline_open_settings_btn": "Управление настройками бота",

        "inline_shake_friend_title": "Трахнуть рандом",
        "inline_shake_stranger_title": "Трахнуть",
        "inline_shake_desc_no_target": "Укажите @username, чтобы трахнуть конкретного человека",
        "inline_shake_desc_with_target": "Цель выбрана, действуй)",
        "inline_shake_text": "Нажми «{btn}», чтобы подтвердить действие {title}.",

        "inline_top_chat_title": "Топ чата",
        "inline_top_chat_desc": "Эти люди могут быть среди вас, или ты среди них",
        "inline_top_chat_text": "Нажми «{btn}», чтобы вывести топ чата.",

        "inline_top_global_title": "Общий топ",
        "inline_top_global_desc": "Популярные секс звезды в мировом розыске",
        "inline_top_global_text": "Нажми «{btn}», чтобы вывести общий топ.",

        "btn_confirm": "Подтвердить",
        "btn_show": "Показать",

        "no_users": "В этом чате ещё не происходило насильственных актов совокупления, исправляйте",
        "top_header_chat": "Топ секс-маньяков чата\n№|Имя|Успех|Всего\n",
        "top_header_global": "Общий топ секс-маньяков\n№|Имя|Успех|Всего\n",

        "limit_reached_text": "Вы уже израсходовали свой лимит.\nСледующая возможность через {time}.",

        "setup_title": "Настройка профиля",
        "setup_choose_lang": "Выбери язык:",
        "setup_choose_gender": "Выбери пол:",
        "setup_done": "Готово ✅\nИспользуй /settings, чтобы открыть меню настроек.",
        "lang_ru": "Русский 🇷🇺",
        "lang_en": "English 🇬🇧",
        "gender_m": "Мужской",
        "gender_f": "Женский",

        "settings_title": "⚙️ Твои настройки:",
        "settings_lang": "Язык",
        "settings_gender": "Пол",
        "settings_btn_lang": "Язык: {val}",
        "settings_btn_gender": "Пол: {val}",
        "settings_lang_ru": "🇷🇺",
        "settings_lang_en": "🇬🇧",
        "settings_gender_m": "♂",
        "settings_gender_f": "♀",

        "start_hint": f"Приветствую тебя в Sex Bot 🫦!\nБот-шутка для отыгрыша интимных сцен, и просто ловли кринжа)\nИспользуй инлайн в чате: @{BOT_USERNAME} и выбери действие.\n\n❗Внимание! Бот не предназначен для использования лицами, не достигшими совершеннолетия!\n🔞",
        "not_your_button": "Не нажимай на чужие кнопки, маленький извращенец))",

        "inline_stats_title": "Статистика",
        "inline_stats_desc": "Отчёт о твоих грязных делах (не читай при маме)",
        "inline_stats_text": "Статистика\n\nНажми «{btn}», чтобы увидеть детали.",
        
        "inline_guard_title": "Нанять телохранителя",
        "inline_guard_desc": "Защити свою жопу от таких же, как и ты",
        "inline_guard_text": "🛡Телохранитель\n\nНажми «{btn}», чтобы защитить себя 2 раза за 600ꙓ.\nᵀʰᵉ ᶜᵒᵐᵖᵃⁿʸ ᵖʳᵒᵛⁱᵈᵉˢ ᵒⁿˡʸ ᵃ ᵇᵒᵈʸᵍᵘᵃʳᵈ ʰⁱʳⁱⁿᵍ ˢᵉʳᵛⁱᶜᵉ ᵃⁿᵈ ⁱˢ ⁿᵒᵗ ʳᵉˢᵖᵒⁿˢⁱᵇˡᵉ ᶠᵒʳ ʸᵒᵘʳ ˢᵃᶠᵉᵗʸ; ᵃⁿʸ ᵃˢˢᵃᵘˡᵗ ᵃᵍᵃⁱⁿˢᵗ ʸᵒᵘ ⁱˢ ⁿᵒᵗ ᶜᵒⁿˢⁱᵈᵉʳᵉᵈ ᵃⁿ ⁱⁿˢᵘʳᵃⁿᶜᵉ ᶜᵃˢᵉ ᵃⁿᵈ ⁿᵒ ᵐᵒⁿᵉʸ ʷⁱˡˡ ᵇᵉ ᶜᵒˡˡᵉᶜᵗᵉᵈ ᶠʳᵒᵐ ᵗʰᵉ ᶜᵒᵐᵖᵃⁿʸ·",
        
        "btn_hire": "Нанять",
        "btn_pay_repeat": "Потратить 100ꙓ",
        "btn_cancel": "Отмена",
        
        "stats_header": "Твоя статистика\n📄Отчёт по изнасилованиям в чате",
        "stats_success": "Удачные",
        "stats_fail": "Неудачные",
        "stats_total": "Всего",
        "stats_pct": "Процент удачи",
        "stats_points": "Секс-коины",
        "stats_top_targets": "Топ по целям",
        
        "points_gain": "<b>{n}</b>ꙓ за успешное изнасилование жертвы",
        "points_bonus": "Бонус за красивые глаза: <b>{n}</b>ꙓ",
        
        "guard_hired_ok": "Телохранитель нанят ✅\n  <b>-600</b>ꙓ на 2 раза",
        "guard_hired_already": "У тебя уже есть телохранитель в этом чате, твоя жопа под защитой. Пока что.",
        "not_enough_points": "Недостаточно секс-коинов.\nНужно: <b>{need}</b>ꙓ\nУ тебя: <b>{have}</b>ꙓ",
        
        "repeat_target_block": "Ой-ёй, вокруг этой жертвы ошиваются полицейские. Судя по их разговорам, они будут находится рядом с твоей целью ещё <b>{left}</b> обхода..\nНайди другую жертву @username или заплати копам <b>100</b>ꙓ.",
        
        "inline_shop_title": "Чёрный рынок",
        "inline_shop_desc": "Упрости свою жизнь этими товарами, не оферта",
        "inline_shop_text": "Скромный магазин :)\n\nНажми «{btn}», чтобы открыть каталог товаров.",
        
        "inline_contest_title": "🎲 Казино",
        "inline_contest_desc": "Ставки, шлюхи, три топора - проверь свою удачу. Напиши свою ставку.",
        "inline_contest_text": "🎲 Казино \n\nНажми «{btn}», чтобы открыть двери в мир депа.",
        
        "shop_header": "Чёрный рынок\nТовары на любой вкус и размер. Практически легально (нет)!",
        "shop_choose": "Выбери товар:",
        "shop_balance": "Баланс: <b>{pts}</b>ꙓ",
        "shop_reset_in": "Обновление ассортимента через {time}\nКолличество товара ограничено! Успей купить, пока это не сделал кто-то другой!",
        "shop_item_info": "<b>{name}</b>\nЦена: <b>{price}</b>\nДоступно: <b>{qty}</b>\n\n{desc}",
        "shop_bought": "Покупка прошла успешно ✅\n−<b>{price}ꙓ</b>\n\nЖдём вас снова по новому адресу: ⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜)",
        "shop_sold_out": "Товар закончился.",
        "btn_buy": "Купить",
        "btn_back": "⬅ Назад",
        
        "contest_need_number": "Перед тем, как зайти в зал, введи свою ставку после @{bot}",
        "contest_not_enough": "У вас не достаточно секс коинов.\nВы ввели: <b>{need}ꙓ</b>. Ваш балланс: <b>{have}ꙓ</b>\n\nВозвращайся,когда будешь чуть, ммм, по-богаче",
        "contest_header": "🎲 Главный зал казино\nМесто, где решается твоя судьба. Либо ты выйдешь миллиардером с личными шлюхами, либо будешь до конца дней работать на трассе.",
        "contest_stake": "Ставка: <b>{stake}</b>",
        "contest_attempts": "Попыток: <b>{left}</b>/5",
        "contest_warning": "<b><u>Участие в азартных играх - плохо, не поощряется разработчиком, используется только для учебной демонстрации, разрешено только лицам, достигшим совершеннолетия, нажимая на кнопку ВЫ подтверждаете, что осведомлены о предупреждении и несёте полную ответственность за свои действия!</u></b>",
        "contest_limit": "Лимит ставок исчерпан.\nВозвращайтесь через {time}.",
        "contest_win": "Вы выиграли <b>{gain}ꙓ</b>! (x{mult})",
        "contest_lose": "Неудача. Вы потеряли <b>{stake}ꙓ</b>",
        "contest_btn_roulette": "🎰",
        "contest_btn_play_again": "Додеп"
    },
    "en": {
        "inline_open_settings_title": "⚙️Open settings",
        "inline_open_settings_desc": "Set your bot's gender and language",
        "inline_open_settings_text": "⚙️ Bot settings\n\nClick the button below to start editing.",
        "inline_open_settings_btn": "Bot settings management",

        "inline_shake_friend_title": "Sex with random",
        "inline_shake_stranger_title": "Sex",
        "inline_shake_desc_no_target": "Specify @username to fuck a particular person",
        "inline_shake_desc_with_target": "The target is chosen, act now)",
        "inline_shake_text": "Click “{btn}” to confirm the {title} action.",

        "inline_top_chat_title": "Chat top",
        "inline_top_chat_desc": "These people might be among you, or you might be among them",
        "inline_top_chat_text": "Click “{btn}” to display the chat leaderboard.",

        "inline_top_global_title": "Global top",
        "inline_top_global_desc": "Popular sex stars on the international wanted list",
        "inline_top_global_text": "Click “{btn}” to show the overall leaderboard.",

        "btn_confirm": "Confirm",
        "btn_show": "Show",

        "no_users": "There have not yet been any acts of sexual intercourse in this chat, please correct it.",
        "top_header_chat": "Top chat sex maniacs\n№|Name|Success|Total\n",
        "top_header_global": "Global top of sex maniacs\n№|Name|Success|Total\n",

        "limit_reached_text": "You have already used up your limit.\nNext opportunity in {time}.",

        "setup_title": "Profile setup",
        "setup_choose_lang": "Choose language:",
        "setup_choose_gender": "Choose gender:",
        "setup_done": "Done ✅\nUse /settings to open settings menu.",
        "lang_ru": "Русский 🇷🇺",
        "lang_en": "English 🇬🇧",
        "gender_m": "Male",
        "gender_f": "Female",

        "settings_title": "⚙️ Your settings:",
        "settings_lang": "Language",
        "settings_gender": "Gender",
        "settings_btn_lang": "Language: {val}",
        "settings_btn_gender": "Gender: {val}",
        "settings_lang_ru": "🇷🇺",
        "settings_lang_en": "🇬🇧",
        "settings_gender_m": "♂",
        "settings_gender_f": "♀",

        "start_hint": f"Welcome to Sex Bot 🫦!\nA joke bot for role-playing intimate scenes and just catching cringe)\nUse inline in the chat: @{BOT_USERNAME} and choose an action.\n\n❗Attention! The bot is not intended for use by minors!\n🔞",
        "not_your_button": "Don't press other people's buttons, you little pervert))",

        "inline_stats_title": "Stats",
        "inline_stats_desc": "A report on your dirty deeds (don't read it in front of mom)",
        "inline_stats_text": "Statistics\n\nClick “{btn}” to show details.",

        "inline_guard_title": "Hire a bodyguard",
        "inline_guard_desc": "Protect your ass from people like yourself",
        "inline_guard_text": "🛡Bodyguard\n\nPress “{btn}” to protect yourself 2 times for 600ꙓ.\nᵀʰᵉ ᶜᵒᵐᵖᵃⁿʸ ᵖʳᵒᵛⁱᵈᵉˢ ᵒⁿˡʸ ᵃ ᵇᵒᵈʸᵍᵘᵃʳᵈ ʰⁱʳⁱⁿᵍ ˢᵉʳᵛⁱᶜᵉ ᵃⁿᵈ ⁱˢ ⁿᵒᵗ ʳᵉˢᵖᵒⁿˢⁱᵇˡᵉ ᶠᵒʳ ʸᵒᵘʳ ˢᵃᶠᵉᵗʸ; ᵃⁿʸ ᵃˢˢᵃᵘˡᵗ ᵃᵍᵃⁱⁿˢᵗ ʸᵒᵘ ⁱˢ ⁿᵒᵗ ᶜᵒⁿˢⁱᵈᵉʳᵉᵈ ᵃⁿ ⁱⁿˢᵘʳᵃⁿᶜᵉ ᶜᵃˢᵉ ᵃⁿᵈ ⁿᵒ ᵐᵒⁿᵉʸ ʷⁱˡˡ ᵇᵉ ᶜᵒˡˡᵉᶜᵗᵉᵈ ᶠʳᵒᵐ ᵗʰᵉ ᶜᵒᵐᵖᵃⁿʸ·",
        
        "btn_hire": "Hire",
        "btn_pay_repeat": "Pay 100ꙓ",
        "btn_cancel": "Cancel",
        
        "stats_header": "Your statistics\n\n📄Report on sexual assaults in the chat",
        "stats_success": "Success",
        "stats_fail": "Fail",
        "stats_total": "Total",
        "stats_pct": "lucky percent",
        "stats_points": "Sex coins",
        "stats_top_targets": "Top by goals",
        
        "points_gain": "<b>{n}</b>ꙓ for the successful rape of a victim",
        "points_bonus": "Bonus for good looks: <b>{n}</b>ꙓ",
        
        "guard_hired_ok": "Bodyguard hired ✅\n  <b>-600</b>ꙓ on 2 times",
        "guard_hired_already": "You already have a bodyguard in this chat, your ass is protected. For now.",
        "not_enough_points": "Not enough sex coins.\nNeeded: <b>{need}</b>ꙓ\nYou have: <b>{have}</b>ꙓ",
        
        "repeat_target_block": "Y-yoy, the police are hanging around this victim. Judging by their conversations, they will be near your <b>{left}</b> detour target..\nFind another victim @username or pay the cops <b>100</b>ꙓ.",
        
        "inline_shop_title": "Black market",
        "inline_shop_desc": "Simplify your life with these products, not an offer",
        "inline_shop_text": "A modest shop :)\n\nClick “{btn}” to open the product catalog.",
        
        "inline_contest_title": "🎲 Casino",
        "inline_contest_desc": "Bets, whores, three axes - test your luck. Place your bet.",
        "inline_contest_text": "🎲 Casino\n\nPress “{btn}” to step into the world of betting.",
        
        "shop_header": "Black Market\n\nGoods for every taste and size. Almost legal (not)!",
        "shop_choose": "Choose an item:",
        "shop_balance": "Balance: <b>{pts}ꙓ</b>",
        "shop_reset_in": "Assortment update in {time}\nThe quantity of the product is limited! Hurry and buy it before someone else does!",
        "shop_item_info": "<b>{name}</b>\nPrice: <b>{price}</b>\nIn stock: <b>{qty}</b>\n\n{desc}",
        "shop_bought": "Purchase completed successfully ✅\n  −<b>{price}ꙓ</b>\n\nWe look forward to seeing you again at our new address: ⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜",
        "shop_sold_out": "Sold out.",
        "btn_buy": "Buy",
        "btn_back": "⬅ Back",
        
        "contest_need_number": "Before entering the hall, place your bet after @{bot}",
        "contest_not_enough": "You don't have enough Sex Coins.\nYou entered: <b>{need}ꙓ</b>. Your balance: <b>{have}ꙓ</b>\n\nCome back when you're a bit, hmm, richer",
        "contest_header": "🎲 Casino Main Hall\nThe place where your fate is decided. Either you walk out a billionaire with your own personal girls, or you'll be working on the track for the rest of your days.",
        "contest_stake": "Stake: <b>{stake}</b>",
        "contest_attempts": "Attempts: <b>{left}</b>/5",
        "contest_warning": "<b><u>Gambling is bad and is not encouraged by the developer. This feature is for educational demonstration only. Adults only. By pressing the button YOU confirm you understand the warning and take full responsibility for your actions!</u></b>",
        "contest_limit": "Betting limit reached.\nCome back in {time}.",
        "contest_win": "You won <b>{gain}ꙓ</b>! (x{mult})",
        "contest_lose": "Bad luck. You lost <b>{stake}ꙓ</b>.",
        "contest_btn_roulette": "🎰",
        "contest_btn_play_again": "Play again"
    }
}

# ---------- Outcome texts loading (from files) ----------
def _write_template(path: str, lang: str):
    if lang == "ru":
        template = """[мужской]
[ {who} повстречал {target}. Бедная жертва ничего не подозревает. Ты быстро подбежал, достал свой член, раздев жертву и раздевшись сам, страстно изнасиловал {target} 💕🥵
{target} оказывается в опастной близости к {who}. Пользуясь ситуацией, ты занялся страстным сексом с жертвой. Со стороны {target} не оказывалось сопротивления 💕🥵 
{who} встретился с {target} взглядом. На лице {target} читалось смущение, однако это не помешало {who}. Раздевшись и прижав {target} к стене лицом, ты поебался с {target} с особой страстью 💕🥵 ]
[ Ой, взгляд {target} направлен прямо на {who} без штанов с членом наперевес! {target} очень быстро набирает номер в своём мобильном. Уноси свои ноги (и член) куда подальше, пока не поздно! 😓
{target} достаёт свой травмат. Пару удачных выстрелов {who} в пах решают возникшую проблему {target}. Жертва сбегает 🥲 ]
[Приближаясь к {target}, ты заметил огромного мужика в черном. Однако это тебя не пугает. Пока телохранитель отвлёкся ты напал на {target} со спины, бустро раздел жертву и с особым кайфом занялся с {target} сексом 💕🥵
Телохранитель ненадолго отошел поссать. Это твой шанс! Ты подошел к {target} и заткнул жертве рот кляпом. Звуки страстного секса залили комнату {target}. Кончив в {target}, ты быстро слинял 💕🥵]
[Стоило тебе только подойти к {target}, тебя тут же скрутил бугай, что ошивался рядом с {target}. Телохранитель тут же приложил лезвие ножа к твоему члену, после толкнул тебя в сторону. Жертва под прикрытием телохранителя уходит.. 🤕
Выследив {target}, ты направился к своей жертве. Но вдруг ты почувствовал в своей жопе что-то инородное. За твоей спиной стоял телохранитель {target}, что засунул в твоё очко резиновую дубинку. Пока {target} чем-то спокойно занимается, тебя страсно ебут в жопу. 💦😵]
[женский]
[ {who} повстречала {target}. Бедная жертва ничего не подозревает. Ты быстро подбежала, достала страпон, раздев жертву и раздевшись сама, изнасиловала несчастного {target} 💕🥵
{target} оказывается в опастной близости к тебе. Пользуясь ситуацией ты занялась страстным сексом с жертвой. Со стороны {target} не оказывалось сопротивления 💕🥵
{who} встретилась с {target} взглядом. На лице {target} читалось смущение, однако это не помешало тебе. Раздевшись и достав двусторонний резиновый член, ты с {target} поебалась с особой страстью 💕🥵 ]
[ Ой, взгляд {target} направлен прямо на тебя с голой грудью наперевес! {target} очень быстро набирает номер в своём мобильном. Уноси свои ноги (и свою вагину) куда подальше, пока не поздно! 😓
{target} достаёт свою перцовку. Пару удачных струй {who} в лицо решают возникшую проблему {target}. Жертва сбегает 🥲 ]
[Приближаясь к {target}, ты заметила огромного мужика в черном. Однако это тебя не пугает. Пока телохранитель отвлёкся ты напала на {target} со спины, бустро раздела жертву и с особым кайфом занялась с {target} сексом 💕🥵
Телохранитель ненадолго отошел поссать. Это твой шанс! Ты подошла к {target} и заткнула жертве рот кляпом. Звуки страстного секса залили комнату {target}. Как только в тебя кончили, ты быстро слиняла 💕🥵]
[Стоило тебе только подойти к {target}, тебя тут же скрутил бугай, что ошивался рядом с {target}. Телохранитель тут же приложил лезвие ножа к твоему клитору, после толкнул тебя в сторону. Жертва под прикрытием телохранителя уходит.. 🤕
Выследив {target}, ты направилась к своей жертве. Но вдруг ты почувствовала в своей вагине что-то инородное. За твоей спиной стоял телохранитель {target}, что засунул в твою пизду резиновую дубинку. Пока {target} чем-то спокойно занимается, тебя страсно ебут в пизду. 💦😵]
"""
    else:
        template = """[male]
[ {who}'ve met {target}. The poor victim does not suspect anything. You quickly ran up, took out your penis, undressed the victim and undressed yourself, raped the unfortunate {target} 💕🥵
{target} ends up in dangerous proximity to {who}. Taking advantage of the situation, you engaged in passionate sex with the victim. {target} offered no resistance 💕🥵
{who} met {target}'s gaze. Embarrassment was visible on {target}'s face, but it didn't stop you. After undressing, you made love with {target} with particular passion 💕🥵 ]
[ Oh, {target}'s gaze is directed straight at you, pantsless with his penis hanging out! {target} quickly dials a number on his mobile. Get your legs (and penis) far away before it's too late! 😓
{target} pulls out their traumatic gun. A couple of lucky shots to your groin solve their problems. The victim runs away 🥲 ]
[As you were approaching {target}, you noticed a huge man in black. However, this does not scare you. While the bodyguard was distracted, you attacked {target} from behind, quickly undressed the victim and had sex 💕🥵 with {target} with a special thrill
The bodyguard went away for a while to piss. This is your chance! You walked up to {target} and gagged the victim. The sounds of passionate sex flooded the {target} room. After cumming in {target}, you quickly faded 💕🥵 ]
[As soon as you approached {target}, you were immediately spun up by a brute that was hanging around {target}. The bodyguard immediately put the knife blade to your, then pushed you aside. 🤕
After tracking down {target}, you headed for your victim. But suddenly you felt something foreign in your asshole. Behind you was a {target} bodyguard who shoved a rubber baton into your. While {target} is quietly doing something, you are fucked in the ass. 💦😵]
[female]
[ {who}'ve met {target}. The poor victim does not suspect anything. You quickly ran up, took out your penis, undressed the victim and undressed yourself, raped the unfortunate {target} 💕🥵
{target} ends up in dangerous proximity to {who}. Taking advantage of the situation, you engaged in passionate sex with the victim. {target} offered no resistance 💕🥵
{who} met {target}'s gaze. Embarrassment was visible on {target}'s face, but it didn't stop you. After undressing, you made love with {target} with particular passion 💕🥵 ]
[ Oh, {target}'s gaze is directed straight at you, pantsless with his penis hanging out! {target} quickly dials a number on his mobile. Get your legs (and penis) far away before it's too late! 😓
{target} pulls out their traumatic gun. A couple of lucky shots to your groin solve their problems. The victim runs away 🥲 ]
[As you were approaching {target}, you noticed a huge man in black, but that doesn't scare you. While the bodyguard was distracted, you attacked {target} from behind, quickly undressed the victim and had sex 💕🥵 with {target} with a special buzz
The bodyguard went away for a while to piss. This is your chance! You walked up to {target} and gagged the victim. The sounds of passionate sex flooded the {target} room. As soon as they in you, you quickly faded 💕🥵 ]
[As soon as you approached {target}, you were immediately twisted by a brute that was hanging around {target}. The bodyguard immediately put the knife blade to your clitoris, then pushed you aside. 🤕
After tracking down {target}, you headed for your victim. But suddenly you felt something foreign in your vagina. Behind you was a {target} bodyguard who shoved a rubber baton into your cunt. While {target} is quietly doing something, you are fucked in the pussy. 💦😵]
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(template)

def _split_list_block(block: str):
    raw = block.strip()
    if "\n" in raw:
        parts = [p.strip(" \t\r") for p in raw.splitlines()]
    else:
        parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]

def parse_outcomes_from_file(path: str, lang: str):
    if not os.path.exists(path):
        _write_template(path, lang)

    data = open(path, "r", encoding="utf-8").read()
    tokens = re.findall(r"\[(.*?)\]", data, flags=re.S)

    outcomes = {
        "m": {"success": [], "fail": [], "guard_success": [], "guard_fail": []},
        "f": {"success": [], "fail": [], "guard_success": [], "guard_fail": []},
    }

    sex_map = {"мужской": "m", "male": "m", "m": "m", "женский": "f", "female": "f", "f": "f"}

    label_map = {
        # ru
        "удача": "success",
        "неудача": "fail",
        "телохранитель_удача": "guard_success",
        "телохранитель_неудача": "guard_fail",
        # en
        "success": "success",
        "failure": "fail",
        "fail": "fail",
        "bodyguard_success": "guard_success",
        "bodyguard_failure": "guard_fail",
        "guard_success": "guard_success",
        "guard_fail": "guard_fail",
    }

    i = 0
    current_sex = None

    def fill_sequential(sex: str, block_text: str):
        order = ["success", "fail", "guard_success", "guard_fail"]
        lst = _split_list_block(block_text)
        for k in order:
            if not outcomes[sex][k]:
                outcomes[sex][k] = lst
                return

    while i < len(tokens):
        tok = tokens[i].strip()
        low = tok.lower()
        if low in sex_map:
            current_sex = sex_map[low]
            i += 1
            continue

        if not current_sex:
            i += 1
            continue

        if low in label_map:
            key = label_map[low]
            if i + 1 < len(tokens):
                outcomes[current_sex][key] = _split_list_block(tokens[i + 1])
                i += 2
                continue

        fill_sequential(current_sex, tok)
        i += 1

    if not outcomes["m"]["success"]:
        outcomes["m"]["success"] = ["{who} successfully shook hands with {target}!"] if lang == "en" else ["{who} успешно пожал руку {target}!"]
    if not outcomes["m"]["fail"]:
        outcomes["m"]["fail"] = ["{who} failed to shake hands with {target}."] if lang == "en" else ["{who} не смог пожал руку {target}."]
    if not outcomes["f"]["success"]:
        outcomes["f"]["success"] = outcomes["m"]["success"][:]
    if not outcomes["f"]["fail"]:
        outcomes["f"]["fail"] = outcomes["m"]["fail"][:]

    for sex in ("m", "f"):
        if not outcomes[sex]["guard_success"]:
            outcomes[sex]["guard_success"] = outcomes[sex]["success"][:]
        if not outcomes[sex]["guard_fail"]:
            outcomes[sex]["guard_fail"] = outcomes[sex]["fail"][:]

    return outcomes

OUTCOMES = {
    "ru": parse_outcomes_from_file(TEXT_RU_FILE, "ru"),
    "en": parse_outcomes_from_file(TEXT_EN_FILE, "en"),
}


# ---------- Contest texts (separate files) ----------
def _write_contest_template(path: str, lang: str):
    if lang == "en":
        data = (
            "[win]\n"
            "[You won <b>{gain}ꙓ</b>! (x{mult})\n"
            "Nice! +<b>{gain}ꙓ</b> (x{mult})]\n"
            "[lose]\n"
            "[Bad luck. You lost <b>{stake}ꙓ</b>.\n"
            "No win this time −<b>{stake}ꙓ</b>.]\n"
        )
    else:
        data = (
            "[win]\n"
            "[Вы выиграли <b>{gain}ꙓ</b>! (x{mult})\n"
            "Удача! +<b>{gain}ꙓ</b> (x{mult})]\n"
            "[lose]\n"
            "[Неудача. Вы потеряли <b>{stake}ꙓ</b>.\n"
            "Сегодня без выигрыша −<b>{stake}ꙓ</b>.]\n"
        )
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)

def parse_contest_from_file(path: str, lang: str):
    if not os.path.exists(path):
        _write_contest_template(path, lang)

    data = open(path, "r", encoding="utf-8").read()
    tokens = re.findall(r"\[(.*?)\]", data, flags=re.S)

    res = {"win": [], "lose": []}
    label_map = {"win": "win", "lose": "lose", "победа": "win", "проигрыш": "lose"}

    i = 0
    current = None
    while i < len(tokens):
        tok = tokens[i].strip()
        low = tok.lower()
        if low in label_map:
            current = label_map[low]
            if i + 1 < len(tokens):
                res[current] = _split_list_block(tokens[i + 1])
                i += 2
                continue
        i += 1

    # fallback
    if not res["win"]:
        res["win"] = [ui(None, "contest_win")]
    if not res["lose"]:
        res["lose"] = [ui(None, "contest_lose")]
    return res

CONTEST_TEXTS = {
    "ru": parse_contest_from_file(CONTEST_RU_FILE, "ru"),
    "en": parse_contest_from_file(CONTEST_EN_FILE, "en"),
}

# ---------- Shop items ----------
SHOP_DEFAULTS = {
    "coffee": {"qty": 6, "price": 400,
               "name": {"ru": "Афродизиак", "en": "Aphrodisiac"},
               "desc": {"ru": "Добавляет +2 попытки, но увеличивает откат на 30 минут.",
                        "en": "Adds +2 attempts but increases cooldown by 30 minutes."}},
    "wine": {"qty": 8, "price": 50,
             "name": {"ru": "Вино", "en": "Wine"},
             "desc": {"ru": "Следующая попытка будет успехом, а следующее после него — гарантированно неудачей.",
                      "en": "Your next attempt will succeed, but the one after that is guaranteed to fail."}},
    "bouquet": {"qty": 2, "price": 700,
                "name": {"ru": "Поддельный значок охранника", "en": "Fake security badge"},
                "desc": {"ru": "Против цели с телохранителем шанс станет 1/2, но откат увеличится на 30 минут.",
                         "en": "Against a protected target your chance becomes 1/2, but cooldown increases by 30 minutes."}},
    "insurance": {"qty": 4, "price": 200,
                  "name": {"ru": "Страхование капитала", "en": "Capital insurance"},
                  "desc": {"ru": "Один проигрыш в казино не спишет ставку.",
                           "en": "One casino loss will not deduct your stake."}},
    "candy": {"qty": 4, "price": 400,
              "name": {"ru": "Презервативы", "en": "Condoms"},
              "desc": {"ru": "При неудаче с шансом 1/2 попытка не будет потрачена.",
                       "en": "On a failed there is a 1/2 chance the attempt won't be consumed."}},
}
def safe_format(s: str, **kwargs) -> str:
    class DD(defaultdict):
        def __missing__(self, key):
            return "{" + key + "}"
    return s.format_map(DD(str, **kwargs))

def format_name_html(username: str | None, first_name: str | None, last_name: str | None, uid: int) -> str:
    """Pretty name for tops: prefer First Last, then @username, never raw id."""
    full = " ".join([p for p in [first_name, last_name] if p]).strip()
    if full:
        name_bold = f"<b>{html_escape(full)}</b>"
        if username:
            return f"{name_bold} (@{html_escape(username)})"
        return name_bold
    if username:
        return f"<b>@{html_escape(username)}</b>"

    return "<b>Без имени</b>"

# ---------- settings helpers ----------
def ensure_settings(uid: int):
    cur.execute(
        "INSERT OR IGNORE INTO settings (user_id, language, gender, configured, setup_msg_id, setup_stage) "
        "VALUES (?, 'ru', 'm', 0, NULL, NULL)",
        (uid,)
    )
    conn.commit()

def get_lang(uid: int) -> str:
    ensure_settings(uid)
    cur.execute("SELECT language FROM settings WHERE user_id=?", (uid,))
    row = cur.fetchone()
    return row[0] if row else "ru"

def get_gender(uid: int) -> str:
    ensure_settings(uid)
    cur.execute("SELECT gender FROM settings WHERE user_id=?", (uid,))
    row = cur.fetchone()
    return row[0] if row else "m"

def is_configured(uid: int) -> bool:
    ensure_settings(uid)
    cur.execute("SELECT configured FROM settings WHERE user_id=?", (uid,))
    row = cur.fetchone()
    return bool(row[0]) if row else False

def set_configured(uid: int, val: bool):
    ensure_settings(uid)
    cur.execute("UPDATE settings SET configured=? WHERE user_id=?", (1 if val else 0, uid))
    conn.commit()

def set_setup_msg(uid: int, msg_id: int | None, stage: str | None):
    ensure_settings(uid)
    cur.execute("UPDATE settings SET setup_msg_id=?, setup_stage=? WHERE user_id=?", (msg_id, stage, uid))
    conn.commit()

def get_setup_msg(uid: int):
    ensure_settings(uid)
    cur.execute("SELECT setup_msg_id, setup_stage FROM settings WHERE user_id=?", (uid,))
    return cur.fetchone() 

def set_lang(uid: int, lang: str):
    ensure_settings(uid)
    cur.execute("UPDATE settings SET language=? WHERE user_id=?", (lang, uid))
    conn.commit()

def set_gender(uid: int, gender: str):
    ensure_settings(uid)
    cur.execute("UPDATE settings SET gender=? WHERE user_id=?", (gender, uid))
    conn.commit()

def ui(uid: int | None, key: str) -> str:
    lang = "ru" if uid is None else get_lang(uid)
    return UI.get(lang, UI["ru"]).get(key, UI["ru"].get(key, ""))

from html import escape as html_escape

CB_SEP = "|"

def cb_pack(base: str, owner_id: int) -> str:
    return f"{base}{CB_SEP}{owner_id}"

def cb_unpack(data: str):
    if CB_SEP in data:
        base, tail = data.rsplit(CB_SEP, 1)
        if tail.isdigit():
            return base, int(tail)
    return data, None

def log_callback(clicker_id: int, owner_id: int | None, base_data: str, ok: bool,
                 group_key: str | None, inline_message_id: str | None):
    try:
        cur.execute(
            "INSERT INTO callback_logs (id, ts, clicker_id, owner_id, base_data, ok, group_key, inline_message_id) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), int(time.time()), clicker_id, owner_id, base_data, 1 if ok else 0, group_key, inline_message_id)
        )
        conn.commit()
    except Exception:
        logger.exception("Failed to log callback")

# ---------- group utilities ----------
def compute_group_key_from_callback(call: CallbackQuery, prefix_len=PREFIX_LEN):
    if getattr(call, "message", None) and getattr(call.message, "chat", None):
        return f"chat:{call.message.chat.id}"
    inline_id = getattr(call, "inline_message_id", None)
    if inline_id:
        pref = inline_id[:prefix_len]
        return f"inline_pref:{pref}"
    return None

def register_user_group(group_key, inline_message_id, user):
    """Register/refresh user presence in a group_key.

    IMPORTANT: uses UPSERT and last_seen so we don't keep stale names and we don't create duplicates.
    """
    now = int(time.time())
    try:
        cur.execute(
            "INSERT INTO registrations (id, group_key, inline_message_id, user_id, username, first_name, last_name, ts, last_seen) "
            "VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(group_key, user_id) DO UPDATE SET "
            "inline_message_id=excluded.inline_message_id, "
            "username=COALESCE(excluded.username, registrations.username), "
            "first_name=COALESCE(excluded.first_name, registrations.first_name), "
            "last_name=COALESCE(excluded.last_name, registrations.last_name), "
            "last_seen=excluded.last_seen",
            (
                str(uuid.uuid4()),
                group_key,
                inline_message_id,
                user.id,
                getattr(user, "username", None),
                getattr(user, "first_name", None),
                getattr(user, "last_name", None),
                now,
                now,
            ),
        )
        conn.commit()

        ensure_user_stats(
            user.id,
            getattr(user, "username", None),
            getattr(user, "first_name", None),
            getattr(user, "last_name", None),
        )
        ensure_user_stats_chat(
            group_key,
            user.id,
            getattr(user, "username", None),
            getattr(user, "first_name", None),
            getattr(user, "last_name", None),
        )

        logger.info("Registered/refreshed user %s for group %s", user.id, group_key)
        return True
    except Exception:
        logger.exception("Failed to register/refresh user")
        return False

def get_users_for_group(group_key):
    cur.execute("SELECT user_id, username, first_name, last_name, ts FROM registrations WHERE group_key = ? ORDER BY ts", (group_key,))
    return cur.fetchall()

def record_group_example(group_key, inline_message_id, observed_chat_id=None):
    now = int(time.time())
    try:
        cur.execute("""
            INSERT INTO group_examples (group_key, sample_inline_id, observed_chat_id, last_seen)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(group_key) DO UPDATE SET
              sample_inline_id=excluded.sample_inline_id,
              observed_chat_id=COALESCE(excluded.observed_chat_id, group_examples.observed_chat_id),
              last_seen=excluded.last_seen
        """, (group_key, inline_message_id, observed_chat_id, now))
        conn.commit()
    except Exception:
        logger.exception("Failed to record group example")

# ---------- stats ----------
def ensure_user_stats(uid, username=None, first_name=None, last_name=None):
    now = int(time.time())
    cur.execute("SELECT 1 FROM stats WHERE user_id = ?", (uid,))
    if not cur.fetchone():
        cur.execute("INSERT INTO stats (user_id, username, first_name, last_name, success_count, total_count, last_update) VALUES (?,?,?,?,?,?,?)",
                    (uid, username, first_name, last_name, 0, 0, now))
        conn.commit()
    else:
        cur.execute("UPDATE stats SET username=COALESCE(?, username), first_name=COALESCE(?, first_name), last_name=COALESCE(?, last_name), last_update=? WHERE user_id=?",
                    (username, first_name, last_name, now, uid))
        conn.commit()

def update_stats_on_result(uid, username, first_name, last_name, success_delta=0, total_delta=0):
    ensure_user_stats(uid, username, first_name, last_name)
    cur.execute("SELECT success_count, total_count FROM stats WHERE user_id=?", (uid,))
    s, t_ = cur.fetchone()
    s = max(0, s + success_delta)
    t_ = max(0, t_ + total_delta)
    cur.execute(
        "UPDATE stats SET success_count=?, total_count=?, username=COALESCE(?, username), first_name=COALESCE(?, first_name), last_name=COALESCE(?, last_name), last_update=? WHERE user_id=?",
        (s, t_, username, first_name, last_name, int(time.time()), uid)
    )
    conn.commit()

def ensure_user_stats_chat(group_key: str, uid: int, username=None, first_name=None, last_name=None):
    if not group_key:
        return
    now = int(time.time())
    cur.execute("SELECT 1 FROM stats_chat WHERE group_key=? AND user_id=?", (group_key, uid))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO stats_chat (group_key, user_id, username, first_name, last_name, success_count, total_count, last_update) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (group_key, uid, username, first_name, last_name, 0, 0, now)
        )
        conn.commit()
    else:
        cur.execute(
            "UPDATE stats_chat SET username=COALESCE(?, username), first_name=COALESCE(?, first_name), last_name=COALESCE(?, last_name), last_update=? WHERE group_key=? AND user_id=?",
            (username, first_name, last_name, now, group_key, uid)
        )
        conn.commit()

def update_stats_chat_on_result(group_key: str, uid: int, username=None, first_name=None, last_name=None, success_delta=0, total_delta=0):
    if not group_key:
        return
    ensure_user_stats_chat(group_key, uid, username, first_name, last_name)
    cur.execute("SELECT success_count, total_count FROM stats_chat WHERE group_key=? AND user_id=?", (group_key, uid))
    s, t_ = cur.fetchone()
    s = max(0, s + success_delta)
    t_ = max(0, t_ + total_delta)
    cur.execute(
        "UPDATE stats_chat SET success_count=?, total_count=?, username=COALESCE(?, username), first_name=COALESCE(?, first_name), last_name=COALESCE(?, last_name), last_update=? "
        "WHERE group_key=? AND user_id=?",
        (s, t_, username, first_name, last_name, int(time.time()), group_key, uid)
    )
    conn.commit()

# ---------- points (per chat) ----------
def get_points_chat(group_key: str, uid: int) -> int:
    if not group_key:
        return 0
    cur.execute("SELECT points FROM stats_chat WHERE group_key=? AND user_id=?", (group_key, uid))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0

def add_points_chat(group_key: str, uid: int, username=None, first_name=None, last_name=None, delta: int = 0) -> int:
    if not group_key:
        return 0
    ensure_user_stats_chat(group_key, uid, username, first_name, last_name)
    cur.execute("SELECT points FROM stats_chat WHERE group_key=? AND user_id=?", (group_key, uid))
    row = cur.fetchone()
    cur_pts = int(row[0]) if row and row[0] is not None else 0
    new_pts = max(0, cur_pts + int(delta))
    cur.execute("UPDATE stats_chat SET points=? WHERE group_key=? AND user_id=?", (new_pts, group_key, uid))
    conn.commit()
    return new_pts

def spend_points_chat(group_key: str, uid: int, cost: int) -> bool:
    have = get_points_chat(group_key, uid)
    if have < cost:
        return False
    add_points_chat(group_key, uid, delta=-cost)
    return True

# ---------- shop / effects / contest helpers ----------
SHOP_RESET_SECONDS = 2 * 24 * 3600
CONTEST_RESET_SECONDS = 24 * 3600
CONTEST_MAX_ATTEMPTS = 5

def ensure_user_effects(group_key: str, uid: int):
    if not group_key:
        return
    cur.execute("INSERT OR IGNORE INTO user_effects (group_key, user_id) VALUES (?,?)", (group_key, uid))
    conn.commit()

def get_user_effects(group_key: str, uid: int):
    if not group_key:
        return {"bonus_attempts": 0, "wine_state": 0, "bouquet": 0, "insurance": 0, "candy": 0}
    ensure_user_effects(group_key, uid)
    cur.execute(
        "SELECT bonus_attempts, wine_state, bouquet, insurance, candy FROM user_effects WHERE group_key=? AND user_id=?",
        (group_key, uid)
    )
    row = cur.fetchone() or (0, 0, 0, 0, 0)
    return {
        "bonus_attempts": int(row[0] or 0),
        "wine_state": int(row[1] or 0),
        "bouquet": int(row[2] or 0),
        "insurance": int(row[3] or 0),
        "candy": int(row[4] or 0),
    }

def set_user_effects(group_key: str, uid: int, **fields):
    if not group_key:
        return
    ensure_user_effects(group_key, uid)
    cols = []
    vals = []
    for k, v in fields.items():
        if k not in ("bonus_attempts", "wine_state", "bouquet", "insurance", "candy"):
            continue
        cols.append(f"{k}=?")
        vals.append(int(v))
    if not cols:
        return
    vals.extend([group_key, uid])
    cur.execute(f"UPDATE user_effects SET {', '.join(cols)} WHERE group_key=? AND user_id=?", tuple(vals))
    conn.commit()

def ensure_shop_reset(group_key: str) -> int:
    """Ensures inventory exists and resets every 2 days per chat."""
    now = int(time.time())
    cur.execute("SELECT next_reset_ts FROM shop_state WHERE group_key=?", (group_key,))
    row = cur.fetchone()
    if not row:
        next_ts = now + SHOP_RESET_SECONDS
        cur.execute("INSERT INTO shop_state (group_key, next_reset_ts) VALUES (?,?)", (group_key, next_ts))
        for k, v in SHOP_DEFAULTS.items():
            cur.execute("INSERT OR REPLACE INTO shop_inventory (group_key, item_key, qty) VALUES (?,?,?)",
                        (group_key, k, int(v["qty"])))
        conn.commit()
        return next_ts

    next_ts = int(row[0] or 0)
    if next_ts and now >= next_ts:
        next_ts = now + SHOP_RESET_SECONDS
        cur.execute("UPDATE shop_state SET next_reset_ts=? WHERE group_key=?", (next_ts, group_key))
        for k, v in SHOP_DEFAULTS.items():
            cur.execute("INSERT OR REPLACE INTO shop_inventory (group_key, item_key, qty) VALUES (?,?,?)",
                        (group_key, k, int(v["qty"])))
        conn.commit()
    return next_ts

def get_shop_qty(group_key: str):
    ensure_shop_reset(group_key)
    cur.execute("SELECT item_key, qty FROM shop_inventory WHERE group_key=?", (group_key,))
    return {r[0]: int(r[1] or 0) for r in cur.fetchall()}

def _apply_shop_effect(group_key: str, uid: int, item_key: str):
    eff = get_user_effects(group_key, uid)
    if item_key == "coffee":
        set_user_effects(group_key, uid, bonus_attempts=eff["bonus_attempts"] + 2)
        add_cooldown_penalty(uid, 1800)
    elif item_key == "wine":
        set_user_effects(group_key, uid, wine_state=2)
    elif item_key == "bouquet":
        set_user_effects(group_key, uid, bouquet=1)
    elif item_key == "insurance":
        set_user_effects(group_key, uid, insurance=1)
    elif item_key == "candy":
        set_user_effects(group_key, uid, candy=1)

def ensure_roulette_stats(group_key: str, uid: int):
    cur.execute(
        "INSERT OR IGNORE INTO roulette_stats (group_key, user_id, win2, win3, lose, games, spent) VALUES (?,?,?,?,?,?,?)",
        (group_key, uid, 0, 0, 0, 0, 0)
    )
    conn.commit()

def get_roulette_stats(group_key: str, uid: int):
    ensure_roulette_stats(group_key, uid)
    cur.execute(
        "SELECT win2, win3, lose, games, spent FROM roulette_stats WHERE group_key=? AND user_id=?",
        (group_key, uid)
    )
    row = cur.fetchone() or (0, 0, 0, 0, 0)
    w2, w3, lose, games, spent = row
    return int(w2 or 0), int(w3 or 0), int(lose or 0), int(games or 0), int(spent or 0)

def inc_roulette_stats(group_key: str, uid: int, win2=0, win3=0, lose=0, games=0, spent=0):
    ensure_roulette_stats(group_key, uid)
    cur.execute(
        """
        UPDATE roulette_stats
        SET win2=win2+?,
            win3=win3+?,
            lose=lose+?,
            games=games+?,
            spent=spent+?
        WHERE group_key=? AND user_id=?
        """,
        (int(win2), int(win3), int(lose), int(games), int(spent), group_key, uid)
    )
    conn.commit()

def record_shop_purchase(group_key: str, uid: int, item_key: str, delta: int = 1):
    cur.execute(
        """
        INSERT INTO shop_purchases (group_key, user_id, item_key, qty)
        VALUES (?,?,?,?)
        ON CONFLICT(group_key, user_id, item_key) DO UPDATE SET qty = qty + excluded.qty
        """,
        (group_key, uid, item_key, int(delta))
    )
    conn.commit()

def get_shop_purchases(group_key: str, uid: int):
    cur.execute(
        "SELECT item_key, qty FROM shop_purchases WHERE group_key=? AND user_id=? AND qty>0 ORDER BY item_key",
        (group_key, uid)
    )
    return [(r[0], int(r[1] or 0)) for r in cur.fetchall()]

def buy_shop_item(group_key: str, uid: int, item_key: str):
    ensure_shop_reset(group_key)
    if item_key not in SHOP_DEFAULTS:
        return False, "Unknown item"
    cur.execute("SELECT qty FROM shop_inventory WHERE group_key=? AND item_key=?", (group_key, item_key))
    row = cur.fetchone()
    qty = int(row[0] or 0) if row else 0
    if qty <= 0:
        return False, ui(uid, "shop_sold_out")
    price = int(SHOP_DEFAULTS[item_key]["price"])
    have = get_points_chat(group_key, uid)
    if have < price:
        return False, ui(uid, "not_enough_points").format(need=price, have=have)
    cur.execute("UPDATE shop_inventory SET qty=qty-1 WHERE group_key=? AND item_key=? AND qty>0", (group_key, item_key))
    conn.commit()
    spend_points_chat(group_key, uid, price)
    _apply_shop_effect(group_key, uid, item_key)
    record_shop_purchase(group_key, uid, item_key, delta=1)
    return True, ui(uid, "shop_bought").format(price=price)

def ensure_contest_state(group_key: str, uid: int):
    cur.execute("INSERT OR IGNORE INTO contest_state (group_key, user_id, attempts_used, reset_ts, stake) VALUES (?,?,?,?,?)",
                (group_key, uid, 0, 0, 0))
    conn.commit()

def get_contest_state(group_key: str, uid: int):
    ensure_contest_state(group_key, uid)
    now = int(time.time())
    cur.execute("SELECT attempts_used, reset_ts, stake FROM contest_state WHERE group_key=? AND user_id=?",
                (group_key, uid))
    used, reset_ts, stake = cur.fetchone() or (0, 0, 0)
    used = int(used or 0); reset_ts = int(reset_ts or 0); stake = int(stake or 0)
    if reset_ts == 0 or now >= reset_ts:
        reset_ts = now + CONTEST_RESET_SECONDS
        used = 0
        cur.execute("UPDATE contest_state SET attempts_used=?, reset_ts=? WHERE group_key=? AND user_id=?",
                    (used, reset_ts, group_key, uid))
        conn.commit()
    left = max(0, CONTEST_MAX_ATTEMPTS - used)
    return used, reset_ts, stake, left

def set_contest_stake(group_key: str, uid: int, stake: int):
    ensure_contest_state(group_key, uid)
    cur.execute("UPDATE contest_state SET stake=? WHERE group_key=? AND user_id=?", (int(stake), group_key, uid))
    conn.commit()

def inc_contest_attempt(group_key: str, uid: int) -> bool:
    used, reset_ts, stake, left = get_contest_state(group_key, uid)
    if left <= 0:
        return False
    cur.execute("UPDATE contest_state SET attempts_used=attempts_used+1 WHERE group_key=? AND user_id=?", (group_key, uid))
    conn.commit()
    return True

def dec_attempt(uid: int):
    count, last_ts, cooldown_until = get_attempts(uid)
    if count <= 0:
        return
    set_attempts(uid, max(0, count - 1), last_ts, cooldown_until)

# ---------- bodyguards ----------
def has_bodyguard(group_key: str, uid: int) -> bool:
    if not group_key:
        return False
    cur.execute(
        "SELECT active, uses_left FROM bodyguards WHERE group_key=? AND user_id=?",
        (group_key, uid)
    )
    row = cur.fetchone()
    if not row:
        return False
    active, uses_left = int(row[0] or 0), int(row[1] or 0)
    return active == 1 and uses_left > 0

def hire_bodyguard(group_key: str, uid: int) -> bool:
    if not group_key:
        return False
    now = int(time.time())
    cur.execute(
        "INSERT INTO bodyguards (group_key, user_id, active, uses_left, since_ts) VALUES (?,?,?,?,?) "
        "ON CONFLICT(group_key, user_id) DO UPDATE SET active=1, uses_left=2, since_ts=excluded.since_ts",
        (group_key, uid, 1, 2, now)
    )
    conn.commit()
    return True

def consume_bodyguard_use(group_key: str, uid: int):
    """
    Decrements uses_left by 1 (min 0). If becomes 0 -> active=0.
    """
    cur.execute(
        "SELECT uses_left FROM bodyguards WHERE group_key=? AND user_id=?",
        (group_key, uid)
    )
    row = cur.fetchone()
    if not row:
        return
    uses_left = int(row[0] or 0)
    uses_left = max(0, uses_left - 1)
    if uses_left == 0:
        cur.execute(
            "UPDATE bodyguards SET uses_left=0, active=0 WHERE group_key=? AND user_id=?",
            (group_key, uid)
        )
    else:
        cur.execute(
            "UPDATE bodyguards SET uses_left=? WHERE group_key=? AND user_id=?",
            (uses_left, group_key, uid)
        )
    conn.commit()

# ---------- chat_state (anti-spam + random no-repeat) ----------
def ensure_chat_state(group_key: str, uid: int):
    cur.execute("SELECT 1 FROM chat_state WHERE group_key=? AND user_id=?", (group_key, uid))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO chat_state (group_key, user_id, last_target_key, last_target_block_rem, last_rand1, last_rand2) "
            "VALUES (?,?,?,?,?,?)",
            (group_key, uid, None, 0, None, None)
        )
        conn.commit()

def get_chat_state(group_key: str, uid: int):
    ensure_chat_state(group_key, uid)
    cur.execute("SELECT last_target_key, last_target_block_rem, last_rand1, last_rand2 FROM chat_state WHERE group_key=? AND user_id=?",
                (group_key, uid))
    return cur.fetchone() 

def set_chat_state(group_key: str, uid: int, last_target_key, rem: int, rand1, rand2):
    ensure_chat_state(group_key, uid)
    cur.execute(
        "UPDATE chat_state SET last_target_key=?, last_target_block_rem=?, last_rand1=?, last_rand2=? WHERE group_key=? AND user_id=?",
        (last_target_key, int(rem), rand1, rand2, group_key, uid)
    )
    conn.commit()

# ---------- target stats (per chat) ----------
def record_target_attempt(group_key: str, uid: int,
                          target_key: str,
                          target_user_id: int | None,
                          target_username: str | None,
                          target_first: str | None,
                          target_last: str | None,
                          success: bool):
    if not group_key:
        return
    now = int(time.time())
    cur.execute(
        "INSERT INTO target_stats (group_key, user_id, target_key, target_user_id, target_username, target_first_name, target_last_name, attempts, successes, last_seen) "
        "VALUES (?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(group_key, user_id, target_key) DO UPDATE SET "
        "attempts=attempts+1, successes=successes+?, target_user_id=COALESCE(excluded.target_user_id, target_stats.target_user_id), "
        "target_username=COALESCE(excluded.target_username, target_stats.target_username), "
        "target_first_name=COALESCE(excluded.target_first_name, target_stats.target_first_name), "
        "target_last_name=COALESCE(excluded.target_last_name, target_stats.target_last_name), "
        "last_seen=excluded.last_seen",
        (group_key, uid, target_key, target_user_id, target_username, target_first, target_last, 1, 1 if success else 0, now, 1 if success else 0)
    )
    conn.commit()

def get_top_targets(group_key: str, uid: int, limit: int = 3):
    cur.execute(
        "SELECT target_key, target_user_id, target_username, attempts, successes "
        "FROM target_stats WHERE group_key=? AND user_id=? AND attempts>0 "
        "ORDER BY (CAST(successes AS REAL)/attempts) DESC, attempts DESC LIMIT ?",
        (group_key, uid, limit)
    )
    return cur.fetchall()

# ---------- cooldown (2h penalty support) ----------
def get_attempts(uid):
    cur.execute("SELECT count, last_ts, cooldown_until FROM attempts WHERE user_id=?", (uid,))
    row = cur.fetchone()
    if not row:
        return 0, 0, 0
    return int(row[0]), int(row[1]), int(row[2] or 0)

def set_attempts(uid, count, last_ts, cooldown_until):
    cur.execute(
        "INSERT INTO attempts (user_id, count, last_ts, cooldown_until) VALUES (?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET count=excluded.count, last_ts=excluded.last_ts, cooldown_until=excluded.cooldown_until",
        (uid, int(count), int(last_ts), int(cooldown_until))
    )
    conn.commit()

def reset_attempts_if_needed(uid):
    count, last_ts, cooldown_until = get_attempts(uid)
    now = int(time.time())
    end = cooldown_until if cooldown_until else (last_ts + 3600 if last_ts else 0)
    if end == 0 or now >= end:
        set_attempts(uid, 0, 0, 0)
        return 0
    return count

def inc_attempt(uid):
    count, last_ts, cooldown_until = get_attempts(uid)
    now = int(time.time())
    end = cooldown_until if cooldown_until else (last_ts + 3600 if last_ts else 0)
    if end == 0 or now >= end:
        set_attempts(uid, 1, now, now + 3600)
        return 1
    count += 1
    set_attempts(uid, count, now, end)
    return count

def time_to_reset(uid):
    _, last_ts, cooldown_until = get_attempts(uid)
    now = int(time.time())
    end = cooldown_until if cooldown_until else (last_ts + 3600 if last_ts else 0)
    return max(0, end - now)

def fmt_cooldown(seconds: int, lang: str = "ru")-> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60

    if lang == "en":
        parts = []
        if h: parts.append(f"{h}h")
        if m: parts.append(f"{m}m")
        if s or not parts: parts.append(f"{s}s")
        return " ".join(parts)
    parts = []
    if h: parts.append(f"{h} ч")
    if m: parts.append(f"{m} мин")
    if s or not parts: parts.append(f"{s} сек")
    return " ".join(parts)

def add_cooldown_penalty(uid, plus_seconds: int):
    count, last_ts, cooldown_until = get_attempts(uid)
    now = int(time.time())
    end = cooldown_until if cooldown_until else (last_ts + 3600 if last_ts else (now + 3600))
    end = max(end, now) + int(plus_seconds)
    set_attempts(uid, count, last_ts if last_ts else now, end)

# ---------- settings UI rendering (single message edit) ----------
def _settings_menu_markup(uid: int) -> InlineKeyboardMarkup:
    lang = get_lang(uid)
    gender = get_gender(uid)

    lang_label = ui(uid, "settings_lang_ru") if lang == "ru" else ui(uid, "settings_lang_en")
    gender_label = ui(uid, "settings_gender_m") if gender == "m" else ui(uid, "settings_gender_f")

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(ui(uid, "settings_btn_lang").format(val=lang_label), callback_data=cb_pack("settings:toggle:lang", uid)))
    kb.add(InlineKeyboardButton(ui(uid, "settings_btn_gender").format(val=gender_label), callback_data=cb_pack("settings:toggle:gender", uid)))
    return kb

def _settings_menu_text(uid: int) -> str:
    lang = get_lang(uid)
    gender = get_gender(uid)

    lang_label = ui(uid, "settings_lang_ru") if lang == "ru" else ui(uid, "settings_lang_en")
    gender_label = ui(uid, "settings_gender_m") if gender == "m" else ui(uid, "settings_gender_f")

    return f"{ui(uid, 'settings_title')}\n{ui(uid,'settings_lang')}: {lang_label}\n{ui(uid,'settings_gender')}: {gender_label}"

def show_settings_menu(chat_id: int, uid: int, prefer_edit: bool = True):
    msg_id, _stage = get_setup_msg(uid)
    text = _settings_menu_text(uid)
    kb = _settings_menu_markup(uid)

    if prefer_edit and msg_id:
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, reply_markup=kb)
            set_setup_msg(uid, msg_id, "menu")
            return
        except Exception:
            pass

    sent = bot.send_message(chat_id, text, reply_markup=kb)
    set_setup_msg(uid, sent.message_id, "menu")

def _setup_lang_markup(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row_width = 2
    kb.add(
        InlineKeyboardButton(ui(uid, "lang_ru"), callback_data="setup:lang:ru"),
        InlineKeyboardButton(ui(uid, "lang_en"), callback_data="setup:lang:en"),
    )
    return kb

def _setup_gender_markup(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row_width = 2
    kb.add(
        InlineKeyboardButton(ui(uid, "gender_m"), callback_data="setup:gender:m"),
        InlineKeyboardButton(ui(uid, "gender_f"), callback_data="setup:gender:f"),
    )
    return kb

def start_setup(chat_id: int, uid: int):
    if is_configured(uid):
        show_settings_menu(chat_id, uid, prefer_edit=True)
        return

    text = f"{ui(uid,'setup_title')}\n\n{ui(uid,'setup_choose_lang')}"
    kb = _setup_lang_markup(uid)

    msg_id, stage = get_setup_msg(uid)
    if msg_id:
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, reply_markup=kb)
            set_setup_msg(uid, msg_id, "lang")
            return
        except Exception:
            pass

    sent = bot.send_message(chat_id, text, reply_markup=kb)
    set_setup_msg(uid, sent.message_id, "lang")

# ---------- Inline query ----------
@bot.inline_handler(func=lambda q: True)
def on_inline(query):
    q_text = (query.query or "").strip()
    uid = query.from_user.id

    target = None
    target_type = None
    if q_text:
        if q_text.startswith("@"):
            target = q_text[1:]
            target_type = "username"
        elif q_text.isdigit():
            target = q_text
            target_type = "id"

    results = []

    settings_kb = InlineKeyboardMarkup()
    settings_kb.add(
        InlineKeyboardButton(
            ui(uid, "inline_open_settings_btn"),
            url=f"https://t.me/{BOT_USERNAME}?start=settings"
        )
    )
    results.append(
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=ui(uid, "inline_open_settings_title"),
            description=ui(uid, "inline_open_settings_desc"),
            input_message_content=InputTextMessageContent(ui(uid, "inline_open_settings_text")),
            reply_markup=settings_kb
        )
    )

    if target:
        title = ui(uid, "inline_shake_stranger_title")
        desc = ui(uid, "inline_shake_desc_with_target")
        cb = f"confirm:shake:target:{target_type}:{target}"
    else:
        title = ui(uid, "inline_shake_friend_title")
        desc = ui(uid, "inline_shake_desc_no_target")
        cb = "confirm:shake:random"

    shake_kb = InlineKeyboardMarkup()
    shake_kb.add(InlineKeyboardButton(ui(uid, "btn_confirm"), callback_data=cb_pack(cb, uid)))
    results.append(
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=title,
            description=desc,
            input_message_content=InputTextMessageContent(ui(uid, "inline_shake_text").format(title=title, btn=ui(uid, "btn_confirm"))),
            reply_markup=shake_kb
        )
    )

    stats_kb = InlineKeyboardMarkup()
    stats_kb.add(InlineKeyboardButton(ui(uid, "btn_show"), callback_data=cb_pack("confirm:stats:chat", uid)))
    results.append(
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=ui(uid, "inline_stats_title"),
            description=ui(uid, "inline_stats_desc"),
            input_message_content=InputTextMessageContent(ui(uid, "inline_stats_text").format(btn=ui(uid, "btn_show"))),
            reply_markup=stats_kb
        )
    )

    guard_kb = InlineKeyboardMarkup()
    guard_kb.add(InlineKeyboardButton(ui(uid, "btn_hire"), callback_data=cb_pack("confirm:guard:hire", uid)))
    results.append(
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=ui(uid, "inline_guard_title"),
            description=ui(uid, "inline_guard_desc"),
            input_message_content=InputTextMessageContent(ui(uid, "inline_guard_text").format(btn=ui(uid, "btn_hire"))),
            reply_markup=guard_kb
        )
    )

    shop_kb = InlineKeyboardMarkup()
    shop_kb.add(InlineKeyboardButton(ui(uid, "btn_confirm"), callback_data=cb_pack("confirm:shop:open", uid)))
    results.append(
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=ui(uid, "inline_shop_title"),
            description=ui(uid, "inline_shop_desc"),
            input_message_content=InputTextMessageContent(ui(uid, "inline_shop_text").format(btn=ui(uid, "btn_confirm"))),
            reply_markup=shop_kb
        )
    )

    stake = None
    m_st = re.search(r"\b(\d{1,9})\b", q_text)
    if m_st:
        try:
            stake = int(m_st.group(1))
        except Exception:
            stake = None

    contest_cb = f"confirm:contest:stake:{stake}" if stake is not None else "confirm:contest:needstake"
    contest_kb = InlineKeyboardMarkup()
    contest_kb.add(InlineKeyboardButton(ui(uid, "btn_confirm"), callback_data=cb_pack(contest_cb, uid)))
    results.append(
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=ui(uid, "inline_contest_title"),
            description=ui(uid, "inline_contest_desc"),
            input_message_content=InputTextMessageContent(ui(uid, "inline_contest_text").format(btn=ui(uid, "btn_confirm"))),
            reply_markup=contest_kb
        )
    )
    top_chat_kb = InlineKeyboardMarkup()

    top_chat_kb.add(InlineKeyboardButton(ui(uid, "btn_show"), callback_data=cb_pack("confirm:top:chat", uid)))
    results.append(
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=ui(uid, "inline_top_chat_title"),
            description=ui(uid, "inline_top_chat_desc"),
            input_message_content=InputTextMessageContent(ui(uid, "inline_top_chat_text").format(btn=ui(uid, "btn_show"))),
            reply_markup=top_chat_kb
        )
    )

    top_global_kb = InlineKeyboardMarkup()
    top_global_kb.add(InlineKeyboardButton(ui(uid, "btn_show"), callback_data=cb_pack("confirm:top:global", uid)))
    results.append(
        InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=ui(uid, "inline_top_global_title"),
            description=ui(uid, "inline_top_global_desc"),
            input_message_content=InputTextMessageContent(ui(uid, "inline_top_global_text").format(btn=ui(uid, "btn_show"))),
            reply_markup=top_global_kb
        )
    )

    bot.answer_inline_query(query.id, results, cache_time=0)



def _edit_inline_or_message(call: CallbackQuery, text: str, reply_markup=None, parse_mode: str | None = None):
    inline_id = getattr(call, "inline_message_id", None)
    if inline_id:
        bot.edit_message_text(text, inline_message_id=inline_id, reply_markup=reply_markup, parse_mode=parse_mode)
        return
    if getattr(call, "message", None):
        bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=reply_markup, parse_mode=parse_mode)

def render_shop_menu(group_key: str, uid: int):
    next_ts = ensure_shop_reset(group_key)
    qty = get_shop_qty(group_key)
    pts = get_points_chat(group_key, uid)
    lang = get_lang(uid)

    lines = [ui(uid, "shop_header"), ui(uid, "shop_balance").format(pts=pts), "" , ui(uid, "shop_choose")]
    kb = InlineKeyboardMarkup()
    for key, meta in SHOP_DEFAULTS.items():
        q = int(qty.get(key, 0))
        if q <= 0:
            continue
        name = meta["name"].get(lang, meta["name"]["ru"])
        price = int(meta["price"])
        lines.append(f"• {name} — <b>{q}</b> — <b>{price}</b>")
        kb.add(InlineKeyboardButton(name, callback_data=cb_pack(f"shop:item:{key}", uid)))
    left = max(0, int(next_ts) - int(time.time()))
    lines.append("")
    lines.append(ui(uid, "shop_reset_in").format(time=fmt_cooldown(left, lang)))

    return "\n".join(lines), kb

def render_shop_item(group_key: str, uid: int, item_key: str):
    ensure_shop_reset(group_key)
    qty = get_shop_qty(group_key).get(item_key, 0)
    lang = get_lang(uid)
    meta = SHOP_DEFAULTS[item_key]
    name = meta["name"].get(lang, meta["name"]["ru"])
    desc = meta["desc"].get(lang, meta["desc"]["ru"])
    price = int(meta["price"])
    text = ui(uid, "shop_item_info").format(name=name, price=price, qty=qty, desc=desc)
    kb = InlineKeyboardMarkup()
    if qty > 0:
        kb.add(InlineKeyboardButton(ui(uid, "btn_buy"), callback_data=cb_pack(f"shop:buy:{item_key}", uid)))
    kb.add(InlineKeyboardButton(ui(uid, "btn_back"), callback_data=cb_pack("shop:back", uid)))
    return text, kb

def render_contest_menu(group_key: str, uid: int):
    used, reset_ts, stake, left = get_contest_state(group_key, uid)
    lang = get_lang(uid)
    lines = [
        ui(uid, "contest_header"),
        ui(uid, "contest_stake").format(stake=stake),
        ui(uid, "contest_attempts").format(left=left),
        "",
        ui(uid, "contest_warning"),
        "",
    ]
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(ui(uid, "contest_btn_roulette"), callback_data=cb_pack("contest:roulette", uid)))
    return "\n".join(lines), kb

@bot.callback_query_handler(func=lambda c: c.data and (c.data.startswith("shop:") or c.data.startswith("contest:")))
def on_shop_or_contest(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        log_callback(clicker, owner, base, ok=False, group_key=compute_group_key_from_callback(call),
                     inline_message_id=getattr(call, "inline_message_id", None))
        bot.answer_callback_query(call.id, ui(clicker, "cb_not_yours_alert"), show_alert=True)
        return

    group_key = compute_group_key_from_callback(call)
    if not group_key:
        bot.answer_callback_query(call.id, "Cannot determine chat/group.", show_alert=True)
        return

    parts = base.split(":")
    kind = parts[0]

    try:
        if kind == "shop":
            action = parts[1] if len(parts) > 1 else "back"
            if action == "back":
                text, kb = render_shop_menu(group_key, clicker)
                _edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            elif action == "item" and len(parts) > 2:
                text, kb = render_shop_item(group_key, clicker, parts[2])
                _edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            elif action == "buy" and len(parts) > 2:
                ok, msg = buy_shop_item(group_key, clicker, parts[2])
                if ok:
                    text, kb = render_shop_menu(group_key, clicker)
                    text = msg + "\n\n" + text
                else:
                    text, kb = render_shop_item(group_key, clicker, parts[2])
                    text = msg + "\n\n" + text
                _edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        if kind == "contest":
            action = parts[1] if len(parts) > 1 else ""
            if action == "roulette":
                used, reset_ts, stake, left = get_contest_state(group_key, clicker)
                if left <= 0:
                    msg = ui(clicker, "contest_limit").format(time=fmt_cooldown(max(0, reset_ts - int(time.time())), get_lang(clicker)))
                    _edit_inline_or_message(call, msg, parse_mode="HTML")
                    bot.answer_callback_query(call.id)
                    return

                have = get_points_chat(group_key, clicker)
                if stake <= 0:
                    msg = ui(clicker, "contest_need_number").format(bot=BOT_USERNAME.strip("@"))
                    _edit_inline_or_message(call, msg, parse_mode="HTML")
                    bot.answer_callback_query(call.id)
                    return
                if have < stake:
                    msg = ui(clicker, "contest_not_enough").format(need=stake, have=have)
                    _edit_inline_or_message(call, msg, parse_mode="HTML")
                    bot.answer_callback_query(call.id)
                    return
                if not inc_contest_attempt(group_key, clicker):
                    msg = ui(clicker, "contest_limit").format(time=fmt_cooldown(max(0, reset_ts - int(time.time())), get_lang(clicker)))
                    _edit_inline_or_message(call, msg, parse_mode="HTML")
                    bot.answer_callback_query(call.id)
                    return

                r = random.randint(1, 15)
                mult = 0
                if r == 1:
                    mult = 3
                elif r in (2, 3):
                    mult = 2

                lang = get_lang(clicker)
                if mult > 0:
                    gain = stake * mult
                    add_points_chat(group_key, clicker, delta=gain)
                    pool = CONTEST_TEXTS.get(lang, CONTEST_TEXTS["ru"]).get("win", [])
                    chosen = random.choice(pool) if pool else ui(clicker, "contest_win")
                    msg = safe_format(chosen, gain=gain, mult=mult, stake=stake)
                    if mult == 3:
                        inc_roulette_stats(group_key, clicker, win3=1, games=1)
                    else:
                        inc_roulette_stats(group_key, clicker, win2=1, games=1)
                else:
                    eff = get_user_effects(group_key, clicker)
                    insurance_used = False
                    if eff.get("insurance", 0):
                        insurance_used = True
                        set_user_effects(group_key, clicker, insurance=0)
                    else:
                        add_points_chat(group_key, clicker, delta=-stake)
                    pool = CONTEST_TEXTS.get(lang, CONTEST_TEXTS["ru"]).get("lose", [])
                    chosen = random.choice(pool) if pool else ui(clicker, "contest_lose")
                    msg = safe_format(chosen, stake=stake, gain=0, mult=1)
                    if insurance_used:
                        msg += "\n\n" + ("Страхование сработало - ставка не списана." if lang == "ru" else "Insurance triggered - stake was not deducted.")
                    spent_delta = stake if not insurance_used else 0
                    inc_roulette_stats(group_key, clicker, lose=1, games=1, spent=spent_delta)

                _u, _rt, _st, left2 = get_contest_state(group_key, clicker)
                kb = InlineKeyboardMarkup()
                if left2 > 0:
                    kb.add(InlineKeyboardButton(ui(clicker, "contest_btn_play_again"), callback_data=cb_pack("contest:roulette", clicker)))
                kb.add(InlineKeyboardButton(ui(clicker, "btn_back"), callback_data=cb_pack("contest:back", clicker)))
                _edit_inline_or_message(call, msg, reply_markup=kb, parse_mode="HTML")
                bot.answer_callback_query(call.id)
                return

            if action == "back":
                text, kb = render_contest_menu(group_key, clicker)
                _edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
                bot.answer_callback_query(call.id)
                return

    except Exception:
        logger.exception("shop/contest handler failed")
        bot.answer_callback_query(call.id, "Failed.", show_alert=True)

# ---------- Confirm handlers ----------
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("confirm:"))
def on_confirm(call: CallbackQuery):
    user = call.from_user
    raw = call.data
    base, owner = cb_unpack(raw)
    if owner is not None and owner != call.from_user.id:
        gk = compute_group_key_from_callback(call)
        log_callback(call.from_user.id, owner, base, ok=False, group_key=gk, inline_message_id=getattr(call, "inline_message_id", None))
        bot.answer_callback_query(call.id, ui(call.from_user.id, "not_your_button"), show_alert=True)
        return

    data = base
    parts = data.split(":", 5)
    kind = parts[1] if len(parts) > 1 else None

    inline_id = getattr(call, "inline_message_id", None)
    group_key = compute_group_key_from_callback(call)

    if group_key and inline_id:
        observed_chat = call.message.chat.id if getattr(call, "message", None) and getattr(call.message, "chat", None) else None
        record_group_example(group_key, inline_id, observed_chat)


    if kind == "shop":
        if not group_key:
            bot.answer_callback_query(call.id, "Cannot determine chat/group.", show_alert=True)
            return
        text, kb = render_shop_menu(group_key, user.id)
        try:
            _edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            log_callback(call.from_user.id, owner, base, ok=True, group_key=group_key, inline_message_id=getattr(call, "inline_message_id", None))
            bot.answer_callback_query(call.id)
        except Exception:
            bot.answer_callback_query(call.id, "Failed.", show_alert=True)
        return

    if kind == "contest":
        if not group_key:
            bot.answer_callback_query(call.id, "Cannot determine chat/group.", show_alert=True)
            return

        sub = parts[2] if len(parts) > 2 else "needstake"
        if sub == "needstake":
            txt = ui(user.id, "contest_need_number").format(bot=BOT_USERNAME.strip("@"))
            _edit_inline_or_message(call, txt, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        if sub == "stake":
            stake = 0
            try:
                stake = int(parts[3]) if len(parts) > 3 else 0
            except Exception:
                stake = 0

            have = get_points_chat(group_key, user.id)
            if stake <= 0:
                txt = ui(user.id, "contest_need_number").format(bot=BOT_USERNAME.strip("@"))
                _edit_inline_or_message(call, txt, parse_mode="HTML")
                bot.answer_callback_query(call.id)
                return
            if have < stake:
                txt = ui(user.id, "contest_not_enough").format(need=stake, have=have)
                _edit_inline_or_message(call, txt, parse_mode="HTML")
                bot.answer_callback_query(call.id)
                return

            set_contest_stake(group_key, user.id, stake)
            text, kb = render_contest_menu(group_key, user.id)
            _edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return
    if kind == "shake":
        reset_attempts_if_needed(user.id)
        curr_count, last_ts, cooldown_until = get_attempts(user.id)
        bonus = 0
        if group_key:
            bonus = get_user_effects(group_key, user.id).get("bonus_attempts", 0)
            if bonus and curr_count == 0 and (last_ts == 0) and (cooldown_until == 0):
                set_user_effects(group_key, user.id, bonus_attempts=0)
                bonus = 0
        limit = 3 + int(bonus or 0)
        if curr_count >= limit:
            left = time_to_reset(user.id)
            lang = get_lang(user.id)
            limit_text = ui(user.id, "limit_reached_text").format(time=fmt_cooldown(left, lang))

            try:
                if inline_id:
                    bot.edit_message_text(limit_text, inline_message_id=inline_id)
                elif getattr(call, "message", None):
                    bot.edit_message_text(limit_text, chat_id=call.message.chat.id, message_id=call.message.message_id)
                bot.answer_callback_query(call.id)
            except Exception:
                bot.answer_callback_query(call.id, limit_text, show_alert=True)
            return

        prev_count = curr_count
        inc_attempt(user.id)
        if group_key and prev_count >= 3 and bonus > 0:
            set_user_effects(group_key, user.id, bonus_attempts=bonus-1)

        if group_key:
            try:
                register_user_group(group_key, inline_id, user)
            except:
                pass

        mode = parts[2] if len(parts) > 2 else "random"
        if mode == "payrepeat":
            target_type = parts[3] if len(parts) > 3 else "username"
            target_value = parts[4] if len(parts) > 4 else ""

            if not group_key:
                bot.answer_callback_query(call.id, "Cannot determine chat/group.", show_alert=True)
                return

            have = get_points_chat(group_key, user.id)
            if have < 100:
                text = ui(user.id, "not_enough_points").format(need=100, have=have)
                if inline_id:
                    bot.edit_message_text(text, inline_message_id=inline_id, parse_mode="HTML")
                else:
                    bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML")
                bot.answer_callback_query(call.id)
                return

            spend_points_chat(group_key, user.id, 100)

            parts = ["confirm", "shake", "target", target_type, target_value]
            mode = "target"

        if mode == "target" and group_key:
            st_last, st_rem, st_r1, st_r2 = get_chat_state(group_key, user.id)

            ttype = parts[3] if len(parts) > 3 else "username"
            tval = parts[4] if len(parts) > 4 else ""
            norm_val = tval.lower().lstrip("@")
            target_key = f"{ttype}:{norm_val}"

            if st_rem > 0 and st_last == target_key:
                text = ui(user.id, "repeat_target_block").format(left=st_rem)
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton(ui(user.id, "btn_pay_repeat"), callback_data=cb_pack(f"confirm:shake:payrepeat:{ttype}:{tval}", user.id)))
                kb.add(InlineKeyboardButton(ui(user.id, "btn_cancel"), callback_data=cb_pack("confirm:top:chat", user.id)))  # harmless cancel: show chat top or replace with any noop you prefer

                if inline_id:
                    bot.edit_message_text(text, inline_message_id=inline_id, reply_markup=kb, parse_mode="HTML")
                else:
                    bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=kb, parse_mode="HTML")
                bot.answer_callback_query(call.id)
                return

        target_display = "stranger"
        mention_html = None


        eff = {"bonus_attempts": 0, "wine_state": 0, "bouquet": 0, "insurance": 0, "candy": 0}
        if group_key:
            eff = get_user_effects(group_key, user.id)

        protected = False
        target_uid_known = None
        target_uname_known = None
        target_fname = None
        target_lname = None

        if mode == "random":
            if not group_key:
                bot.answer_callback_query(call.id, "Cannot determine chat/group.", show_alert=True)
                return
            rows = get_users_for_group(group_key)
            st_last, st_rem, st_r1, st_r2 = get_chat_state(group_key, user.id)
            if not rows:
                text = ui(user.id, "no_users")
                try:
                    if inline_id:
                        bot.edit_message_text(text, inline_message_id=inline_id)
                    elif getattr(call, "message", None):
                        bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id)
                    bot.answer_callback_query(call.id)
                except Exception:
                    bot.answer_callback_query(call.id, text, show_alert=True)
                return

            filtered = [r for r in rows if r[0] not in (st_r1, st_r2)]
            use_rows = filtered if filtered else rows
            candidates = [r for r in use_rows if r[0] != user.id] or use_rows

            target_uid, target_uname, target_fname, target_lname, _ = random.choice(candidates)
            set_chat_state(group_key, user.id, st_last, st_rem, target_uid, st_r1)

            target_uid_known = target_uid
            target_uname_known = target_uname

            if target_uname:
                target_display = f"@{target_uname}"
                mention_html = target_display
            else:
                display = " ".join([p for p in [target_fname, target_lname] if p]) or str(target_uid)
                target_display = display
                mention_html = f'<a href="tg://user?id={target_uid}">{display}</a>'

        else:
            target_type = parts[3] if len(parts) > 3 else "username"
            target_value = parts[4] if len(parts) > 4 else ""

            if target_type == "id":
                try:
                    tid = int(target_value)
                    target_uid_known = tid
                    target_display = str(tid)
                    mention_html = f'<a href="tg://user?id={tid}">{target_display}</a>'
                except Exception:
                    target_display = target_value
                    mention_html = target_display
            else:
                target_uname_known = target_value.lstrip("@")
                target_display = f"@{target_uname_known}" if target_uname_known else target_value
                mention_html = target_display

                if group_key and target_uname_known:
                    cur.execute(
                        "SELECT user_id, first_name, last_name FROM stats_chat WHERE group_key=? AND username=? LIMIT 1",
                        (group_key, target_uname_known),
                    )
                    row = cur.fetchone()
                    if row:
                        target_uid_known = int(row[0])
                        target_fname = row[1]
                        target_lname = row[2]
                    else:
                        cur.execute(
                            "SELECT user_id, first_name, last_name FROM stats WHERE username=? LIMIT 1",
                            (target_uname_known,),
                        )
                        row = cur.fetchone()
                        if row:
                            target_uid_known = int(row[0])
                            target_fname = row[1]
                            target_lname = row[2]

        bouquet_used = False
        if group_key and target_uid_known and has_bodyguard(group_key, target_uid_known):
            protected = True
            ensure_user_stats_chat(
                group_key,
                user.id,
                getattr(user, "username", None),
                getattr(user, "first_name", None),
                getattr(user, "last_name", None),
            )
            ensure_user_stats_chat(group_key, target_uid_known, target_uname_known, target_fname, target_lname)

            cur.execute("SELECT success_count FROM stats_chat WHERE group_key=? AND user_id=?", (group_key, user.id))
            my_rank = int(cur.fetchone()[0] or 0)

            cur.execute("SELECT success_count FROM stats_chat WHERE group_key=? AND user_id=?", (group_key, target_uid_known))
            tg_rank = int(cur.fetchone()[0] or 0)

            p = (1/4) if tg_rank < my_rank else (1/8)
            if eff.get("bouquet", 0):
                p = 1/2
                bouquet_used = True
            success = random.random() < p

            consume_bodyguard_use(group_key, target_uid_known)
        else:
            success = random.random() < (2/3)

        if group_key and eff.get("wine_state", 0) == 2:
            success = True
            set_user_effects(group_key, user.id, wine_state=1)
            eff["wine_state"] = 1
        elif group_key and eff.get("wine_state", 0) == 1:
            success = False
            set_user_effects(group_key, user.id, wine_state=0)
            eff["wine_state"] = 0

        if group_key and bouquet_used:
            set_user_effects(group_key, user.id, bouquet=0)
            add_cooldown_penalty(user.id, 1800)

        if group_key and (not success) and eff.get("candy", 0):
            set_user_effects(group_key, user.id, candy=0)
            if random.random() < 0.5:
                dec_attempt(user.id)
        success_delta = 1
        extra_line = ""

        if protected and success:
            success_delta = 2
            extra_line = "\n" + ui(user.id, "guard_effect_success")
        if protected and (not success):
            add_cooldown_penalty(user.id, 3600)
            extra_line = "\n" + ui(user.id, "guard_effect_fail")

        if success:
            update_stats_on_result(user.id, getattr(user, "username", None), getattr(user, "first_name", None), getattr(user, "last_name", None),
                                   success_delta=success_delta, total_delta=1)
            update_stats_chat_on_result(group_key, user.id, getattr(user, "username", None), getattr(user, "first_name", None), getattr(user, "last_name", None),
                                        success_delta=success_delta, total_delta=1)
            outcome_key = "success"
        else:
            update_stats_on_result(user.id, getattr(user, "username", None), getattr(user, "first_name", None), getattr(user, "last_name", None),
                                   success_delta=0, total_delta=1)
            update_stats_chat_on_result(group_key, user.id, getattr(user, "username", None), getattr(user, "first_name", None), getattr(user, "last_name", None),
                                        success_delta=0, total_delta=1)
            outcome_key = "fail"

        if protected:
            outcome_key = "guard_success" if success else "guard_fail"

        if group_key:
            if mode == "random" and target_uid_known:
                tkey = f"id:{target_uid_known}"
                record_target_attempt(group_key, user.id, tkey, target_uid_known, target_uname_known, target_fname if mode=="random" else None, target_lname if mode=="random" else None, success)
            elif mode == "target":
                ttype = parts[3] if len(parts) > 3 else "username"
                tval = parts[4] if len(parts) > 4 else ""
                if ttype == "id" and tval.isdigit():
                    tid = int(tval)
                    tkey = f"id:{tid}"
                    record_target_attempt(group_key, user.id, tkey, tid, None, None, None, success)
                else:
                    uname = tval.lstrip("@").lower()
                    tkey = f"username:{uname}"
                    record_target_attempt(group_key, user.id, tkey, None, uname, None, None, success)

        if group_key:
            st_last, st_rem, st_r1, st_r2 = get_chat_state(group_key, user.id)
            if st_rem > 0:
                st_rem -= 1

            if mode == "target":
                ttype = parts[3] if len(parts) > 3 else "username"
                tval = parts[4] if len(parts) > 4 else ""
                norm_val = tval.lower().lstrip("@")
                st_last = f"{ttype}:{norm_val}"
                st_rem = 3

            set_chat_state(group_key, user.id, st_last, st_rem, st_r1, st_r2)

        lang = get_lang(user.id)
        gender = get_gender(user.id)
        pool = OUTCOMES.get(lang, OUTCOMES["ru"]).get(gender, {}).get(outcome_key, [])
        chosen = random.choice(pool) if pool else ("{who} ..." )

        who_display = f"@{user.username}" if getattr(user, "username", None) else (user.first_name or str(user.id))
        if mention_html:
            final_text = safe_format(chosen, who=who_display, target=mention_html)
            parse_mode = "HTML"
        else:
            final_text = safe_format(chosen, who=who_display, target=target_display)
            parse_mode = None
        
        points_lines = ""
        if success and group_key:
            base_pts = 50
            add_points_chat(group_key, user.id,
                            getattr(user, "username", None),
                            getattr(user, "first_name", None),
                            getattr(user, "last_name", None),
                            delta=base_pts)
            points_lines += "\n" + ui(user.id, "points_gain").format(n=base_pts)

            if random.random() < (1/8):
                bonus_pts = 100
                add_points_chat(group_key, user.id, delta=bonus_pts)
                points_lines += "\n" + ui(user.id, "points_bonus").format(n=bonus_pts)

        final_text = final_text + points_lines + (extra_line if 'extra_line' in locals() else "")
        parse_mode = "HTML" if (parse_mode or points_lines or (extra_line if 'extra_line' in locals() else "")) else None

        try:
            if inline_id:
                bot.edit_message_text(final_text, inline_message_id=inline_id, parse_mode=parse_mode)
            elif getattr(call, "message", None):
                bot.edit_message_text(final_text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode=parse_mode)
            gk = compute_group_key_from_callback(call)
            log_callback(call.from_user.id, owner, base, ok=True, group_key=gk, inline_message_id=getattr(call, "inline_message_id", None))
            bot.answer_callback_query(call.id)
        except Exception:
            bot.answer_callback_query(call.id, "Failed to send result.", show_alert=True)
        return
    
    if kind == "stats":
        mode = parts[2] if len(parts) > 2 else "chat"
        if mode != "chat" or not group_key:
            bot.answer_callback_query(call.id, "Cannot determine chat/group.", show_alert=True)
            return

        ensure_user_stats_chat(group_key, user.id, getattr(user, "username", None), getattr(user, "first_name", None), getattr(user, "last_name", None))
        cur.execute("SELECT success_count, total_count, points FROM stats_chat WHERE group_key=? AND user_id=?", (group_key, user.id))
        suc, tot, pts = cur.fetchone()
        suc = int(suc or 0); tot = int(tot or 0); pts = int(pts or 0)
        fail = max(0, tot - suc)
        pct = (suc / tot * 100.0) if tot > 0 else 0.0

        top_targets = get_top_targets(group_key, user.id, limit=3)

        lines = []
        lines.append(f"{ui(user.id,'stats_header')}")
        lines.append(f"{ui(user.id,'stats_success')}: <b>{suc}</b>")
        lines.append(f"{ui(user.id,'stats_fail')}: <b>{fail}</b>")
        lines.append(f"{ui(user.id,'stats_total')}: <b>{tot}</b>")
        lines.append(f"{ui(user.id,'stats_pct')}: <b>{pct:.1f}%</b>")
        lines.append(f"{ui(user.id,'stats_points')}: <u><b>{pts}</b>ꙓ</u>")

        w2, w3, rlose, rgames, rspent = get_roulette_stats(group_key, user.id)
        if rgames > 0:
            p2 = w2 / rgames * 100.0
            p3 = w3 / rgames * 100.0
            pl = rlose / rgames * 100.0
        else:
            p2 = p3 = pl = 0.0
        
        lang = get_lang(user.id)
        if lang == "ru":
            roulette_title = "🎰 Рулетка"
            lbl_w2 = "×2"
            lbl_w3 = "×3"
            lbl_l = "Проигрышей"
            lbl_games = "Всего игр"
            lbl_spent = "Потрачено"
            purchases_lbl = "Покупки"
            empty_lbl = "Пусто"
        else:
            roulette_title = "🎰 Roulette"
            lbl_w2 = "×2"
            lbl_w3 = "×3"
            lbl_l = "Losses"
            lbl_games = "Total games"
            lbl_spent = "Spent"
            purchases_lbl = "Purchases"
            empty_lbl = "Empty"
        
        lines.append("")
        lines.append(f"<u>{roulette_title}</u>:")
        lines.append(f"{lbl_w2}: <u><b>{w2}</b></u> (<b>{p2:.1f}%</b>)")
        lines.append(f"{lbl_w3}: <u><b>{w3}</b></u> (<b>{p3:.1f}%</b>)")
        lines.append(f"{lbl_l}: <u><b>{rlose}</b></u> (<b>{pl:.1f}%</b>)")
        lines.append(f"{lbl_games}: <u><b>{rgames}</b></u>")
        lines.append(f"{lbl_spent}: <u><b>{rspent}ꙓ</b></u>")

        purchases = get_shop_purchases(group_key, user.id)
        if not purchases:
            purchases_str = empty_lbl
        else:
            parts_p = []
            for item_key, qty in purchases:
                nm = SHOP_DEFAULTS.get(item_key, {}).get("name", {}).get(lang, item_key)
                parts_p.append(f"{nm} (<u><b>{qty}</b></u>)")
            purchases_str = ", ".join(parts_p)
        lines.append(f"{purchases_lbl}: {purchases_str}")

        lines.append("")
        lines.append(f"<u>{ui(user.id,'stats_top_targets')}</u>:")

        if not top_targets:
            lines.append("—")
        else:
            for i, (_tkey, _tid, tuname, att, sc) in enumerate(top_targets, start=1):
                att = int(att or 0); sc = int(sc or 0)
                rate = (sc / att * 100.0) if att > 0 else 0.0
                label = f"@{tuname}" if tuname else _tkey
                lines.append(f"{i}) {label} — <b>{rate:.1f}%</b> (<b>{sc}</b>/<b>{att}</b>)")

        final = "\n".join(lines)

        try:
            if inline_id:
                bot.edit_message_text(final, inline_message_id=inline_id, parse_mode="HTML")
            elif getattr(call, "message", None):
                bot.edit_message_text(final, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML")
            bot.answer_callback_query(call.id)
        except Exception:
            bot.answer_callback_query(call.id, "Failed.", show_alert=True)
        return
    
    if kind == "guard":
        action = parts[2] if len(parts) > 2 else ""
        if action != "hire" or not group_key:
            bot.answer_callback_query(call.id, "Cannot determine chat/group.", show_alert=True)
            return

        if has_bodyguard(group_key, user.id):
            text = ui(user.id, "guard_hired_already")
            try:
                if inline_id:
                    bot.edit_message_text(text, inline_message_id=inline_id, parse_mode="HTML")
                elif getattr(call, "message", None):
                    bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML")
                bot.answer_callback_query(call.id)
            except Exception:
                bot.answer_callback_query(call.id, text, show_alert=True)
            return

        have = get_points_chat(group_key, user.id)
        if have < 600:
            text = ui(user.id, "not_enough_points").format(need=600, have=have)
            try:
                if inline_id:
                    bot.edit_message_text(text, inline_message_id=inline_id, parse_mode="HTML")
                elif getattr(call, "message", None):
                    bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML")
                bot.answer_callback_query(call.id)
            except Exception:
                bot.answer_callback_query(call.id, text, show_alert=True)
            return

        spend_points_chat(group_key, user.id, 600)
        hire_bodyguard(group_key, user.id)
        text = ui(user.id, "guard_hired_ok")

        try:
            if inline_id:
                bot.edit_message_text(text, inline_message_id=inline_id, parse_mode="HTML")
            elif getattr(call, "message", None):
                bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML")
            bot.answer_callback_query(call.id)
        except Exception:
            bot.answer_callback_query(call.id, "Failed.", show_alert=True)
        return

    if kind == "top":
        mode = parts[2] if len(parts) > 2 else "chat"

        if mode == "chat":
            if not group_key:
                bot.answer_callback_query(call.id, "Cannot determine chat/group.", show_alert=True)
                return
            rows = get_users_for_group(group_key)
            if not rows:
                text = ui(user.id, "no_users")
                try:
                    if inline_id:
                        bot.edit_message_text(text, inline_message_id=inline_id)
                    elif getattr(call, "message", None):
                        bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id)
                    bot.answer_callback_query(call.id)
                except Exception:
                    bot.answer_callback_query(call.id, text, show_alert=True)
                return

            uids = [r[0] for r in rows]
            cur.execute(
                "SELECT user_id, username, first_name, last_name, success_count, total_count FROM stats_chat "
                f"WHERE group_key=? AND user_id IN ({','.join('?'*len(uids))})",
                tuple([group_key] + uids)
            )

            stats_rows = cur.fetchall()
            m = {r[0]: (r[1], r[2], r[3], r[4], r[5]) for r in stats_rows}

            list_items = []
            for uid_ in uids:
                uname, fname, lname, suc, tot = m.get(uid_, (None, None, None, 0, 0))
                list_items.append((uid_, uname, fname, lname, suc, tot))
            list_items.sort(key=lambda x: (x[4], x[5]), reverse=True)

            lines = []
            for idx, (uid_, uname, fname, lname, suc, tot) in enumerate(list_items, start=1):
                lines.append(f"{idx}. {format_name_html(uname, fname, lname, uid_)} — {suc} / {tot}")
            
            final = ui(user.id, "top_header_chat") + "\n" + "\n".join(lines)

            try:
                if inline_id:
                   bot.edit_message_text(final, inline_message_id=inline_id, parse_mode="HTML")
                elif getattr(call, "message", None):
                    bot.edit_message_text(final, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML")
                bot.answer_callback_query(call.id)
            except Exception:
                bot.answer_callback_query(call.id, "Failed to show top.", show_alert=True)
            return

        cur.execute(
            "SELECT user_id, username, first_name, last_name, success_count, total_count "
            "FROM stats WHERE (success_count<>0 OR total_count<>0) "
            "ORDER BY success_count DESC, total_count DESC LIMIT 50"
        )
        rows = cur.fetchall()
        if not rows:
            bot.answer_callback_query(call.id, "No data.", show_alert=True)
            return

        lines = []
        for idx, (uid_, uname, fname, lname, suc, tot) in enumerate(rows, start=1):
            display = uname or fname or str(uid_)
            lines.append(f"{idx}. {format_name_html(uname, fname, lname, uid_)} — {suc} / {tot}")
        final = ui(user.id, "top_header_global") + "\n" + "\n".join(lines)

        try:
            if inline_id:
                bot.edit_message_text(final, inline_message_id=inline_id, parse_mode="HTML")
            elif getattr(call, "message", None):
                bot.edit_message_text(final, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            gk = compute_group_key_from_callback(call)
            log_callback(call.from_user.id, owner, base, ok=True, group_key=gk, inline_message_id=getattr(call, "inline_message_id", None))
        except Exception:
            bot.answer_callback_query(call.id, "Failed to show top.", show_alert=True)
        return

# ---------- setup callbacks (single message edit) ----------
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("setup:"))
def on_setup(call: CallbackQuery):
    uid = call.from_user.id
    ensure_settings(uid)
    _, typ, val = call.data.split(":", 2)
    msg_id, stage = get_setup_msg(uid)
    chat_id = call.message.chat.id

    if typ == "lang" and val in ("ru", "en"):
        set_lang(uid, val)

        text = f"{ui(uid,'setup_title')}\n\n{ui(uid,'setup_choose_gender')}"
        kb = _setup_gender_markup(uid)
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb)
            set_setup_msg(uid, call.message.message_id, "gender")
            bot.answer_callback_query(call.id)
        except Exception:
            bot.answer_callback_query(call.id, "Failed.", show_alert=True)
        return

    if typ == "gender" and val in ("m", "f"):
        set_gender(uid, val)
        set_configured(uid, True)
        text = ui(uid, "setup_done")
        kb = _settings_menu_markup(uid)
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb)
            set_setup_msg(uid, call.message.message_id, "menu")
            bot.answer_callback_query(call.id)
        except Exception:
            bot.answer_callback_query(call.id, "Failed.", show_alert=True)
        return
    bot.answer_callback_query(call.id, "Invalid.", show_alert=True)

# ---------- settings menu callbacks ----------
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("settings:toggle:"))
def on_settings_toggle(call: CallbackQuery):
    uid = call.from_user.id
    ensure_settings(uid)
    base, owner = cb_unpack(call.data)
    if owner is not None and owner != uid:
        bot.answer_callback_query(call.id, ui(uid, "not_your_button"), show_alert=True)
        return
    what = base.split(":", 2)[2]
    lang = get_lang(uid)
    gender = get_gender(uid)
    if what == "lang":
        lang = "en" if lang == "ru" else "ru"
        set_lang(uid, lang)
    elif what == "gender":
        gender = "f" if gender == "m" else "m"
        set_gender(uid, gender)
    try:
        bot.edit_message_text(
            _settings_menu_text(uid),
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=_settings_menu_markup(uid),
        )
        set_setup_msg(uid, call.message.message_id, "menu")
        bot.answer_callback_query(call.id)
    except Exception:
        bot.answer_callback_query(call.id, "Failed.", show_alert=True)

# ---------- /start and /settings ----------
@bot.message_handler(commands=["start"])
def cmd_start(message):
    uid = message.from_user.id
    ensure_settings(uid)
    payload = ""
    parts = message.text.split(maxsplit=1)
    if len(parts) == 2:
        payload = parts[1].strip().lower()
    if payload == "settings":
        if is_configured(uid):
            show_settings_menu(message.chat.id, uid, prefer_edit=True)
        else:
            start_setup(message.chat.id, uid)
        return
    if is_configured(uid):
        show_settings_menu(message.chat.id, uid, prefer_edit=True)
        return
    start_setup(message.chat.id, uid)

@bot.message_handler(commands=["settings"])
def cmd_settings(message):
    uid = message.from_user.id
    ensure_settings(uid)
    show_settings_menu(message.chat.id, uid, prefer_edit=True)

# ---------- Owner tools ----------
def is_owner(msg):
    return msg.from_user and msg.from_user.id == OWNER_ID

@bot.message_handler(commands=["dump_groups"])
def cmd_dump_groups(message):
    if not is_owner(message):
        return
    cur.execute("SELECT group_key, sample_inline_id, observed_chat_id, last_seen FROM group_examples ORDER BY last_seen DESC LIMIT 20")
    rows = cur.fetchall()
    if not rows:
        bot.reply_to(message, "Нет данных про группы.")
        return
    lines = []
    for gk, sid, oc, ls in rows:
        lines.append(f"{gk} — sample:{sid} chat_observed:{oc} last_seen:{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ls))}")
    bot.reply_to(message, "Группы:\n" + "\n".join(lines))

@bot.message_handler(commands=["dump_callbacks", "dump_regs", "list_massage"])
def cmd_owner_misc(message):
    if not is_owner(message):
        return
    cmd = message.text.split()[0].lstrip("/")
    if cmd == "dump_callbacks":
        cur.execute("""
            SELECT ts, clicker_id, owner_id, ok, base_data, group_key, inline_message_id
            FROM callback_logs
            ORDER BY ts DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        if not rows:
            bot.reply_to(message, "Нет истории коллбеков.")
            return
        lines = []
        for ts, clicker, owner, ok, base, gk, iid in rows:
            lines.append(
                f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))} "
                f"clicker:{clicker} owner:{owner} ok:{ok} data:{base} gk:{gk} iid:{(iid[:20]+'…') if iid else None}"
            )
        bot.reply_to(message, "Последние коллбеки:\n" + "\n".join(lines))
        return
    elif cmd == "dump_regs":
        cur.execute("SELECT user_id, username, first_name, group_key, ts FROM registrations ORDER BY ts DESC LIMIT 20")
        rows = cur.fetchall()
        if not rows:
            bot.reply_to(message, "Нет регистраций.")
            return
        lines = []
        for uid, uname, fname, gk, ts in rows:
            lines.append(f"{uid} ({uname or fname}) — {gk} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))}")
        bot.reply_to(message, "Регистрации:\n" + "\n".join(lines))
    elif cmd == "list_massage":
        bot.reply_to(message, "list_massage placeholder — вывод последних сообщений не реализован здесь.")
    else:
        bot.reply_to(message, "Неизвестная команда.")

@bot.message_handler(commands=["add"])
def cmd_admin_add_success(message):
    if not is_owner(message):
        return
    parts = message.text.split()
    if len(parts) != 3:
        bot.reply_to(message, "Использование: /add <user_id> <amount>")
        return
    try:
        uid = int(parts[1])
        amt = int(parts[2])
    except:
        bot.reply_to(message, "Неверные параметры.")
        return
    ensure_user_stats(uid)
    update_stats_on_result(uid, None, None, None, success_delta=amt, total_delta=amt)
    bot.reply_to(message, f"Добавлено {amt} удачных рукопожатий пользователю {uid}.")

@bot.message_handler(commands=["refresh"])
def cmd_admin_refresh_users(message):
    if not is_owner(message):
        return
    parts = message.text.split()
    limit = 200
    if len(parts) >= 2 and parts[1].isdigit():
        limit = int(parts[1])
    cur.execute("""
        SELECT user_id FROM stats
        UNION
        SELECT user_id FROM registrations
        LIMIT ?
    """, (limit,))
    uids = [r[0] for r in cur.fetchall()]
    ok = 0
    fail = 0
    for uid in uids:
        try:
            ch = bot.get_chat(uid) 
            username = getattr(ch, "username", None)
            first_name = getattr(ch, "first_name", None)
            last_name = getattr(ch, "last_name", None)

            cur.execute(
                "UPDATE stats SET username=?, first_name=?, last_name=?, last_update=? WHERE user_id=?",
                (username, first_name, last_name, int(time.time()), uid)
            )
            cur.execute(
                "UPDATE stats_chat SET username=?, first_name=?, last_name=?, last_update=? WHERE user_id=?",
                (username, first_name, last_name, int(time.time()), uid)
            )
            cur.execute(
                "UPDATE registrations SET username=?, first_name=?, last_name=? WHERE user_id=?",
                (username, first_name, last_name, uid)
            )
            conn.commit()
            ok += 1
        except Exception:
            fail += 1
    bot.reply_to(message, f"Готово. Обновлено: {ok}, ошибок/недоступно: {fail}.")

@bot.message_handler(commands=["delete_user"])
def cmd_admin_delete_user(message):
    if not is_owner(message):
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        bot.reply_to(message, "Использование: /delete_user <user_id>")
        return
    uid = int(parts[1])
    cur.execute("DELETE FROM stats WHERE user_id=?", (uid,))
    cur.execute("DELETE FROM stats_chat WHERE user_id=?", (uid,))
    cur.execute("DELETE FROM registrations WHERE user_id=?", (uid,))
    cur.execute("DELETE FROM attempts WHERE user_id=?", (uid,))
    cur.execute("DELETE FROM settings WHERE user_id=?", (uid,))
    cur.execute("DELETE FROM roulette_stats WHERE user_id=?", (uid,))
    cur.execute("DELETE FROM shop_purchases WHERE user_id=?", (uid,))
    conn.commit()
    bot.reply_to(message, f"Удалены данные пользователя {uid} (stats/stats_chat/registrations/attempts/settings/roulette_stats/shop_purchases).")

# ---------- DB maintenance (auto refresh & cleanup) ----------
def _maintenance_tick():
    """Best-effort DB hygiene:
    - keep names filled from registrations/stats_chat
    - prune long-unused rows
    """
    now = int(time.time())
    try:
        cur.execute(
            """
            UPDATE stats_chat
            SET
              username = COALESCE(username, (SELECT r.username FROM registrations r WHERE r.group_key=stats_chat.group_key AND r.user_id=stats_chat.user_id)),
              first_name = COALESCE(first_name, (SELECT r.first_name FROM registrations r WHERE r.group_key=stats_chat.group_key AND r.user_id=stats_chat.user_id)),
              last_name = COALESCE(last_name, (SELECT r.last_name FROM registrations r WHERE r.group_key=stats_chat.group_key AND r.user_id=stats_chat.user_id))
            WHERE username IS NULL OR first_name IS NULL OR last_name IS NULL
            """
        )
        cur.execute(
            """
            UPDATE stats
            SET
              username = COALESCE(username, (SELECT sc.username FROM stats_chat sc WHERE sc.user_id=stats.user_id ORDER BY sc.last_update DESC LIMIT 1)),
              first_name = COALESCE(first_name, (SELECT sc.first_name FROM stats_chat sc WHERE sc.user_id=stats.user_id ORDER BY sc.last_update DESC LIMIT 1)),
              last_name = COALESCE(last_name, (SELECT sc.last_name FROM stats_chat sc WHERE sc.user_id=stats.user_id ORDER BY sc.last_update DESC LIMIT 1))
            WHERE username IS NULL OR first_name IS NULL OR last_name IS NULL
            """
        )
        cur.execute("DELETE FROM callback_logs WHERE ts < ?", (now - 30 * 86400,))
        cur.execute("DELETE FROM target_stats WHERE last_seen IS NOT NULL AND last_seen < ?", (now - 120 * 86400,))
        cur.execute("DELETE FROM registrations WHERE COALESCE(NULLIF(last_seen, 0), ts) < ?", (now - 120 * 86400,))
        conn.commit()
    except Exception:
        logger.exception("Maintenance tick failed")

def start_maintenance_thread():
    def _loop():
        while True:
            _maintenance_tick()
            time.sleep(3600)
    t = threading.Thread(target=_loop, name="db_maintenance", daemon=True)
    t.start()

# ---------- Main ----------
if __name__ == "__main__":
    logger.info("Bot started as @%s", BOT_USERNAME)
    start_maintenance_thread()
    bot.infinity_polling(timeout=60, long_polling_timeout=5)
