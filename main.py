import telegram
print("python-telegram-bot version:", telegram.__version__)

import logging
from datetime import datetime
import pytz
import gspread
import os
import sys
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters
)
from oauth2client.service_account import ServiceAccountCredentials
import time
import json

# Додаємо поточну директорію до шляху пошуку Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Налаштування логування
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфігурація (використовуємо змінні оточення)
SHEET_ID = os.getenv('SHEET_ID', '1fobxr4QwD8CLYFaTh2WXNbGwqQ2mWEuQDPkqDzvzkoU')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TIMEZONE = pytz.timezone('Europe/Kiev')
ORDER_CHAT_ID = int(os.getenv('ORDER_CHAT_ID', '-1002501381102'))
ORDER_TOPIC_ID = int(os.getenv('ORDER_TOPIC_ID', '914'))

# Для сервісного акаунта Google Sheets
SERVICE_ACCOUNT_JSON = os.getenv('SERVICE_ACCOUNT_JSON')

# Перевірка обов'язкових змінних
if not TELEGRAM_TOKEN:
    logger.error("❌ TELEGRAM_TOKEN не встановлено!")
    sys.exit(1)

if not SERVICE_ACCOUNT_JSON:
    logger.error("❌ SERVICE_ACCOUNT_JSON не встановлено!")
    sys.exit(1)

# Кеш для продуктів
PRODUCTS_CACHE = None
PRODUCTS_CACHE_TIME = 0
CACHE_DURATION = 300  # 5 хвилин

# ====== ОПТИМІЗОВАНІ ФУНКЦІЇ ДЛЯ GOOGLE SHEETS ======

def connect_to_google_sheets():
    """Підключення до Google Sheets з кешуванням"""
    try:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Використовуємо JSON з змінної оточення
        service_account_info = json.loads(SERVICE_ACCOUNT_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"Помилка підключення до Google Sheets: {e}")
        raise

def get_user_data(telegram_username):
    """Отримати дані користувача"""
    try:
        clean_username = telegram_username.lstrip('@').lower().strip()
        
        client = connect_to_google_sheets()
        sheet = client.open_by_key(SHEET_ID).worksheet('Баланси')
        all_values = sheet.get_all_values()
        
        if len(all_values) < 2:
            return None
        
        headers = all_values[0]
        
        # Пошук колонок
        name_col = -1
        static_id_col = -1
        tg_col = -1
        total_col = -1
        spent_col = -1
        actual_col = -1
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            if any(x in header_lower for x in ['ім\'я', 'имя', 'name', 'сотрудника']):
                name_col = i
            elif any(x in header_lower for x in ['static', 'статик']):
                static_id_col = i
            elif any(x in header_lower for x in ['telegram', 'tg']):
                tg_col = i
            elif any(x in header_lower for x in ['загальні', 'общ', 'total']):
                total_col = i
            elif any(x in header_lower for x in ['витрачені', 'потрачено', 'spent']):
                spent_col = i
            elif any(x in header_lower for x in ['актуальні', 'актуальные', 'actual']):
                actual_col = i
        
        # Пошук користувача
        for row_num in range(1, len(all_values)):
            row = all_values[row_num]
            if len(row) > max(tg_col, total_col, spent_col, actual_col):
                row_telegram_raw = row[tg_col] if tg_col < len(row) else ""
                row_telegram_clean = row_telegram_raw.strip().lstrip('@').lower()
                
                if row_telegram_clean == clean_username:
                    user_data = {
                        'row_num': row_num + 1,
                        'name': row[name_col] if name_col < len(row) else "",
                        'static_id': row[static_id_col] if static_id_col < len(row) else "",
                        'telegram': row[tg_col] if tg_col < len(row) else "",
                        'total_balance': int(row[total_col]) if total_col < len(row) and row[total_col].isdigit() else 0,
                        'spent_balance': int(row[spent_col]) if spent_col < len(row) and row[spent_col].isdigit() else 0,
                        'actual_balance': int(row[actual_col]) if actual_col < len(row) and row[actual_col].isdigit() else 0,
                        'name_col': name_col,
                        'static_id_col': static_id_col,
                        'tg_col': tg_col,
                        'total_col': total_col,
                        'spent_col': spent_col,
                        'actual_col': actual_col
                    }
                    return user_data
        
        return None
        
    except Exception as e:
        logger.error(f"Помилка отримання даних користувача: {e}")
        return None

def get_products_from_sheet():
    """Отримати товари з кешем"""
    global PRODUCTS_CACHE, PRODUCTS_CACHE_TIME
    
    current_time = time.time()
    if PRODUCTS_CACHE and (current_time - PRODUCTS_CACHE_TIME) < CACHE_DURATION:
        return PRODUCTS_CACHE.copy()
    
    try:
        client = connect_to_google_sheets()
        sheet = client.open_by_key(SHEET_ID).worksheet('Товари')
        all_values = sheet.get_all_values()
        
        if len(all_values) < 2:
            return []
        
        headers = all_values[0]
        
        # Пошук колонок
        id_col = -1
        name_col = -1
        description_col = -1
        price_col = -1
        category_col = -1
        image_col = -1
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            if any(x in header_lower for x in ['id', 'ід', 'номер']):
                id_col = i
            elif any(x in header_lower for x in ['назва', 'name', 'товар']):
                name_col = i
            elif any(x in header_lower for x in ['опис', 'description']):
                description_col = i
            elif any(x in header_lower for x in ['ціна', 'цена', 'price', 'бал']):
                price_col = i
            elif any(x in header_lower for x in ['категорія', 'category']):
                category_col = i
            elif any(x in header_lower for x in ['фото', 'image', 'картинка']):
                image_col = i
        
        products = []
        for row_num in range(1, len(all_values)):
            row = all_values[row_num]
            if len(row) > max(id_col, name_col, price_col, category_col):
                try:
                    product = {
                        'id': int(row[id_col]) if id_col < len(row) and row[id_col].isdigit() else row_num,
                        'name': row[name_col] if name_col < len(row) else f"Товар {row_num}",
                        'description': row[description_col] if description_col < len(row) else "Опис відсутній",
                        'price': int(row[price_col]) if price_col < len(row) and row[price_col].isdigit() else 0,
                        'category': row[category_col] if category_col < len(row) else "other",
                        'image_url': row[image_col] if image_col < len(row) and row[image_col].strip() else None
                    }
                    
                    if product['price'] > 0:
                        products.append(product)
                except Exception as e:
                    continue
        
        PRODUCTS_CACHE = products
        PRODUCTS_CACHE_TIME = current_time
        return products
        
    except Exception as e:
        logger.error(f"Помилка завантаження товарів: {e}")
        return []

def update_spent_balance(user_data, additional_spent):
    """Оновити витрачені бали"""
    try:
        client = connect_to_google_sheets()
        sheet = client.open_by_key(SHEET_ID).worksheet('Баланси')
        
        new_spent = user_data['spent_balance'] + additional_spent
        sheet.update_cell(user_data['row_num'], user_data['spent_col'] + 1, new_spent)
        
        logger.info(f"Баланс оновлено: {user_data['telegram']} - {additional_spent} балів")
        return True
        
    except Exception as e:
        logger.error(f"Помилка оновлення балансу: {e}")
        return False

def log_purchase_to_sheet(user_data, product):
    """Записати покупку в історію"""
    try:
        client = connect_to_google_sheets()
        
        try:
            log_sheet = client.open_by_key(SHEET_ID).worksheet('Історія покупок')
        except:
            spreadsheet = client.open_by_key(SHEET_ID)
            log_sheet = spreadsheet.add_worksheet(title='Історія покупок', rows=1000, cols=10)
            log_sheet.append_row([
                'Дата', 'Ім\'я', 'Static ID', 'Telegram', 'Товар', 'Ціна',
                'Загальні бали', 'Витрачені бали', 'Актуальний баланс'
            ])
        
        log_sheet.append_row([
            datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M'),
            user_data['name'],
            user_data['static_id'],
            user_data['telegram'],
            product['name'],
            product['price'],
            user_data['total_balance'],
            user_data['spent_balance'] + product['price'],
            user_data['actual_balance'] - product['price']
        ])
        
        return True
        
    except Exception as e:
        logger.error(f"Помилка запису в історію: {e}")
        return False

# ====== КРАСИВИЙ ВІЗУАЛ ТА ФОРМАТУВАННЯ ======

def escape_markdown(text):
    """Екранування символів для MarkdownV2"""
    if not text:
        return ""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in str(text))

def format_balance_message(user_data):
    """Форматування повідомлення про баланс"""
    if not user_data:
        return "💎 *Ваш баланс:* Не знайдено в системі\n\n"
    
    return (
        f"💎 *Ваш баланс:* *{user_data['actual_balance']}* балів\n\n"
        f"📊 *Детальна інформація:*\n"
        f"┣ • Загальні бали: {user_data['total_balance']}\n"
        f"┣ • Витрачені бали: {user_data['spent_balance']}\n"
        f"┗ • Актуальний баланс: {user_data['actual_balance']}\n\n"
    )

def format_product_message(product, balance=0):
    """Форматування повідомлення про товар"""
    category_emojis = {
        "transport": "🚗",
        "clothing": "👕", 
        "accessories": "💍",
        "other": "📦"
    }
    
    emoji = category_emojis.get(product['category'], "📦")
    can_afford = balance >= product['price']
    
    status_icon = "✅" if can_afford else "❌"
    status_text = "*Можете придбати!*" if can_afford else f"Недостатньо балів. Потрібно ще {product['price'] - balance}"
    
    return (
        f"{emoji} *{escape_markdown(product['name'])}*\n\n"
        f"📋 *Категорія:* {escape_markdown(product['category'].title())}\n"
        f"💰 *Ціна:* {product['price']} балів\n"
        f"📝 *Опис:* {escape_markdown(product['description'])}\n\n"
        f"💎 *Ваш баланс:* {balance} балів\n\n"
        f"{status_icon} *Статус:* {status_text}\n"
    )

def format_order_message(user_data, product):
    """Форматування повідомлення про замовлення"""
    return (
        f"🛒 НОВЕ ЗАМОВЛЕННЯ\n\n"
        f"👤 Користувач:\n"
        f"┣ • Ім'я: {user_data['name']}\n"
        f"┣ • Static ID: {user_data['static_id']}\n"
        f"┗ • Telegram: {user_data['telegram']}\n\n"
        f"📦 Товар:\n"
        f"┣ • Назва: {product['name']}\n"
        f"┣ • Категорія: {product['category']}\n"
        f"┗ • Ціна: {product['price']} балів\n\n"
        f"💰 Баланс:\n"
        f"┣ • Загальні: {user_data['total_balance']}\n"
        f"┣ • Витрачені: {user_data['spent_balance']} → {user_data['spent_balance'] + product['price']}\n"
        f"┗ • Актуальні: {user_data['actual_balance']} → {user_data['actual_balance'] - product['price']}\n\n"
        f"🕒 Час: {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}\n"
        f"🔖 Тег: #{user_data['telegram'].replace('@', '').replace('_', '')}"
    )

# ====== ОСНОВНІ ФУНКЦІЇ БОТА ======

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    try:
        user = update.effective_user
        
        # Отримуємо дані користувача
        user_data = None
        if user.username:
            user_data = get_user_data(f"@{user.username}")
        
        balance_info = format_balance_message(user_data)
        
        welcome_text = (
            f"👋 *Вітаємо, {escape_markdown(user.first_name)}!*\n\n"
            f"🏪 *Магазин балів Ukraine GTA 5 RP*\n\n"
            f"{balance_info}"
            f"💡 *Як працює система:*\n"
            f"┣ • 1 тікет = 1 бал\n"
            f"┣ • Баланс оновлюється кожні 10 хвилин\n"
            f"┗ • Покупки обробляються автоматично\n\n"
            f"⚡ *Швидкий доступ:*\n"
            f"┣ • /shop - перейти до магазину\n"
            f"┣ • /balance - перевірити баланс\n"
            f"┗ • /help - довідка та підтримка"
        )
        
        keyboard = [
            [InlineKeyboardButton("🛍️ Перейти до магазину", callback_data="main_menu")],
            [InlineKeyboardButton("ℹ️ Допомога", callback_data="help")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.message:
            await update.message.reply_text(
                welcome_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            await update.callback_query.edit_message_text(
                welcome_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        
    except Exception as e:
        logger.error(f"Помилка в start: {e}")
        if update.message:
            await update.message.reply_text("❌ Виникла помилка. Спробуйте пізніше.")
        else:
            await update.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте пізніше.")

async def show_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати баланс користувача"""
    try:
        user = update.effective_user
        
        if not user.username:
            error_text = (
                "❌ *Будь ласка, встановіть ім'я користувача в Telegram!*\n\n"
                "Це необхідно для перевірки вашого балансу в системі.\n\n"
                "*Як це зробити:*\n"
                "1. Відкрийте налаштування Telegram\n"
                "2. Перейдіть у 'Редагувати профіль'\n" 
                "3. Встановіть 'Ім'я користувача' (Username)\n"
                "4. Поверніться та спробуйте знову"
            )
            if update.message:
                await update.message.reply_text(error_text, parse_mode='Markdown')
            else:
                await update.callback_query.edit_message_text(error_text, parse_mode='Markdown')
            return
        
        user_data = get_user_data(f"@{user.username}")
        if not user_data:
            error_text = (
                "❌ *Обліковий запис не знайдено*\n\n"
                "Ваш Telegram не знайдено в системі балансів.\n"
                "Зверніться до адміністратора: @laker_77"
            )
            if update.message:
                await update.message.reply_text(error_text, parse_mode='Markdown')
            else:
                await update.callback_query.edit_message_text(error_text, parse_mode='Markdown')
            return
        
        balance_text = (
            f"💎 *Детальна інформація про баланс*\n\n"
            f"👤 *Профіль:*\n"
            f"┣ • Ім'я: {escape_markdown(user_data['name'])}\n"
            f"┣ • Static ID: {escape_markdown(user_data['static_id'])}\n"
            f"┗ • Telegram: {user_data['telegram']}\n\n"
            f"💰 *Баланс:*\n"
            f"┣ • Загальні бали: {user_data['total_balance']}\n"
            f"┣ • Витрачені бали: {user_data['spent_balance']}\n"
            f"┗ • Актуальний баланс: *{user_data['actual_balance']}*\n\n"
            f"📊 *Пояснення:*\n"
            f"┣ • Загальні = всі тікети\n"
            f"┣ • Актуальні = Загальні - Витрачені\n"
            f"┗ • 1 тікет = 1 бал"
        )
        
        keyboard = [
            [InlineKeyboardButton("🛍️ До магазину", callback_data="main_menu")],
            [InlineKeyboardButton("🔄 Оновити", callback_data="check_balance")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.message:
            await update.message.reply_text(balance_text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.callback_query.edit_message_text(balance_text, reply_markup=reply_markup, parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Помилка в show_balance: {e}")
        error_msg = "❌ Помилка при перевірці балансу. Спробуйте пізніше."
        if update.message:
            await update.message.reply_text(error_msg)
        else:
            await update.callback_query.edit_message_text(error_msg)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати головне меню магазину"""
    try:
        if update.callback_query:
            query = update.callback_query
            await query.answer()
            user = query.from_user
            message = query.message
        else:
            user = update.effective_user
            message = update.message
        
        user_data = get_user_data(f"@{user.username}") if user.username else None
        balance = user_data['actual_balance'] if user_data else 0
        
        products = get_products_from_sheet()
        categories = sorted(set(product["category"] for product in products))
        
        category_buttons = []
        for category in categories:
            emoji = {
                "transport": "🚗",
                "clothing": "👕",
                "accessories": "💍",
                "other": "📦"
            }.get(category, "📦")
            
            category_name = {
                "transport": "Транспорт",
                "clothing": "Одяг", 
                "accessories": "Аксесуари",
                "other": "Інше"
            }.get(category, category.title())
            
            category_buttons.append([InlineKeyboardButton(
                f"{emoji} {category_name}", 
                callback_data=f"category_{category}"
            )])
        
        keyboard = category_buttons + [
            [InlineKeyboardButton("ℹ️ Допомога", callback_data="help")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        menu_text = (
            f"🏪 *Магазин балів Ukraine GTA 5 RP*\n\n"
            f"💎 *Ваш баланс:* *{balance}* балів\n\n"
            f"📂 *Доступні категорії:*\n"
        )
        
        for category in categories:
            emoji = {
                "transport": "🚗",
                "clothing": "👕", 
                "accessories": "💍",
                "other": "📦"
            }.get(category, "📦")
            
            desc = {
                "transport": "ексклюзивні автомобілі",
                "clothing": "стильний одяг та взуття", 
                "accessories": "рюкзаки, сумки та інше",
                "other": "різноманітні товари"
            }.get(category, "товари")
            
            menu_text += f"┣ {emoji} {category.title()} - {desc}\n"
        
        menu_text += f"┗ 📊 Всього товарів: {len(products)}\n\n"
        menu_text += "Оберіть категорію для перегляду товарів:"
        
        if update.callback_query:
            await query.edit_message_text(
                menu_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            await message.reply_text(
                menu_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        
    except Exception as e:
        logger.error(f"Помилка в show_main_menu: {e}")
        if update.callback_query:
            await update.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте пізніше.")
        else:
            await update.message.reply_text("❌ Виникла помилка. Спробуйте пізніше.")

async def show_category(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    """Показати товари в категорії"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        user_data = get_user_data(f"@{user.username}") if user.username else None
        balance = user_data['actual_balance'] if user_data else 0
        
        products = get_products_from_sheet()
        category_products = [p for p in products if p["category"] == category]
        
        if not category_products:
            if query.message.photo:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Товари в цій категорії відсутні"
                )
            else:
                await query.edit_message_text("❌ Товари в цій категорії відсутні")
            return
        
        category_display = {
            "transport": "🚗 Транспорт",
            "clothing": "👕 Одяг",
            "accessories": "💍 Аксесуари", 
            "other": "📦 Інше"
        }.get(category, category.title())
        
        keyboard = []
        for product in category_products:
            can_afford = balance >= product["price"]
            emoji = "🟢" if can_afford else "🔴"
            button_text = f"{emoji} {product['name']} - {product['price']} балів"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"product_{product['id']}")])
        
        keyboard.append([
            InlineKeyboardButton("🔙 Назад", callback_data="main_menu")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        category_text = (
            f"*{category_display}*\n\n"
            f"💎 *Ваш баланс:* *{balance}* балів\n\n"
            f"📦 *Доступні товари ({len(category_products)}):*\n"
            f"🟢 - можете купити\n"
            f"🔴 - недостатньо балів\n\n"
            f"Оберіть товар для детального перегляду:"
        )
        
        # Перевіряємо тип повідомлення
        if query.message.photo:
            # Якщо поточне повідомлення має фото, відправляємо нове текстове
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=category_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            await query.message.delete()
        else:
            # Якщо поточне повідомлення текстове, редагуємо його
            await query.edit_message_text(
                category_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        
    except Exception as e:
        logger.error(f"Помилка в show_category: {e}")
        try:
            if query.message.photo:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Виникла помилка. Спробуйте пізніше."
                )
            else:
                await query.edit_message_text("❌ Виникла помилка. Спробуйте пізніше.")
        except Exception as e2:
            logger.error(f"Помилка при відправці повідомлення про помилку: {e2}")

async def show_product(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    """Показати деталі товару з фото"""
    try:
        query = update.callback_query
        await query.answer()
        
        products = get_products_from_sheet()
        product = next((p for p in products if p["id"] == product_id), None)
        
        if not product:
            if query.message.photo:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Товар не знайдено"
                )
            else:
                await query.edit_message_text("❌ Товар не знайдено")
            return
            
        user = query.from_user
        user_data = get_user_data(f"@{user.username}") if user.username else None
        balance = user_data['actual_balance'] if user_data else 0
        
        product_text = format_product_message(product, balance)
        
        keyboard = []
        if balance >= product["price"]:
            keyboard.append([InlineKeyboardButton("🛒 Купити", callback_data=f"buy_{product['id']}")])
        
        keyboard.append([
            InlineKeyboardButton("🔙 Назад", callback_data=f"category_{product['category']}")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Спроба відправити з фото
        if product.get('image_url') and product['image_url'].startswith(('http://', 'https://')):
            try:
                # Відправляємо нове повідомлення з фото
                await context.bot.send_photo(
                    chat_id=query.message.chat_id,
                    photo=product['image_url'],
                    caption=product_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                # Видаляємо попереднє повідомлення (з категорією)
                await query.message.delete()
                return
            except Exception as e:
                logger.warning(f"Не вдалося відправити фото: {e}")
                # Продовжуємо з текстовим повідомленням
        
        # Якщо фото не вдалося або немає фото
        if query.message.photo:
            # Якщо поточне повідомлення має фото, відправляємо нове текстове
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=product_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            await query.message.delete()
        else:
            # Якщо поточне повідомлення текстове, редагуємо його
            await query.edit_message_text(
                product_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        
    except Exception as e:
        logger.error(f"Помилка в show_product: {e}")
        try:
            if query.message.photo:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ Виникла помилка. Спробуйте пізніше."
                )
            else:
                await query.edit_message_text("❌ Виникла помилка. Спробуйте пізніше.")
        except Exception as e2:
            logger.error(f"Помилка при відправці повідомлення про помилку: {e2}")

async def handle_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    """Обробити покупку товару"""
    try:
        query = update.callback_query
        await query.answer()
        
        products = get_products_from_sheet()
        product = next((p for p in products if p["id"] == product_id), None)
        
        if not product:
            await query.edit_message_text("❌ Товар не знайдено")
            return
            
        user = query.from_user
        
        if not user.username:
            await query.edit_message_text("❌ Будь ласка, встановіть ім'я користувача в Telegram")
            return
        
        user_data = get_user_data(f"@{user.username}")
        if not user_data:
            await query.edit_message_text("❌ Ваш обліковий запис не знайдено в системі")
            return
        
        if user_data['actual_balance'] < product["price"]:
            await query.edit_message_text(
                f"❌ *Недостатньо балів!*\n\n"
                f"💎 Ваш баланс: {user_data['actual_balance']} балів\n"
                f"💰 Ціна товару: {product['price']} балів\n"
                f"🔻 Вам не вистачає: {product['price'] - user_data['actual_balance']} балів"
            )
            return
        
        # Оновлюємо баланс
        if not update_spent_balance(user_data, product["price"]):
            await query.edit_message_text("❌ Помилка при списанні балів. Зверніться до адміністратора.")
            return
        
        # Логуємо покупку
        log_purchase_to_sheet(user_data, product)
        
        # Відправляємо замовлення в групу (без Markdown) В ПРАВИЛЬНИЙ ТОПІК
        order_message = format_order_message(user_data, product)
        
        try:
            if product.get('image_url') and product['image_url'].startswith(('http://', 'https://')):
                await context.bot.send_photo(
                    chat_id=ORDER_CHAT_ID,
                    message_thread_id=ORDER_TOPIC_ID,  # Додаємо ID топика
                    photo=product['image_url'],
                    caption=order_message
                )
            else:
                await context.bot.send_message(
                    chat_id=ORDER_CHAT_ID,
                    message_thread_id=ORDER_TOPIC_ID,  # Додаємо ID топика
                    text=order_message
                )
            logger.info(f"✅ Повідомлення відправлено в групу {ORDER_CHAT_ID}, топик {ORDER_TOPIC_ID}")
        except Exception as e:
            logger.error(f"❌ Помилка відправки в групу {ORDER_CHAT_ID}, топик {ORDER_TOPIC_ID}: {e}")
            # Резервна відправка адміну
            try:
                await context.bot.send_message(
                    chat_id=334700077,  # ID адміністратора
                    text=f"🛒 УВАГА! НОВЕ ЗАМОВЛЕННЯ:\n{user_data['name']} -> {product['name']} за {product['price']} балів\nПомилка відправки в групу: {str(e)}"
                )
            except Exception as admin_error:
                logger.error(f"❌ Помилка відправки адміну: {admin_error}")
        
        # Отримуємо ОНОВЛЕНІ дані користувача для відображення актуального балансу
        updated_user_data = get_user_data(f"@{user.username}")
        
        # Сповіщаємо користувача
        new_balance = updated_user_data['actual_balance'] if updated_user_data else user_data['actual_balance'] - product['price']
        
        success_text = (
            f"✅ *Покупка успішна!*\n\n"
            f"📦 *Товар:* {escape_markdown(product['name'])}\n"
            f"💰 *Списано:* {product['price']} балів\n"
            f"💎 *Новий баланс:* {new_balance} балів\n\n"
            f"📋 *Деталі:*\n"
            f"┣ • Замовлення передано адміністрації\n"
            f"┣ • Зв'язок протягом 24 годин\n"
            f"┗ • Дякуємо за покупку! 🎉"
        )
        
        keyboard = [
            [InlineKeyboardButton("🛒 Продовжити покупки", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Спроба відправити фото підтвердження
        if product.get('image_url') and product['image_url'].startswith(('http://', 'https://')):
            try:
                await context.bot.send_photo(
                    chat_id=query.message.chat_id,
                    photo=product['image_url'],
                    caption=success_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                await query.message.delete()
                return
            except Exception as e:
                logger.warning(f"Не вдалося відправити фото підтвердження: {e}")
                # Продовжуємо з текстовим повідомленням
        
        await query.edit_message_text(
            success_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        logger.info(f"🎉 Покупка завершена: {user.username} -> {product['name']}")
        
    except Exception as e:
        logger.error(f"💥 КРИТИЧНА ПОМИЛКА в handle_purchase: {e}")
        await query.edit_message_text("❌ Виникла критична помилка. Зверніться до @laker_77.")

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати довідку"""
    try:
        query = update.callback_query
        await query.answer()
        
        help_text = (
            "ℹ️ *Довідка по магазину балів*\n\n"
            "💰 *Система балів:*\n"
            "┣ • 1 тікет = 1 бал\n"
            "┣ • Баланс оновлюється кожні 10 хв\n"
            "┗ • Покупки обробляються автоматично\n\n"
            "🛒 *Як купувати:*\n"
            "┣ 1. Перевірте баланс /balance\n"
            "┣ 2. Оберіть категорію товарів\n"
            "┣ 3. Перегляньте товар з фото\n"
            "┣ 4. Натисніть 'Купити'\n"
            "┗ 5. Очікуйте зв'язку адміністратора\n\n"
            "❓ *Поширені питання:*\n"
            "┣ • *Баланс не оновився?* - Зачекайте 10 хв\n"
            "┣ • *Товар не прийшов?* - Зв'яжіться з адміном\n"
            "┣ • *Помилка при покупці?* - @laker_77\n"
            "┗ • *Час обробки?* - До 24 годин\n\n"
            "⚡ *Команди:*\n"
            "┣ • /start - головне меню\n"
            "┣ • /shop - магазин\n"
            "┣ • /balance - баланс\n"
            "┗ • /help - довідка"
        )
        
        keyboard = [
            [InlineKeyboardButton("🛒 До магазину", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            help_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Помилка в show_help: {e}")
        await update.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте пізніше.")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник інлайн кнопок"""
    try:
        query = update.callback_query
        data = query.data
        
        if data == "main_menu":
            await show_main_menu(update, context)
        elif data == "check_balance":
            await show_balance(update, context)
        elif data == "help":
            await show_help(update, context)
        elif data.startswith("category_"):
            category = data.replace("category_", "")
            await show_category(update, context, category)
        elif data.startswith("product_"):
            product_id = int(data.replace("product_", ""))
            await show_product(update, context, product_id)
        elif data.startswith("buy_"):
            product_id = int(data.replace("buy_", ""))
            await handle_purchase(update, context, product_id)
            
    except Exception as e:
        logger.error(f"Помилка в handle_callback: {e}")
        try:
            query = update.callback_query
            if query.message.photo:
                await query.message.reply_text("❌ Виникла помилка. Спробуйте пізніше.")
            else:
                await query.edit_message_text("❌ Виникла помилка. Спробуйте пізніше.")
        except Exception as e2:
            logger.error(f"Помилка при відправці повідомлення про помилку: {e2}")

async def shop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /shop"""
    await show_main_menu(update, context)

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /balance"""
    await show_balance(update, context)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    await show_help(update, context)

def main():
    """Запуск бота"""
    try:
        logger.info("🚀 Запуск бота магазину...")
        
        # Перевіряємо обов'язкові змінні
        if not TELEGRAM_TOKEN:
            logger.error("❌ TELEGRAM_TOKEN не знайдено!")
            return
        
        # Створюємо Application без JobQueue для уникнення помилки weakref
        application = (
            ApplicationBuilder()
            .token(TELEGRAM_TOKEN)
            .concurrent_updates(True)
            .build()
        )
        
        # Обробники команд
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("shop", shop_command))
        application.add_handler(CommandHandler("balance", balance_command))
        application.add_handler(CommandHandler("help", help_command))
        
        # Обробник інлайн кнопок
        application.add_handler(CallbackQueryHandler(handle_callback))
        
        # Обробник текстових повідомлень
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, start))
        
        logger.info("✅ Бот успішно ініціалізовано!")
        logger.info("🔄 Запуск з поллінгом...")
        
        # Запускаємо бота
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )
        
    except Exception as e:
        logger.error(f"❌ Помилка запуску бота: {e}")
        raise

if __name__ == '__main__':
    main()
