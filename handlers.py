import os
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from database import add_user, update_user_settings, get_user_settings, add_movie

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def start(update, context):
    chat_id = update.message.chat_id
    add_user(chat_id)
    update.message.reply_text(
        "Welcome to the Movie Bot! 🎥\n"
        "- Type a movie name (e.g., 'The Kid 1921') to search.\n"
        "- Customize your downloads:\n"
        "  /setthumbnail - Set a custom thumbnail\n"
        "  /setprefix - Set a filename prefix\n"
        "  /setcaption - Set a custom caption\n"
        "- View settings:\n"
        "  /viewthumbnail - See your thumbnail\n"
        "  /viewprefix - See your prefix\n"
        "  /viewcaption - See your caption\n"
        "All movies are legal, public domain content."
    )
    logger.info(f"User {chat_id} started the bot")

def index(update, context):
    chat_id = update.message.chat_id
    DATABASE_CHANNEL_ID = context.bot_data.get("DATABASE_CHANNEL_ID")
    if not DATABASE_CHANNEL_ID:
        update.message.reply_text("Error: DATABASE_CHANNEL_ID is not configured.")
        logger.error("DATABASE_CHANNEL_ID not set")
        return
    try:
        admins = context.bot.get_chat_administrators(DATABASE_CHANNEL_ID)
        if not any(admin.user.id == chat_id for admin in admins):
            update.message.reply_text("Only channel admins can use /index.")
            logger.warning(f"User {chat_id} attempted /index without admin privileges")
            return
        update.message.reply_text("Please forward a message from the database channel to start indexing.")
        context.user_data['indexing'] = True
        logger.info(f"User {chat_id} initiated indexing")
    except Exception as e:
        update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in /index for user {chat_id}: {str(e)}")

def handle_forwarded_message(update, context):
    if not context.user_data.get('indexing'):
        return
    message = update.message
    DATABASE_CHANNEL_ID = context.bot_data.get("DATABASE_CHANNEL_ID")
    if not DATABASE_CHANNEL_ID:
        update.message.reply_text("Error: DATABASE_CHANNEL_ID is not configured.")
        logger.error("DATABASE_CHANNEL_ID not set")
        return
    if not message.forward_from_chat or str(message.forward_from_chat.id) != DATABASE_CHANNEL_ID[1:]:
        update.message.reply_text("Please forward a message from the correct database channel.")
        logger.warning("Forwarded message from incorrect channel")
        return

    try:
        messages = []
        offset = 0
        while True:
            batch = context.bot.get_chat_history(DATABASE_CHANNEL_ID, limit=100, offset=offset)
            if not batch:
                break
            messages.extend(batch)
            offset += 100
        for message in messages:
            if message.document and message.document.file_name.endswith('.mkv'):
                file_name = message.document.file_name
                file_id = message.document.file_id
                message_id = message.message_id
                try:
                    parts = file_name.replace('.mkv', '').split('_')
                    title = parts[0].replace('.', ' ')
                    year = int(parts[1]) if len(parts) > 1 else 0
                    quality = parts[2] if len(parts) > 2 else 'Unknown'
                    file_size = f"{message.document.file_size / (1024 * 1024):.2f}MB"
                    add_movie(title, year, quality, file_size, file_id, message_id)
                    logger.info(f"Indexed movie: {title} ({year}, {quality})")
                except (IndexError, ValueError) as e:
                    logger.warning(f"Skipped invalid file name: {file_name} - {str(e)}")
                    continue
        update.message.reply_text("Indexing complete.")
        logger.info("Indexing completed")
    except Exception as e:
        update.message.reply_text(f"Error indexing: {str(e)}")
        logger.error(f"Error indexing: {str(e)}")
    finally:
        context.user_data['indexing'] = False

def set_thumbnail(update, context):
    update.message.reply_text("Please upload an image for your custom thumbnail or type 'default' for a default thumbnail:")
    logger.info(f"User {update.message.chat_id} initiated /setthumbnail")
    return "SET_THUMBNAIL"

def handle_thumbnail(update, context):
    chat_id = update.message.chat_id
    if update.message.text and update.message.text.lower() == 'default':
        thumbnail_file_id = None
    elif update.message.photo:
        thumbnail_file_id = update.message.photo[-1].file_id
    else:
        update.message.reply_text("Invalid input. Please upload an image or type 'default'.")
        logger.warning(f"Invalid thumbnail input from user {chat_id}")
        return "SET_THUMBNAIL"

    update_user_settings(chat_id, thumbnail_file_id=thumbnail_file_id)
    update.message.reply_text("Thumbnail set successfully!")
    logger.info(f"User {chat_id} set thumbnail: {thumbnail_file_id}")
    return None

def set_prefix(update, context):
    update.message.reply_text("Please enter your custom filename prefix (e.g., MyCollection_):")
    logger.info(f"User {update.message.chat_id} initiated /setprefix")
    return "SET_PREFIX"

def handle_prefix(update, context):
    chat_id = update.message.chat_id
    prefix = update.message.text.strip()
    if not prefix.endswith('_'):
        prefix += '_'

    update_user_settings(chat_id, prefix=prefix)
    update.message.reply_text(f"Prefix set to: {prefix}")
    logger.info(f"User {chat_id} set prefix: {prefix}")
    return None

def set_caption(update, context):
    update.message.reply_text("Please enter your custom caption (e.g., My favorite movie!):")
    logger.info(f"User {update.message.chat_id} initiated /setcaption")
    return "SET_CAPTION"

def handle_caption(update, context):
    chat_id = update.message.chat_id
    caption = update.message.text.strip()

    update_user_settings(chat_id, caption=caption)
    update.message.reply_text(f"Caption set to: {caption}")
    logger.info(f"User {chat_id} set caption: {caption}")
    return None

def view_thumbnail(update, context):
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    if settings and settings[0]:
        update.message.reply_photo(photo=settings[0], caption="Your current thumbnail")
        logger.info(f"User {chat_id} viewed thumbnail")
    else:
        update.message.reply_text("Your thumbnail is set to default (blue square).")
        logger.info(f"User {chat_id} has default thumbnail")

def view_prefix(update, context):
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    if settings and settings[1]:
        update.message.reply_text(f"Your prefix: {settings[1]}")
        logger.info(f"User {chat_id} viewed prefix: {settings[1]}")
    else:
        update.message.reply_text("No prefix set.")
        logger.info(f"User {chat_id} has no prefix set")

def view_caption(update, context):
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    if settings and settings[2]:
        update.message.reply_text(f"Your caption: {settings[2]}")
        logger.info(f"User {chat_id} viewed caption: {settings[2]}")
    else:
        update.message.reply_text("Your caption is set to default: 'Enjoy the movie!'")
        logger.info(f"User {chat_id} has default caption")
