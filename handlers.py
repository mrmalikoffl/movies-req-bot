import os
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from telegram.ext import ConversationHandler
from database import add_user, update_user_settings, get_user_settings, add_movie
from pymongo import MongoClient
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest

# Load environment variables
load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["movie_bot"]
movies_collection = db["movies"]
users_collection = db["users"]

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Conversation states
SET_THUMBNAIL, SET_PREFIX, SET_CAPTION = range(3)

async def start(update, context):
    chat_id = update.message.chat_id
    add_user(chat_id)
    await update.message.reply_text(
        "Welcome to the Movie Bot! 🎥\n"
        "- Type a movie name (e.g., '@YourBotName The Kid 1921') to search.\n"
        "- Customize your downloads:\n"
        "  /setthumbnail - Set a custom thumbnail\n"
        "  /setprefix - Set a filename prefix\n"
        "  /setcaption - Set a custom caption\n"
        "- View settings:\n"
        "  /viewthumbnail - See your thumbnail\n"
        "  /viewprefix - See your prefix\n"
        "  /viewcaption - See your caption\n"
        "- Admin: Use /index and forward a message from a channel where I'm an admin to index all MKV files.\n"
        "All movies are legal, public domain content."
    )
    logger.info(f"User {chat_id} started the bot")

async def index(update, context):
    chat_id = update.message.chat_id
    await update.message.reply_text(
        "Please forward a message from a channel where I am an admin to index all MKV files.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton('Cancel', callback_data='index_cancel')]]
        )
    )
    context.user_data['indexing'] = True
    context.user_data['index_channel_id'] = None
    logger.info(f"User {chat_id} initiated indexing")

async def handle_forwarded_message(update, context):
    if update.callback_query and update.callback_query.data == 'index_cancel':
        context.user_data['indexing'] = False
        context.user_data['index_channel_id'] = None
        await update.callback_query.message.edit_text("Indexing cancelled.")
        logger.info(f"User {update.callback_query.from_user.id} cancelled indexing")
        return

    if not context.user_data.get('indexing'):
        return

    message = update.message
    chat_id = update.message.chat_id

    if not message.forward_from_chat:
        await update.message.reply_text("Please forward a message from a channel.")
        logger.warning(f"User {chat_id} forwarded a non-channel message")
        return

    forwarded_channel_id = str(message.forward_from_chat.id)
    logger.info(f"User {chat_id} forwarded message from channel {forwarded_channel_id}")

    if not forwarded_channel_id.startswith('-100'):
        await update.message.reply_text("Invalid channel ID. Please forward a message from a valid Telegram channel.")
        logger.warning(f"Invalid channel ID {forwarded_channel_id} for user {chat_id}")
        return

    try:
        # Verify bot is admin
        admins = await context.bot.get_chat_administrators(forwarded_channel_id)
        bot_id = context.bot.id
        if not any(admin.user.id == bot_id for admin in admins):
            await update.message.reply_text("I am not an admin of this channel. Please make me an admin and try again.")
            logger.warning(f"Bot is not admin of channel {forwarded_channel_id} for user {chat_id}")
            return

        # Verify user is admin
        if not any(admin.user.id == chat_id for admin in admins):
            await update.message.reply_text("Only channel admins can index movies.")
            logger.warning(f"User {chat_id} is not admin of channel {forwarded_channel_id}")
            return

        context.user_data['index_channel_id'] = forwarded_channel_id
        logger.info(f"User {chat_id} set indexing channel to {forwarded_channel_id}")

        # Use telethon to fetch messages
        try:
            api_id = os.getenv("TELEGRAM_API_ID")
            api_hash = os.getenv("TELEGRAM_API_HASH")
            bot_token = os.getenv("TELEGRAM_BOT_TOKEN")

            # Debug logging for environment variables
            logger.info(f"TELEGRAM_API_ID: {api_id}")
            logger.info(f"TELEGRAM_API_HASH: {api_hash}")
            logger.info(f"TELEGRAM_BOT_TOKEN: {bot_token[:10]}...")  # Partial token for security

            if not all([api_id, api_hash, bot_token]):
                missing = [var for var, val in [
                    ("TELEGRAM_API_ID", api_id),
                    ("TELEGRAM_API_HASH", api_hash),
                    ("TELEGRAM_BOT_TOKEN", bot_token)
                ] if not val]
                error_msg = f"Missing environment variables: {', '.join(missing)}"
                await update.message.reply_text(f"Configuration error: {error_msg}")
                logger.error(f"Indexing failed for channel {forwarded_channel_id}: {error_msg}")
                return

            # Use a unique session name to avoid corrupted session files
            async with TelegramClient('bot_session_2025', int(api_id), api_hash) as client:
                try:
                    await client.start(bot_token=bot_token)
                    logger.info(f"TelegramClient authenticated successfully for channel {forwarded_channel_id}")
                except Exception as auth_error:
                    error_msg = f"Failed to authenticate TelegramClient: {str(auth_error)}"
                    await update.message.reply_text("Authentication error with Telegram. Please contact the administrator.")
                    logger.error(f"Indexing failed for channel {forwarded_channel_id}: {error_msg}")
                    return

                total_files = 0
                duplicate = 0
                errors = 0
                unsupported = 0
                current = 0
                max_messages = 1000

                async for msg in client.iter_messages(int(forwarded_channel_id), limit=max_messages):
                    current += 1
                    if current % 20 == 0:
                        await update.message.reply_text(
                            f"Total messages fetched: {current}\n"
                            f"Total movies saved: {total_files}\n"
                            f"Duplicate movies skipped: {duplicate}\n"
                            f"Unsupported files skipped: {unsupported}\n"
                            f"Errors occurred: {errors}",
                            reply_markup=InlineKeyboardMarkup(
                                [[InlineKeyboardButton('Cancel', callback_data='index_cancel')]]
                            )
                        )

                    if not msg.document or msg.document.mime_type != 'video/x-matroska':
                        unsupported += 1
                        continue

                    file_name = msg.document.attributes[-1].file_name
                    message_id = msg.id

                    # Detect language from file_name
                    language = None
                    if 'tamil' in file_name.lower():
                        language = 'tamil'
                    elif 'english' in file_name.lower():
                        language = 'english'
                    # Add more language detection as needed

                    # Fetch Telegram file ID
                    try:
                        forwarded = await context.bot.forward_message(
                            chat_id=context.bot.id,
                            from_chat_id=forwarded_channel_id,
                            message_id=message_id
                        )
                        if not forwarded.document:
                            unsupported += 1
                            continue
                        file_id = forwarded.document.file_id
                        await context.bot.delete_message(chat_id=context.bot.id, message_id=forwarded.message_id)
                    except TelegramError as te:
                        logger.error(f"Error fetching Telegram file ID for message {message_id}: {str(te)}")
                        errors += 1
                        continue

                    try:
                        parts = file_name.replace('.mkv', '').split('_')
                        title = parts[0].replace('.', ' ')
                        year = int(parts[1]) if len(parts) > 1 else 0
                        quality = parts[2] if len(parts) > 2 else 'Unknown'
                        file_size = f"{msg.document.size / (1024 * 1024):.2f}MB"
                        if add_movie(title, year, quality, file_size, file_id, message_id, language=language):
                            logger.info(f"Indexed movie: {title} ({year}, {quality}, {language}) from channel {forwarded_channel_id}")
                            total_files += 1
                        else:
                            logger.info(f"Skipped duplicate movie: {file_name} in channel {forwarded_channel_id}")
                            duplicate += 1
                    except (IndexError, ValueError) as e:
                        logger.warning(f"Skipped invalid file name: {file_name} in channel {forwarded_channel_id} - {str(e)}")
                        errors += 1

                await update.message.reply_text(
                    f"✅ Indexing completed for channel {forwarded_channel_id}.\n"
                    f"Movies indexed: {total_files}\n"
                    f"Duplicate movies skipped: {duplicate}\n"
                    f"Unsupported files skipped: {unsupported}\n"
                    f"Errors occurred: {errors}",
                    reply_markup=None
                )
                logger.info(f"Indexing completed for channel {forwarded_channel_id}: {total_files} indexed, {duplicate} duplicates, {unsupported} unsupported, {errors} errors")

        except Exception as e:
            await update.message.reply_text(f"Error indexing channel: {str(e)}")
            logger.error(f"Error indexing channel {forwarded_channel_id}: {str(e)}")

    except TelegramError as e:
        await update.message.reply_text(f"Error accessing channel: {str(e)}")
        logger.error(f"Error accessing channel {forwarded_channel_id} for user {chat_id}: {str(e)}")

    finally:
        context.user_data['indexing'] = False
        context.user_data['index_channel_id'] = None

async def set_thumbnail(update, context):
    await update.message.reply_text("Please upload an image for your custom thumbnail or type 'default' for a default thumbnail:")
    logger.info(f"User {update.message.chat_id} initiated /setthumbnail")
    return SET_THUMBNAIL

async def handle_thumbnail(update, context):
    chat_id = update.message.chat_id
    if update.message.text and update.message.text.lower() == 'default':
        thumbnail_file_id = None
        update_user_settings(chat_id, thumbnail_file_id=thumbnail_file_id)
        await update.message.reply_text("✅ Custom thumbnail set to default successfully!")
        logger.info(f"User {chat_id} set thumbnail to default")
        return ConversationHandler.END
    elif update.message.photo:
        thumbnail_file_id = update.message.photo[-1].file_id
        update_user_settings(chat_id, thumbnail_file_id=thumbnail_file_id)
        await update.message.reply_text("✅ Custom thumbnail set successfully!")
        logger.info(f"User {chat_id} set thumbnail: {thumbnail_file_id}")
        return ConversationHandler.END
    else:
        await update.message.reply_text("Invalid input. Please upload an image or type 'default'.")
        logger.warning(f"Invalid thumbnail input from user {chat_id}")
        return SET_THUMBNAIL

async def set_prefix(update, context):
    await update.message.reply_text("Please enter your custom filename prefix (e.g., MyCollection_):")
    logger.info(f"User {update.message.chat_id} initiated /setprefix")
    return SET_PREFIX

async def handle_prefix(update, context):
    chat_id = update.message.chat_id
    prefix = update.message.text.strip()
    if not prefix.endswith('_'):
        prefix += '_'

    update_user_settings(chat_id, prefix=prefix)
    await update.message.reply_text(f"✅ Custom prefix set to: {prefix}")
    logger.info(f"User {chat_id} set prefix: {prefix}")
    return ConversationHandler.END

async def set_caption(update, context):
    await update.message.reply_text("Please enter your custom caption (e.g., My favorite movie!):")
    logger.info(f"User {update.message.chat_id} initiated /setcaption")
    return SET_CAPTION

async def handle_caption(update, context):
    chat_id = update.message.chat_id
    caption = update.message.text.strip()

    update_user_settings(chat_id, caption=caption)
    await update.message.reply_text(f"✅ Custom caption set to: {caption}")
    logger.info(f"User {chat_id} set caption: {caption}")
    return ConversationHandler.END

async def view_thumbnail(update, context):
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    if settings and settings[0]:
        await update.message.reply_photo(photo=settings[0], caption="Your current thumbnail")
        logger.info(f"User {chat_id} viewed thumbnail")
    else:
        await update.message.reply_text("Your thumbnail is set to default (blue square).")
        logger.info(f"User {chat_id} has default thumbnail")

async def view_prefix(update, context):
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    if settings and settings[1]:
        await update.message.reply_text(f"Your prefix: {settings[1]}")
        logger.info(f"User {chat_id} viewed prefix: {settings[1]}")
    else:
        await update.message.reply_text("No prefix set.")
        logger.info(f"User {chat_id} has no prefix set")

async def view_caption(update, context):
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    if settings and settings[2]:
        await update.message.reply_text(f"Your caption: {settings[2]}")
        logger.info(f"User {chat_id} viewed caption: {settings[2]}")
    else:
        await update.message.reply_text("Your caption is set to default: 'Enjoy the movie!'")
        logger.info(f"User {chat_id} has default caption")

async def stats(update, context):
    chat_id = update.message.chat_id
    try:
        total_users = users_collection.count_documents({})
        total_files = movies_collection.count_documents({})
        bot_language = "English"
        owner_name = os.getenv("OWNER_NAME", "MovieBot Team")

        stats_message = (
            "📊 *Movie Bot Stats* 📊\n\n"
            f"👥 *Total Users*: {total_users}\n"
            f"🎥 *Total Movies*: {total_files}\n"
            f"🌐 *Bot Language*: {bot_language}\n"
            f"👤 *Owner*: {owner_name}\n\n"
            "Thank you for using Movie Bot! 🎉"
        )

        await update.message.reply_text(stats_message, parse_mode='Markdown')
        logger.info(f"User {chat_id} viewed bot stats: {total_users} users, {total_files} movies")
    except Exception as e:
        await update.message.reply_text("Error retrieving stats. Please try again later.")
        logger.error(f"Error retrieving stats for user {chat_id}: {str(e)}")
