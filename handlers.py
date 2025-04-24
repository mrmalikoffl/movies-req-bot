import os
import logging
import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, BadRequest
from telegram.ext import ConversationHandler
from database import add_user, update_user_settings, get_user_settings, add_movie, search_movies
from pymongo import MongoClient
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    RPCError,
    FloodWaitError,
    ChannelPrivateError,
    AuthKeyError
)
from bson.objectid import ObjectId  # Added for ObjectId conversion

# Load environment variables
load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["movie_bot"]
movies_collection = db["movies"]
users_collection = db["users"]

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Conversation states
SET_THUMBNAIL, SET_PREFIX, SET_CAPTION = range(3)

async def start(update, context):
    chat_id = update.message.chat_id
    add_user(chat_id)
    await update.message.reply_text(
        "Welcome to the Movie Bot! 🎥\n"
        "Just type a movie name (e.g., 'Mitra 2025' or 'Mitra tamil') to search for movies.\n"
        "Customize your downloads:\n"
        "  /setthumbnail - Set a custom thumbnail\n"
        "  /setprefix - Set a filename prefix\n"
        "  /setcaption - Set a custom caption\n"
        "View settings:\n"
        "  /viewthumbnail - See your thumbnail\n"
        "  /viewprefix - See your prefix\n"
        "  /viewcaption - See your caption\n"
        "Admin: Use /index and forward a message from a channel where I'm an admin to index all MKV files.\n"
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
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

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

        # Initialize progress message
        progress_msg = await update.message.reply_text(
            "Starting indexing process...",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton('Cancel', callback_data='index_cancel')]]
            )
        )

        try:
            client = TelegramClient(StringSession(), int(api_id), api_hash)
            await client.start(bot_token=bot_token)
            logger.info("TelegramClient authenticated successfully")

            total_files = 0
            duplicate = 0
            errors = 0
            unsupported = 0
            current = 0
            max_messages = 1000  # Limit to prevent timeouts

            async for msg in client.iter_messages(int(forwarded_channel_id), limit=max_messages):
                if not context.user_data.get('indexing'):
                    break

                current += 1
                try:
                    # Update progress every 20 messages
                    if current % 20 == 0:
                        await context.bot.edit_message_text(
                            chat_id=progress_msg.chat_id,
                            message_id=progress_msg.message_id,
                            text=(
                                f"Indexing in progress...\n"
                                f"Messages processed: {current}\n"
                                f"Movies indexed: {total_files}\n"
                                f"Duplicates skipped: {duplicate}\n"
                                f"Unsupported skipped: {unsupported}"
                            )
                        )

                    # Skip non-documents or non-MKV files
                    if not msg.document or msg.document.mime_type != 'video/x-matroska':
                        unsupported += 1
                        continue

                    file_name = msg.document.attributes[-1].file_name
                    message_id = msg.id

                    # Detect language from filename
                    language = None
                    name_lower = file_name.lower()
                    if 'tamil' in name_lower:
                        language = 'tamil'
                    elif 'english' in name_lower:
                        language = 'english'
                    elif 'hindi' in name_lower:
                        language = 'hindi'

                    # Get file ID by forwarding to bot
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
                        await context.bot.delete_message(
                            chat_id=context.bot.id,
                            message_id=forwarded.message_id
                        )
                    except (TelegramError, BadRequest) as te:
                        logger.error(f"Error getting file ID for {file_name}: {str(te)}")
                        errors += 1
                        continue

                    # Parse movie info
                    try:
                        clean_name = file_name.replace('.mkv', '').split('_')
                        title = clean_name[0].replace('.', ' ').strip()
                        year = int(clean_name[1]) if len(clean_name) > 1 and clean_name[1].isdigit() else 0
                        quality = clean_name[2] if len(clean_name) > 2 else 'Unknown'
                        
                        # Format file size 
                        size_bytes = msg.document.size
                        if size_bytes >= 1024 * 1024 * 1024:
                            file_size = f"{size_bytes / (1024 * 1024 * 1024):.2f}GB"
                        else:
                            file_size = f"{size_bytes / (1024 * 1024):.2f}MB"

                        # Add to database
                        movie_id = add_movie(
                            title=title,
                            year=year,
                            quality=quality,
                            file_size=file_size,
                            file_id=file_id,
                            message_id=message_id,
                            language=language
                        )
                        
                        if movie_id:
                            total_files += 1
                            logger.info(f"Indexed: {title} ({year})")
                        else:
                            duplicate += 1
                    except (IndexError, ValueError, AttributeError) as e:
                        logger.warning(f"Error parsing {file_name}: {str(e)}")
                        errors += 1

                except Exception as e:
                    logger.error(f"Error processing message {message_id}: {str(e)}")
                    errors += 1
                    continue

            # Final report
            result_msg = (
                f"✅ Indexing completed for channel {forwarded_channel_id}.\n"
                f"• Total messages processed: {current}\n"
                f"• Movies indexed: {total_files}\n"
                f"• Duplicates skipped: {duplicate}\n"
                f"• Unsupported files: {unsupported}\n"
                f"• Errors occurred: {errors}"
            )
            
            await context.bot.edit_message_text(
                chat_id=progress_msg.chat_id,
                message_id=progress_msg.message_id,
                text=result_msg
            )
            logger.info(f"Indexing completed for {forwarded_channel_id}")

        except FloodWaitError as fwe:
            await update.message.reply_text(f"Flood wait error: Please wait {fwe.seconds} seconds before trying again.")
            logger.error(f"Flood wait error: {fwe.seconds} seconds")
        except ChannelPrivateError:
            await update.message.reply_text("I don't have access to this channel. Please make sure I'm an admin.")
            logger.error("Channel access denied")
        except AuthKeyError:
            await update.message.reply_text("Authentication failed. Please check your API credentials.")
            logger.error("Telethon authentication failed")
        except RPCError as rpc_error:
            await update.message.reply_text(f"Telegram API error: {str(rpc_error)}")
            logger.error(f"RPC Error: {str(rpc_error)}")
        except Exception as e:
            await update.message.reply_text(f"Unexpected error: {str(e)}")
            logger.error(f"Indexing failed: {str(e)}", exc_info=True)
        finally:
            if 'client' in locals() and client.is_connected():
                await client.disconnect()
            context.user_data['indexing'] = False
            context.user_data['index_channel_id'] = None

    except TelegramError as te:
        await update.message.reply_text(f"Error accessing channel: {str(te)}")
        logger.error(f"Channel access error: {str(te)}")
        context.user_data['indexing'] = False

async def search_movie(update, context):
    """Handle text-based movie search in personal messages."""
    chat_id = update.message.chat_id
    query = update.message.text.strip()
    logger.info(f"User {chat_id} searched for: '{query}'")

    if not query:
        await update.message.reply_text("Please type a movie name to search (e.g., 'Mitra 2025').")
        logger.info(f"User {chat_id} sent empty search query")
        return

    try:
        # Split query into terms for flexible matching
        search_terms = query.split()
        year = None
        language = None
        try:
            for term in search_terms:
                if term.isdigit() and len(term) == 4:
                    year = int(term)
                    search_terms.remove(term)
                    break
        except ValueError:
            pass

        # Check for language keywords
        language_terms = ['tamil', 'english', 'hindi']
        for term in search_terms[:]:
            if term.lower() in language_terms:
                language = term.lower()
                search_terms.remove(term)

        movie_name = " ".join(search_terms)
        movies = search_movies(movie_name, year=year, language=language)

        # Fallback: If no results with year, try without year
        if not movies and year:
            movies = search_movies(movie_name, language=language)
            logger.info(f"No results for '{query}' with year={year}, falling back to no year")

        if not movies:
            await update.message.reply_text("No movies found. Try another search.")
            logger.info(f"No movies found for query: name={movie_name}, year={year}, language={language}")
            return

        # Prepare the response
        total_results = len(movies)
        page = 1
        total_pages = 1  # Simplified pagination (all results in one message for now)

        header = (
            f"Search Query: {query}  TOTAL RESULTS: {total_results}  PAGE: {page}/{total_pages}\n\n"
            "🔻 Tap on the file button and then start to download. 🔻\n\n"
        )

        # Format results
        results = []
        for movie_data in movies:
            movie_id, title, movie_year, quality, file_size, file_id, message_id = movie_data[:7]
            # Fetch the movie document to get the language
            movie_doc = movies_collection.find_one({"_id": ObjectId(movie_id)})
            movie_language = movie_doc.get('language', '') if movie_doc else ''
            language_str = movie_language if movie_language else (language if language else '')
            year_str = str(movie_year) if movie_year != 0 else ''
            result_line = f"[{file_size}] {title} {year_str} {language_str} {quality}".strip()
            results.append((result_line, movie_id))

        # Send results as a single message with buttons
        message_text = header + "\n".join([line for line, _ in results])
        buttons = [
            [InlineKeyboardButton(line, callback_data=f"download_{movie_id}")] 
            for line, movie_id in results
        ]
        await update.message.reply_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        logger.info(f"Found {total_results} movies for query: name={movie_name}, year={year}, language={language}")

    except TelegramError as te:
        await update.message.reply_text("Error occurred. Please try again later.")
        logger.error(f"Telegram error in search '{query}' by user {chat_id}: {str(te)}")
    except Exception as e:
        await update.message.reply_text("Error occurred. Please try again later.")
        logger.error(f"Error in search '{query}' by user {chat_id}: {str(e)}")

async def button_callback(update, context):
    """Handle download button clicks."""
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id

    if not data.startswith("download_"):
        await query.answer()
        return

    try:
        movie_id = data.split("_", 1)[1]
        # Convert movie_id string to ObjectId
        movie = movies_collection.find_one({"_id": ObjectId(movie_id)})

        if not movie:
            await query.message.reply_text("Movie not found. It may have been deleted.")
            logger.warning(f"Movie not found for download: {movie_id} by user {user_id}")
            await query.answer()
            return

        thumbnail_file_id, prefix, caption = get_user_settings(user_id)
        language_str = f" {movie['language']}" if movie.get('language') else ''
        year_str = str(movie['year']) if movie['year'] != 0 else ''
        default_caption = f"{movie['title']} ({year_str}) {language_str} {movie['quality']}"
        final_caption = caption or default_caption

        await query.message.reply_document(
            document=movie["file_id"],
            caption=f"{final_caption}  {movie['file_size']} MKV",
            thumb=thumbnail_file_id,
            parse_mode=None
        )
        logger.info(f"User {user_id} downloaded movie: {movie['title']} ({movie['_id']})")

        await query.answer(text="Download started!")

    except TelegramError as te:
        await query.message.reply_text("Error sending movie. Please try again later.")
        logger.error(f"Telegram error in download for {movie_id} by user {user_id}: {str(te)}")
        await query.answer(text="Download error.")
    except Exception as e:
        await query.message.reply_text("An error occurred. Please try again later.")
        logger.error(f"Error in download for {movie_id} by user {user_id}: {str(e)}")
        await query.answer(text="Download error.")

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
