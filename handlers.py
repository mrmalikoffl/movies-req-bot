import os
import time
import logging
import asyncio
from PIL import Image
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, BadRequest
from telegram.ext import ConversationHandler
from database import (
    add_user, update_user_settings, get_user_settings, add_movie, add_movies_batch, search_movies, movies_collection
)
from utils import fix_thumb
from telegram.error import NetworkError
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChannelPrivateError, AuthKeyError, RPCError
from bson.objectid import ObjectId

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Conversation states
SET_THUMBNAIL, SET_PREFIX, SET_CAPTION = range(3)

async def start(update, context):
    """Send welcome message when command /start is issued"""
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
    """Initiate channel indexing process (single or batch)"""
    chat_id = update.message.chat_id
    await update.message.reply_text(
        "Please forward a message from a channel where I am an admin to index MKV files.\n"
        "Reply with 'batch' to index in batches or 'single' for single-pass indexing.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton('Cancel', callback_data='index_cancel')]]
        )
    )
    context.user_data['indexing'] = True
    context.user_data['index_channel_id'] = None
    context.user_data['index_mode'] = None
    logger.info(f"User {chat_id} initiated indexing")

async def batch_index(client, channel_id, progress_msg, context, batch_size=100, max_messages=1000):
    """Process channel messages in batches to index MKV files"""
    total_files = 0
    duplicate = 0
    errors = 0
    unsupported = 0
    current = 0
    batch_number = 0
    movie_batch = []

    try:
        async for msg in client.iter_messages(int(channel_id), limit=max_messages):
            if not context.user_data.get('indexing'):
                break

            current += 1
            if current % batch_size == 1:
                batch_number += 1
                await context.bot.edit_message_text(
                    chat_id=progress_msg.chat_id,
                    message_id=progress_msg.message_id,
                    text=(
                        f"Batch {batch_number} in progress...\n"
                        f"Messages processed: {current}\n"
                        f"Movies indexed: {total_files}\n"
                        f"Duplicates skipped: {duplicate}\n"
                        f"Unsupported skipped: {unsupported}"
                    )
                )

            try:
                if not msg.document or msg.document.mime_type != 'video/x-matroska':
                    unsupported += 1
                    continue

                file_name = msg.document.attributes[-1].file_name
                message_id = msg.id

                language = None
                name_lower = file_name.lower()
                if 'tamil' in name_lower:
                    language = 'tamil'
                elif 'english' in name_lower:
                    language = 'english'
                elif 'hindi' in name_lower:
                    language = 'hindi'

                try:
                    # Use Telethon to get file_id (access_hash and id)
                    file_id = f"{msg.document.id}:{msg.document.access_hash}"
                except Exception as e:
                    logger.error(f"Error getting file ID for {file_name}: {str(e)}")
                    errors += 1
                    continue

                try:
                    clean_name = file_name.replace('.mkv', '').split('_')
                    title = clean_name[0].replace('.', ' ').strip()
                    year = int(clean_name[1]) if len(clean_name) > 1 and clean_name[1].isdigit() else 0
                    quality = clean_name[2] if len(clean_name) > 2 else 'Unknown'

                    size_bytes = msg.document.size
                    if size_bytes >= 1024 * 1024 * 1024:
                        file_size = f"{size_bytes / (1024 * 1024 * 1024):.2f}GB"
                    else:
                        file_size = f"{size_bytes / (1024 * 1024):.2f}MB"

                    movie_doc = {
                        "title": title,
                        "year": year,
                        "quality": quality,
                        "file_size": file_size,
                        "file_id": file_id,
                        "message_id": message_id,
                        "channel_id": channel_id
                    }
                    if language:
                        movie_doc["language"] = language
                    movie_batch.append(movie_doc)

                    if len(movie_batch) >= batch_size:
                        inserted_ids = add_movies_batch(movie_batch)
                        total_files += len(inserted_ids)
                        duplicate += batch_size - len(inserted_ids)
                        movie_batch = []

                except (IndexError, ValueError, AttributeError) as e:
                    logger.warning(f"Error parsing {file_name}: {str(e)}")
                    errors += 1

            except Exception as e:
                logger.error(f"Error processing message {message_id}: {str(e)}")
                errors += 1
                continue

            if current % batch_size == 0:
                await asyncio.sleep(5)

        # Insert any remaining movies in the batch
        if movie_batch:
            inserted_ids = add_movies_batch(movie_batch)
            total_files += len(inserted_ids)
            duplicate += len(movie_batch) - len(inserted_ids)

    except FloodWaitError as fwe:
        logger.error(f"Flood wait error in batch {batch_number}: {fwe.seconds} seconds")
        await context.bot.edit_message_text(
            chat_id=progress_msg.chat_id,
            message_id=progress_msg.message_id,
            text=f"Flood wait error: Please wait {fwe.seconds} seconds before trying again."
        )
    except Exception as e:
        logger.error(f"Error in batch {batch_number}: {str(e)}")
        errors += 1

    return total_files, duplicate, errors, unsupported, current

async def handle_forwarded_message(update, context):
    """Process forwarded message for channel indexing (single or batch)"""
    chat_id = update.message.chat_id

    if update.callback_query and update.callback_query.data == 'index_cancel':
        context.user_data['indexing'] = False
        context.user_data['index_channel_id'] = None
        context.user_data['index_mode'] = None
        await update.callback_query.message.edit_text("Indexing cancelled.")
        logger.info(f"User {chat_id} cancelled indexing")
        return

    if not context.user_data.get('indexing'):
        return

    # Check if user specified indexing mode
    if not context.user_data.get('index_mode'):
        if update.message.text.lower() in ['batch', 'single']:
            context.user_data['index_mode'] = update.message.text.lower()
            await update.message.reply_text(
                f"{context.user_data['index_mode'].capitalize()} indexing selected. "
                "Now forward a message from the channel to index."
            )
            logger.info(f"User {chat_id} selected {context.user_data['index_mode']} indexing")
            return
        else:
            await update.message.reply_text(
                "Please specify 'batch' or 'single' for indexing mode."
            )
            logger.warning(f"User {chat_id} provided invalid indexing mode")
            return

    # Log forwarded message details for debugging
    logger.debug(f"Forwarded message details: "
                f"forward_from_chat={getattr(update.message, 'forward_from_chat', None)}, "
                f"forward_from_message_id={getattr(update.message, 'forward_from_message_id', None)}, "
                f"forward_date={getattr(update.message, 'forward_date', None)}, "
                f"chat_id={update.message.chat.id}, "
                f"message_id={update.message.message_id}")

    # Check if the message is forwarded
    if not update.message.forward_date:
        await update.message.reply_text("Please forward a message from a channel.")
        logger.warning(f"User {chat_id} sent a non-forwarded message")
        return

    # Check for channel message using forward_from_chat
    forwarded_channel_id = None
    if hasattr(update.message, 'forward_from_chat') and update.message.forward_from_chat and update.message.forward_from_chat.type == 'channel':
        forwarded_channel_id = str(update.message.forward_from_chat.id)
    elif str(update.message.chat.id).startswith('-100'):
        forwarded_channel_id = str(update.message.chat.id)
        logger.info(f"Using fallback channel ID {forwarded_channel_id} for user {chat_id}")

    if not forwarded_channel_id:
        await update.message.reply_text("Please forward a message directly from a channel.")
        logger.warning(f"User {chat_id} forwarded a non-channel message: "
                      f"forward_from_chat={getattr(update.message, 'forward_from_chat', None)}")
        return

    if not forwarded_channel_id.startswith('-100'):
        await update.message.reply_text("Invalid channel ID. Please forward a message from a valid Telegram channel.")
        logger.warning(f"Invalid channel ID {forwarded_channel_id} for user {chat_id}")
        return

    logger.info(f"User {chat_id} forwarded message from channel {forwarded_channel_id}")

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

        # Initialize progress message
        progress_msg = await update.message.reply_text(
            f"Starting {context.user_data['index_mode']} indexing process...",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton('Cancel', callback_data='index_cancel')]]
            )
        )

        # Set up Telethon client with user session
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        session_string = os.getenv("TELETHON_SESSION_STRING")

        if not all([api_id, api_hash, session_string]):
            missing = [var for var, val in [
                ("TELEGRAM_API_ID", api_id),
                ("TELEGRAM_API_HASH", api_hash),
                ("TELETHON_SESSION_STRING", session_string)
            ] if not val]
            error_msg = f"Missing environment variables: {', '.join(missing)}"
            await update.message.reply_text(f"Configuration error: {error_msg}")
            logger.error(f"Indexing failed for channel {forwarded_channel_id}: {error_msg}")
            return

        try:
            client = TelegramClient(StringSession(session_string), int(api_id), api_hash)
            await client.start()
            logger.info("TelegramClient authenticated successfully")

            total_files = 0
            duplicate = 0
            errors = 0
            unsupported = 0
            current = 0

            if context.user_data['index_mode'] == 'batch':
                total_files, duplicate, errors, unsupported, current = await batch_index(
                    client, forwarded_channel_id, progress_msg, context
                )
            else:
                # Single-pass indexing
                max_messages = 1000
                async for msg in client.iter_messages(int(forwarded_channel_id), limit=max_messages):
                    if not context.user_data.get('indexing'):
                        break

                    current += 1
                    try:
                        if current % 20 == 0:
                            await context.bot.edit_message_text(
                                chat_id=progress_msg.chat_id,
                                message_id=progress_msg.message_id,
                                text=(
                                    f"Single-pass indexing in progress...\n"
                                    f"Messages processed: {current}\n"
                                    f"Movies indexed: {total_files}\n"
                                    f"Duplicates skipped: {duplicate}\n"
                                    f"Unsupported skipped: {unsupported}"
                                )
                            )

                        if not msg.document or msg.document.mime_type != 'video/x-matroska':
                            unsupported += 1
                            continue

                        file_name = msg.document.attributes[-1].file_name
                        message_id = msg.id

                        language = None
                        name_lower = file_name.lower()
                        if 'tamil' in name_lower:
                            language = 'tamil'
                        elif 'english' in name_lower:
                            language = 'english'
                        elif 'hindi' in name_lower:
                            language = 'hindi'

                        try:
                            # Use Telethon to get file_id (access_hash and id)
                            file_id = f"{msg.document.id}:{msg.document.access_hash}"
                        except Exception as e:
                            logger.error(f"Error getting file ID for {file_name}: {str(e)}")
                            errors += 1
                            continue

                        try:
                            clean_name = file_name.replace('.mkv', '').split('_')
                            title = clean_name[0].replace('.', ' ').strip()
                            year = int(clean_name[1]) if len(clean_name) > 1 and clean_name[1].isdigit() else 0
                            quality = clean_name[2] if len(clean_name) > 2 else 'Unknown'

                            size_bytes = msg.document.size
                            if size_bytes >= 1024 * 1024 * 1024:
                                file_size = f"{size_bytes / (1024 * 1024 * 1024):.2f}GB"
                            else:
                                file_size = f"{size_bytes / (1024 * 1024):.2f}MB"

                            movie_id = add_movie(
                                title=title,
                                year=year,
                                quality=quality,
                                file_size=file_size,
                                file_id=file_id,
                                message_id=message_id,
                                language=language,
                                channel_id=forwarded_channel_id
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
                f"✅ {context.user_data['index_mode'].capitalize()} indexing completed for channel {forwarded_channel_id}.\n"
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
            logger.info(f"{context.user_data['index_mode'].capitalize()} indexing completed for {forwarded_channel_id}")

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
            context.user_data['index_mode'] = None

    except TelegramError as te:
        await update.message.reply_text(f"Error accessing channel: {str(te)}")
        logger.error(f"Channel access error: {str(te)}")
        context.user_data['indexing'] = False
        context.user_data['index_channel_id'] = None
        context.user_data['index_mode'] = None

async def search_movie(update, context):
    """Handle text-based movie search in personal messages."""
    chat_id = update.message.chat_id
    query = update.message.text.strip()
    
    # Rate limiting
    current_time = time.time()
    if chat_id in context.bot_data.get('recent_searches', {}):
        last_query, last_time = context.bot_data['recent_searches'][chat_id]
        if query == last_query and current_time - last_time < 30:
            await update.message.reply_text("Please wait before repeating the same search.")
            logger.info(f"User {chat_id} rate-limited for query: {query}")
            return
    context.bot_data.setdefault('recent_searches', {})[chat_id] = (query, current_time)

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
        
        # Extract year from query if present
        for term in search_terms[:]:
            if term.isdigit() and len(term) == 4:
                year = int(term)
                search_terms.remove(term)
                break

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
        header = (
            f"Search Query: {query}  TOTAL RESULTS: {total_results}\n\n"
            "🔻 Tap on the file button and then start to download. 🔻\n\n"
        )

        # Format results
        results = []
        for movie_data in movies:
            movie_id, title, movie_year, quality, file_size, file_id, message_id, channel_id = movie_data
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
        # TODO: Ensure get_movie_by_id is defined in database.py or another module
        movie = get_movie_by_id(movie_id)

        if not movie:
            await query.message.reply_text("Movie not found. It may have been deleted.")
            logger.warning(f"Movie not found for download: {movie_id} by user {user_id}")
            await query.answer()
            return

        # TODO: Ensure process_file is defined in utils.py or another module
        success = await process_file(
            bot=context.bot,
            chat_id=user_id,
            file_id=movie['file_id'],
            title=movie['title'],
            quality=movie['quality'],
            file_size=movie['file_size'],
            message=query.message,
            movie_id=movie_id
        )

        if success:
            await query.answer(text="Download started!")
        else:
            await query.answer(text="Download failed.")

    except TelegramError as te:
        await query.message.reply_text("Error sending movie. Please try again later.")
        logger.error(f"Telegram error in download for {movie_id} by user {user_id}: {str(te)}")
        await query.answer(text="Download error.")
    except Exception as e:
        await query.message.reply_text("An error occurred. Please try again later.")
        logger.error(f"Error in download for {movie_id} by user {user_id}: {str(e)}")
        await query.answer(text="Download error.")

async def set_thumbnail(update, context):
    """Initiate thumbnail setting process"""
    await update.message.reply_text(
        "Please upload a JPEG or PNG image for your custom thumbnail or type 'default' for a default thumbnail:",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton('Cancel', callback_data='cancel_thumbnail')]]
        )
    )
    logger.info(f"User {update.message.chat_id} initiated /setthumbnail")
    return SET_THUMBNAIL

async def handle_thumbnail(update, context):
    """Process thumbnail setting with format validation"""
    chat_id = update.message.chat_id
    
    if update.callback_query and update.callback_query.data == 'cancel_thumbnail':
        await update.callback_query.message.edit_text("Thumbnail setting cancelled.")
        logger.info(f"User {chat_id} cancelled thumbnail setting")
        return ConversationHandler.END
        
    if update.message.text and update.message.text.lower() == 'default':
        update_user_settings(chat_id, thumbnail_file_id=None)
        await update.message.reply_text("✅ Custom thumbnail set to default successfully!")
        logger.info(f"User {chat_id} set thumbnail to default")
        return ConversationHandler.END
        
    elif update.message.photo:
        thumbnail_file_id = update.message.photo[-1].file_id
        try:
            file = await context.bot.get_file(thumbnail_file_id)
            if not file.file_path.lower().endswith(('.jpg', '.jpeg', '.png')):
                await update.message.reply_text("Please upload a JPEG or PNG image.")
                logger.warning(f"Invalid thumbnail format from user {chat_id}")
                return SET_THUMBNAIL
            update_user_settings(chat_id, thumbnail_file_id=thumbnail_file_id)
            await update.message.reply_text("✅ Custom thumbnail set successfully!")
            logger.info(f"User {chat_id} set thumbnail: {thumbnail_file_id}")
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Error validating thumbnail for user {chat_id}: {str(e)}")
            await update.message.reply_text("Error processing thumbnail. Please try again.")
            return SET_THUMBNAIL
        
    else:
        await update.message.reply_text("Invalid input. Please upload a JPEG or PNG image or type 'default'.")
        logger.warning(f"Invalid thumbnail input from user {chat_id}")
        return SET_THUMBNAIL

async def set_prefix(update, context):
    """Initiate prefix setting process"""
    await update.message.reply_text(
        "Please enter your custom filename prefix (e.g., MyCollection_):",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton('Cancel', callback_data='cancel_prefix')]]
        )
    )
    logger.info(f"User {update.message.chat_id} initiated /setprefix")
    return SET_PREFIX

async def handle_prefix(update, context):
    """Process prefix setting"""
    chat_id = update.message.chat_id
    
    if update.callback_query and update.callback_query.data == 'cancel_prefix':
        await update.callback_query.message.edit_text("Prefix setting cancelled.")
        logger.info(f"User {chat_id} cancelled prefix setting")
        return ConversationHandler.END
        
    prefix = update.message.text.strip()
    if not prefix.endswith('_'):
        prefix += '_'

    update_user_settings(chat_id, prefix=prefix)
    await update.message.reply_text(f"✅ Custom prefix set to: {prefix}")
    logger.info(f"User {chat_id} set prefix: {prefix}")
    return ConversationHandler.END

async def set_caption(update, context):
    """Initiate caption setting process"""
    await update.message.reply_text(
        "Please enter your custom caption (e.g., My favorite movie!):",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton('Cancel', callback_data='cancel_caption')]]
        )
    )
    logger.info(f"User {update.message.chat_id} initiated /setcaption")
    return SET_CAPTION

async def handle_caption(update, context):
    """Process caption setting"""
    chat_id = update.message.chat_id
    
    if update.callback_query and update.callback_query.data == 'cancel_caption':
        await update.callback_query.message.edit_text("Caption setting cancelled.")
        logger.info(f"User {chat_id} cancelled caption setting")
        return ConversationHandler.END
        
    caption = update.message.text.strip()
    update_user_settings(chat_id, caption=caption)
    await update.message.reply_text(f"✅ Custom caption set to: {caption}")
    logger.info(f"User {chat_id} set caption: {caption}")
    return ConversationHandler.END

async def view_thumbnail(update, context):
    """Show current thumbnail setting"""
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    thumbnail_file_id = settings[0]
    
    if thumbnail_file_id:
        await update.message.reply_photo(
            photo=thumbnail_file_id,
            caption="Your current thumbnail"
        )
        logger.info(f"User {chat_id} viewed thumbnail")
    else:
        await update.message.reply_text(
            "Your thumbnail is set to default (Telegram's default thumbnail will be used)."
        )
        logger.info(f"User {chat_id} has default thumbnail")

async def view_prefix(update, context):
    """Show current prefix setting"""
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    prefix = settings[1]
    
    if prefix:
        await update.message.reply_text(f"Your prefix: {prefix}")
        logger.info(f"User {chat_id} viewed prefix: {prefix}")
    else:
        await update.message.reply_text("No prefix set.")
        logger.info(f"User {chat_id} has no prefix set")

async def view_caption(update, context):
    """Show current caption setting"""
    chat_id = update.message.chat_id
    settings = get_user_settings(chat_id)
    caption = settings[2]
    
    if caption:
        await update.message.reply_text(f"Your caption: {caption}")
        logger.info(f"User {chat_id} viewed caption: {caption}")
    else:
        await update.message.reply_text(
            "No custom caption set. Default caption will be used."
        )
        logger.info(f"User {chat_id} has default caption")

async def stats(update, context):
    """Show bot statistics"""
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

async def cancel(update, context):
    """Cancel any ongoing conversation or indexing"""
    chat_id = update.message.chat_id
    context.user_data['indexing'] = False
    context.user_data['index_channel_id'] = None
    context.user_data['index_mode'] = None
    await update.message.reply_text('Operation cancelled.')
    logger.info(f"User {chat_id} cancelled operation")
    return ConversationHandler.END
