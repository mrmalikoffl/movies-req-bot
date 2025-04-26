import os
import logging
import aiofiles
import asyncio
from PIL import Image
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
from telegram.error import TelegramError, BadRequest, NetworkError
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Document
from telethon.errors import FloodWaitError, ChannelPrivateError, FileReferenceExpiredError
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SESSION_STRING = os.getenv("TELETHON_SESSION_STRING")  # Align with handlers.py

# Initialize Telethon client
telethon_client = None
if TELEGRAM_API_ID and TELEGRAM_API_HASH and SESSION_STRING:
    try:
        telethon_client = TelegramClient(
            StringSession(SESSION_STRING),
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH
        )
        logger.info("Telethon client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Telethon client: {str(e)}")
else:
    logger.warning("Telethon credentials missing; large file downloads may fail")

async def fix_thumb(thumb_path):
    """Optimize thumbnail image."""
    try:
        async with aiofiles.tempfile.NamedTemporaryFile('wb', suffix='.jpg', delete=False) as temp_file:
            temp_path = temp_file.name
            img = Image.open(thumb_path)
            img = img.convert('RGB')
            img.thumbnail((320, 320))
            img.save(temp_path, 'JPEG', quality=85)
            logger.info(f"Optimized thumbnail: {temp_path}")
            return temp_path
    except Exception as e:
        logger.error(f"Error optimizing thumbnail {thumb_path}: {str(e)}")
        return thumb_path

async def get_file_metadata(file_path):
    """Extract metadata from a media file."""
    try:
        parser = createParser(file_path)
        if not parser:
            logger.warning(f"Could not parse metadata for {file_path}")
            return None
        metadata = extractMetadata(parser)
        if not metadata:
            logger.warning(f"No metadata found for {file_path}")
            return None
        meta_dict = {k: v for k, v in metadata.exportDictionary().items() if isinstance(v, (str, int, float))}
        parser.close()
        logger.info(f"Extracted metadata for {file_path}: {meta_dict}")
        return meta_dict
    except Exception as e:
        logger.error(f"Error extracting metadata for {file_path}: {str(e)}")
        return None

async def process_file(bot, chat_id, file_id, title, quality, file_size, message, movie_id):
    """Download, process, and send a file to the user with retries."""
    from database import get_user_settings
    import aiofiles.os

    max_retries = 3
    temp_file_path = None
    temp_thumb_path = None

    try:
        # Get user settings
        thumb_file_id, prefix, caption = get_user_settings(chat_id)
        caption_text = caption or f"{prefix or ''} {title} [{quality}]".strip()

        # Attempt to download using Bot API
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading file {file_id} ({file_size}) via Bot API for user {chat_id}")
                file = await bot.get_file(file_id)
                file_path = file.file_path

                async with aiofiles.tempfile.NamedTemporaryFile('wb', suffix='.mkv', delete=False) as temp_file:
                    temp_file_path = temp_file.name
                    await file.download_to_path(temp_file_path)
                    logger.info(f"Downloaded file to {temp_file_path} for user {chat_id}")
                    break
            except BadRequest as e:
                if "File is too big" in str(e) or "Wrong file identifier" in str(e):
                    logger.warning(f"Download attempt {attempt + 1} failed for user {chat_id}: {str(e)}. Retrying...")
                    if attempt == max_retries - 1:
                        logger.info(f"Falling back to Telethon for file {file_id} for user {chat_id}")
                        break
                else:
                    logger.error(f"BadRequest in download attempt {attempt + 1} for user {chat_id}: {str(e)}")
                    if attempt == max_retries - 1:
                        raise
            except (NetworkError, TelegramError) as e:
                logger.warning(f"Download attempt {attempt + 1} failed for user {chat_id}: {str(e)}. Retrying...")
                await asyncio.sleep(2 ** attempt)
                if attempt == max_retries - 1:
                    raise
            except Exception as e:
                logger.error(f"Unexpected error in download attempt {attempt + 1} for user {chat_id}: {str(e)}")
                raise

        # If Bot API download failed, try Telethon
        if not temp_file_path and telethon_client:
            from database import get_movie_by_id
            movie = get_movie_by_id(movie_id)
            if not movie:
                logger.error(f"Movie not found for movie_id {movie_id}")
                raise ValueError("Movie not found in database")
            if not movie.get('channel_id') or not movie.get('message_id'):
                logger.error(f"Cannot use Telethon: Missing channel_id or message_id for movie_id {movie_id}")
                raise ValueError("Cannot download large file: Missing channel or message data")

            async with telethon_client:
                try:
                    logger.info(f"Downloading large file {file_id} via Telethon for user {chat_id}")
                    message_obj = await telethon_client.get_messages(
                        entity=movie['channel_id'],
                        ids=movie['message_id']
                    )
                    if not message_obj or not hasattr(message_obj, 'media') or not isinstance(message_obj.media, Document):
                        logger.error(f"No valid media found for message {movie['message_id']} in channel {movie['channel_id']}")
                        raise ValueError("No valid media found")

                    async with aiofiles.tempfile.NamedTemporaryFile('wb', suffix='.mkv', delete=False) as temp_file:
                        temp_file_path = temp_file.name
                        await telethon_client.download_media(
                            message=message_obj,
                            file=temp_file_path
                        )
                        logger.info(f"Downloaded large file to {temp_file_path} via Telethon for user {chat_id}")
                except (FloodWaitError, ChannelPrivateError, FileReferenceExpiredError) as e:
                    logger.error(f"Telethon download failed for user {chat_id}: {str(e)}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected Telethon error for user {chat_id}: {str(e)}")
                    raise
        elif not temp_file_path:
            logger.error(f"Cannot download file {file_id}: Telethon credentials missing")
            await bot.send_message(
                chat_id=chat_id,
                text="Sorry, this file is too large to download due to configuration issues. Please contact the bot administrator."
            )
            raise ValueError("Failed to download file via Bot API or Telethon: Telethon credentials missing")

        # Process thumbnail
        if thumb_file_id:
            try:
                thumb_file = await bot.get_file(thumb_file_id)
                async with aiofiles.tempfile.NamedTemporaryFile('wb', suffix='.jpg', delete=False) as temp_thumb:
                    temp_thumb_path = temp_thumb.name
                    await thumb_file.download_to_path(temp_thumb_path)
                    temp_thumb_path = await fix_thumb(temp_thumb_path)
                    logger.info(f"Processed thumbnail for user {chat_id}: {temp_thumb_path}")
            except Exception as e:
                logger.error(f"Error processing thumbnail for user {chat_id}: {str(e)}")
                temp_thumb_path = None

        # Send file
        async with aiofiles.open(temp_file_path, 'rb') as f:
            await bot.send_document(
                chat_id=chat_id,
                document=f,
                caption=caption_text,
                thumb=temp_thumb_path,
                reply_to_message_id=message.message_id,
                parse_mode='HTML'
            )
            logger.info(f"Sent file {title} to user {chat_id}")

        return True

    except Exception as e:
        logger.error(f"Failed to download file {file_id} for user {chat_id} after {max_retries} attempts: {str(e)}")
        return False
    finally:
        # Clean up temporary files
        try:
            if temp_file_path and await aiofiles.os.path.exists(temp_file_path):
                await aiofiles.os.remove(temp_file_path)
                logger.info(f"Deleted temporary file: {temp_file_path}")
            if temp_thumb_path and await aiofiles.os.path.exists(temp_thumb_path):
                await aiofiles.os.remove(temp_thumb_path)
                logger.info(f"Deleted temporary thumbnail: {temp_thumb_path}")
        except Exception as e:
            logger.error(f"Error cleaning up temporary files: {str(e)}")
