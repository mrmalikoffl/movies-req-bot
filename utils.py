import os
import logging
import asyncio
import time
import tempfile
from PIL import Image
from telegram.error import TelegramError, BadRequest, NetworkError
from database import get_user_settings
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

async def fix_thumb(thumb):
    """Process and resize thumbnail image to Baseline JPEG"""
    width = 0
    height = 0
    try:
        if thumb is not None:
            metadata = extractMetadata(createParser(thumb))
            if metadata is not None:
                if metadata.has("width"):
                    width = metadata.get("width")
                if metadata.has("height"):
                    height = metadata.get("height")
                
                with Image.open(thumb) as img:
                    img = img.convert("RGB")
                    new_width = 320
                    aspect_ratio = height / width if width > 0 else 1
                    new_height = int(new_width * aspect_ratio)
                    img = img.resize((new_width, new_height))
                    img.save(thumb, "JPEG", quality=95, optimize=True, progressive=False)
                return width, height, thumb
    except Exception as e:
        logger.error(f"Error in fix_thumb: {str(e)}")
    return width, height, None

async def process_file(bot, chat_id, file_id, title, quality, file_size, message, retries=3):
    """
    Asynchronously download a movie file, apply user settings (thumbnail, prefix, caption), and send it to the user.
    
    Args:
        bot: Telegram Bot instance
        chat_id: User chat ID (int)
        file_id: File ID of the movie (str)
        title: Movie title (str)
        quality: Movie quality (str)
        file_size: File size (str, e.g., "1.2GB")
        message: Telegram message object for replying
        retries: Number of retry attempts for downloads (int)
    
    Returns:
        bool: True if successful, False otherwise
    """
    settings = get_user_settings(chat_id)
    thumbnail_file_id = settings[0] if settings else None
    prefix = settings[1] if settings else ""
    caption = settings[2] if settings else f"{title} ({quality})"

    with tempfile.NamedTemporaryFile(suffix=".mkv", dir=DOWNLOAD_DIR, delete=False) as movie_tmp, \
         tempfile.NamedTemporaryFile(suffix=".jpg", dir=DOWNLOAD_DIR, delete=False) as thumb_tmp:
        file_path = movie_tmp.name
        thumb_path = thumb_tmp.name

        try:
            # Download movie file with retries
            logger.info(f"Downloading file {file_id} for user {chat_id}")
            attempt = 0
            while attempt < retries:
                try:
                    file = await bot.get_file(file_id)
                    await file.download_to_drive(file_path)
                    break
                except (NetworkError, TelegramError) as e:
                    attempt += 1
                    if attempt == retries:
                        logger.error(f"Failed to download file {file_id} for user {chat_id} after {retries} attempts: {str(e)}")
                        await message.reply_text("Sorry, I couldn't download the file. Please try again later.")
                        return False
                    logger.warning(f"Download attempt {attempt} failed for user {chat_id}: {str(e)}. Retrying...")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

            # Prepare thumbnail
            logger.info(f"Preparing thumbnail for user {chat_id}, thumbnail_file_id: {thumbnail_file_id}")
            processed_thumb = None
            if thumbnail_file_id:
                try:
                    thumb_file = await bot.get_file(thumbnail_file_id)
                    await thumb_file.download_to_drive(thumb_path)
                    _, _, processed_thumb = await fix_thumb(thumb_path)
                    if not processed_thumb:
                        logger.warning(f"Thumbnail processing failed for user {chat_id}")
                        processed_thumb = None
                except (TelegramError, IOError) as e:
                    logger.warning(f"Failed to process custom thumbnail for user {chat_id}: {str(e)}")
                    processed_thumb = None

            # Create default thumbnail if custom thumbnail fails or is not set
            if not processed_thumb:
                with Image.new('RGB', (320, 180), color='blue') as img:
                    img.save(thumb_path, "JPEG", quality=95, optimize=True, progressive=False)
                processed_thumb = thumb_path

            # Prepare filename and caption
            filename = f"{prefix}{title.replace(' ', '_')}_{quality}.mkv"
            final_caption = f"[{file_size}] {caption} MKV"

            # Send file
            logger.info(f"Sending file {filename} to user {chat_id}")
            try:
                with open(file_path, 'rb') as movie_file, open(processed_thumb, 'rb') as thumb_file:
                    await message.reply_document(
                        document=movie_file,
                        filename=filename,
                        caption=final_caption,
                        thumbnail=thumb_file,
                        parse_mode=None
                    )
                return True
            except BadRequest as e:
                logger.error(f"BadRequest sending file to user {chat_id}: {str(e)}")
                await message.reply_text("Error: The file or thumbnail is invalid. Please try again.")
                return False
            except NetworkError as e:
                logger.error(f"Network error sending file to user {chat_id}: {str(e)}")
                await message.reply_text("Network error while sending the file. Please try again later.")
                return False

        except TelegramError as e:
            logger.error(f"Telegram error processing file for user {chat_id}: {str(e)}")
            await message.reply_text("Sorry, there was an error downloading or sending the file. Please try again.")
            return False
        except IOError as e:
            logger.error(f"File I/O error for user {chat_id}: {str(e)}")
            await message.reply_text("Sorry, there was an error processing the file. Please try again.")
            return False
        except Exception as e:
            logger.error(f"Unexpected error for user {chat_id}: {str(e)}")
            await message.reply_text("An unexpected error occurred. Please try again later.")
            return False
        finally:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Deleted temporary file: {file_path}")
                if os.path.exists(thumb_path):
                    os.remove(thumb_path)
                    logger.info(f"Deleted temporary thumbnail: {thumb_path}")
            except OSError as e:
                logger.warning(f"Error cleaning up files for user {chat_id}: {str(e)}")

def cleanup_download_dir():
    """Clean up old files in DOWNLOAD_DIR on startup"""
    for file in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up old file: {file_path}")
        except OSError as e:
            logger.warning(f"Error cleaning up {file_path}: {str(e)}")
