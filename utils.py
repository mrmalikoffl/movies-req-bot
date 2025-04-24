import os
import logging
from PIL import Image
from telegram.error import TelegramError
from database import get_user_settings

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def process_file(bot, chat_id, file_id, title, quality, message):
    """
    Download a movie file, apply user settings (thumbnail, prefix, caption), and send it to the user.
    """
    settings = get_user_settings(chat_id)
    thumbnail_file_id = settings[0] if settings else None
    prefix = settings[1] if settings else ""
    caption = settings[2] if settings else "Enjoy the movie!"

    file_path = os.path.join(DOWNLOAD_DIR, f"{title}_{quality}.mkv")
    thumb_path = os.path.join(DOWNLOAD_DIR, "thumb.jpg")

    try:
        # Download movie file
        logger.info(f"Downloading file {file_id} for user {chat_id}")
        file = bot.get_file(file_id)
        file.download(file_path)

        # Prepare thumbnail
        logger.info(f"Preparing thumbnail for user {chat_id}, thumbnail_file_id: {thumbnail_file_id}")
        if thumbnail_file_id:
            try:
                thumb_file = bot.get_file(thumbnail_file_id)
                thumb_file.download(thumb_path)
                img = Image.open(thumb_path)
                img.thumbnail((128, 128))
                img.save(thumb_path)
            except (TelegramError, IOError) as e:
                logger.warning(f"Failed to process thumbnail for user {chat_id}: {str(e)}")
                # Fallback to default thumbnail
                Image.new('RGB', (128, 128), color='blue').save(thumb_path)
        else:
            Image.new('RGB', (128, 128), color='blue').save(thumb_path)

        # Send file
        filename = f"{prefix}{title}_{quality}.mkv"
        logger.info(f"Sending file {filename} to user {chat_id}")
        message.reply_document(
            document=open(file_path, 'rb'),
            filename=filename,
            thumb=open(thumb_path, 'rb'),
            caption=caption
        )

    except TelegramError as e:
        logger.error(f"Telegram error processing file for user {chat_id}: {str(e)}")
        message.reply_text("Sorry, there was an error downloading or sending the file. Please try again.")
    except IOError as e:
        logger.error(f"File I/O error for user {chat_id}: {str(e)}")
        message.reply_text("Sorry, there was an error processing the file. Please try again.")
    except Exception as e:
        logger.error(f"Unexpected error for user {chat_id}: {str(e)}")
        message.reply_text("An unexpected error occurred. Please try again later.")
    finally:
        # Clean up temporary files
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted temporary file: {file_path}")
            if os.path.exists(thumb_path):
                os.remove(thumb_path)
                logger.info(f"Deleted temporary thumbnail: {thumb_path}")
        except OSError as e:
            logger.warning(f"Error cleaning up files for user {chat_id}: {str(e)}")
