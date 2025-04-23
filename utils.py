import os
from PIL import Image
from database import get_user_settings

DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def process_file(bot, chat_id, file_id, title, quality, message):
    settings = get_user_settings(chat_id)
    thumbnail_file_id = settings[0] if settings else None
    prefix = settings[1] if settings else ""
    caption = settings[2] if settings else "Enjoy the movie!"

    file_path = os.path.join(DOWNLOAD_DIR, f"{title}_{quality}.mkv")
    try:
        file = bot.get_file(file_id)
        file.download(file_path)

        # Prepare thumbnail
        thumb_path = os.path.join(DOWNLOAD_DIR, "thumb.jpg")
        if thumbnail_file_id:
            thumb_file = bot.get_file(thumbnail_file_id)
            thumb_file.download(thumb_path)
            img = Image.open(thumb_path)
            img.thumbnail((128, 128))
            img.save(thumb_path)
        else:
            Image.new('RGB', (128, 128), color='blue').save(thumb_path)

        # Send file
        filename = f"{prefix}{title}_{quality}.mkv"
        message.reply_document(
            document=open(file_path, 'rb'),
            filename=filename,
            thumb=open(thumb_path, 'rb'),
            caption=caption
        )
        os.remove(file_path)
        os.remove(thumb_path)
    except Exception as e:
        raise e