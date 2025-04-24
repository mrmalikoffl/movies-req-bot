import os
import logging
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    Filters,
    InlineQueryHandler,
    CallbackQueryHandler,
    ConversationHandler
)
from handlers import (
    start, index, handle_forwarded_message, set_thumbnail, handle_thumbnail,
    set_prefix, handle_prefix, set_caption, handle_caption,
    view_thumbnail, view_prefix, view_caption, stats
)
from inline import inline_query, button_callback
from database import init_db
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Validate required environment variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

if not all([TELEGRAM_BOT_TOKEN, MONGO_URI]):
    missing = [var for var, val in [
        ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
        ("MONGO_URI", MONGO_URI)
    ] if not val]
    logger.error(f"Missing environment variables: {', '.join(missing)}")
    raise ValueError(f"Missing environment variables: {', '.join(missing)}")

# Conversation states
SET_THUMBNAIL, SET_PREFIX, SET_CAPTION = range(3)

async def main():
    try:
        # Initialize database
        logger.info("Initializing MongoDB database")
        init_db()

        # Set up bot
        logger.info("Starting Telegram bot")
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # Conversation handler for settings
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("setthumbnail", set_thumbnail),
                CommandHandler("setprefix", set_prefix),
                CommandHandler("setcaption", set_caption)
            ],
            states={
                SET_THUMBNAIL: [MessageHandler(Filters.PHOTO | Filters.TEXT, handle_thumbnail)],
                SET_PREFIX: [MessageHandler(Filters.TEXT & ~Filters.COMMAND, handle_prefix)],
                SET_CAPTION: [MessageHandler(Filters.TEXT & ~Filters.COMMAND, handle_caption)]
            },
            fallbacks=[]
        )

        # Register handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("index", index))
        application.add_handler(CommandHandler("stats", stats))
        application.add_handler(MessageHandler(Filters.FORWARDED, handle_forwarded_message))
        application.add_handler(conv_handler)
        application.add_handler(CommandHandler("viewthumbnail", view_thumbnail))
        application.add_handler(CommandHandler("viewprefix", view_prefix))
        application.add_handler(CommandHandler("viewcaption", view_caption))
        application.add_handler(InlineQueryHandler(inline_query))
        application.add_handler(CallbackQueryHandler(button_callback))

        # Start bot
        logger.info("Bot started polling")
        await application.run_polling()

        logger.info("Bot stopped")

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        raise

if __name__ == '__main__':
    try:
        import asyncio
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown by user")
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise
