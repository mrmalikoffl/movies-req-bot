import os
import logging
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
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
import asyncio

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Validate required environment variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")

if not all([TELEGRAM_BOT_TOKEN, MONGO_URI, TELEGRAM_API_ID, TELEGRAM_API_HASH]):
    missing = [var for var, val in [
        ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
        ("MONGO_URI", MONGO_URI),
        ("TELEGRAM_API_ID", TELEGRAM_API_ID),
        ("TELEGRAM_API_HASH", TELEGRAM_API_HASH)
    ] if not val]
    logger.error(f"Missing environment variables: {', '.join(missing)}")
    raise ValueError(f"Missing environment variables: {', '.join(missing)}")

# Conversation states
SET_THUMBNAIL, SET_PREFIX, SET_CAPTION = range(3)

async def error_handler(update, context):
    """Handle errors and log them."""
    logger.error(f"Update {update} caused error: {context.error}")
    if update and hasattr(update, 'message'):
        await update.message.reply_text("An error occurred. Please try again later.")
    elif update and hasattr(update, 'callback_query'):
        await update.callback_query.message.reply_text("An error occurred. Please try again later.")

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
                SET_THUMBNAIL: [MessageHandler(filters.PHOTO | filters.TEXT, handle_thumbnail)],
                SET_PREFIX: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_prefix)],
                SET_CAPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_caption)]
            },
            fallbacks=[]
        )

        # Register handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("index", index))
        application.add_handler(CommandHandler("stats", stats))
        application.add_handler(MessageHandler(filters.FORWARDED, handle_forwarded_message))
        application.add_handler(conv_handler)
        application.add_handler(CommandHandler("viewthumbnail", view_thumbnail))
        application.add_handler(CommandHandler("viewprefix", view_prefix))
        application.add_handler(CommandHandler("viewcaption", view_caption))
        application.add_handler(InlineQueryHandler(inline_query))
        application.add_handler(CallbackQueryHandler(button_callback))
        application.add_handler(CallbackQueryHandler(handle_forwarded_message, pattern='index_cancel'))
        application.add_error_handler(error_handler)

        # Start bot
        logger.info("Bot started polling")
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        logger.info("Bot is polling...")

        # Keep the bot running
        await asyncio.Event().wait()

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        raise
    finally:
        logger.info("Shutting down bot")
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown by user")
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise
    finally:
        loop.close()
