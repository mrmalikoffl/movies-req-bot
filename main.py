import os
import logging
import asyncio
import signal
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters
)
from handlers import (
    start,
    index,
    handle_forwarded_message,
    search_movie,
    button_callback,
    set_thumbnail,
    handle_thumbnail,
    set_prefix,
    handle_prefix,
    set_caption,
    handle_caption,
    view_thumbnail,
    view_prefix,
    view_caption,
    stats,
    SET_THUMBNAIL,
    SET_PREFIX,
    SET_CAPTION
)
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("Missing TELEGRAM_BOT_TOKEN")
    raise ValueError("Missing TELEGRAM_BOT_TOKEN")

# Global variable to track recent searches for deduplication
recent_searches = {}

async def error_handler(update, context):
    logger.error(f"Update {update} caused error: {context.error}")
    if update:
        if hasattr(update, 'message') and update.message:
            await update.message.reply_text("An error occurred. Please try again later.")
        elif hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.message.reply_text("An error occurred. Please try again later.")
            await update.callback_query.answer()

async def main():
    try:
        logger.info(f"Starting Telegram bot with token: {TELEGRAM_BOT_TOKEN[:10]}...")
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # Command handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("index", index))
        application.add_handler(CommandHandler("stats", stats))
        application.add_handler(CommandHandler("viewthumbnail", view_thumbnail))
        application.add_handler(CommandHandler("viewprefix", view_prefix))
        application.add_handler(CommandHandler("viewcaption", view_caption))

        # Conversation handlers
        thumbnail_conv = ConversationHandler(
            entry_points=[CommandHandler("setthumbnail", set_thumbnail)],
            states={
                SET_THUMBNAIL: [MessageHandler(filters.PHOTO | filters.TEXT, handle_thumbnail)]
            },
            fallbacks=[]
        )
        prefix_conv = ConversationHandler(
            entry_points=[CommandHandler("setprefix", set_prefix)],
            states={
                SET_PREFIX: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_prefix)]
            },
            fallbacks=[]
        )
        caption_conv = ConversationHandler(
            entry_points=[CommandHandler("setcaption", set_caption)],
            states={
                SET_CAPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_caption)]
            },
            fallbacks=[]
        )
        application.add_handler(thumbnail_conv)
        application.add_handler(prefix_conv)
        application.add_handler(caption_conv)

        # Indexing handler
        application.add_handler(MessageHandler(filters.FORWARDED & filters.ChatType.PRIVATE, handle_forwarded_message))
        application.add_handler(CallbackQueryHandler(handle_forwarded_message, pattern='^index_cancel$'))

        # Search and download handlers
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, search_movie))
        application.add_handler(CallbackQueryHandler(button_callback, pattern='^download_.*$'))

        # Error handler
        application.add_error_handler(error_handler)

        # Set up signal handlers for graceful shutdown
        def shutdown_handler(signum, frame):
            logger.info("Received shutdown signal, stopping bot...")
            asyncio.create_task(application.updater.stop())
            asyncio.create_task(application.stop())
            asyncio.create_task(application.shutdown())

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        logger.info("Bot started polling")
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        logger.info("Bot is polling...")
        await asyncio.Event().wait()  # Keep the bot running

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
