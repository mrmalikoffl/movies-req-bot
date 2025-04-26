import os
import signal
import logging
import asyncio
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
)
from telegram import Update
from telegram.error import TelegramError
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
    cancel,
    SET_THUMBNAIL,
    SET_PREFIX,
    SET_CAPTION,
)
from database import check_db_connection
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Bot version
BOT_VERSION = "1.0.0"

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

async def error_handler(update: Update, context):
    """Handle errors and log them."""
    try:
        logger.error(f"Update {update} caused error: {context.error}", exc_info=context.error)
        if update and update.message:
            await update.message.reply_text("An error occurred. Please try again later.")
    except TelegramError as te:
        logger.error(f"Error sending error message: {str(te)}")
    except Exception as e:
        logger.error(f"Unexpected error in error_handler: {str(e)}")

async def cleanup_recent_searches(context):
    """Periodically clean up recent_searches to prevent memory growth."""
    while True:
        try:
            current_time = asyncio.get_event_loop().time()
            expired = [
                chat_id
                for chat_id, (_, timestamp) in context.bot_data.get("recent_searches", {}).items()
                if current_time - timestamp > 3600
            ]
            for chat_id in expired:
                del context.bot_data["recent_searches"][chat_id]
            logger.info(f"Cleaned up {len(expired)} expired search records")
        except Exception as e:
            logger.error(f"Error in cleanup_recent_searches: {str(e)}")
        await asyncio.sleep(3600)  # Run hourly

async def main():
    """Main function to set up and run the bot."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN is not set")
        raise ValueError("TELEGRAM_BOT_TOKEN is not set")

    logger.info(f"Starting Telegram bot (version {BOT_VERSION}) with token: {bot_token[:10]}...")

    # Check MongoDB connection
    try:
        if not check_db_connection():
            logger.error("MongoDB connection failed")
            raise ConnectionError("MongoDB connection failed")
        logger.info("MongoDB connection is healthy")
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

    # Build the application
    application = (
        Application.builder()
        .token(bot_token)
        .read_timeout(20.0)
        .write_timeout(20.0)
        .connect_timeout(20.0)
        .build()
    )

    # Add error handler
    application.add_error_handler(error_handler)

    # Command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("index", index))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("viewthumbnail", view_thumbnail))
    application.add_handler(CommandHandler("viewprefix", view_prefix))
    application.add_handler(CommandHandler("viewcaption", view_caption))

    # Conversation handler for settings
    settings_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("setthumbnail", set_thumbnail),
            CommandHandler("setprefix", set_prefix),
            CommandHandler("setcaption", set_caption),
        ],
        states={
            SET_THUMBNAIL: [
                MessageHandler(filters.PHOTO | filters.Regex(r"^(default)$"), handle_thumbnail),
                CallbackQueryHandler(handle_thumbnail, pattern=r"^cancel_thumbnail$"),
            ],
            SET_PREFIX: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_prefix),
                CallbackQueryHandler(handle_prefix, pattern=r"^cancel_prefix$"),
            ],
            SET_CAPTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_caption),
                CallbackQueryHandler(handle_caption, pattern=r"^cancel_caption$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False  # Changed to False to resolve PTBUserWarning
    )
    application.add_handler(settings_conv_handler)

    # Message handlers
    application.add_handler(MessageHandler(filters.Regex(r"^(batch|single)$"), handle_forwarded_message))
    application.add_handler(MessageHandler(filters.FORWARDED, handle_forwarded_message))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, search_movie))

    # Callback query handler
    application.add_handler(CallbackQueryHandler(button_callback))

    # Initialize and start polling
    try:
        await application.initialize()
        await application.start()
        logger.info("Bot started polling")

        # Start cleanup task
        cleanup_task = asyncio.create_task(cleanup_recent_searches(application))

        # Start polling with timeout configuration
        await application.updater.start_polling(
            timeout=20.0,
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )

        # Keep the bot running until a shutdown signal is received
        shutdown_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_event.set)

        await shutdown_event.wait()

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}", exc_info=True)
        raise
    finally:
        try:
            # Cancel background tasks
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                logger.info("Cleanup task cancelled")

            # Stop and shut down the application
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
            logger.info("Bot shut down successfully")
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")

def handle_shutdown(loop, application):
    """Handle shutdown signals gracefully."""
    logger.info("Received shutdown signal, stopping bot...")
    tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task()]
    for task in tasks:
        logger.info(f"Cancelling task: {task}")
        task.cancel()
    loop.run_until_complete(application.updater.stop())
    loop.run_until_complete(application.stop())
    loop.run_until_complete(application.shutdown())
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    logger.info("Shutdown complete")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    # Set up signal handlers
    application = None  # Define application to avoid NameError in handle_shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda: handle_shutdown(loop, application)
        )

    try:
        loop.run_until_complete(main())
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise
    finally:
        if not loop.is_closed():
            loop.close()
