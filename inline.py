import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultDocument
from telegram.error import TelegramError
from database import search_movies, get_user_settings
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["movie_bot"]
movies_collection = db["movies"]

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def inline_query(update, context):
    query = update.inline_query.query.strip()
    user_id = update.inline_query.from_user.id

    logger.info(f"User {user_id} sent inline query: '{query}'")

    if not query:
        await update.inline_query.answer(
            [],
            switch_pm_text="Type a movie name to search",
            switch_pm_parameter="empty_query"
        )
        logger.info(f"User {user_id} sent empty inline query")
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

        if not movies:
            await update.inline_query.answer(
                [],
                switch_pm_text="No movies found. Try another search.",
                switch_pm_parameter="no_results"
            )
            logger.info(f"No movies found for query: '{query}' by user {user_id}")
            return

        results = []
        for title, movie_year, quality, file_size, file_id, message_id in movies:
            result_id = f"{file_id}_{message_id}_{movie_year}"
            results.append(
                InlineQueryResultDocument(
                    id=result_id,
                    title=f"{title} ({movie_year})",
                    document_file_id=file_id,
                    caption=f"{title} ({movie_year}, {quality}, {file_size})",
                    description=f"Quality: {quality}, Size: {file_size}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("Download", callback_data=f"download_{result_id}")
                    ]])
                )
            )

        logger.info(f"Found {len(results)} movies for query: '{query}' by user {user_id}")
        await update.inline_query.answer(
            results,
            cache_time=300,
            switch_pm_text="Search more movies",
            switch_pm_parameter="search"
        )

    except TelegramError as te:
        logger.error(f"Telegram error in inline query '{query}' by user {user_id}: {str(te)}")
        await update.inline_query.answer(
            [],
            switch_pm_text="Error occurred. Try again later.",
            switch_pm_parameter="error"
        )
    except Exception as e:
        logger.error(f"Error in inline query '{query}' by user {user_id}: {str(e)}")
        await update.inline_query.answer(
            [],
            switch_pm_text="Error occurred. Try again later.",
            switch_pm_parameter="error"
        )

async def button_callback(update, context):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id

    if not data.startswith("download_"):
        await query.answer()
        return

    try:
        result_id = data.split("_", 1)[1]
        file_id, message_id, movie_year = result_id.split("_")
        movie = movies_collection.find_one({"file_id": file_id, "message_id": int(message_id)})

        if not movie:
            await query.message.reply_text("Movie not found. It may have been deleted.")
            logger.warning(f"Movie not found for download: {result_id} by user {user_id}")
            await query.answer()
            return

        thumbnail_file_id, prefix, caption = get_user_settings(user_id)
        final_caption = caption or f"{movie['title']} ({movie['year']}, {movie['quality']})"

        await query.message.reply_document(
            document=movie["file_id"],
            caption=final_caption,
            thumb=thumbnail_file_id,
            parse_mode='Markdown'
        )
        logger.info(f"User {user_id} downloaded movie: {movie['title']} ({movie['_id']})")

        await query.answer(text="Download started!")

    except TelegramError as te:
        logger.error(f"Telegram error in download for {result_id} by user {user_id}: {str(te)}")
        await query.message.reply_text("Error sending movie. Please try again later.")
        await query.answer(text="Download error.")
    except Exception as e:
        logger.error(f"Error in download for {result_id} by user {user_id}: {str(e)}")
        await query.message.reply_text("An error occurred. Please try again later.")
        await query.answer(text="Download error.")
