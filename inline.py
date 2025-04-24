import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultDocument
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
    if not query:
        await update.inline_query.answer([])
        logger.info("Empty inline query received")
        return

    logger.info(f"Inline query received: {query}")

    try:
        # Search movies by title (case-insensitive)
        search_terms = query.lower().split()
        regex_patterns = [f".*{term}.*" for term in search_terms]
        movies = movies_collection.find({
            "$or": [
                {"title": {"$regex": pattern, "$options": "i"}}
                for pattern in regex_patterns
            ]
        }).limit(50)

        results = []
        for movie in movies:
            title = movie.get("title", "Unknown")
            year = movie.get("year", 0)
            quality = movie.get("quality", "Unknown")
            file_id = movie.get("file_id")
            file_size = movie.get("file_size", "Unknown")

            if not file_id:
                logger.warning(f"Movie {title} has no file_id")
                continue

            # Create inline result
            results.append(
                InlineQueryResultDocument(
                    id=str(movie["_id"]),
                    title=f"{title} ({year})",
                    document_file_id=file_id,
                    caption=f"{title} ({year}, {quality}, {file_size})",
                    description=f"Quality: {quality}, Size: {file_size}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("Download", callback_data=f"download_{movie['_id']}")
                    ]])
                )
            )

        logger.info(f"Found {len(results)} movies for query: {query}")
        await update.inline_query.answer(results)

    except Exception as e:
        logger.error(f"Error in inline query for '{query}': {str(e)}")
        await update.inline_query.answer([])

async def button_callback(update, context):
    query = update.callback_query
    data = query.data
    if data.startswith("download_"):
        movie_id = data.split("_")[1]
        movie = movies_collection.find_one({"_id": movie_id})
        if movie and movie.get("file_id"):
            await query.message.reply_document(
                document=movie["file_id"],
                caption=f"{movie['title']} ({movie['year']}, {movie['quality']})"
            )
            logger.info(f"User downloaded movie: {movie['title']} ({movie['_id']})")
        else:
            await query.message.reply_text("Movie file not found.")
            logger.warning(f"Movie not found for download: {movie_id}")
        await query.answer()
