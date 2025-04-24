import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultDocument
from database import search_movies
from bson.objectid import ObjectId

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
        # Split query into terms for flexible matching
        search_terms = query.split()
        year = None
        try:
            # Extract year if present (e.g., "1921" in "The Kid 1921")
            for term in search_terms:
                if term.isdigit() and len(term) == 4:
                    year = int(term)
                    search_terms.remove(term)
                    break
        except ValueError:
            pass

        movie_name = " ".join(search_terms)
        movies = search_movies(movie_name, year=year)

        results = []
        for title, movie_year, quality, file_size, file_id, message_id in movies:
            result_id = f"{file_id}_{message_id}"
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

        logger.info(f"Found {len(results)} movies for query: {query}")
        await update.inline_query.answer(results)

    except Exception as e:
        logger.error(f"Error in inline query for '{query}': {str(e)}")
        await update.inline_query.answer([])

async def button_callback(update, context):
    query = update.callback_query
    data = query.data
    if data.startswith("download_"):
        result_id = data.split("_", 1)[1]
        file_id = result_id.split("_")[0]
        movie = movies_collection.find_one({"file_id": file_id})
        if movie:
            await query.message.reply_document(
                document=movie["file_id"],
                caption=f"{movie['title']} ({movie['year']}, {movie['quality']})"
            )
            logger.info(f"User downloaded movie: {movie['title']} ({movie['_id']})")
        else:
            await query.message.reply_text("Movie file not found.")
            logger.warning(f"Movie not found for download: {result_id}")
        await query.answer()
