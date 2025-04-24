import os
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://horrortimestamiloffl:Shahulshaji10@cluster0.dujxdyr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
try:
    client = MongoClient(MONGO_URI)
    db = client["movie_bot"]
    movies_collection = db["movies"]
    users_collection = db["users"]
except PyMongoError as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    raise

def init_db():
    """Initialize database with necessary indexes."""
    try:
        # Unique index for file_id to prevent duplicates
        movies_collection.create_index([("file_id", 1)], unique=True)
        # Text index for title searches
        movies_collection.create_index([("title", "text")])
        # Compound index for year and language queries
        movies_collection.create_index([("year", 1), ("language", 1)])
        # Unique index for chat_id in users_collection
        users_collection.create_index([("chat_id", 1)], unique=True)
        logger.info("Database indexes created successfully")
    except PyMongoError as e:
        logger.error(f"Error creating database indexes: {str(e)}")
        raise

def add_user(chat_id):
    """Add or update a user in users_collection."""
    try:
        if not isinstance(chat_id, int):
            raise ValueError("chat_id must be an integer")
        users_collection.update_one(
            {"chat_id": chat_id},
            {"$setOnInsert": {"chat_id": chat_id}},
            upsert=True
        )
        logger.info(f"Added/updated user {chat_id}")
    except PyMongoError as e:
        logger.error(f"Error adding user {chat_id}: {str(e)}")
        raise
    except ValueError as ve:
        logger.error(f"Invalid chat_id: {str(ve)}")
        raise

def update_user_settings(chat_id, thumbnail_file_id=None, prefix=None, caption=None):
    """Update user settings in users_collection."""
    try:
        if not isinstance(chat_id, int):
            raise ValueError("chat_id must be an integer")
        update_fields = {}
        if thumbnail_file_id is not None:
            update_fields["thumbnail_file_id"] = thumbnail_file_id
        if prefix is not None:
            if not isinstance(prefix, str):
                raise ValueError("prefix must be a string")
            update_fields["prefix"] = prefix
        if caption is not None:
            if not isinstance(caption, str):
                raise ValueError("caption must be a string")
            update_fields["caption"] = caption
        if update_fields:
            users_collection.update_one(
                {"chat_id": chat_id},
                {"$set": update_fields},
                upsert=True
            )
            logger.info(f"Updated settings for user {chat_id}: {update_fields}")
    except PyMongoError as e:
        logger.error(f"Error updating user settings for {chat_id}: {str(e)}")
        raise
    except ValueError as ve:
        logger.error(f"Invalid input for user settings: {str(ve)}")
        raise

def get_user_settings(chat_id):
    """Retrieve user settings from users_collection."""
    try:
        if not isinstance(chat_id, int):
            raise ValueError("chat_id must be an integer")
        user = users_collection.find_one({"chat_id": chat_id})
        if user:
            return (
                user.get("thumbnail_file_id"),
                user.get("prefix"),
                user.get("caption")
            )
        return (None, None, None)
    except PyMongoError as e:
        logger.error(f"Error retrieving user settings for {chat_id}: {str(e)}")
        raise

def add_movie(title, year, quality, file_size, file_id, message_id, language=None):
    """Add a movie to movies_collection if it doesn't exist."""
    try:
        if not isinstance(file_id, str) or not file_id:
            raise ValueError("file_id must be a non-empty string")
        if not isinstance(message_id, int):
            raise ValueError("message_id must be an integer")
        if not isinstance(title, str) or not title:
            raise ValueError("title must be a non-empty string")
        if not isinstance(year, int):
            raise ValueError("year must be an integer")
        if not isinstance(quality, str):
            raise ValueError("quality must be a string")
        if not isinstance(file_size, str):
            raise ValueError("file_size must be a string")
        if language and not isinstance(language, str):
            raise ValueError("language must be a string")

        movie_doc = {
            "title": title,
            "year": year,
            "quality": quality,
            "file_size": file_size,
            "file_id": file_id,
            "message_id": message_id
        }
        if language:
            movie_doc["language"] = language.lower()

        result = movies_collection.insert_one(movie_doc)
        logger.info(f"Added movie: {title} ({year}, {quality}, {language}) with ID {result.inserted_id}")
        return True
    except DuplicateKeyError:
        logger.info(f"Skipped duplicate movie with file_id {file_id}")
        return False
    except (PyMongoError, ValueError) as e:
        logger.error(f"Error adding movie {title}: {str(e)}")
        raise

def search_movies(movie_name, year=None, language=None):
    """Search movies in movies_collection by name, year, and/or language."""
    try:
        query = {}
        if movie_name:
            terms = movie_name.split()
            query["$text"] = {"$search": " ".join([f"\"{term}\"" for term in terms])}
        if year:
            if not isinstance(year, int):
                raise ValueError("year must be an integer")
            query["year"] = year
        if language:
            if not isinstance(language, str):
                raise ValueError("language must be a string")
            query["language"] = language.lower()

        results = movies_collection.find(query).limit(50)
        movies = [
            (r["title"], r["year"], r["quality"], r["file_size"], r["file_id"], r["message_id"])
            for r in results
        ]
        logger.info(f"Found {len(movies)} movies for query: name={movie_name}, year={year}, language={language}")
        return movies
    except PyMongoError as e:
        logger.error(f"Error searching movies: {str(e)}")
        raise
    except ValueError as ve:
        logger.error(f"Invalid search parameters: {str(ve)}")
        raise
