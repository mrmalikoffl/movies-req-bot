import os
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError
from dotenv import load_dotenv
from bson.objectid import ObjectId

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
        movies_collection.create_index([("file_id", 1), ("message_id", 1)], unique=True)
        movies_collection.create_index([("title", "text")])
        movies_collection.create_index([("year", 1), ("language", 1)])
        users_collection.create_index([("chat_id", 1)], unique=True)
        logger.info("Database indexes created successfully")
    except PyMongoError as e:
        logger.error(f"Error creating database indexes: {str(e)}")
        raise

def add_user(chat_id):
    """Add or update a user in users_collection with default settings."""
    try:
        if not isinstance(chat_id, int):
            raise ValueError("chat_id must be an integer")
        users_collection.update_one(
            {"chat_id": chat_id},
            {"$setOnInsert": {
                "chat_id": chat_id,
                "thumbnail_file_id": None,
                "prefix": None,
                "caption": None
            }},
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
            result = users_collection.update_one(
                {"chat_id": chat_id},
                {"$set": update_fields},
                upsert=True
            )
            logger.info(f"Updated settings for user {chat_id}: {update_fields}, matched: {result.matched_count}, modified: {result.modified_count}")
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
        if not user:
            logger.warning(f"User {chat_id} not found in database, adding user")
            add_user(chat_id)
            return None, None, None
        settings = (
            user.get("thumbnail_file_id"),
            user.get("prefix"),
            user.get("caption")
        )
        logger.info(f"Retrieved settings for user {chat_id}: thumbnail_file_id={settings[0]}, prefix={settings[1]}, caption={settings[2]}")
        return settings
    except PyMongoError as e:
        logger.error(f"Error retrieving user settings for {chat_id}: {str(e)}")
        raise

def add_movie(title, year, quality, file_size, file_id, message_id, language=None, channel_id=None):
    """Add a movie to movies_collection if it doesn't exist, return the _id."""
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
        if channel_id and not isinstance(channel_id, str):
            raise ValueError("channel_id must be a string")

        movie_doc = {
            "title": title,
            "year": year,
            "quality": quality,
            "file_size": file_size,
            "file_id": file_id,
            "message_id": message_id,
            "channel_id": channel_id  # Add channel_id to the document
        }
        if language:
            movie_doc["language"] = language.lower()

        result = movies_collection.insert_one(movie_doc)
        logger.info(f"Added movie: {title} ({year}, {quality}, {language}) from channel {channel_id} with ID {result.inserted_id}")
        return str(result.inserted_id)
    except DuplicateKeyError:
        logger.info(f"Skipped duplicate movie with file_id {file_id} and message_id {message_id}")
        return None
    except (PyMongoError, ValueError) as e:
        logger.error(f"Error adding movie {title}: {str(e)}")
        raise

def search_movies(movie_name, year=None, language=None):
    """Search movies in movies_collection by name, year, and/or language with fallback."""
    try:
        if not isinstance(movie_name, str) or not movie_name:
            raise ValueError("movie_name must be a non-empty string")
        if year and not isinstance(year, int):
            raise ValueError("year must be an integer")
        if language and not isinstance(language, str):
            raise ValueError("language must be a string")

        # Primary query with all filters
        query = {}
        if movie_name:
            terms = movie_name.split()
            query["$text"] = {"$search": " ".join([f"\"{term}\"" for term in terms])}
        if year:
            query["year"] = year
        if language:
            query["language"] = language.lower()

        results = movies_collection.find(query).limit(50)
        movies = [
            (str(r["_id"]), r["title"], r["year"], r["quality"], r["file_size"], r["file_id"], r["message_id"], r.get("channel_id", "-1002559398614"))
            for r in results
        ]
        logger.info(f"Found {len(movies)} movies for query: name={movie_name}, year={year}, language={language}")

        # Fallback: If no results and year is specified, try without year
        if not movies and year:
            query.pop("year")
            results = movies_collection.find(query).limit(50)
            movies = [
                (str(r["_id"]), r["title"], r["year"], r["quality"], r["file_size"], r["file_id"], r["message_id"], r.get("channel_id", "-1002559398614"))
                for r in results
            ]
            logger.info(f"Fallback search without year: Found {len(movies)} movies for name={movie_name}, language={language}")

        return movies
    except PyMongoError as e:
        logger.error(f"Error searching movies: {str(e)}")
        raise
    except ValueError as ve:
        logger.error(f"Invalid search parameters: {str(ve)}")
        raise
