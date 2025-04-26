import os
import logging
from pymongo import MongoClient, TEXT, errors
from pymongo.errors import DuplicateKeyError, PyMongoError
from dotenv import load_dotenv
from bson.objectid import ObjectId

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB connection
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    logger.error("MONGO_URI is not set")
    raise ValueError("MONGO_URI is not set")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client.get_database("movie_bot")
    movies_collection = db.movies
    users_collection = db.users
except errors.ConnectionError as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    raise

def check_db_connection():
    """Check if MongoDB connection is healthy."""
    try:
        client.server_info()
        return True
    except errors.ConnectionError as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        return False

def init_db():
    """Initialize database with necessary indexes."""
    try:
        movies_collection.create_index([("file_id", 1)], unique=True)
        movies_collection.create_index([("message_id", 1), ("channel_id", 1)], unique=True)
        movies_collection.create_index([("title", TEXT), ("year", 1), ("language", 1)])
        movies_collection.create_index([("channel_id", 1)])
        users_collection.create_index([("chat_id", 1)], unique=True)
        logger.info("Database indexes created successfully")
    except errors.PyMongoError as e:
        logger.error(f"Error creating indexes: {str(e)}")
        raise

def add_user(chat_id):
    """Add a new user to the database with default settings."""
    try:
        user_doc = {
            "chat_id": chat_id,
            "thumbnail_file_id": None,
            "prefix": None,
            "caption": None
        }
        users_collection.update_one(
            {"chat_id": chat_id},
            {"$setOnInsert": user_doc},
            upsert=True
        )
        logger.info(f"Added/updated user {chat_id}")
    except DuplicateKeyError:
        logger.info(f"User {chat_id} already exists")
    except PyMongoError as e:
        logger.error(f"Error adding user {chat_id}: {str(e)}")
        raise

def update_user_settings(chat_id, thumbnail_file_id=None, prefix=None, caption=None):
    """Update user settings in the database."""
    try:
        update_fields = {}
        if thumbnail_file_id is not None:
            update_fields["thumbnail_file_id"] = thumbnail_file_id
        if prefix is not None:
            update_fields["prefix"] = prefix
        if caption is not None:
            update_fields["caption"] = caption

        if update_fields:
            result = users_collection.update_one(
                {"chat_id": chat_id},
                {"$set": update_fields},
                upsert=True
            )
            logger.info(f"Updated settings for user {chat_id}: {update_fields}")
            return result.modified_count > 0
        return False
    except PyMongoError as e:
        logger.error(f"Error updating settings for user {chat_id}: {str(e)}")
        raise

def get_user_settings(chat_id):
    """Retrieve user settings from the database."""
    try:
        user = users_collection.find_one({"chat_id": chat_id})
        if user:
            return (
                user.get("thumbnail_file_id"),
                user.get("prefix"),
                user.get("caption")
            )
        return None, None, None
    except PyMongoError as e:
        logger.error(f"Error retrieving settings for user {chat_id}: {str(e)}")
        raise

def add_movie(title, year, quality, file_size, file_id, message_id, language=None, channel_id=None, retries=3):
    """Add a single movie to the database with retry logic."""
    movie_doc = {
        "title": title,
        "year": year,
        "quality": quality,
        "file_size": file_size,
        "file_id": file_id,
        "message_id": message_id,
        "channel_id": channel_id
    }
    if language:
        movie_doc["language"] = language

    attempt = 0
    while attempt < retries:
        try:
            result = movies_collection.insert_one(movie_doc)
            logger.info(f"Added movie: {title} ({year}) with ID {result.inserted_id}")
            return str(result.inserted_id)
        except DuplicateKeyError:
            logger.info(f"Movie {title} ({year}) already exists")
            return None
        except PyMongoError as e:
            attempt += 1
            if attempt == retries:
                logger.error(f"Failed to add movie {title} after {retries} attempts: {str(e)}")
                raise
            logger.warning(f"Retrying add_movie for {title} (attempt {attempt + 1}): {str(e)}")
            continue

def add_movies_batch(movies, retries=3):
    """Add a batch of movies to the database with retry logic."""
    if not movies:
        return []

    attempt = 0
    while attempt < retries:
        try:
            result = movies_collection.insert_many(movies, ordered=False)
            inserted_ids = [str(_id) for _id in result.inserted_ids]
            logger.info(f"Inserted {len(inserted_ids)} movies in batch")
            return inserted_ids
        except errors.BulkWriteError as bwe:
            inserted_ids = [str(doc["_id"]) for doc in bwe.details.get("writeErrors", []) if "inserted" in doc]
            logger.info(f"Inserted {len(inserted_ids)} movies, skipped duplicates in batch")
            return inserted_ids
        except PyMongoError as e:
            attempt += 1
            if attempt == retries:
                logger.error(f"Failed to add movie batch after {retries} attempts: {str(e)}")
                raise
            logger.warning(f"Retrying add_movies_batch (attempt {attempt + 1}): {str(e)}")
            continue
    return []

def get_movie_by_id(movie_id):
    """Retrieve a movie by its ID."""
    try:
        movie = movies_collection.find_one({"_id": ObjectId(movie_id)})
        if movie:
            return movie
        logger.warning(f"Movie with ID {movie_id} not found")
        return None
    except PyMongoError as e:
        logger.error(f"Error retrieving movie {movie_id}: {str(e)}")
        raise

def search_movies(title, year=None, language=None, limit=10):
    """Search for movies by title, with optional year and language filters."""
    try:
        query = {"$text": {"$search": title}}
        if year:
            query["year"] = year
        if language:
            query["language"] = language

        movies = movies_collection.find(query).limit(limit)
        results = []
        for movie in movies:
            results.append((
                str(movie["_id"]),
                movie["title"],
                movie["year"],
                movie["quality"],
                movie["file_size"],
                movie["file_id"],
                movie["message_id"],
                movie["channel_id"]
            ))
        logger.info(f"Found {len(results)} movies for query: title={title}, year={year}, language={language}")
        return results
    except PyMongoError as e:
        logger.error(f"Error searching movies: title={title}, year={year}, language={language}, error={str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in search_movies: {str(e)}")
        raise
