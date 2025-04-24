import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://horrortimestamiloffl:Shahulshaji10@cluster0.dujxdyr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
client = MongoClient(MONGO_URI)
db = client["movie_bot"]
movies_collection = db["movies"]
users_collection = db["users"]

def init_db():
    movies_collection.create_index([("file_id", 1)], unique=True)
    users_collection.create_index([("chat_id", 1)], unique=True)
    # Add index for faster title searches
    movies_collection.create_index([("title", "text")])

def add_user(chat_id):
    users_collection.update_one(
        {"chat_id": chat_id},
        {"$setOnInsert": {"chat_id": chat_id}},
        upsert=True
    )

def update_user_settings(chat_id, thumbnail_file_id=None, prefix=None, caption=None):
    update_fields = {}
    if thumbnail_file_id is not None:
        update_fields["thumbnail_file_id"] = thumbnail_file_id
    if prefix is not None:
        update_fields["prefix"] = prefix
    if caption is not None:
        update_fields["caption"] = caption
    if update_fields:
        users_collection.update_one(
            {"chat_id": chat_id},
            {"$set": update_fields},
            upsert=True
        )

def get_user_settings(chat_id):
    user = users_collection.find_one({"chat_id": chat_id})
    if user:
        return (user.get("thumbnail_file_id"), user.get("prefix"), user.get("caption"))
    return (None, None, None)

def add_movie(title, year, quality, file_size, file_id, message_id):
    existing = movies_collection.find_one({"file_id": file_id})
    if existing:
        return False
    movies_collection.insert_one({
        "title": title,
        "year": year,
        "quality": quality,
        "file_size": file_size,
        "file_id": file_id,
        "message_id": message_id
    })
    return True

def search_movies(movie_name, year=None, language=None):
    query = {}
    if movie_name:
        # Split terms for flexible matching
        terms = movie_name.split()
        query["$text"] = {"$search": " ".join([f"\"{term}\"" for term in terms])}
    if year:
        query["year"] = year
    if language:
        query["title"] = {"$regex": language, "$options": "i"}
    results = movies_collection.find(query).limit(50)
    return [(r["title"], r["year"], r["quality"], r["file_size"], r["file_id"], r["message_id"]) for r in results]
