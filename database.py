import sqlite3
import os

DB_FILE = "movies.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Movies table
    c.execute('''CREATE TABLE IF NOT EXISTS movies
                 (id INTEGER PRIMARY KEY, title TEXT, year INTEGER, quality TEXT, file_size TEXT,
                  file_id TEXT, message_id INTEGER, UNIQUE(file_id))''')
    # Users table for custom settings
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (chat_id INTEGER PRIMARY KEY, thumbnail_file_id TEXT, prefix TEXT, caption TEXT)''')
    conn.commit()
    conn.close()

def add_user(chat_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (chat_id) VALUES (?)", (chat_id,))
    conn.commit()
    conn.close()

def update_user_settings(chat_id, thumbnail_file_id=None, prefix=None, caption=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    if thumbnail_file_id is not None:
        c.execute("UPDATE users SET thumbnail_file_id = ? WHERE chat_id = ?", (thumbnail_file_id, chat_id))
    if prefix is not None:
        c.execute("UPDATE users SET prefix = ? WHERE chat_id = ?", (prefix, chat_id))
    if caption is not None:
        c.execute("UPDATE users SET caption = ? WHERE chat_id = ?", (caption, chat_id))
    conn.commit()
    conn.close()

def get_user_settings(chat_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT thumbnail_file_id, prefix, caption FROM users WHERE chat_id = ?", (chat_id,))
    result = c.fetchone()
    conn.close()
    return result

def add_movie(title, year, quality, file_size, file_id, message_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT file_id FROM movies WHERE file_id = ?", (file_id,))
    if c.fetchone():
        conn.close()
        return False
    c.execute("INSERT INTO movies (title, year, quality, file_size, file_id, message_id) VALUES (?, ?, ?, ?, ?, ?)",
              (title, year, quality, file_size, file_id, message_id))
    conn.commit()
    conn.close()
    return True

def search_movies(movie_name, year=None, language=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    sql_query = "SELECT title, year, quality, file_size, file_id, message_id FROM movies WHERE LOWER(title) LIKE ?"
    params = [f"%{movie_name}%"]
    if year:
        sql_query += " AND year = ?"
        params.append(year)
    if language:
        sql_query += " AND LOWER(title) LIKE ?"
        params.append(f"%{language}%")
    c.execute(sql_query, params)
    results = c.fetchall()
    conn.close()
    return results