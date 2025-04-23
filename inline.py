import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultArticle, InputTextMessageContent
from database import search_movies
from utils import process_file

def inline_query(update, context):
    query = update.inline_query.query.strip().lower()
    if not query:
        return

    # Extract movie name, year, and language
    match = re.match(r"(.+?)(?:\s+(\d{4}))?(?:\s+(.+))?$", query)
    if not match:
        return
    movie_name = match.group(1).strip()
    year = int(match.group(2)) if match.group(2) else None
    language = match.group(3) if match.group(3) else None

    results = search_movies(movie_name, year, language)
    inline_results = []
    if not results:
        inline_results.append(
            InlineQueryResultArticle(
                id="no_results",
                title="No Movies Found",
                input_message_content=InputTextMessageContent(
                    "Sorry, we couldn’t find ‘{}’. Try another movie or check back soon! 😊".format(query)
                )
            )
        )
    else:
        for title, year, quality, file_size, file_id, message_id in results:
            inline_results.append(
                InlineQueryResultArticle(
                    id=f"{file_id}:{message_id}",
                    title=f"{title} ({year or 'Unknown'})",
                    description=f"Quality: {quality}, Size: {file_size}",
                    input_message_content=InputTextMessageContent(f"Selected: {title} ({quality})"),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(f"Get {quality} ({file_size})", callback_data=f"{file_id}:{message_id}:{title}:{quality}")]
                    ])
                )
            )

    update.inline_query.answer(inline_results)

def button_callback(update, context):
    query = update.callback_query
    file_id, message_id, title, quality = query.data.split(":", 3)
    chat_id = query.message.chat_id if query.message else update.effective_user.id

    try:
        process_file(context.bot, chat_id, file_id, title, quality, query.message)
    except Exception as e:
        query.message.reply_text(f"Error: {str(e)}")
    query.answer()