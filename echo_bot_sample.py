import logging
import sqlite3
import asyncio
import json
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
KINOPOISK_API_KEY = os.getenv("KINOPOISK_API_KEY")

import scrappers

from collections import Counter
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters.command import Command
from aiogram.types import InlineKeyboardButton, CallbackQuery, FSInputFile

# Инициализация логирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота
BOT_TOKEN = "7594309613:AAEDXWlCMTQOqbV-KUDhaMLJQnabWyypd7w"
KINOPOISK_API_KEY = "NMRNMKK-1AQM21W-J8K5CDS-6K2GRF2"

if not BOT_TOKEN or not KINOPOISK_API_KEY:
    raise ValueError("Необходимо установить BOT_TOKEN и KINOPOISK_API_KEY")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

router = Router()
dp.include_router(router)

# Инициализация базы данных
db_connection = sqlite3.connect("cinema_bot.db")
db_cursor = db_connection.cursor()

# Создание таблиц
db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS history (
        user_id INTEGER,
        query TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
""")

db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS stats (
        title TEXT PRIMARY KEY,
        count INTEGER DEFAULT 0
    )
""")

db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        movie_id INTEGER PRIMARY KEY,
        name TEXT,
        title TEXT,
        description TEXT,
        year INTEGER,
        country TEXT,
        genres TEXT,
        poster TEXT,
        link TEXT,
        rating_industry REAL,
        rating_people REAL
    )
""")

db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS searches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        search_name TEXT UNIQUE,
        movie_ids TEXT
    )
""")

db_connection.commit()


def format_movie_info(movie: dict) -> str:
    """
    Форматирует информацию о фильме для отправки пользователю.
    """
    name = movie.get("name", "Неизвестен, без названия")
    description_film = movie.get("description", "")
    year = movie.get("year", "Неизвестно")
    country = movie.get("country", "Неизвестно")
    genres = movie.get("genres", "Уникальный жанр")
    rating_industry = movie.get("rating_industry", "Нет рейтинга")
    rating_people = movie.get("rating_people", "Нет рейтинга")
    link = movie.get("link", "Ссылка умерла")

    description_res = f"🎬 *{name}* ({year})\n" \
                  f"🌐 Страна: {country}\n" \
                  f"🎥 Жанры: {genres}\n" \
                  f"⭐ Рейтинг Кинопоиска: {rating_industry}/10\n" \
                  f"⭐ Рейтинг IMDb: {rating_people}/10\n" \
                  f"📝 Описание: {description_film}\n\n" \
                  f"🔗 [Смотреть]({link})"
    return description_res


@dp.message(Command("start"))
async def start(message: types.Message):
    """
    Обработчик команды /start.
    """
    await message.reply(
        "Привет! Я твой ELITE KINCHIK 228 – твой кинобомбардир прямо в Telegram! "
        "Ищи любые фильмы на раз-два и не парься с поиском."
    )

@dp.message(Command("help"))
async def help_command(message: types.Message):
    """
    Обработчик команды /help.
    """
    await message.reply(
        "Напиши название фильма или сериала, чтобы получить информацию о нём.\n"
        "Доступные команды:\n"
        "/history - Показать историю запросов\n"
        "/stats - Показать статистику фильмов."
    )

@dp.message(Command("history"))
async def history_command(message: types.Message):
    """
    Показать историю запросов пользователя.
    """
    user_id = message.from_user.id
    db_cursor.execute(
        "SELECT query, timestamp FROM history WHERE user_id = ? ORDER BY timestamp DESC",
        (user_id,)
    )
    rows = db_cursor.fetchall()
    if rows:
        history = "\n".join([f"{row[0]} (запрос от {row[1]})" for row in rows])
        await message.reply(f"Ваша история запросов:\n{history}")
    else:
        await message.reply("История запросов пуста.")


@dp.message(Command("stats"))
async def stats_command(message: types.Message):
    """
    Показать статистику просмотров фильмов и топ-3 любимых жанра.
    """
    # Топ-10 самых популярных фильмов
    db_cursor.execute("SELECT title, count FROM stats ORDER BY count DESC LIMIT 10")
    film_rows = db_cursor.fetchall()
    if film_rows:
        film_stats = "\n".join([f"{row[0]} - {row[1]} просмотр" + (row[1] < 5 and row[1] > 1) * "a" + (row[1] > 4) * "ов"  for row in film_rows])
    else:
        film_stats = "Статистика фильмов пуста."

    # Топ-3 любимых жанра
    db_cursor.execute("""
        SELECT genres FROM movies 
        WHERE movie_id IN (
            SELECT movie_id FROM stats 
            ORDER BY count DESC 
            LIMIT 100  -- Ограничиваем выборку для производительности
        )
    """)
    genre_rows = db_cursor.fetchall()

    genres = []
    for row in genre_rows:
        if row[0]:
            # Разделяем жанры по запятым и удаляем лишние пробелы
            genres.extend([genre.strip() for genre in row[0].split(",")])

    genre_counts = Counter(genres)
    top_genres = genre_counts.most_common(3)

    if top_genres:
        genre_stats = "\n".join([f"{genre} - {count} раз" + (count < 5 and count > 1)*"a" for genre, count in top_genres])
    else:
        genre_stats = "Статистика жанров пуста."

    # Формирование итогового сообщения
    stats_message = f"📊 Статистика фильмов:\n{film_stats}\n\n🎨 Топ-3 любимых жанра:\n{genre_stats}"
    await message.reply(stats_message)


@dp.message(F.content_type == types.ContentType.STICKER)
async def handle_sticker(message: types.Message):
    """
    Обрабатывает сообщения со стикерами.
    """
    # Доступ к стикеру: message.sticker
    # Например, можете отправить ответ пользователю
    await message.reply("Прикольный стикер! 👍")


@dp.message()
async def search_movie(message: types.Message):
    """
    Поиск фильма и предоставление информации.
    """
    query = message.text.strip()
    user_id = message.from_user.id
    if not query:
        await message.reply("Пожалуйста, введите название фильма для поиска.")
        return
    # Логирование запроса в базу данных
    db_cursor.execute("INSERT INTO history (user_id, query) VALUES (?, ?)", (user_id, query))
    db_connection.commit()

    # Проверяем, есть ли уже сохранённый результат для этого запроса
    db_cursor.execute("SELECT movie_ids FROM searches WHERE search_name = ?", (query,))
    row = db_cursor.fetchone()

    if row:
        # Если запрос уже был выполнен, загружаем movie_ids
        movie_ids = json.loads(row[0])  # Десериализуем JSON-строку в список
        logging.info(f"Запрос '{query}' найден в БД. Загружены movie_ids: {movie_ids}")

        # Извлекаем информацию о фильмах из таблицы movies
        movies = []
        for movie_id in movie_ids:
            db_cursor.execute("""
                SELECT movie_id, name, title, description, year, country, genres, poster, link, rating_industry, rating_people 
                FROM movies 
                WHERE movie_id = ?
            """, (movie_id,))
            movie = db_cursor.fetchone()
            if movie:
                movies.append({
                    "movie_id": movie[0],
                    "name": movie[1],
                    "title": movie[2],
                    "description": movie[3],
                    "year": movie[4],
                    "country": movie[5],
                    "genres": movie[6],
                    "poster": movie[7],
                    "link": movie[8],
                    "rating_industry": movie[9],
                    "rating_people": movie[10]
                })

    else:
        # Поиск фильмов через Kinopoisk API и сохранение в базу данных
        movies = await scrappers.search_kino_poisk(query)

        if not movies:
            await message.reply("К сожалению, ничего не найдено. Попробуйте другой запрос.")
            return

        if len(movies) == 1:
            movie = movies[0]
            text = format_movie_info(movie)
            film_ru_id = await scrappers.find_movie_in_filmru(movie['name'])
            poster_url = await scrappers.scrape_film_ru_poster(film_ru_id)
            logging.info(f"Poster URL in search: {poster_url}")
            db_cursor.execute("""
                INSERT OR REPLACE INTO movies (
                    poster
                ) VALUES (?)
            """, (poster_url,))
            if poster_url:
                await bot.send_photo(chat_id=message.chat.id, photo=poster_url, caption=text, parse_mode="Markdown")
            else:
                await message.reply(text, parse_mode="Markdown")
        else:
            # Если найдено несколько фильмов, предоставляем выбор пользователю
            text_for_choice = "Найдено несколько фильмов. Выберите нужный:\n"
            builder = InlineKeyboardBuilder()
            for i, movie in enumerate(movies):
                text_for_choice += f"{i + 1}. {movie['name']} ({movie['year']})\n"
                button_text = f"1. {movie['name']} ({movie['year']})"
                callback_data = f"sel_{movie['movie_id']}"
                builder.add(InlineKeyboardButton(text=button_text, callback_data=callback_data))

            keyboard = builder.as_markup()
            await message.reply(text_for_choice, reply_markup=keyboard)


@router.callback_query(lambda c: c.data.startswith("sel_"))
async def handle_movie_choice(callback: CallbackQuery):
    """
    Обрабатывает выбор пользователя и отправляет соответствующую информацию о фильме.
    """
    try:
        movie_id = int(callback.data.split("_")[1])
    except (IndexError, ValueError):
        await callback.answer("Некорректный выбор.", show_alert=True)
        return

    # Извлечение информации о фильме из базы данных
    db_cursor.execute("""
        SELECT name, title, year, description, country, genres, poster, link, rating_industry, rating_people 
        FROM movies 
        WHERE movie_id = ?
    """, (movie_id,))
    result = db_cursor.fetchone()

    if not result:
        await callback.answer("Информация о фильме не найдена.", show_alert=True)
        return

    name, title, year, description_film, country, genres, poster, link, rating_industry, rating_people = result
    film_ru_id = await scrappers.find_movie_in_filmru(name)
    poster_url = await scrappers.scrape_film_ru_poster(film_ru_id)
    db_cursor.execute("""
        INSERT OR REPLACE INTO movies (
            poster
        ) VALUES (?)
    """, (poster_url,))

    # Форматирование текста для отправки пользователю
    description_res = f"🎬 *{name}* ({year})\n" \
                  f"🌐 Страна: {country}\n" \
                  f"🎥 Жанры: {genres}\n" \
                  f"⭐ Рейтинг Кинопоиска: {rating_industry}/10\n" \
                  f"⭐ Рейтинг IMDb: {rating_people}/10\n" \
                  f"📝 Описание: {description_film}\n\n" \
                  f"🔗 [Смотреть]({link})"

    # Обновление статистики просмотров
    db_cursor.execute("""
        INSERT INTO stats (title, count) 
        VALUES (?, 1) 
        ON CONFLICT(title) DO UPDATE SET count = count + 1
    """, (f"{name} ({year})",))
    db_connection.commit()
    logging.info(f"Poster URL: {poster_url}")
    # Отправка информации о фильме пользователю

    if poster_url:
        print(poster_url[-3:])
        if poster_url[-3:] == "jpg":
            try:
                await bot.send_photo(chat_id=callback.message.chat.id, photo=poster_url, caption=description_res, parse_mode="Markdown")
            except Exception as e:
                logging.error(f"Ошибка отправки изображения: {e}")
                await callback.message.reply(description_res, parse_mode="Markdown")
        else:
            await callback.message.reply("Эх, формат картинку не тот...\nФорматируем...")
            poster_path = await scrappers.download_image(poster_url, "poster.jpg")
            if poster_path:
                file_input = FSInputFile(poster_path)
                await bot.send_photo(chat_id=callback.message.chat.id, photo=file_input, caption=description_res, parse_mode="Markdown")
            else:
                await callback.message.reply(description_res, parse_mode="Markdown")
    else:
        await callback.message.reply(description_res, parse_mode="Markdown")

    await callback.answer()

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
