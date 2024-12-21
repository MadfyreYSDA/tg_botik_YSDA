import logging
import aiohttp
import bs4
from PIL import Image

import echo_bot_sample


async def find_movie_in_filmru(movie_name: str):
    """
    Находит фильм в Film.ru.
    """
    base_url = "https://www.film.ru/search/result?"
    params = {"text": movie_name, "type": 'all'}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url, params=params, headers=headers) as response:
            if response.status != 200:
                return None
            html_content = await response.text()
            soup = bs4.BeautifulSoup(html_content, "html.parser")
            results_links = []
            # results = []
            links_span = soup.find('div', class_={'rating'})
            for div in links_span.findAll('a'):
                results_links.append(f"https://www.film.ru{div['href']}")
                print(results_links[-1])
                # results.append(div.find('strong').text + " (" + div.findAll('span')[1].text.split(',')[0] + ")")
                print(div['href'])
                # print(div.find('strong').text)
                found_year = div.findAll('span')[1].text.split(',')[0]
                print(found_year)
                echo_bot_sample.db_cursor.execute("SELECT year FROM movies WHERE name = ?", (movie_name,))
                year_true = echo_bot_sample.db_cursor.fetchall()
                print(year_true[0][0])
                if str(found_year) == str(year_true[0][0]):
                    movie_url = results_links[-1]
                    print(f"I chose this thing: {results_links[-1]}")
                    await session.close()
                    return movie_url
            if len(results_links) < 1:
                print("OOPS")
                return None
            # if len(results_links) > 1:
            #    return results, results_links
            movie_url = results_links[0]

        await session.close()
        return movie_url


async def scrape_film_ru_poster(movie_link: str) -> str:
    """
    Scrape Film.ru для получения URL постера фильма из ссылки на него.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(movie_link, headers=headers) as response:
                if response.status != 200:
                    logging.error(f"Ошибка при доступе к Film.ru: {response.status}")
                    return ""
                html_content = await response.text()
                soup = bs4.BeautifulSoup(html_content, "html.parser")

                image_span = soup.find('a', class_='wrapper_block_stack wrapper_movies_poster')
                if image_span and image_span.get('data-src'):
                    url_photo_film = 'https://www.film.ru' + image_span['data-src']
                    return url_photo_film
                else:
                    logging.warning(f"Постер не найден для ссылки: {movie_link}")
                    return ""
        except Exception as e:
            logging.error(f"Ошибка при скрапинге Film.ru: {e}")
            return ""


async def download_image(url, file_path):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                with open(file_path, "wb") as f:
                    f.write(await response.read())
                return file_path
            else:
                return None


async def convert_image_to_jpeg(source_url, destination_path):
    async with aiohttp.ClientSession() as session:
        async with session.get(source_url) as response:
            if response.status == 200:
                with open("temp_image", "wb") as temp_file:
                    temp_file.write(await response.read())
                img = Image.open("temp_image")
                img.convert("RGB").save(destination_path, "JPEG")
                return destination_path
            return None


async def search_kino_poisk(movie_name: str, limit: int = 3) -> list:
    """
    Ищет фильмы по названию с помощью Kinopoisk API и
    возвращает json file с информацией о фильмах.
    Сохраняет информацию о фильмах в базу данных.
    """
    search_url = "https://api.kinopoisk.dev/v1.4/movie/search"
    params = {
        "page": 1,
        "limit": limit,
        "query": movie_name
    }
    headers = {
        "accept": "application/json",
        "X-API-KEY": echo_bot_sample.KINOPOISK_API_KEY
    }
    async with (aiohttp.ClientSession() as session):
        async with session.get(search_url, params=params, headers=headers) as response:
            if response.status != 200:
                logging.error(f"Ошибка при запросе к Kinopoisk API: {response.status}")
                return []
            data = await response.json()
            docs = data.get("docs", [])
            movies = []
            for movie in docs:
                try:
                    movie_id = movie.get("id")
                    name_film = movie.get("name", "Неизвестен")
                    title_film = movie.get("alternativeName", "")
                    year_film = movie.get("year", 0)

                    description_film = movie.get("description", "")

                    countries = movie.get("countries", [])
                    country_film = ", ".join([country.get("name", "") for country in countries]) if countries else "Неизвестно"

                    genres = movie.get("genres", [])
                    genres_film = ", ".join([genre.get("name", "") for genre in genres]) if genres else "Неизвестно"

                    poster_url = ""

                    movie_link = construct_flicksbar_url(movie_id)

                    rating_info = movie.get("rating", {})
                    rating_industry = rating_info.get("kp", 0.0)
                    rating_people = rating_info.get("imdb", 0.0)

                    movie_data = {
                        "movie_id": movie_id,
                        "name": name_film,
                        "title": title_film,
                        "description": description_film,
                        "year": year_film,
                        "country": country_film,
                        "genres": genres_film,
                        "poster": poster_url,
                        "link": movie_link,
                        "rating_industry": rating_industry,
                        "rating_people": rating_people
                    }

                    # Сохранение данных о фильме в базу данных
                    echo_bot_sample.db_cursor.execute("""
                        INSERT OR REPLACE INTO movies (
                            movie_id, name, title, description, year, country, genres, poster, link, rating_industry, rating_people
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        movie_data["movie_id"],
                        movie_data["name"],
                        movie_data["title"],
                        movie_data["description"],
                        movie_data["year"],
                        movie_data["country"],
                        movie_data["genres"],
                        movie_data["poster"],
                        movie_data["link"],
                        movie_data["rating_industry"],
                        movie_data["rating_people"]
                    ))
                    echo_bot_sample.db_connection.commit()
                    movies.append(movie_data)
                except Exception as e:
                    logging.error(f"Ошибка при обработке фильма: {e}")
                    continue
            return movies


def construct_flicksbar_url(movie_id: int) -> str:
    """
    Формирует URL FlicksBar для данного movie_id.
    """
    base_url = "https://flicksbar.mom/film/"
    return f"{base_url}{movie_id}/"


def construct_kinopoisk_url(movie_id: int) -> str:
    """
    Формирует URL Kinopoisk для данного movie_id.
    """
    base_url = "https://www.kinopoisk.ru/film/"
    return f"{base_url}{movie_id}/"