import os
import json
import asyncio
import aiohttp
import logging
from pathlib import Path
from datetime import datetime
from dotenv import find_dotenv, load_dotenv

env_path = find_dotenv(".env")
load_dotenv(env_path)
def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip().strip('"').strip("'")

# --- НАЛАШТУВАННЯ ---
API_URL: str = _env("API_URL")
BATCH_SIZE = 30
MAX_RETRIES = 3

log_dir = Path('api_logs')
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f'api_upload_copart_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_photos_map_copart(lots_file_path: Path) -> dict:
    """
    Зчитує файл з фотографіями Copart і збирає абсолютно всі масиви,
    групуючи їх за значенням 'ln'.
    """
    photos_map = {}
    try:
        photos_file_path = Path(str(lots_file_path).replace(f"{os.sep}lots{os.sep}", f"{os.sep}photos{os.sep}"))
        if not photos_file_path.exists():
            return photos_map

        with open(photos_file_path, 'r', encoding='utf-8') as f:
            photos_data = json.load(f)

        if isinstance(photos_data, list):
            for item in photos_data:
                images_list = item.get('data', {}).get('imagesList', {})
                if not isinstance(images_list, dict):
                    continue

                # Проходимо по ВСІХ ключах (категоріях масивів) всередині imagesList
                for category, images_array in images_list.items():
                    if isinstance(images_array, list) and len(images_array) > 0:

                        # Беремо ln з першого елемента саме ЦЬОГО масиву
                        ln = images_array[0].get('ln')

                        if ln is not None:
                            ln_str = str(ln)

                            # Якщо цього лота (ln) ще немає в нашому словнику - створюємо для нього порожній об'єкт
                            if ln_str not in photos_map:
                                photos_map[ln_str] = {}

                            # Якщо такої категорії (наприклад, 'IMAGE') ще немає для цього лота - створюємо масив
                            if category not in photos_map[ln_str]:
                                photos_map[ln_str][category] = []

                            # Додаємо всі елементи поточного масиву до загальної купи цього лота
                            photos_map[ln_str][category].extend(images_array)

    except Exception as e:
        logger.error(f"Error loading photos file: {e}")

    return photos_map

# Генератор для розбиття списку на пачки (по BATCH_SIZE штук)
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

async def send_batch(session: aiohttp.ClientSession, batch: list):
    """Відправляє масив лотів на API"""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(API_URL, json=batch, timeout=30) as resp:
                if resp.status in (200, 201, 202):
                    return len(batch)
                else:
                    logger.warning(f"Server error {resp.status}. Attempt {attempt+1}")
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Network error: {e}. Attempt {attempt+1}")
            await asyncio.sleep(1)
    return 0

async def process_file(file_path: Path, session: aiohttp.ClientSession):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)

        if not isinstance(content, list):
            return 0

        photos_map = load_photos_map_copart(file_path)

        # 1. Готуємо всі лоти з фотографіями
        prepared_lots = []
        for raw_item in content:
            # Дістаємо ln з data -> lotDetails -> ln
            lot_details = raw_item.get('data', {}).get('lotDetails', {})
            ln = lot_details.get('ln')

            if ln is None:
                continue # Якщо раптом лот без ln, пропускаємо (або можна логувати)

            ln_str = str(ln)

            # Додаємо блок зображень до лота (якщо фоток немає, буде порожній словник)
            raw_item['imagesList'] = photos_map.get(ln_str, {})
            prepared_lots.append(raw_item)

        # 2. Розбиваємо на пачки по BATCH_SIZE
        batches = list(chunker(prepared_lots, BATCH_SIZE))

        # 3. Відправляємо пачки асинхронно
        tasks = [asyncio.create_task(send_batch(session, b)) for b in batches]
        if tasks:
            results = await asyncio.gather(*tasks)
            successful_lots = sum(results)
            return successful_lots
        return 0

    except Exception as e:
        logger.error(f"Error processing file {file_path.name}: {e}")
        return 0

async def async_main(minio_dir: str):
    minio_dir = "Minio"
    lots_dir = Path(minio_dir) / "lots"
    json_files = list(lots_dir.rglob("*.json"))

    if not json_files:
        return

    logger.info(f"Found {len(json_files)} JSON files.")
    total_lots_sent = 0

    async with aiohttp.ClientSession() as session:
        for file_path in json_files:
            logger.info(f"Uploading file: {file_path.name}...")
            sent_count = await process_file(file_path, session)
            total_lots_sent += sent_count

            try:
                # 1. Видаляємо файл лотів
                if file_path.exists():
                    file_path.unlink()

                # 2. Знаходимо та видаляємо відповідний файл з фотографіями
                photos_file_path = Path(str(file_path).replace(f"{os.sep}lots{os.sep}", f"{os.sep}photos{os.sep}"))
                if photos_file_path.exists():
                    photos_file_path.unlink()

                # 3. Видаляємо папку лотів, ЯКЩО вона порожня
                try:
                    file_path.parent.rmdir()
                except OSError:
                    pass # Директорія не порожня, пропускаємо

                # 4. Видаляємо папку фотографій, ЯКЩО вона порожня
                try:
                    photos_file_path.parent.rmdir()
                except OSError:
                    pass # Директорія не порожня, пропускаємо

            except Exception as e:
                logger.error(f"Cleanup error for {file_path.name}: {e}")

    logger.info("=" * 50)
    logger.info(f"UPLOAD TO API COMPLETED. Total lots: {total_lots_sent}")
    logger.info("=" * 50)

def upload_to_api_and_cleanup(minio_dir: str):
    """
    Синхронна обгортка для зручного імпорту в інші файли.
    Вона сама запускає асинхронний цикл.
    """
    asyncio.run(async_main(minio_dir))

if __name__ == "__main__":
    asyncio.run(async_main("Minio"))