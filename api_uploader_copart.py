import os
import json
import asyncio
import aiohttp
import logging, ijson
from pathlib import Path
from datetime import datetime
from dotenv import find_dotenv, load_dotenv
from main import DELETE_FILES_LOCALY_WHILE_UPLOAD_TO_API

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
    Зчитує файл з фотографіями потоково (ijson), щоб не вбити пам'ять.
    """
    photos_map = {}
    try:
        photos_file_path = Path(str(lots_file_path).replace(f"{os.sep}lots{os.sep}", f"{os.sep}photos{os.sep}"))
        if not photos_file_path.exists():
            return photos_map

        # ijson краще працює з бінарним режимом 'rb'
        with open(photos_file_path, 'rb') as f:
            # Читаємо масив поелементно
            for item in ijson.items(f, 'item', use_float=True):
                images_list = item.get('data', {}).get('imagesList', {})
                if not isinstance(images_list, dict):
                    continue

                for category, images_array in images_list.items():
                    if isinstance(images_array, list) and len(images_array) > 0:
                        ln = images_array[0].get('ln')
                        if ln is not None:
                            ln_str = str(ln)
                            if ln_str not in photos_map:
                                photos_map[ln_str] = {}
                            if category not in photos_map[ln_str]:
                                photos_map[ln_str][category] = []

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
        photos_map = load_photos_map_copart(file_path)

        successful_lots = 0
        tasks = []
        current_batch = []

        with open(file_path, 'rb') as f:
            # Читаємо головний файл лотів поелементно
            for raw_item in ijson.items(f, 'item', use_float=True):
                lot_details = raw_item.get('data', {}).get('lotDetails', {})
                ln = lot_details.get('ln')

                if ln is not None:
                    ln_str = str(ln)
                    raw_item['imagesList'] = photos_map.get(ln_str, {})

                current_batch.append(raw_item)

                # Як тільки зібрали пачку BATCH_SIZE — створюємо задачу на відправку
                if len(current_batch) >= BATCH_SIZE:
                    tasks.append(asyncio.create_task(send_batch(session, list(current_batch))))
                    current_batch.clear()  # Очищаємо список для наступної пачки

                    # Контроль паралельності: щоб не створювати тисячі тасок,
                    # чекаємо виконання кожних 30 паралельних запитів
                    if len(tasks) >= 30:
                        results = await asyncio.gather(*tasks)
                        successful_lots += sum(results)
                        tasks.clear() # Звільняємо пам'ять від виконаних тасок

        # Відправляємо залишок, якщо файл закінчився, а пачка ще не повна
        if current_batch:
            tasks.append(asyncio.create_task(send_batch(session, list(current_batch))))

        # Чекаємо виконання всіх залишкових задач
        if tasks:
            results = await asyncio.gather(*tasks)
            successful_lots += sum(results)

        return successful_lots

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

            global DELETE_FILES_LOCALY_WHILE_UPLOAD_TO_API

            if DELETE_FILES_LOCALY_WHILE_UPLOAD_TO_API:
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