import json
import os
from pathlib import Path
from datetime import datetime

RES_JSON_PATH = Path('res_json')
MINIO_BASE = Path("Minio")
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

def stream_merge():
    # Створюємо папки
    lots_dir = MINIO_BASE / "lots" / CURRENT_DATE
    photos_dir = MINIO_BASE / "photos" / CURRENT_DATE
    lots_dir.mkdir(parents=True, exist_ok=True)
    photos_dir.mkdir(parents=True, exist_ok=True)

    folders = [d for d in RES_JSON_PATH.iterdir() if d.is_dir()]

    # Словник для відстеження, чи файл новий (щоб ставити коми)
    first_entry_tracker = {}

    for folder in folders:
        # Визначаємо категорію та фінальне ім'я файлу
        is_lots = folder.name.endswith("_lots")
        category = "lots" if is_lots else "photos"
        parts = folder.name.split('_')
        type_letter = parts[2] if len(parts) > 2 else "X"
        brand_name = parts[0]

        final_filename = f"{type_letter}_{brand_name}.json"
        dest_path = (lots_dir if is_lots else photos_dir) / final_filename

        json_files = list(folder.glob("*.json"))
        if not json_files: continue

        print(f"Merging {folder.name} -> {final_filename}...")

        # Відкриваємо файл у режимі 'a' (дозапис)
        is_new_file = not dest_path.exists()
        with open(dest_path, 'a', encoding='utf-8') as outfile:
            if is_new_file:
                outfile.write('[')
                first_entry_tracker[dest_path] = True

            for json_file in json_files:
                with open(json_file, 'r', encoding='utf-8') as infile:
                    try:
                        content = infile.read().strip()
                        if not content: continue

                        # Додаємо кому, якщо це не перший елемент у цьому файлі
                        if not first_entry_tracker.get(dest_path, False):
                            outfile.write(',\n')

                        outfile.write(content)
                        first_entry_tracker[dest_path] = False
                    except Exception as e:
                        print(f"Skip {json_file.name}: {e}")

    # Закриваємо масиви в усіх файлах
    all_merged_files = list(lots_dir.glob("*.json")) + list(photos_dir.glob("*.json"))
    for f_path in all_merged_files:
        with open(f_path, 'a', encoding='utf-8') as outfile:
            outfile.write(']')
    print("\nЛокальна склейка завершена. Файли готові в папці Minio.")

if __name__ == "__main__":
    stream_merge()