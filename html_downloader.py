from seleniumbase import SB
from selenium.common.exceptions import TimeoutException
import time
import re
from pathlib import Path
import json
import os

class HTML_downloader:
    tech_json = Path('tech_json')
    tech_html = Path('html_downloader_tech')
    res_json_dir = Path('res_json')
    html_results = Path('html_results')

    # Створюємо необхідні директорії при запуску
    tech_html.mkdir(parents=True, exist_ok=True)
    html_results.mkdir(parents=True, exist_ok=True)
    tech_json.mkdir(parents=True, exist_ok=True)

    @classmethod
    def save_error(cls, error_object):
        """Зберігає помилку у файл (додає в масив або створює новий)"""
        file_path = cls.tech_html / 'html_downloader_errors.json'
        
        try:
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    try:
                        data = json.load(f)
                        if not isinstance(data, list): data = []
                    except json.JSONDecodeError:
                        data = []
            else:
                data = []

            data.append(error_object)

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"CRITICAL: Failed to save error log: {e}")

    @classmethod
    def save_current_state(cls, brand, page, lot):
        """Зберігає останній успішно оброблений лот, щоб можна було відновити роботу."""
        state = {
            "last_brand": brand,
            "last_page": page,
            "last_lot": lot,
            "timestamp": time.time()
        }
        with open(cls.tech_html / 'last_state.json', 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)

    @classmethod
    def append_final_link_data(cls, data_object):
        """Дописує інформацію про фінальний лінк у файл lots_and_links.json"""
        # Використовуємо режим 'a' (append) і формат JSON Lines (один JSON на рядок)
        # Це набагато безпечніше і швидше, ніж перезаписувати величезний масив [] щоразу.
        file_path = cls.tech_html / 'lots_and_links.json'
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data_object, f, indent=2, ensure_ascii=False) + "," + "\n")

    @classmethod
    def get_rendered_html(cls, lot_number: str, brand: str, page_number: str):
        """
        Завантажує сторінку лоту, перевіряє рендер і зберігає результат.
        Приймає також brand та page_number для правильної структури папок.
        """
        
        # Створюємо папку для конкретної марки та сторінки: html_results/Brand/page_X
        save_dir = cls.html_results / brand / f"page_{page_number}"
        save_dir.mkdir(parents=True, exist_ok=True)
        
        save_path = save_dir / f"{lot_number}.html"

        # ОПТИМІЗАЦІЯ: Якщо файл вже є, не качаємо його знову
        if save_path.exists():
            print(f"Skipped (Already exists): {lot_number}")
            return True

        url = f"https://www.copart.com/lot/{lot_number}"
        print(f"Opening: {url}")

        MAX_RETRIES = 3
        
        for attempt in range(1, MAX_RETRIES + 1):
            if attempt > 1:
                print(f"   Retry {attempt}/{MAX_RETRIES}...")

            try:
                with SB(uc=True, test=False, headless=False) as sb: # headless=False щоб бачити роботу, можна змінити на True
                    sb.driver.set_page_load_timeout(60)
                    sb.activate_cdp_mode()
                    
                    sb.open(url)
                    
                    # --- Перевірка на анти-бот (Cloudflare/Captcha) ---
                    if sb.is_element_visible("#challenge-form") or sb.is_element_visible('iframe[src*="cloudflare"]'):
                        print("Cloudflare challenge visible! Waiting/Retrying...")
                        sb.sleep(5) 
                        # Якщо не зникло - ретрай
                        if sb.is_element_visible("#challenge-form"):
                             continue

                    # --- Чекаємо рендер контенту (Заголовок або блок лоту) ---
                    # Якщо цього елементу нема - значить сторінка пуста або не прогрузилась
                    try:
                        sb.wait_for_element(".lot-vehicle-info, h1.lot-details-heading, .lot-details-page", timeout=15)
                    except Exception:
                        # Спробуємо зловити Access Denied
                        if "access denied" in sb.get_page_title().lower():
                            raise Exception("Access Denied by Server")
                        print("Element not found, simple retry.")
                        continue

                    # --- Скрол і пауза ---
                    sb.scroll_to_bottom()
                    sb.sleep(1)

                    # --- Отримання даних ---
                    final_url = sb.get_current_url()
                    html = sb.get_page_source()

                    # Перевірка чи лот взагалі існує (чи не редіректнуло на головну/пошук)
                    if "lot-not-found" in final_url or "member-home" in final_url:
                        error_obj = {
                            "lot": lot_number,
                            "brand": brand,
                            "page": page_number,
                            "error": "Lot not found (redirected)",
                            "final_url": final_url
                        }
                        print(f"Lot not found: {lot_number}")
                        cls.save_error(error_obj)
                        return False # Не зберігаємо HTML, бо він неправильний

                    # --- ЗБЕРЕЖЕННЯ ---
                    
                    # 1. Зберігаємо HTML
                    with open(save_path, "w", encoding="utf-8") as f:
                        f.write(html)
                    
                    # 2. Зберігаємо посилання в базу
                    link_data = {
                        'brand': brand, 
                        'page': page_number, 
                        'lot': lot_number, 
                        'final_link': final_url
                    }
                    cls.append_final_link_data(link_data)

                    # 3. Оновлюємо стейт (щоб знати де зупинились)
                    cls.save_current_state(brand, page_number, lot_number)

                    print(f"Saved: {brand}/page_{page_number}/{lot_number}.html")
                    return True

            except Exception as ex:
                print(f"   Error on attempt {attempt}: {ex}")
                time.sleep(2) # Пауза перед наступною спробою
        
        # Якщо всі спроби вичерпано
        print(f"FAILED to download lot {lot_number}")
        cls.save_error({
            "lot": lot_number,
            "brand": brand,
            "page": page_number,
            "error": "Max retries exceeded",
            "details": str(ex) if 'ex' in locals() else "Unknown"
        })
        return False

    @staticmethod
    def save_filenames(directory_path, output_file, starts_with):
        files = os.listdir(directory_path)
        files = [
            f for f in files
            if os.path.isfile(os.path.join(directory_path, f)) and f.startswith(starts_with)
        ]
        # Сортуємо щоб page 1 йшла перед page 10
        files.sort(key=lambda f: int(re.search(r'page(\d+)', f).group(1)) if re.search(r'page(\d+)', f) else 0)

        #tmp I think it's not needed to save them into file. There will be too many files (for each brand) without sense
        # with open(output_file, "w", encoding="utf-8") as out:
        #     for filename in files:
        #         out.write(filename + "\n")
        return files

    @classmethod
    def get_list_of_automobile_brands(cls):
        path = cls.tech_json / 'list_of_automobile_brands.json'
        if not path.exists():
             print(f"Error: {path} not found.")
             return []
             
        with open(path, "r", encoding="utf-8") as f:
            content = json.load(f)
        
        brands = []
        try:
            # Адаптуй цей шлях до структури твого JSON, якщо він відрізняється
            # Наприклад, якщо це чистий список: return content
            for brand in content:
                if isinstance(brand, dict) and 'description' in brand:
                     brands.append(brand['description'])
                else:
                     # Fallback якщо формат інший
                     brands.append(str(brand))
            return brands
        except Exception as e:
            print(f"Error parsing brands: {e}")
            return []

    @classmethod   
    def get_all_lot_numbers(cls, filename):
        try:
            with open(cls.res_json_dir / f'{filename}', "r", encoding="utf-8") as f:
                content = json.load(f)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            return []
            
        lots = []
        try:
            data_content = content.get('data', {}).get('results', {}).get('content', [])
            for item in data_content:
                if 'ln' in item:
                    lots.append(item['ln'])
        except Exception as e:
            print(f"Error extracting ln values in file {filename}: {e}")
            return []
        return lots


    @classmethod
    def download_all(cls):
        # 1. Завантажуємо список брендів
        brands = cls.get_list_of_automobile_brands()
        
        # Можна розкоментувати, якщо треба пропустити першу (наприклад 'All'):
        # brands = brands[1:] 

        print(f"Found {len(brands)} brands to process.")

        for brand in brands: 
            safe_brand_name = brand.replace(" ", "_").replace("/", "-") # Для назв файлів
            
            # Генеруємо список файлів для бренду
            path_to_file_with_names_of_all_pages_for_single_brand = cls.tech_html / f"json_files_{safe_brand_name}.txt"
            filenames = cls.save_filenames(
                cls.res_json_dir, 
                path_to_file_with_names_of_all_pages_for_single_brand, 
                f"{brand}" # Фільтр по назві файлу
            )
            
            print(f"--- Processing Brand: {brand} ({len(filenames)} pages) ---")

            for filename in filenames:
                # Витягуємо номер сторінки з назви файлу (наприклад, brand_page1.json -> 1)
                page_match = re.search(r'page(\d+)', filename)
                page_number = page_match.group(1) if page_match else "unknown"

                lots = cls.get_all_lot_numbers(filename)
                
                print(f"  > Page {page_number}: Found {len(lots)} lots")

                for lot in lots:
                    # Основний виклик
                    cls.get_rendered_html(str(lot), safe_brand_name, str(page_number))

# Для запуску:
# if __name__ == "__main__":
#     HTML_downloader.download_all()