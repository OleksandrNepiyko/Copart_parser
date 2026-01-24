"""
logic:
0. download list of all brands that Serhiy send to me
1. extract json with list of all brands
2. for each brand:
2.1  open the first page and get all data about each lot
2.2  get json files with links to photos for each lot
2.3  go to the next page
2.4 step 2. again
"""

import re
from pathlib import Path
import json
import execjs
import requests
import time
from requests_html import HTMLSession
import os
from html_downloader import HTML_downloader
from database_writer import main as db_main, drop_database
import shutil
from datetime import datetime
from seleniumbase import SB
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random
from itertools import count
from minio import Minio
from minio.error import S3Error

tech_json_path = Path('tech_json')
res_json_path = Path('res_json')
db_tech_json_path = Path('db_tech_json')
vehtypes_more_than_1k = Path('res_json/vehicle_types_more_than_1k')
SESSION = requests.Session()
DB_NAME = 'copart_lots_test'
POST_COUNT = 0
POST_LIMITER = 1000  # Number of POST requests before refreshing
#session (it includes pages and photos requests, so one full page = 1 page reques + 20 photos requests = 21 POST requests per full page)

SESSION_LOCK = threading.RLock()

# Global Session Object
# This acts as the "bridge" between the token extractor and safe_post.
SESSION = requests.Session()

MINIO_CONFIG = {
    "access_key": "students",
    "secret_key": "8r5AMTx9x2acYRCndgVykJrr8rj6GewQ",
    "endpoint": "m1.automoto.ua",
    "region": "us-east-1",
    "secure": True #to use https
}
BUCKET_NAME = "usa-auctions"
AUCTION_PREFIX = "copart"
MINIO_BASE_DIR = Path("Minio")

def upload_to_minio(local_file_path: Path):
    """
    Завантажує один конкретний файл у Minio.
    Викликається відразу після створення файлу.
    """
    if not local_file_path.exists():
        print(f"[Minio Upload] Error: File not found {local_file_path}")
        return

    client = Minio(
        MINIO_CONFIG["endpoint"],
        access_key = MINIO_CONFIG["access_key"],
        secret_key = MINIO_CONFIG["secret_key"],
        region = MINIO_CONFIG["region"],
        secure = MINIO_CONFIG["secure"]
    )

    # found = client.bucket_exists(BUCKET_NAME)
    # if not found:
    #     print('Couldn\'t find the bucket provided in BUCKET_NAME')
    #     save_error({
    #         'error_type': 'Couldn\'t find the bucket provided in BUCKET_NAME'
    #     })
    #     return
    # else:
    #     print("Bucket", BUCKET_NAME, "already exists")

    try:
        # Формуємо шлях для хмари:
        # local: Minio/lots/2026-01-24/V_Alfa.json -> remote: copart/lots/2026-01-24/V_Alfa.json
        relative_path = local_file_path.relative_to(MINIO_BASE_DIR)
        linux_path = str(relative_path).replace(os.sep, "/")
        object_name = f"{AUCTION_PREFIX}/{linux_path}"

        print(f"\n\n OBJECT_NAME: {object_name}\n\n")

        client.fput_object(
            BUCKET_NAME,
            object_name,
            str(local_file_path),
            content_type="application/json"
        )
        print(f"[Minio Upload] SUCCESS: Uploaded -> {object_name}")

    except Exception as e:
        print(f"[Minio Upload] FAILED: {e}")
        save_error({'error_type': f"Minio upload fail for {local_file_path.name}: {e}"})

def save_error(error_obj):
    #if an error occurs it should be saved here (only problems in automatic part of the program will be saved)
    error_obj['time_of_errror'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(tech_json_path / 'errors.json', 'a', encoding='utf-8') as f:
        json.dump(error_obj, f, indent=2, ensure_ascii=False)
        f.write(',\n')

def kill_chrome_processes():
    """Force kill stuck chrome/driver processes to prevent port errors on Windows."""
    if os.name == 'nt':
        try:
            os.system("taskkill /f /im chrome.exe >nul 2>&1")
            os.system("taskkill /f /im chromedriver.exe >nul 2>&1")
        except:
            pass

def get_copart_session_data(headless=False):
    """
    Launches a browser (UC mode), bypasses Cloudflare/CAPTCHA,
    and returns a dictionary of cookies and headers.
    """
    kill_chrome_processes()

    # Base structure for the result
    data = {
        "cookies": {},
        "headers": {
            "User-Agent": "",
            "X-XSRF-TOKEN": "",
            "X-Requested-With": "XMLHttpRequest", # Critical for Copart POST requests
            "Content-Type": "application/json;charset=UTF-8"
        }
    }

    # uc=True is mandatory for Cloudflare bypass
    # with SB(uc=True, incognito=True, test=True, headless=headless) as sb:
    with SB(uc=True, incognito=True, headless=headless) as sb:# test=True removed to not see reduntant logs
        try:
            sb.open("https://www.copart.com/vehicleFinder")

            # --- Smart Wait Logic ---
            # Loops for up to 60s to ensure page is fully loaded and CAPTCHA is solved
            page_loaded = False
            for _ in range(60):
                # Check for success indicators (URL or Element)
                if "vehicle" in sb.get_current_url().lower() and \
                   (sb.is_element_visible('#serverSideDataTable') or sb.is_element_visible('.inner-wrap')):
                    page_loaded = True
                    break

                # Auto-solve Cloudflare checkbox if visible
                if sb.is_element_visible('iframe[src*="cloudflare"]'):
                    sb.uc_gui_click_captcha()

                time.sleep(1)

            if not page_loaded:
                raise TimeoutError("Copart page failed to load (Cloudflare or Timeout).")

            time.sleep(2) # Stabilization time for final cookies

            # --- Data Extraction ---
            # 1. User Agent
            data["headers"]["User-Agent"] = sb.get_user_agent()

            # 2. Cookies (via CDP for completeness)
            cookies_data = sb.cdp.get_all_cookies()
            cookie_dict = {}
            xsrf_token = None

            for cookie in cookies_data:
                # Handle SeleniumBase object vs dict differences
                if isinstance(cookie, dict):
                    name = cookie.get('name', '')
                    value = cookie.get('value', '')
                else:
                    name = getattr(cookie, 'name', '')
                    value = getattr(cookie, 'value', '')

                if name:
                    cookie_dict[name] = value
                    # Capture XSRF token if found in cookies
                    if 'xsrf' in name.lower() or 'csrf' in name.lower():
                        xsrf_token = value

            data["cookies"] = cookie_dict

            # 3. XSRF Token (Check Cookies -> then LocalStorage)
            if xsrf_token:
                data["headers"]["X-XSRF-TOKEN"] = xsrf_token
            else:
                try:
                    ls = sb.execute_script("return window.localStorage;")
                    for k, v in ls.items():
                        if 'xsrf' in k.lower():
                            data["headers"]["X-XSRF-TOKEN"] = v
                            break
                except: pass

            return data

        except Exception as e:
            print(f"Error fetching Copart session data: {e}")
            save_error({
                'error_type': f"get_copart_session_data() Exception: {e}"
            })
            return None

def refresh_copart_session(headless=False):
    """
    Helper function to update the global SESSION object with a strict timeout.
    """
    print("taking cookies and headers")
    global SESSION

    # Внутрішня функція для запуску в окремому потоці
    def _get_session_task():
        return get_copart_session_data(headless=headless)

    session_retry_counter = 0
    session_data = None

    while not session_data:
        # Логіка сну (як у вашому коді), але пропускаємо сон для першої спроби (counter=0)
        if session_retry_counter > 0:
            sleep_time = 120 if session_retry_counter <= 3 else 300
            print(f"Waiting {sleep_time}s before retry...")
            time.sleep(sleep_time)

        session_retry_counter += 1
        print(f"Attempt to take cookies and headers #{session_retry_counter}")

        # Гарантовано вбиваємо процеси перед стартом, щоб мати чистий стан
        kill_chrome_processes()

        # Використовуємо ThreadPoolExecutor для встановлення тайм-ауту
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_get_session_task)

            try:
                # Очікуємо результат максимум 60 секунд
                session_data = future.result(timeout=60)
            except TimeoutError:
                print(f"TIMEOUT: get_copart_session_data took longer than 60s. Killing Chrome...")
                # Критично важливо: вбиваємо Chrome, щоб "завислий" потік впав з помилкою
                kill_chrome_processes()
                session_data = None # Гарантуємо, що цикл продовжиться
            except Exception as e:
                print(f"Error executing session update: {e}")
                session_data = None

    # Якщо ми вийшли з циклу, значить session_data отримано
    if session_data:
        print("session refreshed successfully")
        # Оновлюємо глобальну сесію під замком, якщо потрібно (хоча ви викликаєте це в одному потоці)
        with SESSION_LOCK:
            # === ГОЛОВНА ЗМІНА ТУТ ===
            # Ми викидаємо старий об'єкт SESSION на смітник і створюємо чистий.
            # Це вбиває всі старі завислі TCP-пули.
            SESSION = requests.Session()

            SESSION.headers.update(session_data['headers'])
            SESSION.cookies.update(session_data['cookies'])
        return True

    return False

def check_dynamic_details(lot_number):
    url = f"https://www.copart.com/public/data/lot/dynamic-lot-details/{lot_number}"

    # --- ВАЖЛИВО: Додаємо Referer ---
    # Copart думає, що ми на сторінці лота і запитуємо дані звідти
    headers = {
        "Referer": f"https://www.copart.com/lot/{lot_number}",
        "X-Requested-With": "XMLHttpRequest",
        # X-XSRF-TOKEN вже має бути в SESSION.headers, якщо ні - додай вручну з кукі
    }

    # Використовуємо global SESSION
    try:
        r = SESSION.get(url, headers=headers)

        if r.status_code == 200:
            try:
                data = r.json()
                print(f"[Dynamic Details] Success for {lot_number}")
                # Тут можна повернути data, якщо треба
                return data
            except json.JSONDecodeError:
                print(f"[Dynamic Details] Error: Not JSON response for {lot_number}")

        elif r.status_code == 404:
            # Це нормальна ситуація для старих/проданих лотів
            print(f"[Dynamic Details] Info: Lot {lot_number} has no dynamic data (404 - Likely Sold/Inactive).")
            return None

        else:
            print(f"[Dynamic Details] Error {r.status_code} for {lot_number}")
            return None

    except Exception as e:
        print(f"[Dynamic Details] Exception: {e}")
        return None

def safe_post(url, **kwargs):
    global POST_COUNT
    global POST_LIMITER

    if 'timeout' not in kwargs:
        kwargs['timeout'] = 30

    # 1. Перевірка лічильника (стандартна процедура)
    with SESSION_LOCK:
        if POST_COUNT >= POST_LIMITER:
            print(f"[SafePost] Limit {POST_LIMITER} reached. Refreshing session...")
            if not refresh_copart_session():
                raise RuntimeError("Failed to refresh session.")
            POST_COUNT = 0
        POST_COUNT += 1

    # 2. Виконуємо запит з логікою "Refresh on Error"
    for attempt in range(5):
        try:
            # print(f"[SafePost] Sending request (Attempt {attempt+1})...")
            response = SESSION.post(url, **kwargs)
            # print(f"[SafePost] Received response: {response.status_code}")

            # Якщо успіх (200) - перевіряємо, чи це дійсно JSON, а не сторінка блокування Cloudflare
            content_type = response.headers.get("Content-Type", "")
            is_soft_block = (response.status_code == 200 and "application/json" not in content_type)

            if response.status_code == 200 and not is_soft_block:
                return response
                 # Якщо це не JSON, можливо нас блокують, але поки повернемо як є.
                # (Але якщо це Cloudflare, наступний код впаде, тому див. нижче)
                # return response

            # Якщо помилка 403 (Forbidden) або 429 (Too Many Requests) або 503
            if response.status_code in [403, 429, 503] or is_soft_block:
                print(f"[SafePost] Got status {response.status_code}. Attempt {attempt+1}/5. Forcing Refresh...")

                # Блокуємо, щоб інші потоки почекали
                with SESSION_LOCK:
                    # Додаємо невелику затримку, щоб не спамити браузерами
                    time.sleep(2)
                    refresh_copart_session()
                    # Скидаємо лічильник, бо ми щойно оновились
                    POST_COUNT = 0
                continue # Йдемо на наступну ітерацію циклу (повторний запит)

        except requests.exceptions.ConnectionError:
            print(f"[SafePost] Connection error, retry {attempt+1}/5")
            time.sleep(5)
        except Exception as e:
             print(f"[SafePost] Request error: {e}")
             # Якщо сталася дивна помилка, теж спробуємо оновитись на всяк випадок
             with SESSION_LOCK:
                 refresh_copart_session()

    # Якщо після 5 спроб і оновлень нічого не вийшло
    print("[SafePost] Failed after 5 retries.")
    # Повертаємо dummy об'єкт з кодом 500, щоб програма не крашилась, а просто пропускала лот
    dummy = requests.Response()
    dummy.status_code = 500
    dummy._content = b"{}"
    return dummy

def safe_get(url, **kwargs):
    global POST_COUNT
    global POST_LIMITER

    if 'timeout' not in kwargs:
        kwargs['timeout'] = 30

    # Використовуємо той самий лічильник, що і для POST, щоб освіжати сесію
    with SESSION_LOCK:
        if POST_COUNT >= POST_LIMITER:
            print(f"[SafeGet] Limit {POST_LIMITER} reached. Refreshing session...")
            if not refresh_copart_session():
                raise RuntimeError("Failed to refresh session.")
            POST_COUNT = 0
        POST_COUNT += 1

    for attempt in range(5):
        try:
            response = SESSION.get(url, **kwargs)

            # --- Аналіз відповіді ---
            content_type = response.headers.get("Content-Type", "")
            # Успіх: 200 ОК і це JSON
            if response.status_code == 200 and "application/json" in content_type:
                return response

            # Якщо 404 - це означає лот не знайдено (видалений). Це НЕ помилка сесії.
            if response.status_code == 404:
                print(f"[SafeGet] 404 Not Found for {url} (Lot might be removed)")
                return response # Повертаємо як є, обробимо зовні

            # М'який блок (200 OK, але HTML)
            is_soft_block = (response.status_code == 200 and "application/json" not in content_type)

            # Помилки, що вимагають оновлення сесії
            if response.status_code in [403, 429, 503] or is_soft_block:
                reason = "Soft Block (HTML)" if is_soft_block else f"Status {response.status_code}"
                print(f"[SafeGet] Issue: {reason}. Attempt {attempt+1}/5. Refreshing...")

                with SESSION_LOCK:
                    time.sleep(random.uniform(2, 4))
                    refresh_copart_session()
                    POST_COUNT = 0
                continue

        except requests.exceptions.ConnectionError:
            print(f"[SafeGet] Connection error, retry {attempt+1}/5")
            time.sleep(5)
        except Exception as e:
             print(f"[SafeGet] Request error: {e}")
             with SESSION_LOCK:
                 refresh_copart_session()

    print("[SafeGet] Failed after 5 retries.")
    dummy = requests.Response()
    dummy.status_code = 500
    dummy._content = b"{}"
    return dummy

def refresh_table_index():
    try:
        with open (db_tech_json_path / 'table_index.json', 'r', encoding='utf-8') as f:
            table_index_data = json.load(f)
            table_index = table_index_data.get('table_index', 0)
            table_index += 1
            with open (db_tech_json_path / 'table_index.json', 'w', encoding='utf-8') as f_w:
                json.dump({'table_index': table_index}, f_w, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"refresh() Error reading table_index.json: {e}")
        save_error({
                'error_type': f"refresh() Error reading table_index.json: {e}"
            })

def get_table_index():
    try:
        with open (db_tech_json_path / 'table_index.json', 'r', encoding='utf-8') as f:
            table_index_data = json.load(f)
            table_index = table_index_data.get('table_index', 0)
            return table_index
    except Exception as e:
        print(f"get() Error reading table_index.json: {e}")
        save_error({
                'error_type': f"get() Error reading table_index.json: {e}"
            })
        return 0

def get_number_of_vehicle_types_to_skip():
    file_path = tech_json_path / 'number_of_vehicle_types_to_skip.json'
    default_data = {"number_of_vehicle_types_to_skip": 0}
    number_of_vehicle_types_to_skip = 0
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if data is None or not isinstance(data, dict):
                raise ValueError("Data is invalid")
            number_of_vehicle_types_to_skip = data.get("number_of_vehicle_types_to_skip", 0)
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(default_data, f, indent=2)
        number_of_vehicle_types_to_skip = 0
    print(f"get_number_of_vehicle_types_to_skip {number_of_vehicle_types_to_skip}")
    return number_of_vehicle_types_to_skip

def save_start_or_finish_time(writing_start_time):
    table_index = get_table_index()
    history = []

    # 1. Завантажуємо існуючий список
    try:
        with open(tech_json_path / 'working_time.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Перевіряємо, чи це список. Якщо там старий формат (словник), скидаємо в порожній список
            if isinstance(data, list):
                history = data
            else:
                history = []
    except (FileNotFoundError, json.JSONDecodeError):
        history = []

    # Якщо записів більше 10, залишаємо тільки останні 10
    if len(history) > 10:
        history = history[-10:]

    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if writing_start_time:
        # START: Створюємо НОВИЙ об'єкт і додаємо в кінець списку
        new_obj = {
            "table_index": table_index,
            "start_time": current_time_str,
            "finished_writing_to_db": "",
            "time_of_parsing": ""
        }
        history.append(new_obj)
    else:
        # FINISH: Редагуємо ОСТАННІЙ об'єкт у списку
        if not history:
            # Якщо список порожній, але ми намагаємось записати фініш — це помилка логіки,
            # але щоб не крашити, створимо запис з помилкою
            history.append({
                "table_index": table_index,
                "start_time": "",
                "finished_writing_to_db": current_time_str,
                "time_of_parsing": "Error: No start time recorded"
            })

        # Беремо останній елемент (над яким зараз працюємо)
        current_obj = history[-1]

        # Перевірка: чи збігається індекс (опціонально, але корисно для дебагу)
        # current_obj["table_index"] = table_index # Можна примусово оновити, якщо треба

        start_time_str = current_obj.get('start_time', "")
        duration = "Error: No start time found"

        if start_time_str:
            try:
                start_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
                duration = str(datetime.now() - start_dt)
            except ValueError:
                duration = "Error: Invalid start time format"

        current_obj["finished_writing_to_db"] = current_time_str
        current_obj["time_of_parsing"] = duration

    # Ще раз перевіряємо ліміт перед збереженням (на випадок, якщо ми додали 11-й елемент)
    if len(history) > 10:
        history = history[-10:]

    # 2. Зберігаємо список у файл
    try:
        with open(tech_json_path / 'working_time.json', 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving history: {e}")
        try:
            save_error({'error_type': f"Error saving history: {e}"})
        except:
            pass
        return False

    return True

def extract_json_from_list_of_all_brands():
    #extracts all data from js file, but not everything is needed. result is in tech_json/data_from_js.json
    tech_json_path.mkdir(exist_ok=True)

    with open('data_from_base_page_with_all_brands.js', 'r', encoding='utf-8') as f:
        js_content = f.read()

    name_of_var_inside = 'referenceDataLess'

    # Method 1: Simple regex with json.loads
    pattern = rf'var\s+{name_of_var_inside}\s*=\s*(\{{[\s\S]*?\}})\s*;'
    match = re.search(pattern, js_content)

    if match:
        raw_js_object = match.group(1)
        print(f"Found variable, raw length: {len(raw_js_object)}")

        # Clean up - remove trailing commas that break JSON
        cleaned = re.sub(r',\s*}', '}', raw_js_object)
        cleaned = re.sub(r',\s*]', ']', cleaned)

        # Fix the escape sequences that cause warnings
        cleaned = cleaned.replace(r'\/', '/')

        try:
            # Parse as JSON directly
            data = json.loads(cleaned)

            with open(tech_json_path / 'data_from_js.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Successfully extracted {name_of_var_inside}")
            return data

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print("Trying alternative method...")

            # Alternative: Use execjs if available
            try:

                # Create JS context and extract the variable
                ctx = execjs.compile(js_content + f"\nJSON.stringify({name_of_var_inside})")
                json_str = ctx.eval(f"JSON.stringify({name_of_var_inside})")
                data = json.loads(json_str)

                with open(tech_json_path / 'data_from_js.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"Successfully extracted {name_of_var_inside} using execjs")
                return data

            except ImportError:
                print("execjs not available. Install with: pip install pyexecjs")
            except Exception as e2:
                print(f"execjs also failed: {e2}")

            # Last resort: manual conversion
            try:
                # Convert JS to Python literals
                cleaned = cleaned.replace('true', 'True').replace('false', 'False').replace('null', 'None')
                data = eval(cleaned)

                with open(tech_json_path / 'data_from_js.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"Successfully extracted {name_of_var_inside} using eval")
                return data

            except Exception as e3:
                print(f"All methods failed: {e3}")
                # Save problematic content
                save_error({
                        'error_type': str(e3)
                    })
    else:
        print(f"Variable {name_of_var_inside} not found")
        save_error({
                'error_type': f"Variable {name_of_var_inside} not found"
            })

def get_brand_description_variants(brand_name):
    #returns list of possible brand name with space variants for searching
    variants = []
    # variants.append(brand_name) #because I've added it in the place of calling function
    variants.append(brand_name.replace(" ", "_"))
    variants.append(brand_name.replace(" ", "-"))
    variants.append(brand_name.replace(" ", ""))
    return variants

def extract_vehicle_types():
    vehicleTypes = []
    try:
        with open(tech_json_path / 'data_from_js.json', 'r', encoding='utf-8') as f:
            content = json.load(f)
            vehicleTypes = content['vehicleTypes']
    except FileNotFoundError:
        print("extract_vehicle_types() data_from_js.json not found. Run extract_json_from_list_of_all_brands() first.")
        save_error({
                'error_type': "extract_vehicle_types() data_from_js.json not found. Run extract_json_from_list_of_all_brands() first."
            })
        return

    try:
        if len(vehicleTypes)>0 and vehicleTypes != None:
            with open(tech_json_path / 'vehicle_types.json', 'w', encoding='utf-8') as f:
                json.dump(vehicleTypes, f, indent=2, ensure_ascii=False)
        else:
            print("vehicle_types.json is empty or None")
            save_error({
                    'error_type': "vehicle_types.json is empty or None"
                })
    except Exception as e:
        print(f"extract_vehicle_types(): Exception: {e}")
        save_error({
                'error_type': f"extract_vehicle_types(): Exception: {e}"
            })
        return

def filter_unique_brands(brands_list):
    #unused now (it deletes based on brand name while the same brand can
    # produce different types of vehicles, like buses and automobiles from chevrolet etc)
    # after its work remaining 50270 lines vs 62293 lines originally
    # that is 12567 vs 15573 vehicle classes
    """
    Приймає список словників брендів.
    Повертає новий список, де для кожного унікального 'description'
    залишено лише один запис (перший знайдений).
    """
    seen_descriptions = set()
    unique_list = []

    for brand in brands_list:
        # Отримуємо значення description (наприклад "Acura", "BMW")
        description = brand.get('description')

        # Якщо description існує і ми його ще не бачили
        if description and description not in seen_descriptions:
            unique_list.append(brand)
            seen_descriptions.add(description)

    return unique_list

def extract_automobile_brands_list(extract_only_automobile):
    #extracts from tech_json/data_from_js.json only automobile firms and ignores duplicates with suv/sedan/automobile duplications.
    # it extracts only with 'automobile' type and saves it in tech_json/list_of_automobile_brands.json
    try:
        with open(tech_json_path / 'data_from_js.json', 'r', encoding='utf-8') as f:
            content = json.load(f)
            automobile_brands_list = content['vehicleMakes']
    except FileNotFoundError:
        print("Error: data_from_js.json not found. Run extract_json_from_list_of_all_brands() first.")
        return
    except KeyError:
        print("Error: 'vehicleMakes' key not found in JSON.")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
        return

    # to filter duplicates based on 'description' field
    # automobile_brands_list = filter_unique_brands(automobile_brands_list)

    automobile_brands_list_with_automobile_type = []

    for brand in automobile_brands_list:
        try:
            if extract_only_automobile and brand['type'] == 'AUTOMOBILE':
                automobile_brands_list_with_automobile_type.append(brand)
            else:
                automobile_brands_list_with_automobile_type.append(brand)
        except KeyError:
            print(f"Warning: Brand missing 'type' field, skipping: {brand}")
            continue

    try:
        with open(tech_json_path / 'list_of_automobile_brands.json', 'w', encoding='utf-8') as f:
            json.dump(automobile_brands_list_with_automobile_type, f, indent=2, ensure_ascii=False)
        print(f"Successfully saved {len(automobile_brands_list_with_automobile_type)} automobile brands.")
    except IOError as e:
        print(f"Error: Could not write to file - {e}")

def process_single_lot_vehicle_type(file_name, page, number):
    # Випадкова затримка
    time.sleep(random.uniform(0.5, 2.0))

    # dynamic_data = check_dynamic_details(number)

    # if dynamic_data:
    #     # Можеш зберегти це разом з фото або окремим файлом
    #     # Наприклад, додати у файл з фото або записати в окрему папку
    #     try:
    #         dyn_dir = res_json_path / "dynamic_data"
    #         dyn_dir.mkdir(parents=True, exist_ok=True)
    #         with open(dyn_dir / f"{number}_dynamic.json", "w", encoding="utf-8") as f:
    #             json.dump(dynamic_data, f, indent=2)
    #     except Exception as e:
    #         print(f"Error saving dynamic data: {e}")

    url = "https://www.copart.com/public/data/lotdetails/solr/lot-images/"
    payload = {"lotNumber": number}

    # safe_post тепер сам спробує оновитись, якщо отримає 403
    r = safe_post(url, json=payload, timeout = 30)

    if r.status_code != 200:
        print(f"Error {r.status_code} for lot {number} in {file_name} page {page}")
        save_error({
            'file_name': file_name,
            'page': page,
            'lot_number': number,
            'error_type': f"HTTP Error {r.status_code} for lot {number} in {file_name} page {page}"
        })
        return

    try:
        data = r.json()

        target_dir = res_json_path / f"{file_name}_page{page + 1}_photos"
        target_dir.mkdir(parents=True, exist_ok=True)

        with open(target_dir / f"{number}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    except Exception as e:
        # Якщо ми тут, значить safe_post повернув 200 OK, але це НЕ JSON.
        # Це 100% блок від Cloudflare. Треба оновлюватись.
        print(f"JSON Error for lot {number} in {file_name} page {page} (Likely soft-block). Triggering refresh...")
        with SESSION_LOCK:
             # Перевіряємо, може хтось вже оновив поки ми спали
             refresh_copart_session()

def fetch_build_sheet(lot_number, lot_hash):
    """
    Helper function to fetch build sheet data using lotHash.
    """
    url = "https://www.copart.com/public/data/lot/build-sheet"
    payload = {
        "lotId": int(lot_number),
        "lotHash": lot_hash
    }

    # Використовуємо safe_post, який вже має логіку повторів і оновлення сесії
    try:
        r = safe_post(url, json=payload, timeout=20)
        if r.status_code == 200:
            try:
                return r.json()
            except json.JSONDecodeError:
                return None
        elif r.status_code == 404:
             # Build sheet not found - it's normal for some lots
            return None
        else:
            print(f"[BuildSheet] Error {r.status_code} for lot {lot_number}")
            return None
    except Exception as e:
        print(f"[BuildSheet] Exception for lot {lot_number}: {e}")
        return None

def get_lot_details_vehicle_type(file_name, page, number):
    # Випадкова затримка
    time.sleep(random.uniform(0.5, 2.0))

    # dynamic_data = check_dynamic_details(number)

    # if dynamic_data:
    #     # Можеш зберегти це разом з фото або окремим файлом
    #     # Наприклад, додати у файл з фото або записати в окрему папку
    #     try:
    #         dyn_dir = res_json_path / "dynamic_data"
    #         dyn_dir.mkdir(parents=True, exist_ok=True)
    #         with open(dyn_dir / f"{number}_dynamic.json", "w", encoding="utf-8") as f:
    #             json.dump(dynamic_data, f, indent=2)
    #     except Exception as e:
    #         print(f"Error saving dynamic data: {e}")

    url = f"https://www.copart.com/public/data/lotdetails/solr/{number}"
    payload = {"lotNumber": number}

    # safe_post тепер сам спробує оновитись, якщо отримає 403
    r = safe_get(url, timeout = 30)

    if r.status_code != 200:
        print(f"Error {r.status_code} for lot {number} in {file_name} page {page}")
        save_error({
            'file_name': file_name,
            'page': page,
            'lot_number': number,
            'error_type': f"HTTP Error {r.status_code} for lot {number} in {file_name} page {page}"
        })
        return

    try:
        data = r.json()


        # for build-sheet:
        lot_data_obj = data.get('data', {}).get('lotDetails', {})
        if not lot_data_obj:
            lot_data_obj = data.get('data', {}) # fallback

        lot_hash = lot_data_obj.get('lh')
        if not lot_hash:
            lot_hash = data.get('lh') #it'll be impossible if fallback

        if lot_hash:
            build_sheet_data = fetch_build_sheet(number, lot_hash)
            if build_sheet_data:
                data['build_sheet'] = build_sheet_data
            else:
                print(f"Error. get_lot_details_vehicle_type build-sheet returned None")
                save_error({
                    'file_name': file_name,
                    'number': number,
                    'page': page,
                    'error_type': f"Error. get_lot_details_vehicle_type build-sheet returned None"
                })

        target_dir = res_json_path / f"{file_name}_page{page + 1}_lots"
        target_dir.mkdir(parents=True, exist_ok=True)

        with open(target_dir / f"{number}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    except Exception as e:
        # Якщо ми тут, значить safe_post повернув 200 OK, але це НЕ JSON.
        # Це 100% блок від Cloudflare. Треба оновлюватись.
        print(f"JSON Error for lot {number} in {file_name} page {page} (Likely soft-block). Triggering refresh...")
        with SESSION_LOCK:
             # Перевіряємо, може хтось вже оновив поки ми спали
             refresh_copart_session()

def get_lot_details_for_page_vehicle_type(file_name, page, all_ln_values, search_query):
    print(f"get_lot_details_for_page_vehicle_type {file_name}: {all_ln_values} (Total: {len(all_ln_values)})")

    # --- БАГАТОПОТОЧНІСТЬ ---
    # max_workers=3 означає, що одночасно буде качатися 3 фотографій.

    #tmp
    # all_ln_values = all_ln_values[:3]
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for number in all_ln_values:
            # Ми не викликаємо функцію, а плануємо її виконання (submit)
            futures.append(executor.submit(get_lot_details_vehicle_type, file_name, page, number))

        # Чекаємо завершення всіх завдань на цій сторінці
        for future in as_completed(futures):
            try:
                future.result() # Тут вилетить помилка, якщо вона сталася всередині потоку
            except Exception as e:
                print(f"Thread execution failed: {e}")
                save_error({
                    'search_query': search_query,
                    'brand': None,
                    'page': page,
                    'error_type': f"Thread execution failed: {e}"
                })

    # --- ЗБЕРЕЖЕННЯ ТОЧКИ ---
    # Зберігаємо, що ми закінчили цю сторінку (обнуляємо lot_number)
    with open(tech_json_path /'restart_point.json', 'w', encoding='utf-8') as f:
        restart_point = {
            'search_query': search_query,
            'brand': None,
            'page': page + 1,
            'lot_number': 0
        }
        json.dump(restart_point, f, indent=2, ensure_ascii=False)

def get_lot_details(brand, page, type_param, number, sloc_display_name, engn_display_name):
    # Випадкова затримка
    time.sleep(random.uniform(0.5, 2.0))
    brand_with_underscores = brand.replace(" ", "_").replace("/","_")

    # dynamic_data = check_dynamic_details(number)

    # if dynamic_data:
    #     # Можеш зберегти це разом з фото або окремим файлом
    #     # Наприклад, додати у файл з фото або записати в окрему папку
    #     try:
    #         dyn_dir = res_json_path / "dynamic_data"
    #         dyn_dir.mkdir(parents=True, exist_ok=True)
    #         with open(dyn_dir / f"{number}_dynamic.json", "w", encoding="utf-8") as f:
    #             json.dump(dynamic_data, f, indent=2)
    #     except Exception as e:
    #         print(f"Error saving dynamic data: {e}")

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'Origin': 'https://www.copart.com',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
    }

    url = f"https://www.copart.com/public/data/lotdetails/solr/{number}"
    payload = {"lotNumber": number}

    # safe_post тепер сам спробує оновитись, якщо отримає 403
    r = safe_get(url, timeout = 30)

    if r.status_code != 200:
        # print(f"Error {r.status_code} for lot {number}")
        return

    try:
        data = r.json()


        #for build-sheet
        lot_data_obj = data.get('data', {}).get('lotDetails', {})
        if not lot_data_obj:
             lot_data_obj = data.get('data', {}) # fallback

        lot_hash = lot_data_obj.get('lh')
        if not lot_hash:
            lot_hash = data.get('lh')

        if lot_hash:
            build_sheet_data = fetch_build_sheet(number, lot_hash)
            if build_sheet_data:
                data['build_sheet'] = build_sheet_data
            else:
                print(f"Error. get_lot_details build-sheet returned None")
                save_error({
                    'type_param': type_param,
                    'brand': brand,
                    'page': page,
                    'sloc_display_name': sloc_display_name,
                    'engn_display_name': engn_display_name,
                    'error_type': f"Error. get_lot_details build-sheet returned None"
                })

        file_name = f"{brand_with_underscores}_{type_param}_"
        if sloc_display_name is not None:
            file_name += f"{sloc_display_name.replace(" ", "_")}"
        if engn_display_name is not None:
            file_name += f"_{engn_display_name.replace(" ", "_")}"
        file_name += f"_page{page + 1}_lots"
        target_dir = res_json_path / file_name
        target_dir.mkdir(parents=True, exist_ok=True)

        with open(target_dir / f"{number}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    except Exception as e:
        # Якщо ми тут, значить safe_post повернув 200 OK, але це НЕ JSON.
        # Це 100% блок від Cloudflare. Треба оновлюватись.
        print(f"JSON Error for lot {number} : {e} (Likely soft-block). Triggering refresh...")
        with SESSION_LOCK:
             # Перевіряємо, може хтось вже оновив поки ми спали
             refresh_copart_session()

def get_lot_details_for_page(brand, page, type_param, arr_of_lot_numbers, restart_object, sloc_query_index = -1, sloc_display_name = None, engine_volume_index = -1, engn_display_name = None):
    print(f"get_lot_details_for_page: {arr_of_lot_numbers} (Total: {len(arr_of_lot_numbers)})")

    # tmp
    # arr_of_lot_numbers = arr_of_lot_numbers[:3]
    # --- Логіка RESTART ---
    # Фільтруємо список номерів ДО запуску потоків
    restart_lot_number = 0
    if restart_object and isinstance(restart_object, dict):
        restart_lot_number = restart_object.get('lot_number', 0)

    lots_to_process = []
    if restart_lot_number != 0:
        if restart_lot_number in arr_of_lot_numbers:
            idx = arr_of_lot_numbers.index(restart_lot_number)
            lots_to_process = arr_of_lot_numbers[idx:] # Починаємо з місця зупинки
        else:
            # Якщо номер не знайдено (дивна ситуація), беремо всі
            lots_to_process = arr_of_lot_numbers
    else:
        lots_to_process = arr_of_lot_numbers

    # --- БАГАТОПОТОЧНІСТЬ ---
    # max_workers=5 означає, що одночасно буде качатися 5 фотографій.
    # Не ставте занадто багато (наприклад 20), бо Copart може забанити IP.

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for number in lots_to_process:
            # Ми не викликаємо функцію, а плануємо її виконання (submit)
            futures.append(executor.submit(get_lot_details, brand, page, type_param, number, sloc_display_name, engn_display_name))

        # Чекаємо завершення всіх завдань на цій сторінці
        for future in as_completed(futures):
            try:
                future.result() # Тут вилетить помилка, якщо вона сталася всередині потоку
            except Exception as e:
                print(f"get_lot_details_for_page Thread execution failed: {e}")
                save_error({
                    'brand': brand,
                    'page': page,
                    'error_type': f"get_lot_details_for_page Thread execution failed: {e}"
                })

    # --- ЗБЕРЕЖЕННЯ ТОЧКИ ---
    # Зберігаємо, що ми закінчили цю сторінку (обнуляємо lot_number)
    with open(tech_json_path /'restart_point.json', 'w', encoding='utf-8') as f:
        restart_point = {
            'brand': brand,
            'page': page + 1,
            'sloc_query_index': sloc_query_index,
            'engine_volume_index': engine_volume_index,
            'lot_number': 0
        }
        json.dump(restart_point, f, indent=2, ensure_ascii=False)

# Виклич це для будь-якого живого лота
# test_fast_api(98026285)

def process_single_lot(brand, page, type_param, number, sloc_display_name, engn_display_name):
    # Випадкова затримка
    time.sleep(random.uniform(0.5, 2.0))
    brand_with_underscores = brand.replace(" ", "_").replace("/","_")

    # dynamic_data = check_dynamic_details(number)

    # if dynamic_data:
    #     # Можеш зберегти це разом з фото або окремим файлом
    #     # Наприклад, додати у файл з фото або записати в окрему папку
    #     try:
    #         dyn_dir = res_json_path / "dynamic_data"
    #         dyn_dir.mkdir(parents=True, exist_ok=True)
    #         with open(dyn_dir / f"{number}_dynamic.json", "w", encoding="utf-8") as f:
    #             json.dump(dynamic_data, f, indent=2)
    #     except Exception as e:
    #         print(f"Error saving dynamic data: {e}")

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'Origin': 'https://www.copart.com',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
    }

    url = "https://www.copart.com/public/data/lotdetails/solr/lot-images/"
    payload = {"lotNumber": number}

    # safe_post тепер сам спробує оновитись, якщо отримає 403
    r = safe_post(url, json=payload, timeout = 30)

    if r.status_code != 200:
        # print(f"Error {r.status_code} for lot {number}")
        return

    try:
        data = r.json()

        file_name = f"{brand_with_underscores}_{type_param}_"
        if sloc_display_name is not None:
            file_name += f"{sloc_display_name.replace(" ", "_")}"
        if engn_display_name is not None:
            file_name += f"_{engn_display_name.replace(" ", "_")}"
        file_name += f"_page{page + 1}_photos"
        target_dir = res_json_path / file_name
        target_dir.mkdir(parents=True, exist_ok=True)

        with open(target_dir / f"{number}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    except Exception as e:
        # Якщо ми тут, значить safe_post повернув 200 OK, але це НЕ JSON.
        # Це 100% блок від Cloudflare. Треба оновлюватись.
        print(f"JSON Error for lot {number} : {e} (Likely soft-block). Triggering refresh...")
        with SESSION_LOCK:
             # Перевіряємо, може хтось вже оновив поки ми спали
             refresh_copart_session()

def download_photos_from_lot(brand, page, type_param, arr_of_lot_numbers, restart_object, sloc_query_index = -1, sloc_display_name = None, engine_volume_index = -1, engn_display_name = None):
    print(f"Download_photos_for_lot: {arr_of_lot_numbers} (Total: {len(arr_of_lot_numbers)})")

    # tmp
    # arr_of_lot_numbers = arr_of_lot_numbers[:3]
    # --- Логіка RESTART ---
    # Фільтруємо список номерів ДО запуску потоків
    restart_lot_number = 0
    if restart_object and isinstance(restart_object, dict):
        restart_lot_number = restart_object.get('lot_number', 0)

    lots_to_process = []
    if restart_lot_number != 0:
        if restart_lot_number in arr_of_lot_numbers:
            idx = arr_of_lot_numbers.index(restart_lot_number)
            lots_to_process = arr_of_lot_numbers[idx:] # Починаємо з місця зупинки
        else:
            # Якщо номер не знайдено (дивна ситуація), беремо всі
            lots_to_process = arr_of_lot_numbers
    else:
        lots_to_process = arr_of_lot_numbers

    # --- БАГАТОПОТОЧНІСТЬ ---
    # max_workers=5 означає, що одночасно буде качатися 5 фотографій.
    # Не ставте занадто багато (наприклад 20), бо Copart може забанити IP.

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for number in lots_to_process:
            # Ми не викликаємо функцію, а плануємо її виконання (submit)
            futures.append(executor.submit(process_single_lot, brand, page, type_param, number, sloc_display_name, engn_display_name))

        # Чекаємо завершення всіх завдань на цій сторінці
        for future in as_completed(futures):
            try:
                future.result() # Тут вилетить помилка, якщо вона сталася всередині потоку
            except Exception as e:
                print(f"Thread execution failed: {e}")
                save_error({
                    'brand': brand,
                    'page': page,
                    'error_type': f"Thread execution failed: {e}"
                })

    # --- ЗБЕРЕЖЕННЯ ТОЧКИ ---
    # Зберігаємо, що ми закінчили цю сторінку (обнуляємо lot_number)
    with open(tech_json_path /'restart_point.json', 'w', encoding='utf-8') as f:
        restart_point = {
            'brand': brand,
            'page': page + 1,
            'sloc_query_index': sloc_query_index,
            'engine_volume_index': engine_volume_index,
            'lot_number': 0
        }
        json.dump(restart_point, f, indent=2, ensure_ascii=False)

def refresh_home_and_get_actual_vehicle_types_list():
    #here I will download HOME.json and then save it pretty formatted
    home_filename = "HOME.json"
    response_home = None

    url = "https://www.copart.com/public/data/quickPickCounts/HOME"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
        'Referer': 'https://www.copart.com/'
    }
    cookies = {}

    try:
        response_home = SESSION.get(
            url=url,
            headers=headers
        )
    except Exception as e:
        print(f"Connection error in refresh_home: {e}")
        return None

    content_type = response_home.headers.get("Content-Type", "")
    is_soft_block = (response_home.status_code == 200 and "application/json" not in content_type)

    if is_soft_block or response_home.status_code != 200:
        print(f"Error. HOME.json failed. Status: {response_home.status_code}, SoftBlock: {is_soft_block}")
        save_error({
            "error_type": f"Error. HOME.json refresh failed. Status: {response_home.status_code}"
        })
        # Якщо тут блок, можна спробувати оновити сесію, але в цій функції це небезпечно робити рекурсивно.
        # Просто повертаємо None, а main() спробує ще раз.
        return None

    try:
        data = response_home.json()
    except json.JSONDecodeError:
        print("Error decoding HOME.json")
        return None

    if not data or data.get('data') == []:
        print("Error. HOME.json data is empty.")
        return None

    try:
        with open(tech_json_path / home_filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Warning: Could not save HOME.json: {e}")

    # home_content = {} #to make it pretty json format, not one line
    # with open (tech_json_path / home_filename, "r", encoding="utf-8") as f:
    #     home_content = json.load(f)

    # with open (tech_json_path / home_filename, "w", encoding="utf-8") as f:
    #     json.dump(home_content, f, ensure_ascii=False, indent=2)

    veht_array = data.get('data', {}).get('quickPicks', {}).get('VEHT', [])

    if not veht_array:
        print("VEHT array is empty in HOME.json")
        save_error({
            'error_type': "VEHT array is empty in HOME.json"
        })
        return None

    print("HOME.json refreshed successfully")
    return veht_array


def clean_payload(payload: dict) -> dict:
    """
    Очищає Copart UI payload і повертає нормальний робочий payload для API.
    """

    allowed_keys = {
        "query",
        "filter",
        "sort",
        "page",
        "size",
        "start",
        "watchListOnly",
        "freeFormSearch",
        "hideImages",
        "includeTagByField"
    }

    clean = {}

    for key in allowed_keys:
        if key in payload:
            clean[key] = payload[key]

    return clean

def download_photos_from_lot_vehicle_type(file_name, page, all_ln_values, search_query):
    print(f"Download_photos_for_lot {file_name}: {all_ln_values} (Total: {len(all_ln_values)})")

    # --- БАГАТОПОТОЧНІСТЬ ---
    # max_workers=3 означає, що одночасно буде качатися 3 фотографій.

    #tmp
    # all_ln_values = all_ln_values[:3]
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for number in all_ln_values:
            # Ми не викликаємо функцію, а плануємо її виконання (submit)
            futures.append(executor.submit(process_single_lot_vehicle_type, file_name, page, number))

        # Чекаємо завершення всіх завдань на цій сторінці
        for future in as_completed(futures):
            try:
                future.result() # Тут вилетить помилка, якщо вона сталася всередині потоку
            except Exception as e:
                print(f"Thread execution failed: {e}")
                save_error({
                    'search_query': search_query,
                    'brand': None,
                    'page': page,
                    'error_type': f"Thread execution failed: {e}"
                })

    # --- ЗБЕРЕЖЕННЯ ТОЧКИ ---
    # Зберігаємо, що ми закінчили цю сторінку (обнуляємо lot_number)
    with open(tech_json_path /'restart_point.json', 'w', encoding='utf-8') as f:
        restart_point = {
            'search_query': search_query,
            'brand': None,
            'page': page + 1,
            'lot_number': 0
        }
        json.dump(restart_point, f, indent=2, ensure_ascii=False)

#old iconic version to download photos (use it as a core fo new versions)
def download_photos_from_lot_old(brand, page, type_param, arr_of_lot_numbers, restart_object):
    print(f"Download_photos_for_lot: {arr_of_lot_numbers} (Total: {len(arr_of_lot_numbers)})")

    # --- Логіка RESTART ---
    # Фільтруємо список номерів ДО запуску потоків
    restart_lot_number = 0
    if restart_object and isinstance(restart_object, dict):
        restart_lot_number = restart_object.get('lot_number', 0)

    lots_to_process = []
    if restart_lot_number != 0:
        if restart_lot_number in arr_of_lot_numbers:
            idx = arr_of_lot_numbers.index(restart_lot_number)
            lots_to_process = arr_of_lot_numbers[idx:] # Починаємо з місця зупинки
        else:
            # Якщо номер не знайдено (дивна ситуація), беремо всі
            lots_to_process = arr_of_lot_numbers
    else:
        lots_to_process = arr_of_lot_numbers

    # --- БАГАТОПОТОЧНІСТЬ ---
    # max_workers=5 означає, що одночасно буде качатися 5 фотографій.
    # Не ставте занадто багато (наприклад 20), бо Copart може забанити IP.

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for number in lots_to_process:
            # Ми не викликаємо функцію, а плануємо її виконання (submit)
            futures.append(executor.submit(process_single_lot, brand, page, type_param, number))

        # Чекаємо завершення всіх завдань на цій сторінці
        for future in as_completed(futures):
            try:
                future.result() # Тут вилетить помилка, якщо вона сталася всередині потоку
            except Exception as e:
                print(f"Thread execution failed: {e}")
                save_error({
                    'brand': brand,
                    'page': page,
                    'error_type': f"Thread execution failed: {e}"
                })

    # --- ЗБЕРЕЖЕННЯ ТОЧКИ ---
    # Зберігаємо, що ми закінчили цю сторінку (обнуляємо lot_number)
    with open(tech_json_path /'restart_point.json', 'w', encoding='utf-8') as f:
        restart_point = {
            'brand': brand,
            'page': page + 1,
            'lot_number': 0
        }
        json.dump(restart_point, f, indent=2, ensure_ascii=False)


#old iconic version to download data from pages of single brand (use it as a core fo new versions)
def download_data_from_pages_of_single_brand_old(brand, type_param, restart_object):
    print(f"download_data_from_pages_of_single_brand: {brand}")

    brand_upper = brand.upper()
    brand_with_underscores = brand.replace(" ", "_").replace("/","_")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
    }
    cookies = {}

    if restart_object == None or restart_object == '':
        restart_page = 0
    else:
        restart_page = max(0, restart_object['page'] - 1)

    for page in range (restart_page, 21):
        # time.sleep(0.1)
        print(f"Brand: {brand}, page: {page + 1}")
        start = page * 100

        payload = clean_payload({"query":["*"],"filter":{"VEHT":[f"vehicle_type_code:VEHTYPE_{type_param}"],"MAKE":[f"lot_make_desc:\"{brand_upper}\""]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":page,"size":100,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"VEHT":"{!tag=VEHT}","MAKE":"{!tag=MAKE}"},"rawParams":{}})

        url = "https://www.copart.com/public/lots/vehicle-finder-search-results"

        # for correct multi-threading
        response_json = None

        response = safe_post(
            url,
            headers=headers,
            cookies=cookies,
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            print(f"Failed to load page {page + 1} for {brand}. Status: {response.status_code}")
            continue # Пропускаємо ітерацію, не йдемо вниз

        try:
            response_json = response.json()
        except Exception as e:
            print(f"JSON Decode Error on page {page + 1}: {e}")
            # Можливо, safe_post повернув HTML. Ми не можемо продовжувати з цією сторінкою.
            continue

        # --- FIX: Перевірка на NoneType перед доступом ---
        if response_json is None:
            print(f"response_json is None for page {page + 1}. Skipping.")
            continue
        # -------------------------------------------------

        if response_json.get('data', {}).get('results', {}).get('content', []) == []:
            print(f"No content for {brand} on page {page+1}. Finishing brand.")
            break

        try:
            with open(res_json_path / f'{brand_with_underscores}_{type_param}_page{page + 1}.json', 'w', encoding='utf-8') as f:
                json.dump(response_json, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"File save error: {e}")

        all_ln_values = []
        try:
            # Тут вже безпечно, бо ми перевірили response_json вище
            content = response_json.get('data', {}).get('results', {}).get('content', [])
            for item in content:
                if 'ln' in item:
                    all_ln_values.append(item['ln'])
        except Exception as e:
            print(f"Error extracting ln values on page {page + 1}: {e}")
            continue

        per_page_restart = None
        if restart_object and isinstance(restart_object, dict) and restart_object.get('page') == page:
            per_page_restart = restart_object

        if len(all_ln_values) != 0:
            download_photos_from_lot(brand, page, type_param, all_ln_values, per_page_restart)
        else:
            print(f"No lot numbers found on page {page+1}")

        with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
            json.dump({"brand": brand, "page": page + 1, "lot_number": 0}, f)

def request_with_vehicle_type(search_query, include_tag_by_field, restart_object, download_photos_bool):
    """
    makes one request for whole vehicle type

    returns:
    - False if no content found
    - dict with response_json if download_photos_bool is True
    """

    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
    }
    cookies = {}

    if restart_object == None or restart_object == '':
        restart_page = 0
    else:
        restart_page = max(0, restart_object['page'] - 1)

    #tmp
    # for page in range (restart_page, 1):
    for page in range (restart_page, 21):
        start = page * 100
        payload = clean_payload({"query":["*"],"filter":{"VEHT":[f"{search_query}"]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":page,"size":100,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"VEHT":f"{include_tag_by_field}"},"rawParams":{}})

        url = "https://www.copart.com/public/lots/vehicle-finder-search-results"

        # --- FIX: Очищаємо змінні перед запитом ---
        response_json = None
        # ------------------------------------------

        response = safe_post(
            url,
            headers=headers,
            cookies=cookies,
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            print(f"Error request_with_vehicle_type Failed to load page Status: {response.status_code}")
            save_error({
                'search_query': search_query,
                'brand': None,
                'page': page,
                'error_type': f"Error request_with_vehicle_type Failed to load page Status: {response.status_code}"
            })
            return False

        try:
            response_json = response.json()
        except Exception as e:
            print(f"Error request_with_vehicle_type JSON Decode Erro: {e}")
            # Можливо, safe_post повернув HTML. Ми не можемо продовжувати з цією сторінкою.
            return False

        if response_json is None:
            print(f"Error request_with_vehicle_type response_json is None")
            return False

        if response_json.get('data', {}).get('results', {}).get('content', []) == []:
            print(f"No content request_with_vehicle_type for {search_query} page {page}. Probably no more pages.")
            return False

        if download_photos_bool:
            return response_json

        file_name = search_query.split(":")[1]
        try:
            with open(res_json_path / f'{file_name}_page{page + 1}.json', 'w', encoding='utf-8') as f:
                json.dump(response_json, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"File save error: {e}")

        all_ln_values = []
        try:
            # Тут вже безпечно, бо ми перевірили response_json вище
            content = response_json.get('data', {}).get('results', {}).get('content', [])
            for item in content:
                if 'ln' in item:
                    all_ln_values.append(item['ln'])
        except Exception as e:
            print(f"Error extracting ln values on page {page + 1}: {e}")
            continue

        if len(all_ln_values) != 0:
            download_photos_from_lot_vehicle_type(file_name, page, all_ln_values, search_query)
            get_lot_details_for_page_vehicle_type(file_name, page, all_ln_values, search_query)
        else:
            print(f"No lot numbers found on page {page+1}")

        with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
            json.dump({"search_query": search_query, "brand": None, "page": page + 1, "lot_number": 0}, f)

def get_possible_values_of_filters(brand, headers, cookies, type_param, brand_upper, brand_count, arr_of_additional_fields_to_include):
    """
    makes one request for specific brand and page but without specifying the MODG
    To get posible values of the filters that are transmited by arr_of_additional_fields_to_include
    These values can be used in payloads if you need to add a new sorting paramether

    returns:
    - False if no content found (indicating no more pages for this brand)
    - Sale locations (dict with 'queries' and 'display_names' lists if successful)
    - Array of dictionaries with filter names 'quickPickCode', 'brand_upper's, 'queries's and 'display_names's lists if successful
    """
    start = 0

    # print(f"type param: {type_param}, brand_description_config: {brand_description_config}")
    payload = {
    "query": ["*"],
    "filter": {
        "VEHT": [f"vehicle_type_code:{type_param}"],
        "MAKE": [f"lot_make_desc:\"{brand_upper}\""]
    },
    "sort": [
        "salelight_priority asc",
        "member_damage_group_priority asc",
        "auction_date_type desc",
        "auction_date_utc asc"
    ],
    "page": 0,
    "size": 100,
    "start": start,
    "watchListOnly": False,
    "freeFormSearch": False,
    "hideImages": False,
    "defaultSort": False,
    "specificRowProvided": False,
    "displayName": "",
    "searchName": "",
    "backUrl": "",
    "includeTagByField": {
        "VEHT": "{!tag=VEHT}",
        "MAKE": "{!tag=MAKE}"
    },
    "rawParams": {}
    }

    # if arr_of_additional_fields_to_include:
    #     for item in arr_of_additional_fields_to_include:
    #         code = item.get('quickPickCode')  # Наприклад: "YEAR"
    #         query = item.get('query')         # Наприклад: 'engine:"1.4L 4"'
    #         if code and query:
    #             # Додаємо у секцію "filter"
    #             # Copart очікує список: "YEAR": ["engine..."]
    #             payload["filter"][code] = [query]
    #             # Додаємо у секцію "includeTagByField"
    #             # Формат: "YEAR": "{!tag=YEAR}"
    #             payload["includeTagByField"][code] = f"{{!tag={code}}}"

    payload = clean_payload(payload)

    url = "https://www.copart.com/public/lots/vehicle-finder-search-results"

    # --- FIX: Очищаємо змінні перед запитом ---
    response_json = None
    # ------------------------------------------

    response = safe_post(
        url,
        headers=headers,
        cookies=cookies,
        json=payload,
        timeout=30
    )

    if response.status_code != 200:
        print(f"Error get_search_results_without_sloc_query. Failed to load for {brand}. Status: {response.status_code}")
        return False

    try:
        response_json = response.json()
    except Exception as e:
        print(f"Error get_search_results_without_sloc_query JSON Decode: {e}")
        # Можливо, safe_post повернув HTML. Ми не можемо продовжувати з цією сторінкою.
        return False

    # --- FIX: Перевірка на NoneType перед доступом ---
    if response_json is None:
        print(f"Error get_search_results_without_sloc_query. response_json is None. Skipping.")
        return False
    # -------------------------------------------------

    if response_json.get('data', {}).get('results', {}).get('content', []) == []:
        print(f"Error get_search_results_without_sloc_query. No content for {brand}. Finishing brand.")
        return False

    vehicle_types_that_have_more_that_one_k_lots_and_needs_to_be_reviewed = "vehicle_types_more_than_1k"
    output_dir = res_json_path / vehicle_types_that_have_more_that_one_k_lots_and_needs_to_be_reviewed
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / f'{brand}_{type_param}_without_sloc_query.json', 'w', encoding='utf-8') as f:
        json.dump(response_json, f, ensure_ascii=False, indent=2)
    with open(res_json_path / vehicle_types_that_have_more_that_one_k_lots_and_needs_to_be_reviewed / f'brands_list.json', 'a', encoding='utf-8') as f:
        obj = {
            "brand": brand,
            "type_param": type_param,
            "brand_count": brand_count
        }
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write(",\n")

    try:
        # Тут вже безпечно, бо ми перевірили response_json вище
        content = response_json.get('data', {}).get('results', {}).get('facetFields', [])
        ret_obj = []
        for item in content:
            for field_to_include in arr_of_additional_fields_to_include:
                if item.get('quickPickCode').upper() == field_to_include.upper():
                    query_in_facet_counts = []
                    display_names_in_facet_counts = []
                    number_of_lots_counts = []
                    includeTag = item.get('includeTag')
                    facet_counts = item.get('facetCounts')
                    for facet_count in facet_counts:
                        query_in_facet_counts.append(facet_count.get('query'))
                        display_names_in_facet_counts.append(facet_count.get('displayName'))
                        number_of_lots_counts.append(facet_count.get('count'))
                    ret_obj.append({
                        'brand_upper': brand_upper,
                        'quickPickCode': field_to_include.upper(),
                        'queries': query_in_facet_counts,
                        'display_names': display_names_in_facet_counts,
                        'counts': number_of_lots_counts,
                        'includeTag': includeTag
                    })

        return ret_obj

    except Exception as e:
        print(f"Error get_search_results_without_sloc_query. Error extracting query_and_display_names: {e}")
        save_error({
            'brand': brand,
            'error_type': f"Error get_search_results_without_sloc_query. Error extracting query_and_display_names: {e}"
        })
        return False

def get_search_results_without_sloc_query(restart_page, brand, headers, cookies, type_param, brand_upper, brand_count):
    """
    SHOULD BE USED FOR BRANDS THAT HAVE MORE THAT 1000 LOTS ONLY
    makes one request for specific brand and page but without specifying the MODG To get all the possible MODGs for that brand

    returns:
    - False if no content found (indicating no more pages for this brand)
    - Sale locations (dict with 'queries' and 'display_names' lists if successful)
    """

    brand_description_configs = [brand_upper]
    # now get_brand_description_variants is useless because I already have all the right brands
    # brand_description_configs = get_brand_description_variants(brand_upper)
    # for brand_description_config in brand_description_configs: #to try for all configuration of brand name variants
        #tmp
        # for page in range (restart_page, 1):
        # time.sleep(0.1)
    # print(f"Brand: {brand}, page: {page + 1}")
    start = 0

    # print(f"type param: {type_param}, brand_description_config: {brand_description_config}")
    payload = clean_payload({"query":["*"],"filter":{"VEHT":[f"vehicle_type_code:{type_param}"],"MAKE":[f"lot_make_desc:\"{brand_upper}\""]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":0,"size":100,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"VEHT":"{!tag=VEHT}","MAKE":"{!tag=MAKE}"},"rawParams":{}})

    url = "https://www.copart.com/public/lots/vehicle-finder-search-results"

    # --- FIX: Очищаємо змінні перед запитом ---
    response_json = None
    # ------------------------------------------

    response = safe_post(
        url,
        headers=headers,
        cookies=cookies,
        json=payload,
        timeout=30
    )

    if response.status_code != 200:
        print(f"Error get_search_results_without_sloc_query. Failed to load for {brand}. Status: {response.status_code}")
        return False

    try:
        response_json = response.json()
    except Exception as e:
        print(f"Error get_search_results_without_sloc_query JSON Decode: {e}")
        # Можливо, safe_post повернув HTML. Ми не можемо продовжувати з цією сторінкою.
        return False

    # --- FIX: Перевірка на NoneType перед доступом ---
    if response_json is None:
        print(f"Error get_search_results_without_sloc_query. response_json is None. Skipping.")
        return False
    # -------------------------------------------------

    if response_json.get('data', {}).get('results', {}).get('content', []) == []:
        print(f"Error get_search_results_without_sloc_query. No content for {brand}. Finishing brand.")
        return False

    vehicle_types_that_have_more_that_one_k_lots_and_needs_to_be_reviewed = "vehicle_types_more_than_1k"
    output_dir = res_json_path / vehicle_types_that_have_more_that_one_k_lots_and_needs_to_be_reviewed
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / f'{brand}_{type_param}_without_sloc_query.json', 'w', encoding='utf-8') as f:
        json.dump(response_json, f, ensure_ascii=False, indent=2)
    with open(res_json_path / vehicle_types_that_have_more_that_one_k_lots_and_needs_to_be_reviewed / f'brands_list.json', 'a', encoding='utf-8') as f:
        obj = {
            "brand": brand,
            "type_param": type_param,
            "brand_count": brand_count
        }
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write(",\n")

    try:
        # Тут вже безпечно, бо ми перевірили response_json вище
        content = response_json.get('data', {}).get('results', {}).get('facetFields', [])
        query_and_display_names = None
        for item in content:
            if item.get('quickPickCode') == "MODG":
                query_in_facet_counts = []
                display_names_in_facet_counts = []
                facet_counts = item.get('facetCounts')
                for facet_count in facet_counts:
                    query_in_facet_counts.append(facet_count.get('query'))
                    display_names_in_facet_counts.append(facet_count.get('displayName'))
                query_and_display_names = {
                    'brand_upper': brand_upper,
                    'queries': query_in_facet_counts,
                    'display_names': display_names_in_facet_counts
                }
                return query_and_display_names
    except Exception as e:
        print(f"Error get_search_results_without_sloc_query. Error extracting query_and_display_names: {e}")
        save_error({
            'brand': brand,
            'error_type': f"Error get_search_results_without_sloc_query. Error extracting query_and_display_names: {e}"
        })
        return False

def check_if_brand_has_at_least_one_page(restart_page, brand, headers, cookies, type_param, brand_upper):
    """
    makes one request for specific brand To get all the possible MODGs for that brand

    returns:
    - False if no content found (indicating no more pages for this brand)
    - dict with 'queries' and 'display_names' lists if successful
    """

    brand_description_configs = [brand_upper]
    # now get_brand_description_variants is useless because I already have all the right brands
    # brand_description_configs = get_brand_description_variants(brand_upper)
    for brand_description_config in brand_description_configs: #to try for all configuration of brand name variants
        for page in range (restart_page, 21):
            # time.sleep(0.1)
            print(f"Brand: {brand}, page: {page + 1}")
            start = page * 100

            print(f"type param: {type_param}, brand_description_config: {brand_description_config}")
            payload = clean_payload({"query":["*"],"filter":{"VEHT":[f"vehicle_type_code:{type_param}"],"MAKE":[f"lot_make_desc:\"{brand_description_config}\""]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":page,"size":100,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"VEHT":"{!tag=VEHT}","MAKE":"{!tag=MAKE}"},"rawParams":{}})

            url = "https://www.copart.com/public/lots/vehicle-finder-search-results"

            # --- FIX: Очищаємо змінні перед запитом ---
            response_json = None
            # ------------------------------------------

            response = safe_post(
                url,
                headers=headers,
                cookies=cookies,
                json=payload,
                timeout=30
            )

            if response.status_code != 200:
                print(f"Failed to load page {page + 1} for {brand}. Status: {response.status_code}")
                break

            try:
                response_json = response.json()
            except Exception as e:
                print(f"JSON Decode Error on page {page + 1}: {e}")
                # Можливо, safe_post повернув HTML. Ми не можемо продовжувати з цією сторінкою.
                break

            # --- FIX: Перевірка на NoneType перед доступом ---
            if response_json is None:
                print(f"response_json is None for page {page + 1}. Skipping.")
                break
            # -------------------------------------------------

            if response_json.get('data', {}).get('results', {}).get('content', []) == []:
                print(f"No content for {brand} on page {page+1}. Finishing brand.")
                return False
            else:
                return True

def transfer_brand_data_to_minio(brand_name, type_param):
    """
    Агрегує всі завантажені дані по конкретній марці з res_json
    і зберігає в структуру Minio/category/date/Type_Brand.json
    """
    print(f"\n[Minio Transfer] Starting transfer for {brand_name} (Type: {type_param})...")

    # Нормалізація імен для пошуку файлів
    brand_fs_name = brand_name.replace(" ", "_")
    type_letter = type_param.split('_')[-1]

    current_date = datetime.now().strftime("%Y-%m-%d")
    minio_base = Path("Minio")

    # Категорії, які шукаємо в кінці назв папок/файлів
    categories = ["lots", "photos"]

    for category in categories:
        # 1. Створюємо цільову папку Minio/lots/2026-01-23/
        dest_dir = minio_base / category / current_date
        dest_dir.mkdir(parents=True, exist_ok=True)

        # 2. Формуємо ім'я фінального файлу: V_Alfa_Romeo.json
        final_filename = f"{type_letter}_{brand_fs_name}.json"
        dest_file_path = dest_dir / final_filename

        aggregated_data = []
        found_sources = 0

        # 3. Шукаємо всі папки в res_json, які містять Brand, Type і закінчуються на category
        # Патерн: Alfa_Romeo_V_*_photos (зірочка покриває page1, page2, sloc, engine і т.д.)
        search_pattern = f"{brand_fs_name}_{type_param}_*_{category}"

        # Проходимось по res_json
        for item in res_json_path.glob(search_pattern):
            if item.is_dir():
                # Якщо це папка (наприклад, page1_photos), читаємо всі .json всередині
                # Це окремі файли лотів (12345.json)
                json_files = list(item.glob("*.json"))
                if not json_files:
                    continue

                print(f"  Processing folder: {item.name} ({len(json_files)} files)")

                for json_file in json_files:
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            # Якщо всередині один об'єкт, додаємо його в список
                            if isinstance(data, dict):
                                aggregated_data.append(data)
                            elif isinstance(data, list):
                                aggregated_data.extend(data)
                    except Exception as e:
                        print(f"  Error reading {json_file.name}: {e}")

                found_sources += 1

            # elif item.is_file() and item.suffix == '.json':
            #     # Якщо це файл сторінки (хоча ми перейшли на збереження окремих лотів,
            #     # але на випадок якщо збереглись файли списків)
            #     try:
            #         with open(item, 'r', encoding='utf-8') as f:
            #             data = json.load(f)
            #             if isinstance(data, list):
            #                 aggregated_data.extend(data)
            #             elif isinstance(data, dict):
            #                  # Якщо це структура Copart search results, треба діставати content
            #                  # Але зазвичай сюди потрапляють вже збережені деталі
            #                  aggregated_data.append(data)
            #         found_sources += 1
            #     except Exception as e:
            #         print(f"  Error reading file {item.name}: {e}")

        # 4. Записуємо результат в Minio, якщо є дані
        if aggregated_data:
            # Якщо файл вже існує (наприклад, з попереднього запуску), дочитаємо його і допишемо?
            # Або перезапишемо? Логічніше для "перезбереження" - перезаписати або об'єднати.
            # Тут робимо повний перезапис зібраного за цей сеанс.

            try:
                # [Опціонально] Якщо треба дописувати до існуючого файлу в Minio:
                if dest_file_path.exists():
                     with open(dest_file_path, 'r', encoding='utf-8') as f:
                         existing_data = json.load(f)
                         # Щоб уникнути дублікатів, можна перевіряти по ID, але це повільно.
                         # Просто додаємо нові (припускаємо, що res_json має свіжі дані)
                         existing_data.extend(aggregated_data)
                     aggregated_data = existing_data

                with open(dest_file_path, 'w', encoding='utf-8') as f:
                    json.dump(aggregated_data, f, indent=2, ensure_ascii=False)

                print(f"[Minio Transfer] SUCCESS: Saved {len(aggregated_data)} items to {dest_file_path}")

                upload_to_minio(dest_file_path)
                # [Опціонально] Видалення з res_json після успішного переносу?
                # shutil.rmtree(item) # Небезпечно, краще поки залишити.

            except Exception as e:
                print(f"[Minio Transfer] Error writing to Minio: {e}")
                save_error({'error_type': f"Minio write error for {brand_name}: {e}"})
        else:
            print(f"[Minio Transfer] No data found for category '{category}'")

def download_data_from_pages_of_single_brand_with_vehicle_type_and_brand(search_query, brand, type_param, restart_object):
    """
    makes requests for specific brand and vehicle type
    """
    print(f"download_data_from_pages_of_single_brand_with_vehicle_type_and_brand: {brand}")

    brand_upper = brand.upper()
    brand_with_underscores = brand.replace(" ", "_").replace("/","_")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
    }
    cookies = {}

    if restart_object == None or restart_object == '':
        restart_page = 0
    else:
        restart_page = max(0, restart_object['page'] - 1)

    brand_has_at_least_one_page = check_if_brand_has_at_least_one_page(restart_page, brand, headers, cookies, type_param, brand_upper)

    if not brand_has_at_least_one_page:
        print(f"Skipping {brand} because initial search returned no content.")
        return

    # brand_upper = brand_has_at_least_one_page.get('brand_upper', brand_upper) #becaues if this
    # brand have received response for some configuration of brand name variant,
    # you should use this configuration because it's confirmed to work

    # tmp
    # for page in range (restart_page, 1):
    for page in range (restart_page, 21):
        # time.sleep(0.1)
        # print(f"Brand: {brand}, page: {page + 1}")
        start = page * 100

        payload = clean_payload({"query":["*"],"filter":{"VEHT":[f"vehicle_type_code:{type_param}"],"MAKE":[f"lot_make_desc:\"{brand_upper}\""]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":page,"size":100,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"VEHT":"{!tag=VEHT}","MAKE":"{!tag=MAKE}"},"rawParams":{}})

        url = "https://www.copart.com/public/lots/vehicle-finder-search-results"

        # Очищаємо змінні перед запитом для багатопоточності
        response_json = None

        response = safe_post(
            url,
            headers=headers,
            cookies=cookies,
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            print(f"Failed to load page {page + 1} for {brand}. Status: {response.status_code}")
            continue # Пропускаємо ітерацію, не йдемо вниз

        try:
            response_json = response.json()
        except Exception as e:
            print(f"JSON Decode Error on page {page + 1}: {e}")
            # Можливо, safe_post повернув HTML. Ми не можемо продовжувати з цією сторінкою.
            continue

        # --- FIX: Перевірка на NoneType перед доступом ---
        if response_json is None:
            print(f"response_json is None for page {page + 1}. Skipping.")
            continue
        # -------------------------------------------------

        if response_json.get('data', {}).get('results', {}).get('content', []) == []:
            print(f"No content for {brand} on page {page+1}. Finishing brand.")
            break

        try:
            with open(res_json_path / f'{brand_with_underscores}_{type_param}_page{page + 1}.json', 'w', encoding='utf-8') as f:
                json.dump(response_json, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"File save error: {e}")

        all_ln_values = []
        try:
            # Тут вже безпечно, бо ми перевірили response_json вище
            content = response_json.get('data', {}).get('results', {}).get('content', [])
            for item in content:
                if 'ln' in item:
                    all_ln_values.append(item['ln'])
        except Exception as e:
            print(f"Error extracting ln values on page {page + 1}: {e}")
            continue

        per_page_restart = None
        if restart_object and isinstance(restart_object, dict) and restart_object.get('page') == page:
            per_page_restart = restart_object

        if len(all_ln_values) != 0:
            download_photos_from_lot(brand, page, type_param, all_ln_values, per_page_restart)
            get_lot_details_for_page(brand, page, type_param, all_ln_values, per_page_restart)
        else:
            print(f"No lot numbers found on page {page+1}")

        transfer_brand_data_to_minio(brand, type_param)
        with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
            json.dump({"search_query": search_query, "brand": brand, "page": page + 1, "sloc_query_index": -1, "engine_volume_index": -1, "lot_number": 0}, f)

def download_data_from_pages_of_single_brand_with_vehicle_type_and_brand_and_sloc(search_query, brand, type_param, restart_object, brand_count):
    """
    makes requests for specific brand, vehicle type and MODGs
    """
    print(f"download_data_from_pages_of_single_brand_with_vehicle_type_and_brand_and_sloc: {brand}")

    brand_upper = brand.upper()
    brand_with_underscores = brand.replace(" ", "_").replace("/", "_")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
    }
    cookies = {}

    restart_sloc_index = 0
    restart_engine_index = 0
    restart_page = 0

    if restart_object:
        restart_page = max(0, restart_object.get('page', 1) - 1)
        restart_engine_index = max(0, restart_object.get('engine_volume_index', 0))
        restart_sloc_index = max(0, restart_object.get('sloc_query_index', 0))

    brand_has_at_least_one_page = check_if_brand_has_at_least_one_page(restart_page, brand, headers, cookies, type_param, brand_upper)

    if brand_has_at_least_one_page == False:
        print(f"Skipping {brand} because initial search returned no content.")
        return

    #number of fields in arr_of_additional_fields_to_include should corespond to the number
    # of nested loops below in this function
    # so in that way you will adjust that if there are more than 1000 lots with one filter
    # the nested loop will use the next filter, if not the nested loop will run
    # only once (you should change the 'until' condition in the nested loop
    # to make it run only once)
    arr_of_additional_fields_to_include = ["MODG", "YEAR"]
    # or you can use all filters in each request simultaniously.
    # But it can lead to overdetailed requests

    values_and_filters = get_possible_values_of_filters(brand, headers, cookies, type_param, brand_upper, brand_count, arr_of_additional_fields_to_include)

    if values_and_filters == False:
        print(f"No MODG data found for {brand}")
        return

    sloc_data = None
    engn_data = None

    if isinstance(values_and_filters, list):
        for value_and_filter in values_and_filters:
            if value_and_filter.get('quickPickCode') == "MODG":
                sloc_data = value_and_filter
            if value_and_filter.get('quickPickCode') == "YEAR":
                engn_data = value_and_filter
    elif isinstance(values_and_filters, dict):
        sloc_data = values_and_filters.get('MODG')
        engn_data = values_and_filters.get('YEAR')

    if sloc_data is None:
        print(f"Error. sloc_data is None for {brand}")
        return

    if engn_data is None:
        print(f"Error. engn_data is None for {brand}")

    sloc_queries = sloc_data['queries']
    sloc_display_names = sloc_data['display_names']
    sloc_counts = sloc_data.get('counts', [])
    brand_upper = sloc_data.get('brand_upper', brand.upper())

    if not sloc_queries:
        print(f"No MODG queries found for brand {brand}.")
        return

    for sloc_query_index in range(len(sloc_queries)):
        if sloc_query_index < restart_sloc_index:
            continue

        is_restart_sloc_step = (sloc_query_index == restart_sloc_index)

        current_start_engine = restart_engine_index if is_restart_sloc_step else 0

        if sloc_counts[sloc_query_index] > 1000:
            engn_queries = engn_data['queries']
            engn_display_names = engn_data['display_names']
            engine_volumes_loop = engn_queries
        else:
            engine_volumes_loop = ['']
            engn_display_names = ['all_engines']

        for engine_volume_index in range(len(engine_volumes_loop)): #_loop because it can
            # have only one element if there is no need to make requests with
            # specifying the engine volume (<1000 lots per sloc)

            if engine_volume_index < current_start_engine:
                continue

            is_restart_engine_step = (is_restart_sloc_step and engine_volume_index == current_start_engine)

            current_start_page = restart_page if is_restart_engine_step else 0

            for page in range(current_start_page, 21):

                print(f"\nBrand: {brand}, MODG idx: {sloc_query_index}, Engn idx: {engine_volume_index}, Page: {page + 1}")
                start = page * 100

                #on the website there is no tags at all. But here I can add tag for MODG if its needed
                payload = {
                    "query": ["*"],
                    "filter": {
                        "VEHT": [f"vehicle_type_code:{type_param}"],
                        "MAKE": [f"lot_make_desc:\"{brand_upper}\""],
                        "MODG": [f"{sloc_queries[sloc_query_index]}"]
                    },
                    "sort": ["salelight_priority asc", "member_damage_group_priority asc", "auction_date_type desc", "auction_date_utc asc"],
                    "page": page,
                    "size": 100,
                    "start": start,
                    "watchListOnly": False,
                    "freeFormSearch": False,
                    "hideImages": False,
                    "defaultSort": False,
                    "specificRowProvided": False,
                    "displayName": "",
                    "searchName": "",
                    "backUrl": "",
                    "includeTagByField": {
                        "VEHT": "{!tag=VEHT}",
                        "MAKE": "{!tag=MAKE}",
                        "MODG": "{!tag=MODG}"
                    },
                    "rawParams": {}
                }

                if len(engine_volumes_loop) > 1 and engine_volumes_loop[0] != '':
                    code = "YEAR"
                    query = engn_queries[engine_volume_index] # Тут виправлено .get, бо queries це зазвичай список рядків у твоїй структурі вище? Перевір це.
                    # Якщо engn_queries це список словників, то залиш як було: engn_queries[engine_volume_index].get('query')

                    if query:
                        payload["filter"][code] = [query]
                        payload["includeTagByField"][code] = f"{{!tag={code}}}"

                payload = clean_payload(payload)
                url = "https://www.copart.com/public/lots/vehicle-finder-search-results"

                response = safe_post(url, headers=headers, cookies=cookies, json=payload, timeout=30)

                if response.status_code != 200:
                    print(f"Failed to load page {page + 1}. Status: {response.status_code}")
                    continue

                try:
                    response_json = response.json()
                except Exception as e:
                    print(f"JSON Decode Error: {e}")
                    continue

                if response_json is None or response_json.get('data', {}).get('results', {}).get('content', []) == []:
                    print(f"No content on page {page+1}. Finishing branch.")
                    break

                try:
                    # Безпечне формування назви файлу
                    sloc_name = sloc_display_names[sloc_query_index].replace(" ", "_").replace("/", "-")
                    engn_name = engn_display_names[engine_volume_index].replace(" ", "_").replace("/", "-")

                    file_name = f'{brand_with_underscores}_{type_param}_{sloc_name}_{engn_name}_page{page + 1}.json'
                    with open(res_json_path / file_name, 'w', encoding='utf-8') as f:
                        json.dump(response_json, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    print(f"File save error: {e}")

                all_ln_values = []
                try:
                    content = response_json.get('data', {}).get('results', {}).get('content', [])
                    for item in content:
                        if 'ln' in item:
                            all_ln_values.append(item['ln'])
                except Exception as e:
                    print(f"Error extracting ln: {e}")
                    continue

                # Передаємо restart_object тільки якщо ми на тій самій сторінці, де зупинились
                per_page_restart = None
                if is_restart_engine_step and page == current_start_page:
                    per_page_restart = restart_object

                if len(all_ln_values) != 0:
                    download_photos_from_lot(
                        brand, page, type_param, all_ln_values, per_page_restart,
                        sloc_query_index, sloc_display_names[sloc_query_index],
                        engine_volume_index, engn_display_names[engine_volume_index]
                    )
                    get_lot_details_for_page(
                        brand, page, type_param, all_ln_values, per_page_restart,
                        sloc_query_index, sloc_display_names[sloc_query_index],
                        engine_volume_index, engn_display_names[engine_volume_index]
                    )
                else:
                    print(f"No lot numbers found on page {page+1}")

                transfer_brand_data_to_minio(brand, type_param)
                with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
                    json.dump({
                        "search_query": search_query,
                        "brand": brand,
                        "page": page + 1,
                        "sloc_query_index": sloc_query_index,
                        "engine_volume_index": engine_volume_index,
                        "lot_number": 0
                    }, f, ensure_ascii=False, indent=2)


def download_data_from_pages_of_each_brand(veht_array):
    #goes through brands from tech_json/list_of_automobile_brands.json and for each brand call the
    #download_data_from_pages_of_single_page() func which downloads all 50 pages for single brand that is transmited to it

    if veht_array is None or len(veht_array) == 0:
        print("No vehicle types provided to download_data_from_pages_of_each_brand.")
        save_error({
            'error_type': "No vehicle types provided to download_data_from_pages_of_each_brand."
        })
        return

    # try:
    #     with open(tech_json_path / 'list_of_automobile_brands.json', 'r', encoding='utf-8') as f:
    #         content = json.load(f)
    # except Exception as e:
    #     print(e)
    #     return

    # Завантажуємо доступні типи Copart
    try:
        with open(tech_json_path / 'vehicle_types.json', 'r', encoding='utf-8') as f:
            vehicle_types_data = json.load(f)
    except Exception as e:
        print(f"Error loading vehicle_types.json: {e}")
        return

    restart_search_query = None
    restart_obj = None

    try:
        file_path = tech_json_path / 'restart_point.json'
        if file_path.exists() and file_path.stat().st_size > 0:
            with open(file_path, 'r', encoding='utf-8') as f:
                restart_obj = json.load(f)

                if isinstance(restart_obj, dict):
                    restart_search_query = restart_obj.get('search_query')
    except Exception as e:
        print(f"download_data_from_pages_of_each_brand restart file opening error {e}")
        pass

# Якщо є збережений restart_search_query, вмикаємо режим пропуску (True)
    should_skip = restart_search_query is not None

    for veht_item in veht_array:
        search_query = veht_item.get('searchQuery')
        include_tag_by_field = veht_item.get('includeTagByField')
        number_of_lots = veht_item.get('count')

        print(f"\n>>> Checking vehicle type: {search_query}, should_skip={should_skip}")

        if should_skip:
            if search_query == restart_search_query:
                should_skip = False
                print(f">>> FOUND restart point, will process this type")
            else:
                print(f">>> SKIPPING (not restart point yet)")
                continue

        print(f">>> PROCESSING {search_query} with {number_of_lots} lots")

        # print(f"search_query: {search_query}")
        # print(f"restart_search_query: {restart_search_query}")
        # print()

        current_restart_obj = restart_obj if search_query == restart_search_query else None
        if number_of_lots <= 1000:
            print(f"Processing vehicle type: {search_query} with {number_of_lots} lots.")
            request_with_vehicle_type(search_query, include_tag_by_field, current_restart_obj, False)
            restart_search_query = None
        elif number_of_lots > 1000:
            print(f"search_query: {search_query}")
            response_json = request_with_vehicle_type(search_query, include_tag_by_field, current_restart_obj, True)
            if response_json == False:
                print(f"Error. The func: request_with_vehicle_type with {search_query} returned False instead of response_json.")
                save_error({
                    'search_query': search_query,
                    'error_type': f"Error. The func: request_with_vehicle_type with {search_query} returned False instead of response_json."
                })
                continue
            facet_fields = response_json.get('data', {}).get('results', {}).get('facetFields', [])
            # This finds the FIRST item that matches and stops searching immediately
            make_array = next((item for item in facet_fields if item.get("quickPickCode") == "MAKE"), None)
            if make_array is None:
                print(f"Error. No MAKE facet found for vehicle type: {search_query}. Skipping.")
                save_error({
                    'search_query': search_query,
                    'error_type': f"Error. No MAKE facet found for vehicle type: {search_query}."
                })
                continue
            brand_array = make_array.get('facetCounts', [])

            #tmp to limit only first brand
            # brand_array = brand_array[:1]

            skip_brands = False
            restart_brand_name = None
            print(f"current_restart_obj: {current_restart_obj}")
            if current_restart_obj and current_restart_obj.get('brand'):
                restart_brand_name = current_restart_obj.get('brand')
                skip_brands = True

            #tmp
            # brand_array = brand_array[0:3]
            for brand in brand_array:
                brand_description = brand.get('displayName')

                #tmp for test launch only for bmw. I will work in match with if actual_vehicle_types_list: # actual_vehicle_types_list = actual_vehicle_types_list[:3] in main func
                # if brand_description.upper() != "BMW":
                #     # Можна розкоментувати прінт, щоб бачити що пропускається
                #     # print(f"Skipping {brand_description}, looking for BMW...")
                #     continue

                brand_count = brand.get('count')
                vehtype = search_query.split(":")[1]

                # print(f"brand_description: {brand_description}")
                # print(f"restart_brand_name: {restart_brand_name}")

                passed_restart_obj = None
                if skip_brands:
                    if brand_description.upper() == restart_brand_name.upper():
                        # Знайшли бренд, на якому зупинилися
                        skip_brands = False
                        passed_restart_obj = current_restart_obj # Передаємо рестарт (сторінки, лоти)
                    else:
                        # Це ще не той бренд, пропускаємо
                        continue

                print(f"\nProcessing brand: {brand_description} with {brand_count} lots under vehicle type: {vehtype}.")
                if brand_count <= 1000:
                    # print(f"passed restart 1: {passed_restart_obj}")
                    download_data_from_pages_of_single_brand_with_vehicle_type_and_brand(search_query, brand_description, vehtype, passed_restart_obj)
                    restart_obj = None
                elif brand_count > 1000:
                    # print(f"passed restart 2: {passed_restart_obj}")
                    download_data_from_pages_of_single_brand_with_vehicle_type_and_brand_and_sloc(search_query, brand_description, vehtype, passed_restart_obj, brand_count)
                    restart_obj = None

                current_restart_obj = None

        restart_search_query = None
        restart_obj = None
        try:
            with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
                json.dump({}, f)  # Порожній об'єкт означає "немає рестарту"
        except Exception as e:
            print(f"Warning: Could not clear restart_point.json: {e}")

            # for brand in content:
            #     if skip_brand:
            #         if brand_description == restart_brand:
            #             skip_brand = False
            #             download_data_from_pages_of_single_brand(brand_description, type_param, restart_obj)
            #         continue
            #     else:
            #         download_data_from_pages_of_single_brand(brand_description, type_param, None)

def clean_working_files():
    """Clean all working files and directories AND DROPS DATABASE"""

    drop_database(DB_NAME)

    # 1. Clean JSON files (create empty ones)
    tech_json_path.mkdir(exist_ok=True)
    db_tech_json_path.mkdir(exist_ok=True)
    HTML_downloader.html_results.mkdir(exist_ok=True)

    # Clear JSON files
    files_to_clear = {
        tech_json_path: ['errors.json', 'list_of_automobile_brands.json', 'restart_point.json', 'number_of_vehicle_types_to_skip.json'],
        db_tech_json_path: ['error_list.json', 'last_written_to_db_review.json', 'all_json_names.txt'],
        HTML_downloader.tech_html: ['lots_and_links.json', 'last_state.json']
    }

    for directory, filenames in files_to_clear.items():
        for filename in filenames:
            file_path = directory / filename
            file_path.write_text('', encoding='utf-8')

    # 2. Clean res_json_path directory
    if res_json_path.exists():
        shutil.rmtree(res_json_path)

    directories_to_wipe = [res_json_path, HTML_downloader.html_results, vehtypes_more_than_1k]

    for directory in directories_to_wipe:
        if directory.exists():
            try:
                shutil.rmtree(directory)
            except Exception as e:
                print(f"Error deleting {directory}: {e}")

        # Recreate empty directory
        directory.mkdir(parents=True, exist_ok=True)

    # Recreate empty directory
    res_json_path.mkdir(parents=True, exist_ok=True)
    print(f"Cleaned and recreated directory: {res_json_path}")

def main():
    saved_start_time = save_start_or_finish_time(True)
    if not saved_start_time:
        return
    clean_working_files_bool = False
    if clean_working_files_bool:
        clean_working_files()
    extract_only_automobile = False
    # extract_json_from_list_of_all_brands()
    # extract_vehicle_types()
    # extract_automobile_brands_list(extract_only_automobile) #if True then only vehicles with 'automobile' type will be extracted
                                            # if False then all vehicles types will be extracted
    res_json_path.mkdir(parents=True, exist_ok=True)

    if not refresh_copart_session(headless=True):
        print("Error. Could not initialize the very first session. Exiting.")
        save_error({
            'error_type': "Error. Could not initialize the very first session."
        })
        return

    while True: # to restart if something went wrong
        try:
            number_of_vehicle_types_to_skip = get_number_of_vehicle_types_to_skip()
            while True: # to refresh HOME.json with each next vehicle type
                actual_vehicle_types_list = refresh_home_and_get_actual_vehicle_types_list()

                #tmp to limit only one vehtype (but if you want to limit only vehtype_V it should be 3 and in number..to_skip.json must be 2)
                # if actual_vehicle_types_list:
                #     actual_vehicle_types_list = actual_vehicle_types_list[:3]

                if actual_vehicle_types_list is None:
                    print("Error. Could not get actual vehicle types list from HOME.json. Retrying in 60 sec...")
                    save_error({
                        'error_type': "Error. Could not get actual vehicle types list from HOME.json."
                    })
                    time.sleep(60)
                    continue

                # if we are out of bounse:
                if number_of_vehicle_types_to_skip >= len(actual_vehicle_types_list):
                    print("All vehicle types processed. Resetting counter.")
                    print(f"number_of_vehicle_types_to_skip: {number_of_vehicle_types_to_skip}")
                    print(f"len of actual_vehicle_types_list: {len(actual_vehicle_types_list)}")
                    # number_of_vehicle_types_to_skip = 0
                    # with open(tech_json_path / 'number_of_vehicle_types_to_skip.json', 'w', encoding='utf-8') as f:
                    #     json.dump({"number_of_vehicle_types_to_skip": 0}, f)
                    break

                current_vehicle_type_batch = [actual_vehicle_types_list[number_of_vehicle_types_to_skip]]
                download_data_from_pages_of_each_brand(current_vehicle_type_batch)
                number_of_vehicle_types_to_skip += 1

                with open(tech_json_path / 'number_of_vehicle_types_to_skip.json', 'w', encoding='utf-8') as f:
                    json.dump({"number_of_vehicle_types_to_skip": number_of_vehicle_types_to_skip}, f, indent=2)
            break
        except (requests.exceptions.ConnectionError, RuntimeError, Exception) as e:
            print(f"Critial error or Network error: {e}")
            print("Restarting in 60 sec...")
            save_error({
                'error_type': f"Critial error or Network error: {e}"
            })
            time.sleep(60)

    #launch database writing
    refresh_table_index()
    table_index = get_table_index()
    table_name = f"copart_lots_{table_index}"
    db_main(DB_NAME, table_name, res_json_path, table_index)
    save_start_or_finish_time(False)

    # if you wont to download html pages with photos uncomment the line below
    # and fix tudu at the start of this file
    # HTML_downloader.download_all()

if __name__ == '__main__':
    main()
