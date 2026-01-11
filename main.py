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

tech_json_path = Path('tech_json')
res_json_path = Path('res_json')
db_tech_json_path = Path('db_tech_json')
SESSION = requests.Session()
DB_NAME = 'copart_lots_test'
POST_COUNT = 0
POST_LIMITER = 105  # Number of POST requests before refreshing
#session (it includes pages and photos requests, so one full page = 1 page reques + 20 photos requests = 21 POST requests per full page)

SESSION_LOCK = threading.Lock()

# Global Session Object
# This acts as the "bridge" between the token extractor and safe_post.
SESSION = requests.Session()

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
    Helper function to update the global SESSION object.
    Call this ONCE at the start of your program.
    """
    print("taking cookies and headers")
    session_data = get_copart_session_data(headless=headless)
    if session_data:
        SESSION.headers.update(session_data['headers'])
        SESSION.cookies.update(session_data['cookies'])
        return True
    return False

def safe_post(url, **kwargs):
    global POST_COUNT
    global POST_LIMITER

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
            response = SESSION.post(url, **kwargs)

            # Якщо успіх (200) - перевіряємо, чи це дійсно JSON, а не сторінка блокування Cloudflare
            if response.status_code == 200:
                # Copart іноді віддає 200 OK, але всередині HTML з капчею.
                # Спробуємо перевірити content-type або просто повернемо, а process_single_lot розбереться
                if "application/json" in response.headers.get("Content-Type", ""):
                    return response

                # Якщо це не JSON, можливо нас блокують, але поки повернемо як є.
                # (Але якщо це Cloudflare, наступний код впаде, тому див. нижче)
                return response

            # Якщо помилка 403 (Forbidden) або 429 (Too Many Requests) або 503
            if response.status_code in [403, 429, 503]:
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

    url = "https://www.copart.com/public/data/lotdetails/solr/lot-images/"
    payload = {"lotNumber": number}

    # safe_post тепер сам спробує оновитись, якщо отримає 403
    r = safe_post(url, json=payload)

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

def process_single_lot(brand, page, type_param, number, sloc_display_name):
    # Випадкова затримка
    time.sleep(random.uniform(0.5, 2.0))
    brand_with_underscores = brand.replace(" ", "_").replace("/","_")

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'Origin': 'https://www.copart.com',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
    }

    url = "https://www.copart.com/public/data/lotdetails/solr/lot-images/"
    payload = {"lotNumber": number}

    # safe_post тепер сам спробує оновитись, якщо отримає 403
    r = safe_post(url, json=payload)

    if r.status_code != 200:
        # print(f"Error {r.status_code} for lot {number}")
        return

    try:
        data = r.json()

        target_dir = res_json_path / f"{brand_with_underscores}_{type_param}_{sloc_display_name}_page{page + 1}_photos"
        target_dir.mkdir(parents=True, exist_ok=True)

        with open(target_dir / f"{number}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    except Exception as e:
        # Якщо ми тут, значить safe_post повернув 200 OK, але це НЕ JSON.
        # Це 100% блок від Cloudflare. Треба оновлюватись.
        print(f"JSON Error for lot {number} (Likely soft-block). Triggering refresh...")
        with SESSION_LOCK:
             # Перевіряємо, може хтось вже оновив поки ми спали
             refresh_copart_session()

def download_photos_from_lot(brand, page, type_param, arr_of_lot_numbers, restart_object, sloc_query_index = -1, sloc_display_name = None):
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
            futures.append(executor.submit(process_single_lot, brand, page, type_param, number, sloc_display_name))

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
            'lot_number': 0
        }
        json.dump(restart_point, f, indent=2, ensure_ascii=False)

def refresh_home_and_get_actual_vehicle_types_list():
    #here I will download HOME.json and then save it pretty formatted

    home_content = {} #to make it pretty json format, not one line
    with open (Path("tech_json/HOME.json"), "r", encoding="utf-8") as f:
        home_content = json.load(f)

    with open (Path("tech_json/HOME.json"), "w", encoding="utf-8") as f:
        json.dump(home_content, f, ensure_ascii=False, indent=2)

    veht_array = []
    try:
        with open(tech_json_path / 'HOME.json', 'r', encoding='utf-8') as f:
            home_full = json.load(f)
            if home_full:
                veht_array = home_full.get('data', {}).get('quickPicks', {}).get('VEHT', [])
                if not veht_array or veht_array == []:
                    print("VEHT array is empty in HOME.json")
                    save_error({
                        'error_type': "VEHT array is empty in HOME.json"
                    })
                    return None
                else:
                    return veht_array
    except Exception as e:
        print(e)
        return None


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
        else:
            print(f"No lot numbers found on page {page+1}")

        with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
            json.dump({"search_query": search_query, "brand": None, "page": page + 1, "lot_number": 0}, f)


def get_search_results_without_sloc_query(restart_page, brand, headers, cookies, type_param, brand_upper):
    """
    SHOULD BE USED FOR BRANDS THAT HAVE MORE THAT 1000 LOTS ONLY
    makes one request for specific brand and page but without specifying the SLOC To get all the possible SLOCs for that brand

    returns:
    - False if no content found (indicating no more pages for this brand)
    - Sale locations (dict with 'queries' and 'display_names' lists if successful)
    """

    brand_description_configs = [brand_upper]
    # now get_brand_description_variants is useless because I already have all the right brands
    # brand_description_configs = get_brand_description_variants(brand_upper)
    for brand_description_config in brand_description_configs: #to try for all configuration of brand name variants
        #tmp
        # for page in range (restart_page, 1):
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

            with open(res_json_path / f'{brand}_{type_param}_page{page + 1}_without_sloc_query.json', 'w', encoding='utf-8') as f:
                json.dump(response_json, f, ensure_ascii=False, indent=2)

            try:
                # Тут вже безпечно, бо ми перевірили response_json вище
                content = response_json.get('data', {}).get('results', {}).get('facetFields', [])
                query_and_display_names = []
                for item in content:
                    if 'quickPickCode' in item == "SLOC":
                        query_in_facet_counts = None
                        display_names_in_facet_counts = []
                        facet_counts = item.get('facetCounts')
                        for facet_count in facet_counts:
                            query_in_facet_counts.append(facet_count.get('query'))
                            display_names_in_facet_counts.append(facet_count.get('displayName'))
                        query_and_display_names = {
                            'brand_upper': brand_description_config,
                            'queries': query_in_facet_counts,
                            'display_names': display_names_in_facet_counts
                        }
                        return query_and_display_names
            except Exception as e:
                print(f"Error extracting query_and_display_names: {e}")
                save_error({
                    'brand': brand,
                    'page': page,
                    'error_type': f"Error extracting query_and_display_names: {e}"
                })
                break

def check_if_brand_has_at_least_one_page(restart_page, brand, headers, cookies, type_param, brand_upper):
    """
    makes one request for specific brand To get all the possible SLOCs for that brand

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

def download_data_from_pages_of_single_brand_with_vehicle_type_and_brand(search_query, brand, type_param, restart_object):
    """
    makes requests for specific brand and vehicle type
    """
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
        else:
            print(f"No lot numbers found on page {page+1}")

        with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
            json.dump({"search_query": search_query, "brand": brand, "page": page + 1, 'sloc_query_index': -1, "lot_number": 0}, f)

def download_data_from_pages_of_single_brand_with_vehicle_type_and_brand_and_sloc(brand, type_param, restart_object):
    """
    makes requests for specific brand, vehicle type and SLOCs
    """
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

    brand_has_at_least_one_page = check_if_brand_has_at_least_one_page(restart_page, brand, headers, cookies, type_param, brand_upper)

    if not brand_has_at_least_one_page:
        print(f"Skipping {brand} because initial search returned no content.")
        return

    # brand_upper = brand_has_at_least_one_page.get('brand_upper', brand_upper) #becaues if this
    # brand have received response for some configuration of brand name variant,
    # you should use this configuration because it's confirmed to work

    sloc_queries = get_search_results_without_sloc_query(restart_page, brand, headers, cookies, type_param, brand_upper)
    sloc_display_names = None
    if brand_has_at_least_one_page == False:
        return
    elif brand_has_at_least_one_page != None:
        sloc_queries = brand_has_at_least_one_page['queries']
        sloc_display_names = brand_has_at_least_one_page['display_names']
    else:
        print(f"Error getting SLOC queries for brand {brand}. Skipping and moving to next brand.")
        save_error({
            'brand': brand,
            'error_type': "Error getting SLOC queries."
        })
        return

    if sloc_queries == None or len(sloc_queries) == 0:
        print(f"No SLOC queries found for brand {brand}. Skipping and moving to next brand.")
        save_error({
            'brand': brand,
            'error_type': "No SLOC queries found."
        })
        return

    for sloc_query_index in range(len(sloc_queries)):
        # tmp
        # for page in range (restart_page, 1):
        for page in range (restart_page, 21):
            # time.sleep(0.1)
            print(f"Brand: {brand}, page: {page + 1}")
            start = page * 100

            #on the website there is no tags at all. But here I can add tag for SLOC if its needed
            payload = clean_payload({"query":["*"],"filter":{"VEHT":[f"vehicle_type_code:{type_param}"],"MAKE":[f"lot_make_desc:\"{brand_upper}\""],"SLOC":[f"{sloc_queries[sloc_query_index]}"]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":page,"size":100,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"VEHT":"{!tag=VEHT}","MAKE":"{!tag=MAKE}","SLOC":"{!tag=SLOC}"},"rawParams":{}})

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
                with open(res_json_path / f'{brand_with_underscores}_{type_param}_{sloc_display_names[sloc_query_index]}_page{page + 1}.json', 'w', encoding='utf-8') as f:
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
                download_photos_from_lot(brand, page, type_param, all_ln_values, per_page_restart, sloc_query_index, sloc_display_names[sloc_query_index])
            else:
                print(f"No lot numbers found on page {page+1}")

            with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
                json.dump({"brand": brand, "page": page + 1, 'sloc_query_index': sloc_query_index, "lot_number": 0}, f)


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
                brand_count = brand.get('count')
                vehtype = search_query.split(":")[1]

                # print(f"brand_description: {brand_description}")
                # print(f"restart_brand_name: {restart_brand_name}")

                passed_restart_obj = None
                if skip_brands:
                    if brand_description.upper() == restart_brand_name.upper():
                        print("\n\nskipping\n\n")
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
                    download_data_from_pages_of_single_brand_with_vehicle_type_and_brand_and_sloc(brand_description, vehtype, passed_restart_obj)
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

    directories_to_wipe = [res_json_path, HTML_downloader.html_results]

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
    extract_json_from_list_of_all_brands()
    extract_vehicle_types()
    extract_automobile_brands_list(extract_only_automobile) #if True then only vehicles with 'automobile' type will be extracted
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
    HTML_downloader.download_all()

if __name__ == '__main__':
    main()
