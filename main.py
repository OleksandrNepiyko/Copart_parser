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

#TODO question about priority of vehicles, now there is no priority

#TODO data_from_js.json should be saved dynamicaly from the site
#TODO ти не парсиш ще два розділи: Salvage vehicles  і Used vehicles. Треба і їх додати
#TODO make smth with bans 

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

tech_json_path = Path('tech_json')
res_json_path = Path('res_json')
db_tech_json_path = Path('db_tech_json')
SESSION = requests.Session()
DB_NAME = 'copart_lots_test'
POST_COUNT = 0
POST_LIMITER = 105  # Number of POST requests before refreshing
#session (it includes pages and photos requests, so one full page = 1 page reques + 20 photos requests = 21 POST requests per full page)

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

    if POST_COUNT >= POST_LIMITER:
        POST_COUNT = 0
    
    if POST_COUNT == 0:
        if not refresh_copart_session():
            raise RuntimeError("Failed to refresh session.")
            
    POST_COUNT += 1

    # 2. Perform the request
    for attempt in range(5):
        try:
            return SESSION.post(url, **kwargs)
        except requests.exceptions.ConnectionError:
            print(f"Connection error, retry {attempt+1}/5")
            time.sleep(5)
        except Exception as e:
             print(f"Request error: {e}")
             break

    raise RuntimeError("Network failed after 5 retries")

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

    #tmp
    # automobile_brands_list = automobile_brands_list[:50]

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


def download_photos_from_lot(brand, page, type_param, arr_of_lot_numbers, restart_object):
    #goes through arr of lot numbers that is provided and downloads photos links
    print(f"Download_photos_for_lot: {arr_of_lot_numbers}")
    brand_with_underscores = brand.replace(" ", "_").replace("/","_")
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'Origin': 'https://www.copart.com',
        'Referer': 'https://www.copart.com/lot/92156325/clean-title-2024-toyota-rav4-xse-la-baton-rouge',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
        'x-xsrf-token': 'd2ab64f8-f4b4-4375-b62f-046feffc7b07'
    }
    # cookies = {
    #     "anonymousCrmId": "7f62f400-a31c-40a1-9ec6-3882600d94af",
    #     "nlbi_242093": "GtLwHyNVDAsWMox4ie/jegAAAAAJbMaLBXI7o0sn1hVYNNVy",
    #     "_gcl_au": "1.1.135331524.1763655305",
    #     "userCategory": "RPU",
    #     "timezone": "Europe%2FKiev",
    #     "googtrans": "/en/ru",
    #     "visid_incap_242093": "l4BVG1w4S8yiXVL+fdF8idscHmkAAAAAREIPAAAAAACAYJHAAbmSeb5ni73/A+8E5nZBukKZqgd5",
    #     "OptanonAlertBoxClosed": "2025-11-24T13:12:10.554Z",
    #     "__eoi": "ID=3643d9cf80e64b0b:T=1763678384:RT=1764066291:S=AA-AfjbApzZYR5-V4goE6TvOXwWQ",
    #     "userLangChanged_CPRTUS": "true",
    #     "userLang": "ru",
    #     "incap_ses_519_242093": "dcXgbcfvwEzIqrtEJNwzB/R2NmkAAAAADGSYNxs20bLHXesgDl0c4w==",
    #     "incap_ses_108_242093": "CpdrAjc1yGqm5cdl3bF/Adl/NmkAAAAA0btxx6LjxwHSDd2l4UvMrg==",
    #     "incap_ses_788_242093": "yEHEXtUmaWtcu0wu/YnvCsHfNmkAAAAAi/vfWRkPK6v/hsM73clJ7w==",
    #     "incap_ses_255_242093": "ZZUvMzBrP3BSDx/pr/GJAyXUN2kAAAAAer74fUKmMCdKyvVxk139hg==",
    #     "incap_ses_687_242093": "0Z0wdVeKAhyyOxQMTLeICQbxN2kAAAAAKGBK/MrWSq2wwreDKbdFlg==",
    #     "g2usersessionid": "2763b116147121b40f878f6069f35fe2",
    #     "g2app.search-table-rows": "20",
    #     "usersessionid": "054dd1c973a5d0a46e2164cc622ffe17",
    #     "G2JSESSIONID": "3D552CD3BE64314EF22D94E21CDE962D-n1",
    #     "incap_ses_686_242093": "rMllJWSiaQhfXZRwzSmFCYO1OWkAAAAANrgBCParNcfeoq4eyJdb+Q==",
    #     "reese84": "3:yJzc+ilHwkSymjIG6YlouA==:AH39SW66JSv/5sFOWuqLZApn2VSiMWRh8IEIEgy8yrR2b809t2WRqmZKznvWQJe08w7mG53wS0IKwV3EEdEujfAbUSFiM5UDbXKTu+b1QZ/TY18fioNIloYKqnKCqHsksH32mEnJgZCZippUhRK1IHNWDcOatO64qS6XhB8M8h08+CR/AftMqtQbCz+az1lQ55yTjF7c6DLHjqc40bCZxD+BRecuyvMc4Qmtw+tSMQNOlN4t7I6ydUppzjC9KFy03tSfXRaAtUc1bifWIyT6j7r3ZtLQk9UgEg3wtJQw1lEpLY5F67+rTmL83F6TChLGmtlOzXOmfPaU0wkbbaOzLoFZ/67hHQdXNbyYKWnIsiEYlRm41ub8DxcJWpq/l2I+D84wwtVz1PfG3YCquJivI6WivEWyAZ4tYIJdQpiCb5De+9DBoJ0YSMrRhqM6D2l2Uhv3D0k3dtNHW3wyBr5hGA==:xDGzO0tTCrWetTWoQvpchKbkgEEm/9kVEAHDoxLb9RM=",
    #     "FCNEC": "%5B%5B%22AKsRol820AzPw9CCLzQ38o4eycoiceZdq8TzanJxJRXKY5GGGOY__3qTer-_mGdnVYHwbn3W5pODlfjdn-EPer7ptuJORB5dvERgWgLKuhqR0rt-wliKCMVdiN6XB1nQ40VPyRuc_0liDnYHISmWY_XyjNOoyt75lw%3D%3D%22%5D%5D",
    #     "OptanonConsent": "isGpcEnabled=0&datestamp=Wed+Dec+10+2025+20%3A02%3A48+GMT%2B0200+(Eastern+European+Standard+Time)&version=202510.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=8dda50fe-1f46-4974-b215-351ce89ca87c&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0%2CC0005%3A0&AwaitingReconsent=false&intType=3&geolocation=UA%3B05",
    #     "nlbi_242093_2147483392": "Wc8wJGxi/m062mwBie/jegAAAACh2TegyUfRmPhkfWXbyH/z",
    #     "copartTimezonePref": "%7B%22displayStr%22%3A%22GMT%2B2%22%2C%22offset%22%3A2%2C%22dst%22%3Afalse%2C%22windowsTz%22%3Anull%7D",
    #     "lhnStorageType": "cookie",
    #     "_uetsid": "6965b820d5d111f08abd956bdcf7cd89",
    #     "_uetvid": "13491b80c62c11f08525d5471aa25869",
    #     "FCCDCF": "%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%2248c5e301-e5b2-463e-bfe9-3992203a711b%5C%22%2C%5B1763655305%2C452000000%5D%5D%22%5D%5D%5D"
    # }

    cookies = {
        "anonymousCrmId": "a1f7ba15-f8bc-4767-a015-ff9451e3a8f3",
        "userCategory": "RPU",
        "visid_incap_242093": "l4BVG1w4S8yiXVL+fdF8idscHmkAAAAAQkIPAAAAAACAT4XAAbmSeb51M8Y/FbD51Q9/o5KtkDHe",
        "g2usersessionid": "2763b116147121b40f878f6069f35fe2",
        "G2JSESSIONID": "313694319C269FD1CEEE37099DA9BDD3-n1",
        "userLang": "en",
        "nlbi_242093": "UovxHpRulCtpyl3Nie/jegAAAAAt466XoZiQTIWi0ovgSAdU",
        "incap_ses_325_242093": "zi8xJURInBvPeWD/RKKCBATpUmkAAAAA4uR/0XTlvPtYpTuO8Wi3Eg==",
        "timezone": "Europe%2FKiev",
        "reese84": "3:TD1VVR4cjsNCM3iBpWxDZg==:UIC0zy3Igfbhgh4Zd3GbZeRVp1GbNHPAORuKi0ggPbkkfP4Moxymm7DN7RrEtB86GQb0YXOtzfPkkTg8Z4DnINRtR5azKZLpr4YEvgC2g8ju5gCMipf+sTgb98eNCVHAk+1JPHQe7UkCiW0cIUu1DKUSeHp29o7szaEiqTTsEg8dpU/+Oifk+hwb5ETmanCNCbZcmZXDOpYAnPOIujOauxsfs6oql4XlyfgPHppwZF/lhulPSGuCqJHKnrAYljvlPuIii3AZyxiicczlKgcRqWbrpDOneQ+5DRDCacYYk7FRNLtEhhoqulMTf9IDLtvfa8HnP0egM9LTsyc421xHc160KhqkNFQot/LqtUzkGMKpF3aFeSXdwH3Xi0dOpTmnzN9nH5l+kg6L3HJEmr9Xa1Fs2ai4NNKFYAsaCJmwJ4MRJgW6hCCBCF0WXZgMK7tWAf0GeRWnyUNQvJqUT2c8qg==:k8OTfdE7YWAJwRQwzeuEw3hLlRX0Rc2IlsSb3FNGkgM=",
        "FCCDCF": "%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%224f0bb0dc-4cca-42ab-b226-de8b2b20a0e2%5C%22%2C%5B1763581155%2C60000000%5D%5D%22%5D%5D%5D",
        "FCNEC": "%5B%5B%22AKsRol_rt591JzshIGYsUVgpm1rOD55pg8EXa8u2dEM0_oKzb9njjGD9pBsYMWyBMvwdnwxsbcetFJ09lLI-ihvRjVe5Npf7CDr5XujL8ejGmU_J7zkFdYt_U5WO4LYJsYpvYC3OZ2XV3qVtYbbsGrNDkXKpDgWWuA%3D%3D%22%5D%5D",
        "usersessionid": "054dd1c973a5d0a46e2164cc622ffe17",
        "g2app.search-table-rows": "20",
        "nlbi_242093_2147483392": "2qIxJJHJAkZ8UPZBie/jegAAAACT0JxrW3zHjSLWB3+Ld+wh",
        "OptanonConsent": "isGpcEnabled=0&datestamp=Mon+Dec+29+2025+22%3A50%3A26+GMT%2B0200+(Eastern+European+Standard+Time)&version=202510.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=caa4939f-136d-4205-8de3-a787b51ac75d&interactionCount=0&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0%2CC0005%3A0&AwaitingReconsent=false",
        "copartTimezonePref": "%7B%22displayStr%22%3A%22GMT%2B2%22%2C%22offset%22%3A2%2C%22dst%22%3Afalse%2C%22windowsTz%22%3Anull%7D"
    }

    url = "https://www.copart.com/public/data/lotdetails/solr/lot-images/"

    if len(arr_of_lot_numbers)<=0:
        print(f"arr_of_lot_numbers<=0 for {brand} page {page + 1}")
        save_error({
                'brand': brand,
                'page': page + 1,                    
                'error_type': "arr_of_lot_numbers<=0"
            })
    
    if restart_object == None or restart_object == '':
        restart_lot_number = 0
    else:
        restart_lot_number = restart_object['lot_number']

    skip = restart_lot_number != 0

    #tmp
    # arr_of_lot_numbers = arr_of_lot_numbers[:1]

    for number in arr_of_lot_numbers:
        if skip:
            if number == restart_lot_number:
                skip = False 
            else:
                continue
        time.sleep(1)
        payload = {"lotNumber": number}

        # r = session.post(url, headers=headers, cookies=cookies, json=payload)
        r = safe_post(url, headers=headers, cookies=cookies, json=payload)

        # print(number)
        # print("STATUS:", r.status_code)

        if r.status_code != 200:
            print(f"Error to get photos for brand: {brand} page: {page + 1} number of lot: {number}")
            save_error({
                    'brand': brand,
                    'page': page + 1,
                    'number': number,
                    'error_type': "Error to get photos"
                })
            return

        try:
            data = r.json()
        except Exception as e:
            print(f"Exception in r.json() in photos : {e}")
            # print(r.json())
            print(r.text)
            save_error({
                'brand': brand,
                'page': page + 1,
                'lot_number': number,
                'error_type': f"Exception in r.json() in photos {e}"
            })
            
        # (res_json_path / f"{brand_with_underscores}_{type_param}_page{page + 1}_photos").mkdir(exist_ok=True)
        # Додаємо parents=True, щоб воно створило і папку res_json, якщо її немає
        (res_json_path / f"{brand_with_underscores}_{type_param}_page{page + 1}_photos").mkdir(parents=True, exist_ok=True)

        with open(res_json_path / f"{brand_with_underscores}_{type_param}_page{page + 1}_photos" / f"{number}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        with open(tech_json_path /'restart_point.json', 'w', encoding='utf-8') as f:
            restart_point = {
                'brand': brand,
                'page': page + 1,
                'lot_number': number
            }
            json.dump(restart_point, f, indent=2, ensure_ascii=False)


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


def download_data_from_pages_of_single_brand(brand, type_param, restart_object):
    #for transmited brand goes through all 50 pages and downloads data about each lot on page (? does all data is available on this page or 
    # it's needed to open each lot (you will open each lot because you need photos for this lot))
    
    print(f"download_data_from_pages_of_single_brand: {brand}")

    brand_upper = brand.upper()
    brand_with_underscores = brand.replace(" ", "_").replace("/","_")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36',
    }

    cookies = {
        # "g2usersessionid": "02db60a883b957d26afa942ef1644a63",
        # "G2JSESSIONID": "6E41C900EC34DC08CBD4B5DD1769A49C-n1",
        # "usersessionid": "8b7a8968a7dfa001c484e404e5df0266",
        # "visid_incap_242093": "l4BVG1w4S8yiXVL+fdF8idscHmkAAAAAQ0IPAAAAAACAT4XAAbmSeb6WO/lBY1ffSzbZ7FHDHQ8l",
        # "incap_ses_788_242093": "nc54ZDSfuHERwzHP+onvCkFGH2kAAAAAg+ILLZmelDQeaBW3mIt/Vw==",
        # "reese84": "3:wfeLqHpa1LHnkFPxKBd+CQ==:MvQBcXVmBVZudOu3Pom+vVTolvQL3qduhSO04LVpEvaBj+geu8zcQ6TpyC..."
    }

    if restart_object == None or restart_object == '':
        restart_page = 0
    else:
        restart_page = max(0, restart_object['page'] - 1)
#tmp
    # for page in range (restart_page, 1):
    for page in range (restart_page, 51):
        time.sleep(0.1)
        print(f"Brand: {brand}, page: {page + 1}")
        start = page * 20
        #change page and start = page * 20;   pages starts from 0  i.e. page 2: "page":2,"size":20,"start":40
        payload = clean_payload({"query":["*"],"filter":{"VEHT":[f"vehicle_type_code:VEHTYPE_{type_param}"],"MAKE":[f"lot_make_desc:\"{brand_upper}\""]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":page,"size":20,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"VEHT":"{!tag=VEHT}","MAKE":"{!tag=MAKE}"},"rawParams":{}})
        #списание тобто salvage
        # payload = clean_payload({"query":["*"],"filter":{"VEHT":["vehicle_type_code:VEHTYPE_V"],            "MISC":["member_lot_condition:SALVAGE"]},    "sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":0,"size":20,"start":0,       "watchListOnly":false,"freeFormSearch":false,"hideImages":false,"defaultSort":false,"specificRowProvided":false,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"VEHT":"{!tag=VEHT}"},"rawParams":{}})
        # print(f"Payload for brand {brand} page {page + 1}: {payload}")
        # print(f"payload: {payload}")

        #old but working version of payload below
        # payload = clean_payload({"query":["*"],"filter":{"MAKE":[f"lot_make_desc:\"{brand_upper}\""]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":page,"size":20,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"MAKE":"{!tag=MAKE}"},"rawParams":{}})
        # print(f"Payload 2 for brand {brand} page {page + 1}: {payload}")

        # payload = clean_payload({"query":["*"],"filter":{"ODM":["odometer_reading_received:[0 TO 9999999]"],"YEAR":["lot_year:[2015 TO 2026]"],"MISC":["#VehicleTypeCode:VEHTYPE_V","#MakeCode:ALFA OR #MakeDesc:Alfa Romeo"]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":0,"size":20,"start":0,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{},"rawParams":{}})
        
        # this one site sends to load list of models in the bottom:    
        # payload = clean_payload({"query":["*"],"filter":{"MAKE":["lot_make_desc:\"ALFA ROMEO\""]},"sort":["auction_date_type desc","auction_date_utc asc"],"page":0,"size":20,"start":0,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{},"rawParams":{}})

        url = "https://www.copart.com/public/lots/vehicle-finder-search-results"

        response = safe_post(
            url,
            headers=headers, 
            cookies=cookies,
            json=payload,
            timeout=30
        )
        # response.raise_for_status() #it's not needed because it will crush the program and my handling 
        # below won't be reached 

        if response.status_code != 200:
            print(f"Downloading from single page failed for brand: {brand} at page: {page + 1}")
            save_error({
                    'brand': brand,
                    'page': page + 1,
                    'error_type': "Downloading from single page failed"
                })

        try:
            response_json = response.json()

            if response_json.get('data', {}).get('results', {}).get('content', []) == []:
                break
            
            with open(res_json_path / f'{brand_with_underscores}_{type_param}_page{page + 1}.json', 'w', encoding='utf-8') as f:
                json.dump(response_json, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"download_data_from_pages_of_single_brand for {brand} page: {page + 1} exception: {e}")
            save_error({
                'brand': brand,
                'page': page + 1,                    
                # 'error_type': "Failed to get json in response download_data_from_pages_of_single_brand"
                'error_type': f"download_data_from_pages_of_single_brand() for {brand} page: {page + 1} exception: {e}"
            })

        all_ln_values = []
        try:
            content = response_json.get('data', {}).get('results', {}).get('content', [])
            if len(content) == 0:
                break
            for item in content:
                if 'ln' in item:
                    all_ln_values.append(item['ln'])
        except Exception as e:
            print(f"Error extracting ln values on page {page + 1}: {e}")
            # save_error({ #to avoid duplicates (it is already saved in the except block above)
            #     'page': page + 1,
            #     'error_type': str(e)
            # })
        per_page_restart = None
        if restart_object and isinstance(restart_object, dict) and restart_object.get('page') == page:
            per_page_restart = restart_object
        if len(all_ln_values) != 0:
            download_photos_from_lot(brand, page, type_param, all_ln_values, per_page_restart)

        with open(tech_json_path / 'restart_point.json', 'w', encoding='utf-8') as f:
            json.dump({"brand": brand, "page": page + 1, "lot_number": 0}, f)
        

def download_data_from_pages_of_each_brand():
    #goes through brands from tech_json/list_of_automobile_brands.json and for each brand call the 
    #download_data_from_pages_of_single_page() func which downloads all 50 pages for single brand that is transmited to it
    try:
        with open(tech_json_path / 'list_of_automobile_brands.json', 'r', encoding='utf-8') as f:
            content = json.load(f)
    except Exception as e:
        print(e)
        return
    
    # Завантажуємо доступні типи Copart
    try:
        with open(tech_json_path / 'vehicle_types.json', 'r', encoding='utf-8') as f:
            vehicle_types_data = json.load(f)
    except Exception as e:
        print(f"Error loading vehicle_types.json: {e}")
        return

    restart_brand = None
    restart_obj = None

    try:
        file_path = tech_json_path / 'restart_point.json'
        if file_path.exists() and file_path.stat().st_size > 0:
            with open(file_path, 'r', encoding='utf-8') as f:
                restart_obj = json.load(f)
                
                # 2. Check if it's a dictionary and use .get() to avoid KeyError
                if isinstance(restart_obj, dict):
                    restart_brand = restart_obj.get('brand')
    except Exception as e:
        print(f"download_data_from_pages_of_each_brand restart file opening error {e}")
        pass

    skip = restart_brand is not None
    for brand in content:
        brand_description = brand['description']
        type_param = brand['type']
        
        # try:
        #     with open(tech_json_path /'vehicle_types.json', 'r', encoding='utf-8') as f:
        #         vehicle_types = json.load(f)
        #         for vt in vehicle_types:
        #             print(f"Comparing {vt['vehicle_type_description']} with {type_param}")
        #             if vt['vehicle_type_description'] == type_param:
        #                 type_param = vt['vehicle_type_code']
        #                 break
        # except Exception as e:
        #     print(f"download_data_from_pages_of_each_brand vehicle types file opening error {e}")
        #     save_error({
        #         'brand': brand_description,
        #         'error_type': f"vehicle types file opening error {e}"
        #     })
        #     type_param = None

        raw_type_from_file = brand.get('type') 

        # 2. Знаходимо код (V, C, M тощо) у vehicle_types.json
        type_param = None
        
        for vt in vehicle_types_data:
            # Порівнюємо без урахування регістру для надійності
            if vt['code'].strip().lower() == raw_type_from_file.strip().lower():
                type_param = vt['groupCode']
                break
        
        # Якщо код не знайдено, за замовчуванням ставимо 'V' (Automobiles) або пропускаємо
        if type_param is None:
            print(f"Warning: Could not map type '{raw_type_from_file}' for brand '{brand_description}'. Defaulting to 'V' (Automobiles).")
            try:
                # vehicle_types_path = Path(tech_json_path/'vehicle_type_problems.json')
                # vehicle_types_path.mkdir(exist_ok=True)
                with open (tech_json_path / 'vehicle_type_problems.json', 'a', encoding='utf-8') as f:
                    json.dump({
                        'raw_type_from_file': raw_type_from_file,
                        'brand_description': brand_description
                    }, f, indent=2, ensure_ascii=False)
                    f.write(',\n')
            except Exception as e:
                print(f"type param error: {e} for brand: {brand}")
                save_error({
                    'error_type': f"type param error: {e} for brand: {brand}"
                })
            type_param = "V"

        if skip:
            if brand_description == restart_brand:
                skip = False
                download_data_from_pages_of_single_brand(brand_description, type_param, restart_obj)
            continue
        else:
            download_data_from_pages_of_single_brand(brand_description, type_param, None)

def clean_working_files():
    """Clean all working files and directories AND DROPS DATABASE"""
    
    drop_database(DB_NAME) 

    # 1. Clean JSON files (create empty ones)
    tech_json_path.mkdir(exist_ok=True)
    db_tech_json_path.mkdir(exist_ok=True)
    HTML_downloader.html_results.mkdir(exist_ok=True)
    
    # Clear JSON files
    files_to_clear = {
        tech_json_path: ['errors.json', 'list_of_automobile_brands.json', 'restart_point.json'],
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

    while True:
        try:
            download_data_from_pages_of_each_brand()
            break
        except (requests.exceptions.ConnectionError, RuntimeError, Exception) as e:
            print(f"Critial error or Network error: {e}")
            print("Restarting in 60 sec...")
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
