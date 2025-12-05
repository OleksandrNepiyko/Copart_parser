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

#TODO delete redundant pages from res_json based on photos folders. after bmw everything should be well. now 78 redundant files compare to number of photos folders
#TODO make something with pages numeration now you have 0-49, sometimes 0-50 
#BUG somehow there were few page files with 50th index with just error inside, but the problem that 
# they were saved and there were only 10 of them and as a result of final code version

#TODO make that if there is no content array don't download json of this page
#TODO make switcher for cars and other types of vehicles (maybe talk about priority)

import re
from pathlib import Path
import json
import execjs
import requests
import time
from requests_html import HTMLSession
import os
from html_downloader import HTML_downloader
from database_writer import main as db_main

tech_json_path = Path('tech_json')
res_json_path = Path('res_json')
SESSION = requests.Session()

def safe_post(url, **kwargs):
    for attempt in range(5):
        try:
            return SESSION.post(url, **kwargs)
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error, retry {attempt+1}/5")
            time.sleep(5)

    raise RuntimeError("Network failed after 5 retries")

def save_error(error_obj):
    #if an error occurs it should be saved here (only problems in automatic part of the program will be saved)
    with open(tech_json_path / 'errors.json', 'a', encoding='utf-8') as f:
        json.dump(error_obj, f, indent=2, ensure_ascii=False)

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
        cleaned = cleaned.replace('\/', '/')
        
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


def extract_automobile_brands_list(): 
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
    
    automobile_brands_list_with_automobile_type = []

    for brand in automobile_brands_list:
        try:
            if brand['type'] == 'AUTOMOBILE':
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


def download_photos_from_lot(brand, page, arr_of_lot_numbers, restart_object):
    #goes through arr of lot numbers that is provided and downloads photos links
    print(f"Download_photos_for_lot: {arr_of_lot_numbers}")
    session = requests.Session()
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'Origin': 'https://www.copart.com',
        'Referer': 'https://www.copart.com/lot/92745685/2013-bmw-m5-ca-hayward',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'x-xsrf-token': '15ef9352-6b78-4960-8c22-3d6c7ae511fb'
    }

    cookies = {
        "anonymousCrmId": "7f62f400-a31c-40a1-9ec6-3882600d94af",
        "nlbi_242093": "GtLwHyNVDAsWMox4ie/jegAAAAAJbMaLBXI7o0sn1hVYNNVy",
        "_gcl_au": "1.1.135331524.1763655305",
        "userCategory": "RPU",
        "timezone": "Europe/Kiev",
        "googtrans": "/en/ru",
        "__eoi": "ID=3643d9cf80e64b0b:T=1763678384:RT=1763710894:S=AA-AfjbApzZYR5-V4goE6TvOXwWQ",
        "g2usersessionid": "2763b116147121b40f878f6069f35fe2",
        "G2JSESSIONID": "08C38284F647FE7B7445FB64159219B6-n1",
        "usersessionid": "8b7a8968a7dfa001c484e404e5df0266",
        "visid_incap_242093": "l4BVG1w4S8yiXVL+fdF8idscHmkAAAAAREIPAAAAAACAYJHAAbmSeb5ni73/A+8E5nZBukKZqgd5",
        "g2app.search-table-rows": "20",
        "incap_ses_788_242093": "1ubiVz9v3U1vW/sd/YnvCsAYImkAAAAAbWGGCS9E7MRyIBINNRCmKg==",
        "reese84": "3:NejAT/1S7D7w5zdRhj7IAA==:zAV+...",
        "userLang": "en",
        "_clck": "s3k3kn^2^g18^0^2152",
    }

    url = "https://www.copart.com/public/data/lotdetails/solr/lot-images/"

    if len(arr_of_lot_numbers)<=0:
        print(f"arr_of_lot_numbers<=0 for {brand} page {page}")
        save_error({
                'brand': brand,
                'page': page,                    
                'error_type': "arr_of_lot_numbers<=0"
            })
    
    if restart_object == None or restart_object == '':
        restart_lot_number = 0
    else:
        restart_lot_number = restart_object['lot_number']

    skip = restart_lot_number != 0

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
            print(f"Error to get photos for brand: {brand} page: {page} number of lot: {number}")
            save_error({
                    'brand': brand,
                    'page': page,
                    'number': number,
                    'error_type': "Error to get photos"
                })
            return

        try:
            data = r.json()
        except Exception as e:
            print(f"Exception in r.json() in photos : {e}")
            print(r.json())
            save_error({
                'brand': brand,
                'page': page,
                'lot_number': number,
                'error_type': f"Exception in r.json() in photos {e}"
            })
            
        (res_json_path / f"{brand}_page{page}_photos").mkdir(exist_ok=True)

        with open(res_json_path / f"{brand}_page{page}_photos" / f"{number}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        with open(tech_json_path /'restart_point.json', 'w', encoding='utf-8') as f:
            restart_point = {
                'brand': brand,
                'page': page,
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


def download_data_from_pages_of_single_brand(brand, restart_object):
    #for transmited brand goes through all 50 pages and downloads data about each lot on page (? does all data is available on this page or 
    # it's needed to open each lot (you will open each lot because you need photos for this lot))
    
    print(f"download_data_from_pages_of_single_brand: {brand}")

    brand_upper = brand.upper()
    
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    }

    cookies = {
        "g2usersessionid": "02db60a883b957d26afa942ef1644a63",
        "G2JSESSIONID": "6E41C900EC34DC08CBD4B5DD1769A49C-n1",
        "usersessionid": "8b7a8968a7dfa001c484e404e5df0266",
        "visid_incap_242093": "l4BVG1w4S8yiXVL+fdF8idscHmkAAAAAQ0IPAAAAAACAT4XAAbmSeb6WO/lBY1ffSzbZ7FHDHQ8l",
        "incap_ses_788_242093": "nc54ZDSfuHERwzHP+onvCkFGH2kAAAAAg+ILLZmelDQeaBW3mIt/Vw==",
        "reese84": "3:wfeLqHpa1LHnkFPxKBd+CQ==:MvQBcXVmBVZudOu3Pom+vVTolvQL3qduhSO04LVpEvaBj+geu8zcQ6TpyC..."
    }

    if restart_object == None or restart_object == '':
        restart_page = 0
    else:
        restart_page = restart_object['page']

    for page in range (restart_page, 51):
        time.sleep(0.1)
        print(f"Brand: {brand}, page: {page}")
        start = page * 20
        #change page and start = page * 20;   pages starts from 0  i.e. page 2: "page":2,"size":20,"start":40
        payload = clean_payload({"query":["*"],"filter":{"MAKE":[f"lot_make_desc:\"{brand_upper}\""]},"sort":["salelight_priority asc","member_damage_group_priority asc","auction_date_type desc","auction_date_utc asc"],"page":page,"size":20,"start":start,"watchListOnly":False,"freeFormSearch":False,"hideImages":False,"defaultSort":False,"specificRowProvided":False,"displayName":"","searchName":"","backUrl":"","includeTagByField":{"MAKE":"{!tag=MAKE}"},"rawParams":{}})
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
            print(f"Downloading from single page failed for brand: {brand} at page: {page}")
            save_error({
                    'brand': brand,
                    'page': page,
                    'error_type': "Downloading from single page failed"
                })

        try:
            response_json = response.json()

            with open(res_json_path / f'{brand}_page{page}.json', 'w', encoding='utf-8') as f:
                json.dump(response_json, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Failed to get json in response download_data_from_pages_of_single_brand for {brand} page: {page}")
            save_error({
                'brand': brand,
                'page': page,                    
                'error_type': "Failed to get json in response download_data_from_pages_of_single_brand"
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
            print(f"Error extracting ln values on page {page}: {e}")
            save_error({
                'page': page,
                'error_type': str(e)
            })
        per_page_restart = None
        if restart_object and isinstance(restart_object, dict) and restart_object.get('page') == page:
            per_page_restart = restart_object
        if len(all_ln_values) != 0:
            download_photos_from_lot(brand, page, all_ln_values, per_page_restart)

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

    restart_brand = None
    restart_obj = None

    try:
        with open(tech_json_path /'restart_point.json', 'r', encoding='utf-8') as f:
            restart_obj = json.load(f)
            restart_brand = restart_obj['brand']
    except Exception as e:
        print(f"download_data_from_pages_of_each_brand restart file opening error {e}")
        pass

    skip = restart_brand is not None
    for brand in content:
        brand_description = brand['description']
        # if restart_brand == '' or content == None or brand_description == restart_brand:
        #     download_data_from_pages_of_single_brand(brand_description, restart_obj)
        #     restart_brand = 
        if skip:
            if brand_description == restart_brand:
                skip = False
                download_data_from_pages_of_single_brand(brand_description, restart_obj)
            continue
        else:
            download_data_from_pages_of_single_brand(brand_description, None)


def main():
    # extract_json_from_list_of_all_brands()
    # extract_automobile_brands_list()
    
    while True:
        try:
            download_data_from_pages_of_each_brand()
            break
        except requests.exceptions.ConnectionError as e:
            print("Network error, restarting in 60 sec:", e)
            time.sleep(60)

    #launch database writing
    db_main('copart_lots_test', 'second_copart_lots_test')

    # if you wont to download html pages with photos uncomment the line below 
    # and fix tudu at the start of this file
    # HTML_downloader.download_all()

if __name__ == '__main__':
    main()