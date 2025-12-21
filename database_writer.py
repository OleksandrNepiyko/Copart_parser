#opens folder with json files (res_json), search for files with names provided in 
#db_tech_json/all_json_names. If error occurs only the name of last completely 
#save file will be stored in db_tech_json/last_written_to_db_review. So it's 
#impossible to track how much reviews from the next file were saved to db
#the resulting db backup will be saved in writing_json_to_db (current dir)
#with namef {backup_name}.sql

#BEFOR START CHANGE THE NAME OF THE DB AND TABLE IN main() FUNCTION

#it's completely ready-to-run* version of json->db saver (*read the text above)

#TODO make that if you restart the main func do not resave the last files which are already inserted before the program stopped

import mysql.connector
import os
import re
import subprocess
import json
from pathlib import Path
from datetime import datetime
import time 
from natsort import natsorted

def save_error(error_obj):
    #if an error occurs it should be saved here (only problems in automatic part of the program will be saved)
    with open(db_tech_json_path / 'error_list.json', 'a', encoding='utf-8') as f:
        json.dump(error_obj, f, indent=2, ensure_ascii=False)
        f.write(',')

cursor = None
db = None

def create_db(db_name):
    db = mysql.connector.connect(
        host="10.30.0.100",
		port=3310, 
        user="root",
        password="root",
        auth_plugin='mysql_native_password'
    )
    cursor = db.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

def create_table(db_name, table_name):
    db = mysql.connector.connect(
        host="10.30.0.100",
		port=3310, 
        user="root",
        password="root",
        database=f"{db_name}",
        auth_plugin='mysql_native_password'
    )
    cursor = db.cursor()
    if db is None:
        print("Database connection is not initialized.")
        save_error({
            'error_type': "Database connection is not initialized."
        })
        return
    if cursor is None:
        print("Database cursor is not initialized.")
        save_error({
            'error_type': "Database cursor is not initialized."
        })
        return
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    `id` int NOT NULL AUTO_INCREMENT,
    
    /* Блок "Інформація про транспортний засіб" */
    `brand` varchar(100) DEFAULT NULL,
    `model` varchar(100) DEFAULT NULL,
    `memberVehicleType` varchar(100) DEFAULT NULL, 
    `vehicleTypeCode` varchar(100) DEFAULT NULL, 
    `manufacture_year` int DEFAULT NULL,
    `complectation` varchar(100) DEFAULT NULL,
    `full_url` text,
    `thumbnail_url` text,
    `highres_url` text,
    `video` text,
    `png` text,
    `lot_number` varchar(100) DEFAULT NULL,
    `vin_number` varchar(50) DEFAULT NULL,
    `ownership_certificate_code` varchar(100) DEFAULT NULL,
    `odometer_km` int DEFAULT NULL,
    `primary_damage` varchar(255) DEFAULT NULL,
    `secondary_damage` varchar(255) DEFAULT NULL,
    `cylinders` int DEFAULT NULL,
    `color` varchar(50) DEFAULT NULL,
    `engine_type` varchar(50) DEFAULT NULL,
    `transmission` varchar(50) DEFAULT NULL,
    `drive_type` varchar(50) DEFAULT NULL,
    `vehicle_classification` varchar(100) DEFAULT NULL,
    `fuel_type` varchar(50) DEFAULT NULL,
    `car_keys` varchar(50) DEFAULT NULL,
    `highlights` text,
    `notes` text,
    `estimated_retail_price` decimal(10,2) DEFAULT NULL,
    `json` longtext,
    
    /* Блок "Інформація про ставку" */
    `current_bid` decimal(10,2) DEFAULT NULL,
    `price_without_auction` decimal(10,2) DEFAULT NULL,
    `starting_bid` decimal(10,2) DEFAULT NULL,
    
    /* Блок "Інформація про продаж" */
    `sale_name` varchar(255) DEFAULT NULL,
    `sale_location` varchar(255) DEFAULT NULL,
    `sale_date` date DEFAULT NULL,
    `last_updated` datetime DEFAULT NULL,
    
    /* Технічні поля */
    `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (`id`),
    KEY `lot_number_idx` (`lot_number`),
    KEY `brand_model_idx` (`brand`, `model`)
    )
    """)
    db.commit()
    return cursor, db
        

reviews_json_path = Path("res_json")
db_tech_json_path = Path("db_tech_json")
backup_name = 'copart_backup'
# last_saved_review_in_file_id = 0

def extract_brand_model_from_url(url):
    """
    Extracts car brand and model from Drom.ru review URLs
    
    Args:
        url (str): Drom.ru review URL
        
    Returns:
        dict: Dictionary with 'car_brand' and 'car_model' car_keys
    """
    try:
        # Remove protocol and domain, split by '/'
        parts = url.replace('https://www.drom.ru/reviews/', '').split('/')
        
        if len(parts) < 2:
            return {'car_brand': None, 'car_model': None}
        
        brand = parts[0]  # First part is brand
        model = parts[1]  # Second part is model
        
        # Clean up the model name
        model = model.replace('_', ' ').title()
        
        return {
            'car_brand': brand.title(),
            'car_model': model
        }
        
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return {'car_brand': None, 'car_model': None}

# def process_json_file(file_path, skip_until_id=0):
#     """Process one JSON file and add data to database"""
#     try:
#         with open(file_path, 'r', encoding='utf-8') as f:
#             data = json.load(f)

#         print(f"Processing file: {file_path}")

#         breadcrumbs = data.get('header', {}).get('breadcrumbs', [])
#         brand = "Unknown"
#         if len(breadcrumbs) >= 3:
#             brand = breadcrumbs[2].get('name', 'Unknown')
        
#         # Check if shortReviews exists
#         if 'shortReviews' not in data:
#             with open(db_tech_json_path / "error_list.json", 'a', encoding='utf-8') as f:
#                 obj = {
#                     "file_path": str(file_path),
#                     "error_type": "file doesn't have shortReviews"
#                 }
#                 json.dump(obj, f, ensure_ascii=False, indent=2)
#                 f.write('\n')
#             print(f"File {file_path} doesn't have shortReviews")
#             return 0
        
#         inserted_count = 0
#         skip_reviews = skip_until_id > 0
        
#         # Process all reviews in the file
#         for review in data['shortReviews']:
#             current_review_id = review.get('id', 'N/A')

#             if skip_reviews:
#                 if current_review_id == skip_until_id:
#                     skip_reviews = False
#                     print(f"Reached target review ID: {current_review_id}, continuing...")
#                 else:
#                     continue

#             try:
#                 # Prepare photo URLs (comma separated)
#                 photo_urls = ""
#                 if review.get('photos'):
#                     photo_urls = ",".join([photo['original'] for photo in review['photos']])
                
#                 # Convert technical parameters - keep original values
#                 title_params = review.get('titleParams', {})

#                 # Convert transmission code to text
#                 transmission_map = {'1': 'механика', '2': 'автомат', '3': 'вариатор', '4': 'робот'}
#                 transmission = transmission_map.get(str(title_params.get('transmission', '')), '')

#                 # Convert drive type code to text
#                 drive_map = {'1': 'передний', '2': 'задний', '3': '4WD'}
#                 drive_type = drive_map.get(str(title_params.get('privod', '')), '')

#                 # Convert fuel type code to text
#                 fuel_map = {'0': 'бензин', '1': 'бензин', '2': 'дизель', '3': 'гибрид', '4': 'электричество'}
#                 fuel_type = fuel_map.get(str(title_params.get('fuelType', '')), '')
                
#                 # Insert data into database
#                 cursor.execute("""
#                 INSERT INTO car_reviews (
#                     author_name, publication_date, parsing_datetime, photo_urls,
#                     car_brand, car_model, manufacture_year, body_type,
#                     engine_volume, transmission, drive_type, fuel_type, advantages, disadvantages, 
#                     breakages, about_car, json
#                 ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """, (
#                     review.get('user', ''),
#                     review.get('time', ''),  # Keep original time text
#                     photo_urls,
#                     brand,
#                     title_params.get('model', ''),
#                     title_params.get('year'),
#                     # title_params.get('aboutCar', ''),  # there is no body type in short
#                     str(int(title_params.get('volume', 0)) / 1000) if title_params.get('volume') else '',
#                     transmission,
#                     drive_type,
#                     fuel_type,
#                     review.get('advantages', ''),
#                     review.get('disadvantages', ''),
#                     review.get('breakages', ''),
#                     title_params.get('aboutCar', ''),
#                     json.dumps(review, ensure_ascii=False)
#                     # json.dumps(review)
#                 ))
                
#                 inserted_count += 1
#                 print(f"Added review ID: {current_review_id}")
                
#             except Exception as e:
#                 with open(db_tech_json_path / "error_list.json", 'a', encoding='utf-8') as f:
#                     obj = {
#                         "file_path": str(file_path),
#                         "review_id": current_review_id,
#                         "error_type": f"Error processing review: {e}"
#                     }
#                     json.dump(obj, f, ensure_ascii=False, indent=2)
#                     f.write('\n')
#                 print(f"Error processing review ID {current_review_id}: {e}")
#                 continue
#             with open(db_tech_json_path / "last_written_to_db_review.json", 'w', encoding='utf-8') as f:
#                 obj = {
#                     "file_path": str(file_path),
#                     "id": current_review_id
#                 }
#                 json.dump(obj, f, ensure_ascii=False, indent=2)
#         db.commit()
#         return inserted_count
        
#     except Exception as e:
#         with open(db_tech_json_path / "error_list.json", 'a', encoding='utf-8') as f:
#             obj = {
#                 "file_path": str(file_path),
#                 "error_type": f"Error reading file: {e}"
#             }
#             json.dump(obj, f, ensure_ascii=False, indent=2)
#             f.write('\n')
#         print(f"Error reading file {file_path}: {e}")
#         return 0

def fetch_photos(file_path, lot_number):
    print(f"fetch photos with input {file_path}, {lot_number}")
    """
    Витягує фото, відео та PNG зображення для лоту.
    Повертає об'єкт з трьома полями: photos, video, png_image
    """
    data = None

    base_path = file_path.replace('.json', '')  # видаляємо .json
    photos_dir = f"{base_path}_photos"
    photos_file_path = f"{photos_dir}/{lot_number}.json"
    try:
        with open(photos_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error in fetch_photos for {file_path}_photos/{lot_number}.json : {e}")
        save_error({
            'file_path': file_path,
            'lot_number': lot_number,
            'error_type': f"Error reading photos file: {e}"
        })
        return None

    if data is None:
        save_error({
            'file_path': file_path,
            'lot_number': lot_number,
            'error_type': "Photos file is empty or not found"
        })
        return None

    result = {
        'photos': [],
        'video': None,
        'png_image': None
    }

    try:
        images_list = data.get('data', {}).get('imagesList', {})
        # print(images_list)
        
        # Обробка фото (IMAGE) - обов'язкове поле
        photos_data = []
        try:
            photos_data = images_list.get('IMAGE', [])
            # print(f"photos_data: {photos_data}") #it gives correct info
        except Exception as e:
            print(f"Error getting IMAGE data for lot {lot_number}: {e}")
            save_error({
                'file_path': file_path,
                'lot_number': lot_number,
                'error_type': f"Error getting IMAGE data: {e}"
            })
            return None
        
        if not photos_data:
            save_error({
                'file_path': file_path,
                'lot_number': lot_number,
                'error_type': "No photos found in IMAGE array"
            })
            return None
        
        # Формуємо масив об'єктів з фотками з трьома розширеннями
        for photo in photos_data:
            try:
                photo_obj = {
                    'full_url': photo.get('fullUrl'),
                    'thumbnail_url': photo.get('thumbnailUrl'),
                    'highres_url': photo.get('highResUrl'),
                }
                result['photos'].append(photo_obj)
            except Exception as e:
                print(f"Error processing photo for lot {lot_number}: {e}")
                continue

        # Обробка відео (VIDEO) - необов'язкове поле
        video_data = []
        try:
            video_data = images_list.get('VIDEO', [])
        except Exception as e:
            None
            # Не записуємо помилку, бо VIDEO може не бути
            
        if video_data and len(video_data) > 0:
            try:
                video = video_data[0]  # Беремо перше відео
                result['video'] = {
                    'highres_url': video.get('highResUrl')
                }
            except Exception as e:
                print(f"Error processing video for lot {lot_number}: {e}")
                save_error({
                    'file_path': file_path,
                    'lot_number': lot_number,
                    'error_type': f"Error processing video for lot {lot_number}: {e}"
                })


        # Обробка PNG зображення (DTLE) - необов'язкове поле
        png_data = []
        try:
            png_data = images_list.get('DTLE', [])
        except Exception as e:
            None
            # Не записуємо помилку, бо DTLE може не бути
            
        if png_data and len(png_data) > 0:
            try:
                png_image = png_data[0]  # Беремо перше PNG
                result['png_image'] = {
                    'full_url': png_image.get('fullUrl'),
                    'thumbnail_url': png_image.get('thumbnailUrl'),
                    'highres_url': png_image.get('highResUrl')
                }
            except Exception as e:
                print(f"Error processing PNG image for lot {lot_number}: {e}")
                save_error({
                    'file_path': file_path,
                    'lot_number': lot_number,
                    'error_type': f"Error processing PNG image for lot {lot_number}: {e}"
                })
        # print(result)
        return result #returns correct data

    except Exception as e:
        print(f"Error processing photos data for lot {lot_number}: {e}")
        save_error({
            'file_path': file_path,
            'lot_number': lot_number,
            'error_type': f"Error processing photos data: {e}"
        })
        return None
        

def parse_copart_lot(lot_obj, file_path, cursor, db, db_name, table_name): #extracts data from single lot and adds it into db
    """
    Парсить один об'єкт Copart лоту і вставляє дані у таблицю first_copart_lots.
    
    lot_obj – dict (об'єкт Copart)
    cursor  – курсор MySQL
    db      – з'єднання MySQL
    """

    # -----------------------------
    # 1. Basic vehicle information
    # -----------------------------
    brand = lot_obj.get("mkn")                               # VOLVO
    model = lot_obj.get("mmod")                              # XC60
    memberVehicleType = lot_obj.get("memberVehicleType")     #suv, automobile, motorcycle, etc.
    vehicleTypeCode = lot_obj.get("vehicleTypeCode")         # VEHTYPE_C, VEHTYPE_V, etc.
    manufacture_year = lot_obj.get("lcy")                    # 2017
    complectation = lot_obj.get("lm")                        # XC60 T6 DY

    vin_number = lot_obj.get("fv")                           # VIN
    lot_number = lot_obj.get("lotNumberStr")                 # 93484295
    ownership_certificate_code = lot_obj.get("ts")           # VA

    # Odometer
    odometer_km = None
    try:
        if lot_obj.get("orr"):
            odometer_km = int(float(lot_obj["orr"]) * 1.609344)
    except:
        odometer_km = None

    primary_damage = lot_obj.get("dd")                       # FRONT END
    secondary_damage = lot_obj.get("sdd")                    # UNDERCARRIAGE (у notes)

    cylinders = None
    try:
        cylinders = int(lot_obj.get("cy", None))
    except:
        cylinders = None

    color = lot_obj.get("clr")                               # BEIGE
    engine_type = lot_obj.get("egn")                         # "2.0L  4"
    transmission = lot_obj.get("tmtp")                       # AUTOMATIC
    drive_type = lot_obj.get("drv")                          # All wheel drive
    vehicle_classification = lot_obj.get("bstl")             # 4DR SPOR
    fuel_type = lot_obj.get("ft")                            # GAS
    car_keys = lot_obj.get("hk")                                 # YES

    highlights = lot_obj.get("lcd")                          # ENGINE START PROGRAM
    notes = lot_obj.get("sdd")                     # записуємо у notes

    # estimated retail price
    estimated_retail_price = lot_obj.get("la")               # 16786.0
    json_value = json.dumps(lot_obj, ensure_ascii=False)

    # -----------------------------
    # 2. Bid information
    # -----------------------------
    current_bid = lot_obj.get("hb")                          # current bid
    price_without_auction = lot_obj.get("lotPlugAcv")        # ACV
    starting_bid = lot_obj.get("bnp")                        # buy now price / starting bid

    # -----------------------------
    # 3. Sale information
    # -----------------------------
    sale_name = lot_obj.get("yn")                            # PA - CHAMBERSBURG
    sale_location = lot_obj.get("syn")                       # PA - CHAMBERSBURG

    # Sale date (timestamp → date)
    sale_date = None
    try:
        if isinstance(lot_obj.get("lad"), (int, float)):
            sale_date = datetime.fromtimestamp(lot_obj["lad"] / 1000).date()
    except:
        sale_date = None

    # last updated – беремо системний час
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # -----------------------------
    # 4. Photos (call external function)
    # -----------------------------
    photos = []
    video = None
    png = None
    res_full_url = None
    res_thumbnail_url = None
    res_highres_url = None
    try:
        car_photos = fetch_photos(file_path, lot_number)

        separator = ", "

        video_insert = None
        try:
            # video = car_photos.get('video', None)
            video = car_photos.get('video', None)
            video_insert = video.get('highres_url', '')
            # print(f"Video insert for lot {lot_number}: {video_insert}")
        except:
            pass

        png_insert = None
        try:
            png = car_photos.get('png_image', None)
            png_full = png.get('full_url', '')
            png_thumbnail = png.get('thumbnail_url', '')
            png_highres = png.get('highres_url', '')
            png_insert = f"'full_url:' {png_full}{separator}'thumbnail_url:' {png_thumbnail}{separator}'highres_url:' {png_highres}"
            # print(f"PNG insert for lot {lot_number}: {png_insert}")
        except:
            pass

        photos = []
        try:
            photos = car_photos.get('photos', [])
            if photos is None:
                photos = []
        except:
            photos = []

        full_url = []
        thumbnail_url = []
        highres_url = []

        try:
            for photo in photos:
                try:
                    full_url.append(photo.get('full_url', ''))
                    thumbnail_url.append(photo.get('thumbnail_url', ''))
                    highres_url.append(photo.get('highres_url', ''))
                except:
                    pass
        except:
            pass

        res_full_url = separator.join(full_url)
        res_thumbnail_url = separator.join(thumbnail_url)
        res_highres_url = separator.join(highres_url)

    except:
        car_photos = ""

    print(f"parse db_name: {db_name}")
    sql = f"""
    INSERT INTO {table_name} (
        brand, model, memberVehicleType, vehicleTypeCode, manufacture_year, complectation, full_url, thumbnail_url, highres_url,
        lot_number, vin_number, ownership_certificate_code,
        odometer_km, primary_damage, secondary_damage, cylinders, color, engine_type,
        transmission, drive_type, vehicle_classification, fuel_type,
        car_keys, highlights, notes, estimated_retail_price, json,
        current_bid, price_without_auction, starting_bid,
        sale_name, sale_location, sale_date, last_updated, video, png
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    """

    params = (
        brand, model, memberVehicleType, vehicleTypeCode, manufacture_year, complectation, res_full_url, res_thumbnail_url, res_highres_url,
        lot_number, vin_number, ownership_certificate_code,
        odometer_km, primary_damage, secondary_damage, cylinders, color, engine_type,
        transmission, drive_type, vehicle_classification, fuel_type,
        car_keys, highlights, notes, estimated_retail_price, json_value,
        current_bid, price_without_auction, starting_bid,
        sale_name, sale_location, sale_date, last_updated, video_insert, png_insert
    )

    try:
        cursor.execute(sql, params)
        db.commit()
        print(f"Inserted lot {lot_number} | VIN {vin_number}")
    except Exception as e:
        print(f"ERROR inserting lot {lot_number}: {e}")
        raise
    

def process_json_file(file_path, db, cursor, resume_lot, db_name, table_name, lot_number=0):
    file_path = str(file_path)

    """Process one JSON file and add data to database"""
    try:
        with open(str(file_path), 'r', encoding='utf-8') as f:
            file_content = f.read().strip()
        
        print(f"Processing file: {file_path}")

        # Skip if file is empty
        if not file_content:
            print(f"File {file_path} is empty")
            save_error({
                'file_path': file_path,
                'error': f"File {file_path} is empty"
            })
            return 0

        # Parse JSON array
        lots_data = None
        try:
            lots_data = json.loads(file_content)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON in file {file_path}: {e}")
            save_error({
                'file_path': file_path,
                'error': f"Error parsing JSON in file {file_path}: {e}"
            })
            return 0
        
        # print(lots_data)

        # # Check if it's a list
        # if not isinstance(lots_data, list):
        #     print(f"File {file_path} doesn't contain a JSON array")
        #     save_error({
        #         'file_path': file_path,
        #         'error': f"File {file_path} doesn't contain a JSON array"
        #     })
        #     return 0

        inserted_count = 0
        
        try:
            content = lots_data.get('data').get('results').get('content')

            if not content or len(content) == 0:
                print(f"Content array (here is all data about all 20 lots on page) is empty in file: {file_path}")
                save_error({
                    'file_path': file_path,
                    'error': f"Content array (here is all data about all 20 lots on page) is empty in file: {file_path}"
                })
            else:
                # start_from_first = True if lot_number == 0 else False
                start_index = -1

                if lot_number != 0:
                    start_index = next(
                        (i for i, obj in enumerate(content) if obj.get('ln') == lot_number),
                        -1
                    )

                    if start_index == -1:#that's if lot is not found
                        save_error({
                            "type": "LOT_NOT_FOUND",
                            "file_path": str(file_path),
                            "searched_lot_number": lot_number,
                            "all_lot_numbers_on_page": [
                                obj.get("ln") for obj in content
                            ]
                        })

                        #in this case start from the beginning of the file 
                        #BUG here can be a duplication (if the provided last lot number is not found 
                        # in the provided file) (But it's not neccessary that this will happen, 
                        # it's not a bug, just to highlight this place)
                        # start_index = -1

                print(start_index)
                #to not get out of bounse exception because the last saved can be the last at the page
                if start_index + 1 < len(content):
                    for lot in content[start_index + 1:]:
                        if db is None:
                            print("process_json_file(): Database connection is not initialized.")
                            save_error({
                                'error_type': "process_json_file(): Database connection is not initialized."
                            })
                            return
                        print(f"process db_name: {db_name}")
                        parse_copart_lot(lot, file_path, cursor, db, db_name, table_name)
                        inserted_count += 1

            db.commit()
            
        except Exception as e:
            with open(db_tech_json_path / "error_list.json", 'a', encoding='utf-8') as f:
                obj = {
                    "file_path": str(file_path),
                    "lot_number": lot_number,
                    "error_type": f"Error processing review: {e}"
                }
                json.dump(obj, f, ensure_ascii=False, indent=2)
                f.write('\n')
            print(f"Error processing review in {str(obj)}")
        
        time.sleep(3)
        return inserted_count
        
    except Exception as e:
        with open(db_tech_json_path / "error_list.json", 'a', encoding='utf-8') as f:
            obj = {
                "file_path": str(file_path),
                "error_type": f"Error reading file: {e}"
            }
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.write('\n')
        print(f"Error reading file of errors {file_path}: {e}")
        return 0
    
def save_filenames(directory_path, output_file, starts_with=""):
    #saves names of files and returns it. Saving is to make it easier to restore program after crash
    # Get all files in the directory
    files = os.listdir(directory_path)

    # Filter only files AND those that start with the needed prefix
    files = [
        f for f in files
        if os.path.isfile(os.path.join(directory_path, f)) and f.startswith(starts_with)
    ]

    files.sort(
        key=lambda f: int(re.search(r'page(\d+)', f).group(1))
    )

    # Write file names to output file
    with open(output_file, "w", encoding="utf-8") as out:
        for filename in files:
            out.write(filename + "\n")
    return files

def main(db_name, table_name, results_json_path, table_index):
    create_db(db_name)
    cursor, db = create_table(db_name, table_name)
    # Check if directory exists
    if not reviews_json_path.exists():
        print(f"Directory {reviews_json_path} not found!")
        return
    
    # Create tech directory if it doesn't exist
    db_tech_json_path.mkdir(exist_ok=True)

    save_filenames(reviews_json_path, db_tech_json_path / "all_json_names.txt")
    
    # Check if all_json_names file exists and is not empty
    all_json_names_file = db_tech_json_path / "all_json_names.txt"
    last_review_file = db_tech_json_path / "last_written_to_db_review.json"
    
    json_files = []
    start_from_beginning = True
    resume_file = ""
    resume_lot = 0
    
    #this block detects if we have list of all json files, if not else block will create it by taking all 
    #names of all files inside of dir in reviews_json_path variable and write them into file by 
    # path in all_json_names_file variable 
    if all_json_names_file.exists() and all_json_names_file.stat().st_size > 0:
        # Read existing file list
        with open(all_json_names_file, 'r', encoding='utf-8') as f:
            json_files = [Path(line.strip()) for line in f if line.strip()]
        
        # Check if we have last processed file
        if last_review_file.exists():
            try:
                with open(last_review_file, 'r', encoding='utf-8') as f:
                    last_data = json.load(f)
                    resume_file = None
                    try:
                        resume_file = last_data['file_name']
                        resume_lot = last_data['lot'] #index of lot at the page
                    except Exception as e:
                        print(f"No file_name and id params in {db_tech_json_path}/last_written_to_db_review.json found")

                    if resume_file:
                        # Find the index of the last processed file
                        try:
                            file_index = json_files.index(Path(resume_file))
                            json_files = json_files[file_index:]  # Start from last file
                            start_from_beginning = False
                            print(f"Resuming from file: {resume_file}")
                            if resume_lot > 0:
                                print(f"Will skip reviews until ID: {resume_lot}")
                        except ValueError:
                            print(f"Last processed file {resume_file} not found in list, starting from beginning")
            except Exception as e:
                print(f"Error reading last review file: {e}")
    else:
        # Get all JSON files and save to all_json_names
        json_files = list(reviews_json_path.glob("*.json"))
        json_files = natsorted(json_files)
        with open(all_json_names_file, 'w', encoding='utf-8') as f:
            for file_path in json_files:
                f.write(f"{reviews_json_path}/{file_path}" + '\n')
        print(f"Created new file list with {len(json_files)} files")
    
    if not json_files:
        print("No JSON files found!")
        return
    
    print(f"Processing {len(json_files)} JSON files")
    
    total_inserted = 0
    current_file_index = 0
    
    # Process each file
    for file_path in json_files:
        current_file_index += 1
        # For the first file when resuming, pass the resume_lot
        if not start_from_beginning and current_file_index == 1:
            # print(f"1 file_path: {file_path}")
            inserted = process_json_file(f"{results_json_path}/{file_path}", db, cursor, resume_lot, db_name, table_name)
        else:
            # print(f"2 file_path: {file_path}")
            inserted = process_json_file(f"{results_json_path}/{file_path}", db, cursor, 0, db_name, table_name)
        
        total_inserted += inserted
        
        # Update last processed file (after each file)
        with open(last_review_file, 'w', encoding='utf-8') as f:
            json.dump({
                "file_name": str(file_path),
                "lot": 0
            }, f, ensure_ascii=False, indent=2)
        
        print(f"File {file_path}: added {inserted} reviews\n")
    
    # Save changes
    if db is None:
        print("main(): Database connection is not initialized.")
        save_error({
            'error_type': "main(): Database connection is not initialized."
        })
        return
    db.commit()
    
    print(f"Completed! Total added {total_inserted} reviews")
    
    try:
        with open(f'{table_index}_{backup_name}.sql', 'w', encoding='utf-8') as f:
            subprocess.run(['mysqldump', '-h10.30.0.100', '-P3310', '-u', 'root', '-proot', f"{db_name}"], stdout=f, check=True)
        print(f"Database exported to {table_index}_{backup_name}.sql")
    except Exception as e:
        print(f"Export error: {e}")
    if db is None:
        print("main()2: Database connection is not initialized.")
        save_error({
            'error_type': "main()2: Database connection is not initialized."
        })
        return
    if cursor is None:
        print("main(): cursor is not initialized.")
        save_error({
            'error_type': "main(): cursor is not initialized."
        })
        return
    cursor.close()
    db.close()

# if __name__ == "__main__":
#     main('copart_lots_test', 'copart_lots_test')
