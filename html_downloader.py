from seleniumbase import SB
import time
import re
from pathlib import Path
import json
import os

#TODO: add saving the final url 
#TODO sort the pages into folders like page 0, page 1 ...
#TODO add saving errors 
#TODO make the base of lots and respective final urls to shrink time of executing 

#опис того, яка логіка записаний в download_all(). Збереження html сторікок не готова до повноцінного 
# запуску, як мінімум треба зробити всі туду і передивитись чи все окей

# запуск завантаження html сторінок можна виконати в main.py 
# або розкоментувавши рядок в кінці цього файлу

class HTML_downloader:
    tech_json = Path('tech_json')
    res_json_dir = Path('res_json')
    html_results = Path('html_results')

    @classmethod
    def save_error(cls, error_object):
        with open(cls.tech_json / 'html_downloader_errors.json', 'a', encoding='utf-8') as f:
            json.dump(error_object, f, indent=2, ensure_ascii=False)

    # @classmethod
    # def get_rendered_html(cls, lot_number: str):
    #     url = f"https://www.copart.com/lot/{lot_number}"

    #     with SB(uc=True, test=False) as sb:
    #         sb.driver.set_page_load_timeout(40)

    #         print(f"Opening: {url}")
    #         sb.open(url)

    #         # --- Чекаємо поки Copart зробить redirect ---
    #         time.sleep(5)

    #         # --- Прокручуємо вниз, щоб прогрузився React ---
    #         sb.scroll_to_bottom()
    #         time.sleep(2)

    #         sb.scroll_to_bottom()
    #         time.sleep(2)

    #         # --- Чекаємо доки React добуде дані ---
    #         sb.wait_for_ready_state_complete(timeout=20)

    #         # --- Збираємо HTML ---
    #         html = sb.get_page_source()

    #         # --- Виводимо фінальний URL після redirect ---
    #         print("Final URL:", sb.get_current_url())

    #         with open(HTML_downloader.html_results / f'{lot_number}.html', "w", encoding="utf-8") as f:
    #             f.write(html)

    @classmethod
    def get_rendered_html(cls, lot_number: str):
        import time
        from selenium.webdriver.common.by import By
        from selenium.common.exceptions import TimeoutException

        url = f"https://www.copart.com/lot/{lot_number}"
        print(f"Opening: {url}")

        # Кількість спроб
        MAX_RETRIES = 5

        for attempt in range(1, MAX_RETRIES + 1):
            print(f"\nTRY {attempt}/{MAX_RETRIES}")

            try:
                with SB(uc=True, test=False) as sb:
                    sb.driver.set_page_load_timeout(40)

                    # -------------------------------
                    # 1. Увімкнути CDP mode (дуже важливо)
                    # -------------------------------
                    sb.activate_cdp_mode()

                    # -------------------------------
                    # 2. Підміна User-Agent (блокує більшість антиботів)
                    # -------------------------------
                    sb.driver.execute_cdp_cmd(
                        "Network.setUserAgentOverride",
                        {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                                    " AppleWebKit/537.36 (KHTML, like Gecko)"
                                    " Chrome/121.0.0.0 Safari/537.36"}
                    )

                    # -------------------------------
                    # 3. Відкриваємо URL з retry
                    # -------------------------------
                    sb.open(url)
                    time.sleep(2)

                    # Якщо завис на Google пробуємо ще раз
                    current = sb.get_current_url()
                    if "google" in current.lower():
                        print("Still stuck on Google retry")
                        continue

                    # -------------------------------
                    # 4. Чекаємо, поки Copart відрендериться
                    # -------------------------------
                    time.sleep(3)
                    sb.scroll_to_bottom()
                    time.sleep(2)
                    sb.scroll_to_bottom()
                    time.sleep(2)

                    # React load complete
                    sb.wait_for_ready_state_complete(timeout=20)

                    # -------------------------------
                    # 5. Anti-bot / Cloudflare check
                    # -------------------------------
                    html = sb.get_page_source()

                    if "captcha" in html.lower() or "verify you are human" in html.lower():
                        print("Captcha detected retry")
                        continue

                    if "Checking your browser" in html:
                        print("Cloudflare check retry")
                        continue

                    # -------------------------------
                    # 6. Зберігаємо результат
                    # -------------------------------
                    print("Final URL:", sb.get_current_url())

                    save_path = HTML_downloader.html_results / f"{lot_number}.html"
                    with open(save_path, "w", encoding="utf-8") as f:
                        f.write(html)

                    print(f"Saved: {save_path}")
                    return html

            except TimeoutException:
                print("Timeout retry")
                continue
            except Exception as ex:
                print(f"Unexpected error: {ex} retry")
                continue

        print("FAILED after all retries")
        return None

        
    @staticmethod
    def save_filenames(directory_path, output_file, starts_with):
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

    def get_list_of_automobile_brands():
        #TODO make choise of parsing only automobile categories or all categories
        with open(HTML_downloader.tech_json / 'list_of_automobile_brands.json', "r", encoding="utf-8") as f:
            content = json.load(f)
        brands = []
        try:
            # content = content.get('data', {}).get('results', {}).get('content', [])
            for brand in content:
                brands.append(brand['description'])
            return brands
        except Exception as e:
            print(f"Error in get_list_of_automobile_brands(): {e}")
            return []

    @classmethod   
    def get_all_lot_numbers(cls, filename):
        print(filename)
        # витягає масив лотів з сторінки
        try:
            with open(HTML_downloader.res_json_dir / f'{filename}', "r", encoding="utf-8") as f:
                content = json.load(f)
        except Exception as e:
            print(f"get_all_lot_numbers {e}")
        lots = []
        try:
            content = content.get('data', {}).get('results', {}).get('content', [])
            for item in content:
                if 'ln' in item:
                    lots.append(item['ln'])
        except Exception as e:
            print(f"Error extracting ln values in file {filename}: {e}")
            return []
        return lots


    @classmethod
    def download_all(cls):
        # 1. витягнути список марок щоб потім по ньому витягати всі скачані сторінки для кожної марки
        # 2. для кожної марки витягнути список сторінок (загалом список сторінок буде дуже великий, 
        #    по 1000 сторінок на кожну марку 113 марок -> 113000 ) тому юзаєм save_file_names де
        #    вказуватимемо для якої саме марки, щоб масив не був занадто велики (буде одна директорія 
        #    для всіх json сторінок всіх марок і буде простіше кожну витягати бо менше директорій в які 
        #    заходити треба )
        # 3. витягнути масив лотів з сторінки 
        # 4. скачати кожеш лот функцією get_rendered_html() 
        
        brands = HTML_downloader.get_list_of_automobile_brands()
        brands = brands[1:]
        for brand in brands: 
            filenames = cls.save_filenames(cls.res_json_dir, f"{str(cls.tech_json)}/list_of_json_files_names_for_single_brand.txt", f"{brand}")
            # у list_of_json_files_names_for_single_brand міститься список назв json файлів 
            # для однієї (поточної) марки кожен файл json містить всю інформацію про 20 лотів, 
            # що знаходяться на одній сторінці (номер сторінкки в назві файлу)   
            for filename in filenames:
                lots = cls.get_all_lot_numbers(filename)
                for lot in lots:
                    cls.get_rendered_html(str(lot))

HTML_downloader.download_all()