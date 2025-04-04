import os
import requests
import shutil
import time
import psutil
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# مسیر پوشه‌ای که فایل‌های m3u در آن قرار دارند
input_folder = 'specialiptvs'
# مسیر پوشه‌ای که فایل‌های معتبر در آن قرار می‌گیرند
best_folder = 'best'

def clean_best_folder():
    """پاک کردن کامل پوشه best در صورت وجود"""
    if os.path.exists(best_folder):
        try:
            shutil.rmtree(best_folder)
            print(f"پوشه {best_folder} با موفقیت پاک شد.")
        except Exception as e:
            print(f"خطا در پاک کردن پوشه {best_folder}: {e}")
    
    # ایجاد مجدد پوشه best
    os.makedirs(best_folder, exist_ok=True)
    # ایجاد فایل .gitkeep
    gitkeep_path = os.path.join(best_folder, ".gitkeep")
    with open(gitkeep_path, "w") as f:
        f.write("")

def download_stream(url, duration=80):
    start_time = time.time()
    total_downloaded = 0
    start_net_io = psutil.net_io_counters().bytes_recv
    valid = True

    try:
        response = requests.get(url, stream=True, timeout=duration)
        response.raise_for_status()

        file_size = int(response.headers.get('content-length', 0))
        chunk_size = 1024
        progress_bar = tqdm(total=file_size, unit='B', unit_scale=True, desc="Downloading")

        for chunk in response.iter_content(chunk_size=chunk_size):
            elapsed_time = time.time() - start_time
            if elapsed_time >= duration:
                break
            if chunk:
                progress_bar.update(len(chunk))
                total_downloaded += len(chunk)
                # بررسی سرعت دانلود فقط بعد از 5 ثانیه
                if elapsed_time > 5 and (total_downloaded / elapsed_time) < 40 * 1024:
                    valid = False
                    break

        progress_bar.close()
        return valid

    except Exception as e:
        print(f"Error downloading stream: {e}")
        return False

def process_m3u_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
    except UnicodeDecodeError:
        try:
            with open(file_path, 'r', encoding='iso-8859-1') as file:
                lines = file.readlines()
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            return None

    if len(lines) >= 15:
        stream_url = lines[14].strip()  # خط 15 (شاخص 14 در لیست)
        if stream_url.startswith('http'):
            print(f"\nProcessing file: {os.path.basename(file_path)}")
            print(f"Stream URL: {stream_url}")
            if download_stream(stream_url):
                print("Stream is valid. Will be moved to best folder.")
                return file_path
            else:
                print("Stream is invalid. Ignoring file.")
                return None
        else:
            print("Invalid stream URL. Ignoring file.")
            return None
    else:
        print(f"File {os.path.basename(file_path)} does not have 15 lines. Ignoring file.")
        return None

def main():
    # پاک کردن و ایجاد مجدد پوشه best
    clean_best_folder()
    
    # لیست تمام فایل‌های m3u در پوشه ورودی
    m3u_files = [f for f in os.listdir(input_folder) if f.endswith('.m3u')]
    
    # پردازش فایل‌ها به صورت موازی
    valid_files = []
    with ThreadPoolExecutor(max_workers=len(m3u_files)) as executor:
        futures = [executor.submit(process_m3u_file, os.path.join(input_folder, filename)) for filename in m3u_files]
        
        for future in tqdm(as_completed(futures), total=len(m3u_files), desc="Processing files"):
            try:
                result = future.result()
                if result:
                    valid_files.append(result)
            except Exception as e:
                print(f"Error processing file: {e}")

    # کپی فایل‌های معتبر به پوشه best با نام‌های مرتب
    for index, file_path in enumerate(valid_files, start=1):
        best_file_path = os.path.join(best_folder, f"best{index}.m3u")
        shutil.copy(file_path, best_file_path)
        print(f"Copied to {best_file_path}")
        
        # اگر اولین فایل معتبر است، آن را به عنوان mvp.m3u نیز کپی کنید
        if index == 2:
            mvp_file_path = os.path.join(os.getcwd(), "mvp.m3u")
            if os.path.exists(mvp_file_path):
                os.remove(mvp_file_path)
            shutil.copy(file_path, mvp_file_path)
            print(f"File {best_file_path} copied to {mvp_file_path}")

if __name__ == "__main__":
    main()
