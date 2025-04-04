# -*- coding: utf-8 -*-
import os
import requests
import shutil
import time
import psutil
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import traceback
import signal
from urllib.parse import urlparse, urlunparse
import json # برای پردازش پاسخ DoH

# مسیر پوشه‌ای که فایل‌های m3u در آن قرار دارند
input_folder = 'specialiptvs'
# مسیر پوشه‌ای که فایل‌های معتبر در آن قرار می‌گیرند
best_folder = 'best'
# نقطه پایانی DoH شکن
SHECAN_DOH_URL = "https://free.shecan.ir/dns-query"

# --- Helper Function for Colored Output ---
def print_colored(text: str, color: str) -> None:
    """Prints colored text to the console."""
    colors = {
        "green": "\033[92m", "red": "\033[91m", "yellow": "\033[93m",
        "cyan": "\033[96m", "magenta": "\033[95m", "white": "\033[97m"
    }
    if sys.stdout.isatty() and os.name != 'nt':
        try:
            print(f"{colors.get(color.lower(), '')}{text}\033[0m")
        except Exception:
            print(text)
    else:
        print(text)

# --- Function to Clean Output Folder ---
def clean_best_folder():
    """پاک کردن کامل پوشه best و ایجاد مجدد آن با .gitkeep"""
    if os.path.exists(best_folder):
        try:
            shutil.rmtree(best_folder)
            print_colored(f"پوشه {best_folder} با موفقیت پاک شد.", "green")
        except Exception as e:
            print_colored(f"خطا در پاک کردن پوشه {best_folder}: {e}", "red")

    try:
        os.makedirs(best_folder, exist_ok=True)
        gitkeep_path = os.path.join(best_folder, ".gitkeep")
        with open(gitkeep_path, "w") as f:
            f.write("")
    except Exception as e:
        print_colored(f"خطا در ایجاد پوشه {best_folder}: {e}", "red")
        sys.exit(1)


# --- تابع جدید برای ترجمه DNS با استفاده از DoH (با هدر اصلاح شده) ---
def resolve_with_doh(hostname, doh_url=SHECAN_DOH_URL, timeout=10):
    """Resolves hostname to IP using the specified DNS over HTTPS endpoint."""
    # --- *** تغییر: حذف یا تغییر هدر Accept *** ---
    # سعی می‌کنیم بدون ارسال هدر Accept خاص، درخواست دهیم.
    # یا می‌توان هدر عمومی‌تری مثل 'Accept': '*/*' را امتحان کرد.
    headers = {
         'User-Agent': 'Mozilla/5.0 Python-Requests-Script' # ارسال یک User-Agent ساده
         # 'Accept': '*/*' # امتحان کردن این اگر حذف کردن کار نکرد
    }
    params = {'name': hostname, 'type': 'A'} # Query for IPv4 address (A record)

    try:
        # print(f"DoH Query: {doh_url} with params={params}") # Debugging line
        response = requests.get(doh_url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status() # Check for HTTP errors (4xx, 5xx)

        # print(f"DoH Response Status: {response.status_code}") # Debugging line
        # print(f"DoH Response Headers: {response.headers}") # Debugging line
        # print(f"DoH Response Text: {response.text[:500]}") # Debugging line (first 500 chars)


        # --- بررسی Content-Type پاسخ ---
        # سرور DoH ممکن است JSON برنگرداند اگر هدر Accept مشخص نباشد
        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/dns-json' in content_type:
            data = response.json()
        elif 'application/dns-message' in content_type:
             # سرور ممکن است پاسخ باینری DNS را برگردانده باشد که پردازش آن سخت‌تر است
             print_colored(f"DoH: Received binary dns-message response for {hostname}, cannot parse easily.", "yellow")
             return None
        else:
             # فرمت نامشخص یا خطای HTML
             print_colored(f"DoH: Received unexpected Content-Type '{content_type}' for {hostname}. Body: {response.text[:200]}", "yellow")
             return None

        # Check if the response status indicates success (Status: 0 typically means NOERROR)
        if data.get('Status') == 0 and 'Answer' in data:
            for answer in data['Answer']:
                if answer.get('type') == 1: # Type 1 corresponds to A record (IPv4)
                    ip_address = answer.get('data')
                    # Basic validation of the returned IP
                    if ip_address and '.' in ip_address: # Simple check for IPv4 format
                         return ip_address
            print_colored(f"DoH: No A record found for {hostname} in JSON response.", "yellow")
        else:
            status = data.get('Status', 'N/A')
            print_colored(f"DoH: Resolution failed for {hostname} (Status: {status}).", "yellow")

    except requests.exceptions.Timeout:
        print_colored(f"DoH: Timeout connecting to {doh_url} for {hostname}", "yellow")
    except requests.exceptions.RequestException as e:
        # Check if response exists before accessing status_code
        status_code = getattr(e.response, 'status_code', 'N/A') if e.response is not None else 'N/A'
        print_colored(f"DoH: Request Error for {hostname} (Status: {status_code}): {e}", "yellow")
    except json.JSONDecodeError:
        print_colored(f"DoH: Failed to parse JSON response from {doh_url} for {hostname}. Body: {response.text[:200]}", "yellow")
    except Exception as e:
        print_colored(f"DoH: Unexpected error resolving {hostname}: {type(e).__name__}", "yellow")

    return None # Return None if resolution fails


# --- تابع تست و دانلود استریم با DoH (بدون تغییر نسبت به قبل) ---
def download_stream(url, duration=80):
    """
    Tests a stream URL by downloading for a duration, using DoH resolution.
    Checks download speed after an initial period.
    """
    start_time = time.time()
    total_downloaded = 0
    valid = True
    resolved_ip = None
    original_host = None
    scheme = None

    try:
        # --- مرحله 1: تجزیه URL و ترجمه DNS با DoH ---
        try:
            parsed_url = urlparse(url)
            original_host = parsed_url.hostname
            scheme = parsed_url.scheme
            if not original_host or scheme not in ['http', 'https']:
                print_colored(f"Invalid URL: {url}", "red")
                return False
        except Exception as parse_err:
             print_colored(f"Error parsing URL {url}: {parse_err}", "red")
             return False

        resolved_ip = resolve_with_doh(original_host)

        if not resolved_ip:
            print_colored(f"Cannot test stream for {original_host} due to DNS failure.", "red")
            return False

        print_colored(f"DoH OK: {original_host} -> {resolved_ip} (via Shecan)", "cyan")

        # --- مرحله 2: ساخت URL جدید و هدر Host ---
        new_netloc = resolved_ip
        if parsed_url.port:
            new_netloc += f":{parsed_url.port}"

        url_with_ip_parts = (scheme, new_netloc, parsed_url.path or '/', parsed_url.params, parsed_url.query, parsed_url.fragment)
        url_with_ip = urlunparse(url_with_ip_parts)

        headers = {'Host': original_host,
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}

        # --- مرحله 3: درخواست به IP با هدر Host و verify=False برای HTTPS ---
        verify_ssl = True
        if scheme == 'https':
            verify_ssl = False # !!! SECURITY WARNING !!!
            # print_colored(f"Warning: Using verify=False for HTTPS connection to {original_host} ({resolved_ip}).", "yellow")

        with requests.Session() as session:
             session.headers.update(headers)
             response = session.get(url_with_ip, stream=True, timeout=duration, verify=verify_ssl)
             response.raise_for_status()

             # --- مرحله 4: دانلود و بررسی سرعت ---
             file_size = int(response.headers.get('content-length', 0))
             chunk_size = 8192
             # Disable progress bar if not a TTY or explicitly requested
             # Note: disable=None auto-detects, disable=True forces off, disable=False forces on
             progress_bar_disabled = not sys.stdout.isatty()
             progress_bar = tqdm(total=file_size if file_size > 0 else None,
                                 unit='B', unit_scale=True,
                                 desc=f"Testing {original_host[:20]}",
                                 leave=False, disable=progress_bar_disabled)

             min_speed_kbps = 40
             initial_buffer_time = 5
             download_started = False

             for chunk in response.iter_content(chunk_size=chunk_size):
                 download_started = True # Mark that we received at least one chunk
                 elapsed_time = time.time() - start_time
                 if elapsed_time >= duration:
                     break

                 if chunk:
                     len_chunk = len(chunk)
                     progress_bar.update(len_chunk)
                     total_downloaded += len_chunk

                     if elapsed_time > initial_buffer_time:
                         current_speed_bps = total_downloaded / elapsed_time
                         current_speed_kbps = current_speed_bps / 1024
                         if current_speed_kbps < min_speed_kbps:
                             valid = False
                             break
             progress_bar.close()

             # If file size was known > 0 but we downloaded nothing OR we never started
             if not download_started and file_size != 0:
                 print_colored(" No data downloaded despite known size. Invalid.", "red")
                 valid = False
             # If file size was unknown and we downloaded nothing
             elif total_downloaded == 0 and file_size == 0:
                  print_colored(" No data downloaded (and size unknown). Invalid.", "red")
                  valid = False


             response.close()


    except requests.exceptions.Timeout:
        print_colored(f" Request Timed Out testing {original_host}.", "red")
        valid = False
    except requests.exceptions.RequestException as e:
        status = getattr(e.response, 'status_code', 'N/A')
        print_colored(f" Request Error (Status: {status}) testing {original_host}. Invalid.", "red")
        valid = False
    except Exception as e:
        print_colored(f" Unexpected Error testing {original_host}: {type(e).__name__}", "red")
        valid = False

    if valid:
        print_colored(f"Stream OK ({total_downloaded / 1024 / 1024:.2f} MB).", "green")

    return valid


# --- تابع پردازش فایل M3U (بدون تغییر) ---
def process_m3u_file(file_path):
    lines = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()
    except Exception as e:
         print_colored(f"Error reading {os.path.basename(file_path)}: {e}", "red")
         return None

    required_line_index = 14

    if len(lines) > required_line_index:
        stream_url_line = lines[required_line_index].strip()
        if stream_url_line.startswith(('http://', 'https://')) and '.' in stream_url_line:
            # print(f"\nProcessing: {os.path.basename(file_path)}") # Reduce verbosity
            if download_stream(stream_url_line):
                return file_path
            else:
                return None
        else:
            return None
    else:
        return None


# --- تابع اصلی (بدون تغییر) ---
def main():
    clean_best_folder()

    if not os.path.isdir(input_folder):
         print_colored(f"Error: Input folder '{input_folder}' not found.", "red")
         sys.exit(1)

    m3u_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.m3u')]
    if not m3u_files:
        print_colored(f"No .m3u files found in '{input_folder}'. Exiting.", "yellow")
        return

    print_colored(f"Found {len(m3u_files)} .m3u files. Testing streams using Shecan DoH...", "magenta")

    valid_files = []
    num_workers = min(max(4, os.cpu_count() * 4 ), 200)
    print_colored(f"Using {num_workers} concurrent workers.", "cyan")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_m3u_file, os.path.join(input_folder, filename)) for filename in m3u_files]

        for future in tqdm(as_completed(futures), total=len(m3u_files), desc="Testing Streams", unit="file"):
            try:
                result = future.result()
                if result:
                    valid_files.append(result)
            except Exception as e:
                 print_colored(f"\nError processing a file future: {e}", "red")

    print_colored(f"\nFound {len(valid_files)} valid files. Copying to '{best_folder}'...", "magenta")
    copied_count = 0
    mvp_copied = False
    valid_files.sort()
    for index, file_path in enumerate(valid_files, start=1):
        try:
            base_filename = os.path.basename(file_path)
            best_file_path = os.path.join(best_folder, f"best{index}.m3u")
            shutil.copy(file_path, best_file_path)
            copied_count += 1

            if index == 2:
                mvp_file_path = os.path.join(os.getcwd(), "mvp.m3u")
                try:
                    if os.path.exists(mvp_file_path): os.remove(mvp_file_path)
                    shutil.copy(file_path, mvp_file_path)
                    print_colored(f"Copied '{base_filename}' -> 'mvp.m3u' (as 2nd valid)", "green")
                    mvp_copied = True
                except Exception as mvp_e:
                     print_colored(f"Error copying {base_filename} to mvp.m3u: {mvp_e}", "red")

        except Exception as copy_e:
             print_colored(f"Error copying file {file_path} to {best_folder}: {copy_e}", "red")


    print_colored(f"\n--- Summary ---", "magenta")
    print_colored(f"Total files processed: {len(m3u_files)}", "cyan")
    print_colored(f"Valid streams found: {len(valid_files)}", "cyan")
    print_colored(f"Files copied to '{best_folder}': {copied_count}", "green")
    if mvp_copied:
         print_colored(f"MVP file 'mvp.m3u' created.", "green")
    elif len(valid_files) >= 1 :
         print_colored(f"MVP file not created (needed >= 2 valid streams, found {len(valid_files)}).", "yellow")


# --- Entry Point ---
if __name__ == "__main__":
    if sys.version_info < (3, 7):
        print_colored("Error: This script requires Python 3.7 or higher.", "red")
        sys.exit(1)

    def signal_handler(sig, frame):
        print_colored('\nCtrl+C detected. Exiting...', 'yellow')
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    main()
