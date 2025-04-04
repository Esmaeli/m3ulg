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
import random # برای انتخاب تصادفی پراکسی

# --- نیازمندی پراکسی SOCKS ---
# pip install requests[socks]
# یا pip install PySocks requests

# مسیر پوشه‌ای که فایل‌های m3u در آن قرار دارند
input_folder = 'specialiptvs'
# مسیر پوشه‌ای که فایل‌های معتبر در آن قرار می‌گیرند
best_folder = 'best'

# --- لیست اولیه پراکسی های ایرانی ---
PROXY_LIST = [
    "128.140.113.110:5153", "91.107.186.37:80", "91.107.154.214:80",
    "31.47.58.37:80", "185.105.102.179:80", "109.230.92.50:3128",
    "185.172.214.112:80", "31.57.156.166:8888", "185.105.102.189:80",
    "87.248.129.26:80", "5.78.124.240:40000", "5.161.103.41:88",
    "77.104.75.97:5678", "2.188.229.150:7020", "188.121.121.6:3128",
    "81.12.106.158:8080", "80.191.2.7:1080", # SOCKS5 assumed
    "188.136.160.222:7060", "78.38.99.74:8080", "91.92.213.58:8080",
    "91.108.113.52:3128", "31.57.228.216:3128", "5.161.146.73:41914",
    "185.80.196.118:15000", "80.75.7.58:8080", "5.75.168.247:8010",
    "185.42.226.218:4000",
]

# --- Helper Function for Colored Output ---
def print_colored(text: str, color: str) -> None:
    """Prints colored text to the console."""
    colors = {"green": "\033[92m", "red": "\033[91m", "yellow": "\033[93m",
              "cyan": "\033[96m", "magenta": "\033[95m", "white": "\033[97m"}
    if sys.stdout.isatty() and os.name != 'nt':
        try: print(f"{colors.get(color.lower(), '')}{text}\033[0m")
        except Exception: print(text)
    else: print(text)

# --- Function to Clean Output Folder ---
def clean_best_folder():
    """پاک کردن کامل پوشه best و ایجاد مجدد آن با .gitkeep"""
    if os.path.exists(best_folder):
        try:
            shutil.rmtree(best_folder)
            # print_colored(f"پوشه {best_folder} پاک شد.", "green") # Less verbose
        except Exception as e:
            print_colored(f"خطا در پاک کردن {best_folder}: {e}", "red")
    try:
        os.makedirs(best_folder, exist_ok=True)
        with open(os.path.join(best_folder, ".gitkeep"), "w") as f: f.write("")
    except Exception as e:
        print_colored(f"خطا در ایجاد {best_folder}: {e}", "red")
        sys.exit(1)

# --- تابع پیش-بررسی پراکسی (جدید) ---
def check_proxy(proxy_str, check_url='http://httpbin.org/ip', timeout=8):
    """Tries to connect to check_url via the proxy. Returns proxy_str if successful, None otherwise."""
    protocol = 'http'
    if ':1080' in proxy_str or ':1088' in proxy_str or ':9050' in proxy_str:
        protocol = 'socks5h'

    proxies = {
        'http': f'{protocol}://{proxy_str}',
        'https': f'{protocol}://{proxy_str}'
    }
    try:
        # Use a simple HEAD request for speed, or GET if HEAD fails often
        response = requests.get(check_url, proxies=proxies, timeout=timeout, headers={'User-Agent': 'ProxyChecker/1.0'})
        # Check for successful status code (2xx)
        if 200 <= response.status_code < 300:
            # print_colored(f"Proxy OK: {proxy_str}", "green") # Debugging
            return proxy_str
        # else: # Debugging
        #    print_colored(f"Proxy BAD Status ({response.status_code}): {proxy_str}", "yellow")

    except (requests.exceptions.ProxyError, requests.exceptions.ConnectTimeout,
            requests.exceptions.SSLError, requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout, requests.exceptions.Timeout):
        # print_colored(f"Proxy FAILED Connection: {proxy_str}", "red") # Debugging
        pass # Ignore connection errors for this proxy
    except Exception:
         # print_colored(f"Proxy FAILED Unexpected: {proxy_str} - {e}", "red") # Debugging
         pass # Ignore other errors too
    return None

def check_proxies_concurrently(proxy_list=PROXY_LIST, check_url='http://httpbin.org/ip', timeout=8, max_workers=200):
    """Checks a list of proxies concurrently and returns a list of live ones."""
    print_colored(f"Checking {len(proxy_list)} proxies concurrently (max workers: {max_workers})...", "cyan")
    live_proxies = []
    # Use a limited number of s for checking to avoid overwhelming the network/target
    num_check_workers = min(max_workers, len(proxy_list))

    with ThreadPoolExecutor(max_workers=num_check_workers) as executor:
        futures = [executor.submit(check_proxy, proxy_str, check_url, timeout) for proxy_str in proxy_list]
        for future in tqdm(as_completed(futures), total=len(proxy_list), desc="Checking Proxies", unit="proxy"):
            result = future.result()
            if result:
                live_proxies.append(result)

    print_colored(f"Found {len(live_proxies)} live proxies out of {len(proxy_list)}.",
                  "green" if live_proxies else "red")
    return live_proxies

# --- تابع تست و دانلود استریم با استفاده از پراکسی‌های زنده (اصلاح شده) ---
def download_stream(url, duration=80, live_proxies=None): # Accept live_proxies list
    """
    Tests a stream URL using a randomly chosen live proxy.
    Checks download speed after an initial period.
    """
    if not live_proxies: # If list is empty or None
        print_colored("No live proxies available to test stream.", "red")
        return False

    # --- انتخاب تصادفی یک پراکسی از لیست زنده‌ها ---
    selected_proxy_str = random.choice(live_proxies)
    # print_colored(f"Selected live proxy: {selected_proxy_str}", "cyan") # Debugging

    protocol = 'http'
    if ':1080' in selected_proxy_str or ':1088' in selected_proxy_str or ':9050' in selected_proxy_str:
        protocol = 'socks5h'

    proxies = {
        'http': f'{protocol}://{selected_proxy_str}',
        'https': f'{protocol}://{selected_proxy_str}'
    }

    start_time = time.time()
    total_downloaded = 0
    valid = True
    original_host = urlparse(url).hostname or "UnknownHost"

    try:
        # --- درخواست با پراکسی انتخاب شده ---
        print_colored(f"Testing {original_host[:25]}... via Live Proxy: {selected_proxy_str}", "cyan")
        # Start with verify=True. If SSL errors occur frequently THROUGH proxies, consider verify=False.
        response = requests.get(url, stream=True, timeout=duration, proxies=proxies, verify=True)
        response.raise_for_status()

        # --- دانلود و تست سرعت ---
        file_size = int(response.headers.get('content-length', 0))
        chunk_size = 8192
        progress_bar_disabled = not sys.stdout.isatty()
        progress_bar = tqdm(total=file_size if file_size > 0 else None,
                            unit='B', unit_scale=True,
                            desc=f"Test {original_host[:20]} @{selected_proxy_str}",
                            leave=False, disable=progress_bar_disabled)

        min_speed_kbps = 40
        initial_buffer_time = 5
        download_started = False

        for chunk in response.iter_content(chunk_size=chunk_size):
            download_started = True
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
                        print_colored(f" Speed low ({current_speed_kbps:.1f} KB/s) via {selected_proxy_str}. Invalid.", "red")
                        valid = False
                        break
        progress_bar.close()
        response.close()

        if not download_started and file_size != 0:
            print_colored(f" No data via {selected_proxy_str} (size known). Invalid.", "red")
            valid = False
        elif total_downloaded == 0 and file_size == 0:
             print_colored(f" No data via {selected_proxy_str} (size unknown). Invalid.", "red")
             valid = False
        elif valid and download_started: # Ensure it's still considered valid and we got *something*
             print_colored(f"Stream OK via {selected_proxy_str} ({total_downloaded/1024/1024:.2f} MB).", "green")
             # valid remains True

    except requests.exceptions.Timeout:
        print_colored(f" Timeout testing {original_host} via {selected_proxy_str}. Invalid.", "red")
        valid = False
    except requests.exceptions.SSLError as e:
         print_colored(f" SSL Error testing {original_host} via {selected_proxy_str}. Invalid. (Try verify=False if trusted?)", "red")
         valid = False
    except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError) as e:
        # If the PRE-CHECKED proxy fails HERE, it might have died recently. Mark stream as invalid.
        print_colored(f" Live Proxy {selected_proxy_str} failed during stream test for {original_host}: {type(e).__name__}. Invalid.", "red")
        valid = False
    except requests.exceptions.RequestException as e:
        status = getattr(e.response, 'status_code', 'N/A')
        print_colored(f" Request Error (Status: {status}) testing {original_host} via {selected_proxy_str}. Invalid.", "red")
        valid = False
    except Exception as e:
        print_colored(f" Unexpected Error testing {original_host} via {selected_proxy_str}: {type(e).__name__}", "red")
        valid = False

    return valid


# --- تابع پردازش فایل M3U (اصلاح شده برای پاس دادن پراکسی‌های زنده) ---
def process_m3u_file(file_path, live_proxies): # Accept live_proxies
    """
    Processes a single M3U file, extracts the 15th line as URL, and tests it using one live proxy.
    Returns the file_path if the stream is valid, otherwise None.
    """
    lines = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()
    except Exception as e:
         # print_colored(f"Error reading {os.path.basename(file_path)}: {e}", "red") # Reduce noise
         return None

    required_line_index = 14

    if len(lines) > required_line_index:
        stream_url_line = lines[required_line_index].strip()
        if stream_url_line.startswith(('http://', 'https://')) and '.' in stream_url_line:
            # Pass live_proxies list to download_stream
            if download_stream(stream_url_line, live_proxies=live_proxies):
                return file_path
            else:
                return None
        else:
            return None
    else:
        return None


# --- تابع اصلی (اصلاح شده برای پیش-بررسی پراکسی) ---
def main():
    clean_best_folder()

    if not os.path.isdir(input_folder):
         print_colored(f"Error: Input folder '{input_folder}' not found.", "red")
         sys.exit(1)

    m3u_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.m3u')]
    if not m3u_files:
        print_colored(f"No .m3u files found in '{input_folder}'. Exiting.", "yellow")
        return

    # --- *** مرحله 1: پیش-بررسی پراکسی‌ها *** ---
    live_proxies = check_proxies_concurrently()
    if not live_proxies:
        print_colored("No live proxies found after checking. Cannot test streams. Exiting.", "red")
        sys.exit(1)
    # Optionally shuffle the live list to distribute load better if using sequentially later
    random.shuffle(live_proxies)
    print_colored(f"Proceeding to test streams using {len(live_proxies)} live proxies...", "magenta")
    # -----------------------------------------

    valid_files = []
    # Adjust workers - less impact from proxy checking now, focus on stream testing limits
    num_workers = min(max(4, os.cpu_count() * 4 ), 50) # Keep reasonable limit
    print_colored(f"Using {num_workers} concurrent workers for stream testing.", "cyan")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Pass the list of live_proxies to each worker task
        futures = [executor.submit(process_m3u_file, os.path.join(input_folder, filename), live_proxies)
                   for filename in m3u_files]

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
    valid_files.sort() # Sort alphabetically for consistent naming
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
        # Potentially add code here to signal threads to stop if possible/needed
        os._exit(1) # Force exit more abruptly if graceful shutdown is hard
    signal.signal(signal.SIGINT, signal_handler)

    main()
