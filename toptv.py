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


# --- تابع جدید برای ترجمه DNS با استفاده از DoH ---
def resolve_with_doh(hostname, doh_url=SHECAN_DOH_URL, timeout=10):
    """Resolves hostname to IP using the specified DNS over HTTPS endpoint."""
    headers = {'Accept': 'application/dns-json'}
    params = {'name': hostname, 'type': 'A'} # Query for IPv4 address (A record)

    try:
        response = requests.get(doh_url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status() # Check for HTTP errors

        data = response.json()

        # Check if the response status indicates success (Status: 0 typically means NOERROR)
        if data.get('Status') == 0 and 'Answer' in data:
            # Find the first A record in the Answer array
            for answer in data['Answer']:
                if answer.get('type') == 1: # Type 1 corresponds to A record (IPv4)
                    return answer.get('data') # Return the IP address string
            # If loop finishes without returning, no A record found
            print_colored(f"DoH: No A record found for {hostname} in response.", "yellow")
        else:
            # Log status if resolution failed from DoH server perspective
            status = data.get('Status', 'N/A')
            print_colored(f"DoH: Resolution failed for {hostname} (Status: {status}).", "yellow")

    except requests.exceptions.Timeout:
        print_colored(f"DoH: Timeout connecting to {doh_url} for {hostname}", "yellow")
    except requests.exceptions.RequestException as e:
        print_colored(f"DoH: Request Error for {hostname}: {e}", "yellow")
    except json.JSONDecodeError:
        print_colored(f"DoH: Failed to parse JSON response from {doh_url} for {hostname}", "yellow")
    except Exception as e:
        print_colored(f"DoH: Unexpected error resolving {hostname}: {type(e).__name__}", "yellow")

    return None # Return None if resolution fails


# --- تابع تست و دانلود استریم با DoH ---
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

        # --- *** استفاده از تابع DoH *** ---
        resolved_ip = resolve_with_doh(original_host)

        if not resolved_ip:
            # Reason already printed by resolve_with_doh
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
            verify_ssl = False # !!! SECURITY WARNING: Disabling SSL verification !!!
            # print_colored(f"Warning: Using verify=False for HTTPS connection to {original_host} ({resolved_ip}).", "yellow") # Reduce verbosity

        # Use a session for potential connection pooling within the test
        with requests.Session() as session:
             session.headers.update(headers) # Set Host header for the session
             response = session.get(url_with_ip, stream=True, timeout=duration, verify=verify_ssl) # verify=False for HTTPS!
             response.raise_for_status()

             # --- مرحله 4: دانلود و بررسی سرعت ---
             file_size = int(response.headers.get('content-length', 0))
             chunk_size = 8192
             progress_bar = tqdm(total=file_size if file_size > 0 else None, unit='B', unit_scale=True, desc=f"Testing {original_host[:20]}", leave=False, disable=None)

             min_speed_kbps = 40
             initial_buffer_time = 5

             for chunk in response.iter_content(chunk_size=chunk_size):
                 elapsed_time = time.time() - start_time
                 if elapsed_time >= duration:
                     # print_colored(" Test duration reached.", "yellow") # Less verbose
                     break

                 if chunk:
                     len_chunk = len(chunk)
                     if progress_bar: progress_bar.update(len_chunk)
                     total_downloaded += len_chunk

                     if elapsed_time > initial_buffer_time:
                         current_speed_bps = total_downloaded / elapsed_time
                         current_speed_kbps = current_speed_bps / 1024
                         if current_speed_kbps < min_speed_kbps:
                             # print_colored(f" Speed low ({current_speed_kbps:.1f} KB/s). Invalid.", "red") # Less verbose
                             valid = False
                             break
             if progress_bar: progress_bar.close()

             if total_downloaded == 0 and file_size != 0: # Check if nothing downloaded unless expected size is 0
                 print_colored(" No data downloaded. Invalid.", "red")
                 valid = False

             # Ensure response stream is fully read or closed (important with sessions)
             response.close()


    except requests.exceptions.Timeout:
        print_colored(f" Request Timed Out testing {original_host}.", "red")
        valid = False
    except requests.exceptions.RequestException as e:
        status = getattr(e.response, 'status_code', 'N/A')
        print_colored(f" Request Error (Status: {status}) testing {original_host}. Invalid.", "red")
        valid = False
    except Exception as e:
        print_colored(f" Unexpected Error testing {original_host}: {type(e).__name__} - {e}", "red")
        # print(traceback.format_exc()) # Uncomment for debugging
        valid = False

    # Final result message (more concise)
    if valid:
        print_colored(f"Stream OK ({total_downloaded / 1024 / 1024:.2f} MB).", "green")
    # else: (reason should have been printed)

    return valid


# --- تابع پردازش فایل M3U ---
def process_m3u_file(file_path):
    """
    Processes a single M3U file, extracts the 15th line as URL, and tests it using DoH.
    Returns the file_path if the stream is valid, otherwise None.
    """
    lines = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()
    except Exception as e:
         print_colored(f"Error reading file {os.path.basename(file_path)}: {e}", "red")
         return None

    required_line_index = 14

    if len(lines) > required_line_index:
        stream_url_line = lines[required_line_index].strip()
        # Basic check if it looks like a URL before processing
        if stream_url_line.startswith(('http://', 'https://')) and '.' in stream_url_line:
            print(f"\nProcessing: {os.path.basename(file_path)}")
            # print(f"Stream URL (Line 15): {stream_url_line}") # Less verbose
            if download_stream(stream_url_line):
                return file_path
            else:
                return None
        else:
            # print(f"Line 15 in {os.path.basename(file_path)} is not a valid URL.") # Less verbose
            return None
    else:
        # print(f"File {os.path.basename(file_path)} has < 15 lines.") # Less verbose
        return None


# --- تابع اصلی ---
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
    # Adjust workers based on typical GitHub Actions runner resources (e.g., 2-4 cores)
    # Network calls (DoH + Stream test) are IO bound, but too many can still cause issues.
    num_workers = min(max(4, os.cpu_count() * 4 ), 200) # Heuristic: start with 4x CPU cores, max 200
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
    # Sort alphabetically for deterministic output order
    valid_files.sort()
    for index, file_path in enumerate(valid_files, start=1):
        try:
            base_filename = os.path.basename(file_path)
            best_file_path = os.path.join(best_folder, f"best{index}.m3u")
            shutil.copy(file_path, best_file_path)
            # print_colored(f"Copied '{base_filename}' -> '{os.path.basename(best_file_path)}'", "green") # Less verbose
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
