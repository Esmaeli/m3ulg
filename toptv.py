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

# --- کتابخانه‌های مورد نیاز برای ترجمه DNS ---
try:
    import dns.resolver
    from urllib.parse import urlparse, urlunparse
except ImportError:
    print("Error: 'dnspython' library not found. Please install it: pip install dnspython")
    sys.exit(1)

# مسیر پوشه‌ای که فایل‌های m3u در آن قرار دارند
input_folder = 'specialiptvs'
# مسیر پوشه‌ای که فایل‌های معتبر در آن قرار می‌گیرند
best_folder = 'best'
# سرورهای DNS شکن
SHECAN_DNS = ['185.51.200.2', '178.22.122.100']

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
        # ایجاد فایل .gitkeep برای پوشه‌های خالی در گیت
        gitkeep_path = os.path.join(best_folder, ".gitkeep")
        with open(gitkeep_path, "w") as f:
            f.write("")
    except Exception as e:
        print_colored(f"خطا در ایجاد پوشه {best_folder}: {e}", "red")
        # Exit if we cannot create the output folder
        sys.exit(1)


# --- تابع کمکی برای ترجمه DNS ---
def resolve_with_custom_dns(hostname, dns_servers):
    """Resolves hostname to IP using specified DNS servers."""
    # Add default port for resolver if needed, though usually not necessary for standard DNS
    resolver = dns.resolver.Resolver(configure=False) # Do not use system config
    resolver.nameservers = dns_servers
    resolver.timeout = 5 # Set a timeout for DNS query
    resolver.lifetime = 5 # Total time allowed for query

    try:
        # Try resolving A record (IPv4) first
        answers = resolver.resolve(hostname, 'A', raise_on_no_answer=False)
        if answers:
            return answers[0].to_text() # Return the first IPv4 found
        else:
            # If no A record, try AAAA (IPv6) - optional
            # answers = resolver.resolve(hostname, 'AAAA', raise_on_no_answer=False)
            # if answers:
            #     return answers[0].to_text() # Return first IPv6
             print_colored(f"DNS: No A record found for {hostname} using {dns_servers}", "yellow")

    except dns.exception.Timeout:
         print_colored(f"DNS Timeout resolving {hostname} using {dns_servers}", "yellow")
    except dns.resolver.NoNameservers as e:
         print_colored(f"DNS No Nameservers ({dns_servers}): {e}", "yellow")
    except dns.resolver.NXDOMAIN:
         print_colored(f"DNS NXDOMAIN: Domain {hostname} not found using {dns_servers}", "yellow")
    except Exception as e:
        print_colored(f"DNS resolution failed for {hostname} using {dns_servers}: {type(e).__name__}", "yellow")

    return None # Return None if resolution fails for any reason

# --- تابع تست و دانلود استریم با DNS سفارشی ---
def download_stream(url, duration=80):
    """
    Tests a stream URL by downloading for a duration, using custom DNS resolution.
    Checks download speed after an initial period.
    """
    start_time = time.time()
    total_downloaded = 0
    valid = True # Assume valid initially
    resolved_ip = None
    original_host = None
    scheme = None

    try:
        # --- مرحله 1: تجزیه URL و ترجمه DNS ---
        try:
            parsed_url = urlparse(url)
            original_host = parsed_url.hostname # Returns None if parsing fails
            scheme = parsed_url.scheme
            if not original_host or scheme not in ['http', 'https']:
                print_colored(f"Invalid URL format: {url}", "red")
                return False
        except Exception as parse_err:
             print_colored(f"Error parsing URL {url}: {parse_err}", "red")
             return False

        resolved_ip = resolve_with_custom_dns(original_host, SHECAN_DNS)

        if not resolved_ip:
            print_colored(f"DNS Failed for {original_host}. Cannot test stream.", "red")
            return False # Cannot proceed without IP

        print_colored(f"DNS OK: {original_host} -> {resolved_ip} (via Shecan)", "cyan")

        # --- مرحله 2: ساخت URL جدید و هدر Host ---
        new_netloc = resolved_ip
        if parsed_url.port:
            new_netloc += f":{parsed_url.port}"

        url_with_ip_parts = (scheme, new_netloc, parsed_url.path or '/', parsed_url.params, parsed_url.query, parsed_url.fragment)
        url_with_ip = urlunparse(url_with_ip_parts)

        headers = {'Host': original_host,
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'} # Common UA


        # --- مرحله 3: درخواست به IP با هدر Host و verify=False برای HTTPS ---
        verify_ssl = True # Default to verify
        if scheme == 'https':
            verify_ssl = False # !!! IMPORTANT SECURITY WARNING !!!
            print_colored(f"Warning: Using verify=False for HTTPS connection to {original_host} ({resolved_ip}). SSL/TLS verification disabled!", "yellow")

        response = requests.get(url_with_ip, stream=True, timeout=duration, headers=headers, verify=verify_ssl)
        response.raise_for_status() # Check for HTTP errors (4xx, 5xx)

        # --- مرحله 4: دانلود و بررسی سرعت ---
        # Note: Content-Length might be unreliable when connecting via IP
        # tqdm might show 0 total if Content-Length is missing/wrong
        file_size = int(response.headers.get('content-length', 0))
        chunk_size = 8192 # Slightly larger chunk
        progress_bar = tqdm(total=file_size if file_size > 0 else None, unit='B', unit_scale=True, desc=f"Testing {original_host[:20]}", leave=False, disable=None) # Auto-disable if not TTY

        min_speed_kbps = 40 # Minimum speed in KB/s after initial buffer
        initial_buffer_time = 5 # Seconds before checking speed

        for chunk in response.iter_content(chunk_size=chunk_size):
            elapsed_time = time.time() - start_time
            if elapsed_time >= duration:
                print_colored(" Test duration reached.", "yellow")
                break # Stop after specified duration

            if chunk:
                len_chunk = len(chunk)
                if progress_bar: progress_bar.update(len_chunk)
                total_downloaded += len_chunk

                # بررسی سرعت دانلود فقط بعد از چند ثانیه اولیه
                if elapsed_time > initial_buffer_time:
                    current_speed_bps = total_downloaded / elapsed_time
                    current_speed_kbps = current_speed_bps / 1024
                    if current_speed_kbps < min_speed_kbps:
                        print_colored(f" Speed low ({current_speed_kbps:.1f} KB/s < {min_speed_kbps} KB/s). Invalid.", "red")
                        valid = False
                        break # Stop if speed is too low

            # Prevent tight loop hammering CPU if no data arrives for a bit
            # time.sleep(0.01) # Small sleep if needed? Might interfere with speed calc.

        if progress_bar: progress_bar.close()

        # Additional check: was anything downloaded at all?
        if total_downloaded == 0:
             print_colored(" No data downloaded. Invalid.", "red")
             valid = False

        # Ensure response is closed
        response.close()

    except requests.exceptions.Timeout:
        print_colored(" Request Timed Out.", "red")
        valid = False
    except requests.exceptions.SSLError as e:
         # This might still happen even with verify=False if other SSL issues occur
         print_colored(f" SSL Error connecting to {resolved_ip} for {original_host}: {e}", "red")
         valid = False
    except requests.exceptions.RequestException as e:
        status = getattr(e.response, 'status_code', 'N/A')
        print_colored(f" Request Error (Status: {status}) testing {original_host}. Invalid.", "red")
        valid = False
    except Exception as e:
        # Catch any other unexpected error during the process
        print_colored(f" Unexpected Error testing {original_host}: {type(e).__name__} - {e}", "red")
        # print(traceback.format_exc()) # Uncomment for debugging
        valid = False

    # Final result message
    if valid:
        print_colored(f"Stream OK ({total_downloaded / 1024 / 1024:.2f} MB downloaded).", "green")
    # else: (already printed reason for invalidity)

    return valid


# --- تابع پردازش فایل M3U ---
def process_m3u_file(file_path):
    """
    Processes a single M3U file, extracts the 15th line as URL, and tests it.
    Returns the file_path if the stream is valid, otherwise None.
    """
    lines = []
    try:
        # Try reading with UTF-8 first
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()
    except Exception as e:
         print_colored(f"Error reading file {os.path.basename(file_path)}: {e}", "red")
         return None # Cannot process if read fails


    required_line_index = 14 # 15th line has index 14

    if len(lines) > required_line_index:
        stream_url_line = lines[required_line_index].strip()
        if stream_url_line.startswith(('http://', 'https://')):
            print(f"\nProcessing: {os.path.basename(file_path)}")
            print(f"Stream URL (Line 15): {stream_url_line}")
            # Call the modified download_stream function
            if download_stream(stream_url_line):
                # print("Stream is valid.") # Message printed by download_stream now
                return file_path
            else:
                # print("Stream is invalid.") # Message printed by download_stream now
                return None
        else:
            print(f"Line 15 in {os.path.basename(file_path)} is not a valid URL: '{stream_url_line[:50]}...'")
            return None
    else:
        print(f"File {os.path.basename(file_path)} has < 15 lines.")
        return None


# --- تابع اصلی ---
def main():
    # پاک کردن و ایجاد مجدد پوشه best
    clean_best_folder()

    # لیست تمام فایل‌های m3u در پوشه ورودی
    if not os.path.isdir(input_folder):
         print_colored(f"Error: Input folder '{input_folder}' not found or not a directory.", "red")
         sys.exit(1)

    m3u_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.m3u')]
    if not m3u_files:
        print_colored(f"No .m3u files found in '{input_folder}'. Exiting.", "yellow")
        return # Exit gracefully if no files

    print_colored(f"Found {len(m3u_files)} .m3u files in '{input_folder}'. Starting processing...", "magenta")

    # پردازش فایل‌ها به صورت موازی
    valid_files = []
    # Limit max_workers to avoid overwhelming the system/network even more with DNS lookups + downloads
    # Start lower and increase if stable. Using len(m3u_files) can be dangerous.
    num_workers = min(100, len(m3u_files)) # Start with a more reasonable number of workers
    print_colored(f"Using {num_workers} concurrent workers.", "cyan")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_m3u_file, os.path.join(input_folder, filename)) for filename in m3u_files]

        for future in tqdm(as_completed(futures), total=len(m3u_files), desc="Processing M3U files", unit="file"):
            try:
                result = future.result()
                if result:
                    valid_files.append(result) # Store path of valid file
            except Exception as e:
                 # This catches errors in the future itself (less likely now with try/except in worker)
                 print_colored(f"\nError processing a file future: {e}", "red")


    # کپی فایل‌های معتبر به پوشه best با نام‌های مرتب
    print_colored(f"\nFound {len(valid_files)} valid files. Copying to '{best_folder}'...", "magenta")
    copied_count = 0
    mvp_copied = False
    # Sort valid_files alphabetically before assigning numbers? Or keep processing order?
    # Let's keep processing order for now.
    for index, file_path in enumerate(valid_files, start=1):
        try:
            base_filename = os.path.basename(file_path)
            best_file_path = os.path.join(best_folder, f"best{index}.m3u")
            shutil.copy(file_path, best_file_path)
            print_colored(f"Copied '{base_filename}' -> '{os.path.basename(best_file_path)}'", "green")
            copied_count += 1

            # کپی فایل دوم (index == 2) به عنوان mvp.m3u
            # ** توجه: اگر کمتر از 2 فایل معتبر باشد، mvp.m3u ساخته نمی‌شود **
            if index == 2:
                mvp_file_path = os.path.join(os.getcwd(), "mvp.m3u") # In current working directory
                try:
                    if os.path.exists(mvp_file_path):
                        os.remove(mvp_file_path)
                    shutil.copy(file_path, mvp_file_path)
                    print_colored(f"Copied '{base_filename}' -> 'mvp.m3u'", "green")
                    mvp_copied = True
                except Exception as mvp_e:
                     print_colored(f"Error copying {base_filename} to mvp.m3u: {mvp_e}", "red")

        except Exception as copy_e:
             print_colored(f"Error copying file {file_path} to {best_folder}: {copy_e}", "red")


    print_colored(f"\n--- Summary ---", "magenta")
    print_colored(f"Total files processed: {len(m3u_files)}", "cyan")
    print_colored(f"Valid files found: {len(valid_files)}", "cyan")
    print_colored(f"Files copied to '{best_folder}': {copied_count}", "green")
    if mvp_copied:
         print_colored(f"MVP file 'mvp.m3u' created from the second valid file.", "green")
    elif len(valid_files) >= 1 :
         print_colored(f"MVP file 'mvp.m3u' was not created (needed at least 2 valid files, found {len(valid_files)}).", "yellow")


# --- Entry Point ---
if __name__ == "__main__":
    if sys.version_info < (3, 7):
        print_colored("Error: This script requires Python 3.7 or higher.", "red")
        sys.exit(1)

    # Handle Ctrl+C
    def signal_handler(sig, frame):
        print_colored('\nCtrl+C detected. Exiting...', 'yellow')
        # Consider if more graceful shutdown is needed (e.g., waiting for workers)
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    main()
