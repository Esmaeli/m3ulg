# -*- coding: utf-8 -*-
import requests
import time
import os
import shutil
import re # Import regular expressions for parsing
import io  # Import for handling bytes in memory
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys # Import sys for version check and exit
import traceback # For detailed error logging
import signal # For Ctrl+C handling

# --- Constants ---
MAX_SIZE_MB = 30 # Maximum file size in Megabytes to process
MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024 # Convert MB to Bytes

# --- Helper Function for Colored Output ---
def print_colored(text: str, color: str) -> None:
    """Prints colored text to the console."""
    colors = {
        "green": "\033[92m", "red": "\033[91m", "yellow": "\033[93m",
        "cyan": "\033[96m", "magenta": "\033[95m", "white": "\033[97m"
    }
    # Check if output is a TTY (terminal)
    if sys.stdout.isatty() and os.name != 'nt': # Basic check, might need refinement for Windows
        try:
            print(f"{colors.get(color.lower(), '')}{text}\033[0m")
        except Exception:
            print(text) # Fallback if encoding fails
    else:
        print(text) # Print without color if not a TTY or on Windows without specific support

# --- Function to Read M3U URLs from File ---
def get_m3u_urls_from_file(file_path: str) -> List[str]:
    """Reads M3U URLs from a file, ignoring comments and empty lines."""
    m3u_urls = []
    line_count = 0
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for line_num, line in enumerate(file, 1):
                line_count = line_num
                try:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if line.startswith('http://') or line.startswith('https://'):
                            m3u_urls.append(line)
                        else:
                            print_colored(f"Warning: Line {line_num} ignored (not a valid URL): '{line[:100]}...'", "yellow")
                except Exception as e:
                    print_colored(f"Warning: Could not process line {line_num} in '{file_path}': {e}", "yellow")
        print_colored(f"Read {line_count} lines, successfully parsed {len(m3u_urls)} potential URLs from {file_path}", "green")
    except FileNotFoundError:
        print_colored(f"Error: Input file '{file_path}' not found.", "red")
    except Exception as e:
        print_colored(f"Error reading file '{file_path}': {e}", "red")
    return m3u_urls

# --- Group Sorting Function ---
def sort_groups(group_names: List[str]) -> List[str]:
    """
    Sort groups based on specific priority rules:
    1. First priority (exact order): iran -> persian -> ir (specific)
    2. Second priority (exact order): bein -> sport -> spor -> canal+ -> dazn -> paramount
    3. All other groups alphabetically.
    Args:
        group_names: List of group names to sort
    Returns:
        Sorted list of group names
    """
    normalized_map = {str(name).lower(): str(name) for name in group_names if isinstance(name, (str, int, float))} # Handle simple types
    lower_groups_unique = list(normalized_map.keys())

    priority1_lower = []
    priority2_lower = []
    processed_lower = set()

    p1_terms = [('iran', lambda g: 'iran' in g),
                ('persian', lambda g: 'persian' in g),
                ('ir', lambda g: 'ir' in g and 'iraq' not in g and 'ireland' not in g)]

    p2_terms = [('bein', lambda g: 'bein' in g),
                ('sport', lambda g: 'sport' in g),
                ('spor', lambda g: 'spor' in g),
                ('canal+', lambda g: 'canal+' in g),
                ('dazn', lambda g: 'dazn' in g),
                ('paramount', lambda g: 'paramount' in g)]

    for _, condition in p1_terms:
        for group_lower in list(lower_groups_unique):
            if condition(group_lower) and group_lower not in processed_lower:
                priority1_lower.append(group_lower)
                processed_lower.add(group_lower)

    for _, condition in p2_terms:
        for group_lower in list(lower_groups_unique):
            if condition(group_lower) and group_lower not in processed_lower:
                priority2_lower.append(group_lower)
                processed_lower.add(group_lower)

    other_groups_lower = sorted([
        g for g in lower_groups_unique if g not in processed_lower
    ])

    priority1_original = [normalized_map[g] for g in priority1_lower]
    priority2_original = [normalized_map[g] for g in priority2_lower]
    other_groups_original = [normalized_map[g] for g in other_groups_lower]

    return priority1_original + priority2_original + other_groups_original


# --- Function to Parse M3U Content ---
def parse_m3u_content(m3u_content: str) -> Tuple[List[Dict[str, Any]], List[str], bool]:
    """
    Parses M3U content into a list of channel dictionaries and checks for 'Bein'.
    Args:
        m3u_content: The M3U content as a string.
    Returns:
        A tuple containing:
        - List of channel dictionaries
        - List of all unique group titles found.
        - Boolean indicating if a 'Bein' group was found.
    """
    channels = []
    group_titles = set()
    found_bein = False
    lines = m3u_content.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith('#EXTINF:'):
            raw_extinf = line
            attributes = {}
            name = "Unnamed Channel" # Default name
            group_title = "General"
            duration = -1

            match_extinf = re.match(r'#EXTINF:(?P<duration>-?\d+)(?P<attributes_str>.*?),\s*(?P<name>.*)', line)
            if match_extinf:
                try:
                    duration = int(match_extinf.group('duration'))
                except ValueError:
                    duration = -1
                name = match_extinf.group('name').strip() or name # Use default if name is empty
                attributes_str = match_extinf.group('attributes_str').strip()

                try:
                    # Regex to capture key="value" or key=value
                    attr_matches = re.findall(r'([a-zA-Z0-9_\-]+)=?(?:"([^"]*)"|([^ ]*))', attributes_str)
                    for key, val_quoted, val_unquoted in attr_matches:
                        attributes[key] = val_quoted if val_quoted else val_unquoted
                except Exception:
                     pass # Ignore attribute parsing errors silently? Or log?

                group_title_raw = attributes.get('group-title', "General")
                group_title = str(group_title_raw) if group_title_raw is not None else "General"

                if "bein" in group_title.lower():
                    found_bein = True

                group_titles.add(group_title)

            url = ""
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if next_line and not next_line.startswith('#'):
                    url = next_line
                    i = j
                    break
                elif next_line.startswith('#EXTINF:') or next_line.startswith('#EXTM3U') or next_line.startswith('#EXT-X-'):
                     break
                j += 1

            if match_extinf: # Only add channel if EXTINF was parsed
                channels.append({
                    'duration': duration, 'name': name, 'attributes': attributes,
                    'url': url, 'group_title': group_title, 'raw_extinf': raw_extinf
                })
                # Reduce verbosity: only warn if URL is missing AND name is not default
                if not url and name != "Unnamed Channel":
                     print_colored(f"Warning: No URL found for '{name}'", "yellow")

        i += 1

    return channels, list(group_titles), found_bein


# --- Download/Process Function with Size Limit ---
def download_process_and_save_m3u(m3u_url: str, file_index: int, output_folder: str) -> bool:
    """
    Downloads (with size limit), parses, saves an M3U file ONLY IF it contains 'Bein',
    and sorts groups before saving. Skips files > MAX_SIZE_BYTES.
    Args:
        m3u_url: The URL of the M3U file.
        file_index: The index for naming the output file.
        output_folder: The directory to save the file.
    Returns:
        True if processed and saved successfully, False otherwise.
    """
    output_filename = f"M3U{file_index}.m3u"
    output_filepath = os.path.join(output_folder, output_filename)
    success = False
    downloaded_size = 0
    expected_size = None
    m3u_content_bytes = None
    session = requests.Session()
    # Add a small random delay before starting? Might help with massive concurrency.
    # time.sleep(random.uniform(0, 0.5))

    print_colored(f"https://www.ibm.com/support/pages/node/520321/stub Attempt: {m3u_url}", "cyan")

    # 1. Initial Request and Size Check (if possible)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
            'Accept': '*/*', # Be more lenient with accept header
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        # --- *** TIMEOUT REMAINS 30 SECONDS *** ---
        response = session.get(m3u_url, timeout=30, headers=headers, stream=True, allow_redirects=True)
        response.raise_for_status()

        # --- *** SIZE CHECK BASED ON Content-Length *** ---
        content_length_str = response.headers.get('Content-Length')
        if content_length_str:
            try:
                expected_size = int(content_length_str)
                if expected_size > MAX_SIZE_BYTES:
                    print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Skipping: Size ({expected_size / 1024 / 1024:.1f}MB) exceeds limit ({MAX_SIZE_MB}MB) based on Content-Length.", "magenta")
                    response.close() # Close the connection without reading body
                    session.close()
                    return False # Skip this file
                # else: # Optional: log expected size if within limit
                #    print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Expected size: {expected_size / 1024 / 1024:.2f} MB (within limit).", "cyan")
            except ValueError:
                expected_size = None # Treat invalid Content-Length as unknown

        # 2. Download content chunk by chunk with size monitoring
        content_buffer = io.BytesIO()
        current_download_size = 0
        for chunk in response.iter_content(chunk_size=65536): # Larger chunk size for potentially faster downloads
            if chunk:
                content_buffer.write(chunk)
                current_download_size += len(chunk)
                # --- *** SIZE CHECK DURING DOWNLOAD *** ---
                if current_download_size > MAX_SIZE_BYTES:
                    print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Skipping: Download exceeded size limit ({MAX_SIZE_MB}MB) during transfer.", "magenta")
                    response.close() # Stop reading
                    content_buffer.close() # Discard buffer
                    session.close()
                    return False # Skip this file

        m3u_content_bytes = content_buffer.getvalue()
        downloaded_size = current_download_size # Final size is the accumulated size
        content_buffer.close() # Release memory buffer

        # Final check: Incomplete download if server closed connection early but size is still acceptable
        if expected_size is not None and downloaded_size < expected_size:
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Warning: Final size {downloaded_size} less than expected {expected_size}. File might be incomplete.", "yellow")

        if downloaded_size == 0 and expected_size != 0:
             raise ValueError("Downloaded content is empty.")

        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Downloaded {downloaded_size / 1024 / 1024:.2f} MB.", "cyan")

    except requests.exceptions.Timeout:
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Timeout (30s).", "red")
        return False
    except requests.exceptions.RequestException as e:
        status_code = getattr(e.response, 'status_code', 'N/A')
        # Log common informative errors
        if status_code == 404:
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Not Found (404).", "red")
        elif status_code == 403:
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Forbidden (403). Check UA/Headers/IP?", "red")
        elif status_code in [500, 502, 503, 504]:
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Server Error ({status_code}).", "red")
        else:
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Download: {type(e).__name__} (Status: {status_code})", "red")
        return False
    except ValueError as e:
         print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Download: {e}", "red")
         return False
    except Exception as e:
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Download (Unexpected): {type(e).__name__} - {e}", "red")
        # Optionally print full traceback for unexpected errors
        # print(traceback.format_exc())
        return False
    finally:
         # Ensure session is closed even if errors occurred before assignment
         if 'session' in locals() and session:
              session.close()


    # 3. Parse the downloaded content and Check for 'Bein' (only if downloaded)
    try:
        if m3u_content_bytes is None: # Should not happen if download logic is correct, but check anyway
            print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Internal Error: Content bytes are None before parsing.", "red")
            return False

        m3u_text_content = m3u_content_bytes.decode('utf-8', errors='ignore')

        if not m3u_text_content.strip().startswith('#EXTM3U'):
            print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Not valid M3U (no #EXTM3U). Skipping.", "red")
            return False

        channels, unique_groups, found_bein = parse_m3u_content(m3u_text_content)

        if not channels:
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Warning: No channels parsed. Skipping.", "yellow")
             return False

        # --- CORE LOGIC: SKIP IF 'Bein' IS NOT FOUND ---
        if not found_bein:
            print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Skipping: No 'Bein' group.", "magenta")
            return False
        # else: # Reduce verbosity
        #      print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub 'Bein' group found. Proceeding...", "cyan")

    except Exception as e:
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Parsing: {type(e).__name__} - {e}", "red")
        return False

    # 4. Sort Groups
    try:
        sorted_group_names = sort_groups(unique_groups)
        # print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Groups sorted.", "cyan") # Reduce verbosity

    except Exception as e:
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Sorting Groups: {type(e).__name__}", "red")
        return False

    # 5. Reconstruct M3U and Save
    temp_filepath = output_filepath + f".{os.getpid()}.tmp" # Add PID for more unique temp names
    try:
        os.makedirs(output_folder, exist_ok=True)

        with open(temp_filepath, 'wb') as f:
            f.write(b'#EXTM3U\n')

            channels_written = 0
            valid_channels_count = sum(1 for ch in channels if ch.get('url'))

            for group_name in sorted_group_names:
                for channel in channels:
                    if channel.get('group_title') == group_name and channel.get('url'):
                        extinf_parts = [f"#EXTINF:{channel.get('duration', -1)}"]
                        attributes = channel.get('attributes', {})
                        attributes['group-title'] = group_name
                        for key, value in attributes.items():
                             safe_value = str(value).replace('"', "'")
                             extinf_parts.append(f'{key}="{safe_value}"')

                        extinf_line = " ".join(extinf_parts) + f",{channel.get('name', 'Unnamed Channel')}"

                        try:
                            f.write(extinf_line.encode('utf-8', errors='ignore') + b'\n')
                            f.write(channel['url'].encode('utf-8', errors='ignore') + b'\n')
                            channels_written += 1
                        except Exception as write_err:
                             # Log write errors less verbosely or collect them
                             pass # print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error writing channel '{channel.get('name')}': {write_err}", "yellow")


            if channels_written != valid_channels_count:
                 print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Warning: Channel write count mismatch ({channels_written}/{valid_channels_count})", "yellow")

        # Atomic move/replace
        if os.path.exists(output_filepath):
             os.remove(output_filepath) # Remove existing file first on some systems for reliability
        shutil.move(temp_filepath, output_filepath)

        final_size = os.path.getsize(output_filepath)
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Saved: {output_filename} ({final_size / 1024 / 1024:.2f} MB)", "green")
        success = True

    except Exception as e:
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Saving File: {type(e).__name__} - {e}", "red")
        success = False
    finally:
        # Clean up temp file if move failed or error occurred
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError:
                pass

    # Clean up final file if saving clearly failed
    if not success and os.path.exists(output_filepath):
        try:
            os.remove(output_filepath)
        except OSError:
            pass

    return success


# --- Main Function ---
def main() -> None:
    """Main function to read URLs, download, process, and save M3U files."""
    # --- Parameters ---
    input_file = "m3ulinks.txt" # Input file name
    output_folder = "specialiptvs" # Output folder
    max_concurrent_workers = 2000 # Max concurrent workers
    # MAX_SIZE_MB is defined as a constant at the top

    start_time = time.time()

    print_colored(f"--- M3U Downloader & Processor ---", "magenta")
    print_colored(f"Input file: '{input_file}'", "cyan")
    print_colored(f"Output folder: '{output_folder}'", "cyan")
    print_colored(f"Required Group: 'Bein' (case-insensitive)", "yellow")
    print_colored(f"Max File Size: {MAX_SIZE_MB} MB", "yellow")
    print_colored(f"--- WARNING: Max concurrent workers set to {max_concurrent_workers}! ---", "red")
    print_colored(f"--- High worker count likely unstable & may cause IP blocks! ---", "red")
    print_colored(f"--- Download timeout set to 30 seconds. ---", "yellow")
    print_colored(f"----------------------------------", "magenta")


    m3u_urls = get_m3u_urls_from_file(input_file)
    if not m3u_urls:
        if not os.path.exists(input_file):
             print_colored(f"Input file '{input_file}' not found. Exiting.", "red")
        else:
             print_colored(f"No valid URLs found in '{input_file}'. Exiting.", "red")
        sys.exit(1) # Exit if no URLs


    # Clean and prepare output directory
    if os.path.exists(output_folder):
         print_colored(f"Removing existing output folder: {output_folder}...", "yellow")
         try:
            shutil.rmtree(output_folder)
            time.sleep(0.5)
         except OSError as e:
             print_colored(f"Warning: Could not remove folder '{output_folder}': {e}.", "yellow")

    try:
        os.makedirs(output_folder, exist_ok=True)
    except OSError as e:
         print_colored(f"Fatal Error: Could not create output folder '{output_folder}': {e}", "red")
         sys.exit(1)


    print_colored(f"Starting parallel processing of {len(m3u_urls)} M3U files...", "magenta")

    processed_count = 0
    saved_count = 0
    skipped_size_count = 0
    skipped_no_bein_count = 0
    error_count = 0

    # Global flag to signal shutdown
    shutdown_flag = False
    def signal_handler(sig, frame):
        nonlocal shutdown_flag
        if not shutdown_flag: # Prevent multiple prints if Ctrl+C is hit repeatedly
             print_colored('\nCtrl+C detected. Attempting graceful shutdown (may take time)...', 'yellow')
             shutdown_flag = True
    signal.signal(signal.SIGINT, signal_handler)

    try:
        with ThreadPoolExecutor(max_workers=max_concurrent_workers) as executor:
            futures = {
                executor.submit(download_process_and_save_m3u, m3u_url, idx, output_folder): (idx, m3u_url)
                for idx, m3u_url in enumerate(m3u_urls, start=1)
            }

            for future in as_completed(futures):
                # Check shutdown flag before processing next result
                if shutdown_flag:
                    # Optionally try to cancel pending futures (though not guaranteed)
                    # for f in futures:
                    #     if not f.done(): f.cancel()
                    print_colored("Shutdown signaled, stopping result processing.", "yellow")
                    break

                idx, url = futures[future]
                processed_count += 1
                try:
                    was_successful = future.result()
                    if was_successful:
                        saved_count += 1
                    else:
                        # Can't easily distinguish reason here, rely on function logs
                        error_count += 1 # Increment general non-save counter
                except Exception as e:
                    print_colored(f"Critical error retrieving result for URL #{idx}: {e}", "red")
                    error_count += 1

    except Exception as e:
         print_colored(f"\nFatal error during thread pool execution: {type(e).__name__} - {e}", "red")
         error_count = len(m3u_urls) - saved_count # Assume remaining failed


    end_time = time.time()
    duration = end_time - start_time

    # Final Summary (Counts for skipped reasons are not precise from here)
    print_colored(f"\n--- Processing Summary ---", "magenta")
    print_colored(f"Total URLs attempted: {len(m3u_urls)}", "cyan")
    print_colored(f"Successfully saved (contained 'Bein', <= {MAX_SIZE_MB}MB): {saved_count}", "green")
    print_colored(f"Skipped or Failed: {error_count + (processed_count - saved_count - error_count)}", "red") # Estimate skipped based on difference
    print_colored(f"(Check logs for skips: size limit, no 'Bein', errors)", "yellow")
    print_colored(f"Total processing time: {duration:.2f} seconds", "cyan")
    print_colored(f"--------------------------", "magenta")

# --- Entry Point ---
if __name__ == "__main__":
    if sys.version_info < (3, 7):
        print_colored("Error: This script requires Python 3.7 or higher.", "red")
        sys.exit(1)

    main()
