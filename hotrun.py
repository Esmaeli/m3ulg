import requests
import time
import os
import shutil
import re # Import regular expressions for parsing
import io  # Import for handling bytes in memory
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys # Import sys for version check and exit

# --- Helper Function for Colored Output ---
def print_colored(text: str, color: str) -> None:
    """Prints colored text to the console."""
    colors = {
        "green": "\033[92m", "red": "\033[91m", "yellow": "\033[93m",
        "cyan": "\033[96m", "magenta": "\033[95m", "white": "\033[97m"
    }
    # Simple check for basic terminal color support
    if os.getenv('TERM') and 'color' in os.getenv('TERM') and sys.stdout.isatty():
        print(f"{colors.get(color.lower(), '')}{text}\033[0m")
    else:
        print(text) # Print without color if support seems absent or redirected

# --- Function to Read M3U URLs from File ---
def get_m3u_urls_from_file(file_path: str) -> List[str]:
    """Reads M3U URLs from a file, ignoring comments and empty lines."""
    m3u_urls = []
    try:
        # Explicitly handle potential encoding errors during file read
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # Basic validation: check if it looks like a URL
                        if line.startswith('http://') or line.startswith('https://'):
                            m3u_urls.append(line)
                        else:
                            print_colored(f"Warning: Line {line_num} in '{file_path}' does not look like a valid URL: '{line}'. Skipping.", "yellow")
                except Exception as e:
                    print_colored(f"Warning: Could not process line {line_num} in '{file_path}': {e}", "yellow")
        print_colored(f"Successfully read {len(m3u_urls)} potential URLs from {file_path}", "green")
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
    # Normalize group names for case-insensitive comparison, keeping original casing
    # Handle potential non-string group names gracefully
    normalized_map = {str(name).lower(): str(name) for name in group_names if isinstance(name, (str, bytes, int, float))}
    lower_groups_unique = list(normalized_map.keys())

    priority1_lower = []
    priority2_lower = []
    processed_lower = set()

    # Define search terms and their conditions
    p1_terms = [('iran', lambda g: 'iran' in g),
                ('persian', lambda g: 'persian' in g),
                ('ir', lambda g: 'ir' in g and 'iraq' not in g and 'ireland' not in g)]

    p2_terms = [('bein', lambda g: 'bein' in g),
                ('sport', lambda g: 'sport' in g),
                ('spor', lambda g: 'spor' in g),
                ('canal+', lambda g: 'canal+' in g),
                ('dazn', lambda g: 'dazn' in g),
                ('paramount', lambda g: 'paramount' in g)]

    # Populate Priority 1
    for _, condition in p1_terms:
        for group_lower in list(lower_groups_unique): # Iterate over a copy
            if condition(group_lower) and group_lower not in processed_lower:
                priority1_lower.append(group_lower)
                processed_lower.add(group_lower)

    # Populate Priority 2
    for _, condition in p2_terms:
        for group_lower in list(lower_groups_unique): # Iterate over a copy
            if condition(group_lower) and group_lower not in processed_lower:
                priority2_lower.append(group_lower)
                processed_lower.add(group_lower)

    # Other groups (sorted alphabetically for consistency)
    other_groups_lower = sorted([
        g for g in lower_groups_unique if g not in processed_lower
    ])

    # Map back to original casing using the dictionary
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
        - List of channel dictionaries (keys: 'name', 'attributes', 'url', 'group_title', 'raw_extinf')
        - List of all unique group titles found.
        - Boolean indicating if a 'Bein' group was found (True if found, False otherwise).
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
            name = ""
            group_title = "General" # Default group
            duration = -1

            match_extinf = re.match(r'#EXTINF:(?P<duration>-?\d+)(?P<attributes_str>.*?),\s*(?P<name>.*)', line)
            if match_extinf:
                try:
                    duration = int(match_extinf.group('duration'))
                except ValueError:
                    duration = -1 # Default if duration is not a valid number
                name = match_extinf.group('name').strip()
                attributes_str = match_extinf.group('attributes_str').strip()

                # Improved attribute parsing to handle potential edge cases
                try:
                    attr_matches = re.findall(r'([a-zA-Z0-9_\-]+)=?(?:"([^"]*)"|([^ ]*))', attributes_str)
                    for key, val_quoted, val_unquoted in attr_matches:
                        attributes[key] = val_quoted if val_quoted else val_unquoted
                except Exception:
                    print_colored(f"Warning: Could not parse attributes string: {attributes_str}", "yellow")


                group_title_raw = attributes.get('group-title', "General")
                # Ensure group_title is a string before lowercasing
                group_title = str(group_title_raw) if group_title_raw is not None else "General"

                if "bein" in group_title.lower():
                    found_bein = True

                group_titles.add(group_title)

            url = ""
            j = i + 1
            # Look ahead more carefully for the URL
            while j < len(lines):
                next_line = lines[j].strip()
                # Check if the line is likely a URL (simple check)
                if next_line and not next_line.startswith('#'):
                    url = next_line
                    i = j # Consume the URL line
                    break
                # Stop if we hit the next channel entry or a directive that isn't #EXTVLCOPT etc.
                elif next_line.startswith('#EXTINF:') or next_line.startswith('#EXTM3U') or next_line.startswith('#EXT-X-'):
                     break
                j += 1

            # Store channel info only if EXTINF was successfully parsed
            if match_extinf:
                channels.append({
                    'duration': duration,
                    'name': name if name else 'Unnamed Channel',
                    'attributes': attributes,
                    'url': url if url else '', # Store empty string if no URL found
                    'group_title': group_title,
                    'raw_extinf': raw_extinf # Keep original for potential debugging
                })
                if not url:
                     # Reduce log noise: only log if name was present
                     if name:
                         print_colored(f"Warning: No URL found for '{name}' after {raw_extinf}", "yellow")


        i += 1 # Move to the next line

    # Return unique groups as a list
    return channels, list(group_titles), found_bein


# --- Download/Process Function: Save ONLY if 'Bein' is Present ---
def download_process_and_save_m3u(m3u_url: str, file_index: int, output_folder: str) -> bool:
    """
    Downloads, parses, saves an M3U file ONLY IF it contains a 'Bein' group,
    and sorts groups before saving.
    Args:
        m3u_url: The URL of the M3U file.
        file_index: The index for naming the output file.
        output_folder: The directory to save the file.
    Returns:
        True if processed and saved successfully (contained 'Bein'), False otherwise.
    """
    output_filename = f"M3U{file_index}.m3u"
    output_filepath = os.path.join(output_folder, output_filename)
    success = False
    downloaded_size = 0
    expected_size = None
    m3u_content_bytes = None
    session = requests.Session() # Use a session for potential connection reuse per worker

    print_colored(f"https://www.ibm.com/support/pages/node/520321/stub Attempting: {m3u_url}", "cyan")

    # 1. Download content into memory (BytesIO)
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36', # More common UA
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', # Common Accept header
                   'Accept-Encoding': 'gzip, deflate', # Allow compressed responses
                   'Connection': 'keep-alive' # Request keep-alive
                  }
        # --- *** TIMEOUT CHANGED TO 30 SECONDS *** ---
        response = session.get(m3u_url, timeout=30, headers=headers, stream=True, allow_redirects=True) # Follow redirects
        response.raise_for_status() # Check for HTTP errors (4xx, 5xx)

        content_length_str = response.headers.get('Content-Length')
        if content_length_str:
            try:
                expected_size = int(content_length_str)
            except ValueError:
                expected_size = None

        content_buffer = io.BytesIO()
        for chunk in response.iter_content(chunk_size=32768): # Increased chunk size
            if chunk:
                content_buffer.write(chunk)

        m3u_content_bytes = content_buffer.getvalue()
        downloaded_size = len(m3u_content_bytes)
        content_buffer.close() # Release memory buffer

        if expected_size is not None and downloaded_size < expected_size: # Check if downloaded is LESS than expected
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Download incomplete (DL: {downloaded_size}, Exp: {expected_size}).", "red")
             return False # Treat incomplete download as error

        if downloaded_size == 0 and expected_size != 0: # Allow 0-byte files if server explicitly says Content-Length: 0
             raise ValueError("Downloaded content is empty.")

        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Downloaded {downloaded_size / 1024 / 1024:.2f} MB.", "cyan")

    except requests.exceptions.Timeout:
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Request Timed Out (30s).", "red")
        return False
    except requests.exceptions.SSLError as e:
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: SSL verification failed. {e}", "red")
        return False
    except requests.exceptions.ConnectionError as e:
         print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Connection Failed. {e}", "red")
         return False
    except requests.exceptions.RequestException as e: # Catch other request errors
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Download: {type(e).__name__} - Status: {getattr(e.response, 'status_code', 'N/A')}", "red")
        return False
    except ValueError as e:
         print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Download: {e}", "red")
         return False
    except Exception as e:
        # Log unexpected errors during download phase more clearly
        import traceback
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Download (Unexpected): {type(e).__name__} - {e}\n{traceback.format_exc()}", "red")
        return False
    finally:
        session.close() # Close the session for this worker


    # 2. Parse the downloaded content and Check for 'Bein'
    try:
        # Decode using UTF-8, ignore errors
        m3u_text_content = m3u_content_bytes.decode('utf-8', errors='ignore')

        # Minimal validation
        if not m3u_text_content.strip().startswith('#EXTM3U'):
            print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error: Not valid M3U (no #EXTM3U). Skipping.", "red")
            return False

        channels, unique_groups, found_bein = parse_m3u_content(m3u_text_content)

        if not channels:
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Warning: No channels parsed. Skipping.", "yellow")
             return False

        # --- *** CORE LOGIC: SKIP IF 'Bein' IS NOT FOUND *** ---
        if not found_bein:
            print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Skipping: No 'Bein' group.", "magenta")
            return False
        else:
             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub 'Bein' group found. Proceeding...", "cyan")

    except Exception as e:
        import traceback
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Parsing: {type(e).__name__} - {e}\n{traceback.format_exc()}", "red")
        return False

    # 3. Sort Groups (only if 'Bein' was found)
    try:
        sorted_group_names = sort_groups(unique_groups)
        # print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Groups sorted.", "cyan") # Reduce verbosity

    except Exception as e:
        import traceback
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Sorting Groups: {type(e).__name__}\n{traceback.format_exc()}", "red")
        return False

    # 4. Reconstruct M3U and Save (only if 'Bein' was found)
    try:
        # Ensure output folder exists right before writing
        os.makedirs(output_folder, exist_ok=True)

        # Write atomically: write to temp file then rename
        temp_filepath = output_filepath + ".tmp"

        with open(temp_filepath, 'wb') as f:
            f.write(b'#EXTM3U\n')

            channels_written = 0
            valid_channels_count = 0
            for channel in channels:
                 if channel.get('url'): # Count channels that should be written
                      valid_channels_count +=1

            for group_name in sorted_group_names:
                for channel in channels:
                    if channel.get('group_title') == group_name and channel.get('url'):
                        # Build EXTINF line robustly
                        extinf_parts = [f"#EXTINF:{channel.get('duration', -1)}"]
                        attributes = channel.get('attributes', {})
                        # Ensure group-title is correct and present
                        attributes['group-title'] = group_name
                        for key, value in attributes.items():
                             # Basic quoting/escaping: replace " with ' inside value
                             safe_value = str(value).replace('"', "'")
                             extinf_parts.append(f'{key}="{safe_value}"')

                        extinf_line = " ".join(extinf_parts) + f",{channel.get('name', 'Unnamed Channel')}"

                        # Write lines, ignore encoding errors for robustness
                        try:
                            f.write(extinf_line.encode('utf-8', errors='ignore') + b'\n')
                            f.write(channel['url'].encode('utf-8', errors='ignore') + b'\n')
                            channels_written += 1
                        except Exception as write_err:
                             print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error writing channel '{channel.get('name')}': {write_err}", "yellow")


            if channels_written != valid_channels_count:
                 print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Warning: Channel count mismatch (Written: {channels_written}, Parsed w/ URL: {valid_channels_count})", "yellow")

        # Atomically replace final file
        shutil.move(temp_filepath, output_filepath)

        final_size = os.path.getsize(output_filepath)
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Saved: {output_filename} ({final_size / 1024 / 1024:.2f} MB)", "green")
        success = True

    except IOError as e:
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Saving File (IO): {e}", "red")
        success = False
    except OSError as e:
         print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Saving File (OS): {e}", "red")
         success = False
    except Exception as e:
        import traceback
        print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Error Saving File (Unexpected): {type(e).__name__}\n{traceback.format_exc()}", "red")
        success = False
    finally:
        # Clean up temp file if it still exists (e.g., due to error during move)
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError:
                pass


    # Clean up final file if process failed AFTER writing started but before success
    if not success and os.path.exists(output_filepath):
        try:
            os.remove(output_filepath)
            print_colored(f"  https://www.ibm.com/support/pages/node/520321/stub Cleaned up failed output file.", "yellow")
        except OSError:
            pass

    return success


# --- Main Function ---
def main() -> None:
    """Main function to read URLs, download, process, and save M3U files."""
    # --- *** PARAMETERS CHANGED AS REQUESTED *** ---
    input_file = "m3ulinks.txt" # Input file name changed
    output_folder = "specialiptvs" # Output folder remains the same
    max_concurrent_workers = 800 # Number of workers increased to 800

    start_time = time.time()

    print_colored(f"--- M3U Downloader & Processor ---", "magenta")
    print_colored(f"Input file: '{input_file}'", "cyan")
    print_colored(f"Output folder: '{output_folder}'", "cyan")
    print_colored(f"Required Group: 'Bein' (case-insensitive)", "yellow")
    print_colored(f"--- WARNING: Max concurrent workers set to {max_concurrent_workers}! ---", "red")
    print_colored(f"--- High worker count may cause instability, errors, or IP blocks! ---", "red")
    print_colored(f"--- Download timeout set to 30 seconds. ---", "yellow")
    print_colored(f"----------------------------------", "magenta")


    m3u_urls = get_m3u_urls_from_file(input_file)
    if not m3u_urls:
        # Check if file exists but was empty vs file not found
        if not os.path.exists(input_file):
             print_colored(f"Input file '{input_file}' not found. Exiting.", "red")
        else:
             print_colored(f"No valid URLs found in '{input_file}'. Exiting.", "red")
        return

    # Clean and prepare output directory
    if os.path.exists(output_folder):
         print_colored(f"Removing existing output folder: {output_folder}...", "yellow")
         try:
            shutil.rmtree(output_folder)
            time.sleep(0.5) # Brief pause
         except OSError as e:
             print_colored(f"Warning: Could not remove existing folder '{output_folder}': {e}. Files might be overwritten or errors may occur.", "yellow")
             # Consider exiting if removal fails? Or proceed with caution.
             # return

    try:
        os.makedirs(output_folder, exist_ok=True)
        print_colored(f"Output folder '{output_folder}' ensured.", "cyan")
    except OSError as e:
         print_colored(f"Fatal Error: Could not create output folder '{output_folder}': {e}", "red")
         return

    print_colored(f"Starting parallel processing of {len(m3u_urls)} M3U files...", "magenta")

    processed_count = 0
    saved_count = 0
    # skipped_missing_bein = 0 # Can't easily count this specific reason from main loop
    error_or_skipped_count = 0

    # Using ThreadPoolExecutor
    try:
        with ThreadPoolExecutor(max_workers=max_concurrent_workers) as executor:
            futures = {
                executor.submit(download_process_and_save_m3u, m3u_url, idx, output_folder): (idx, m3u_url)
                for idx, m3u_url in enumerate(m3u_urls, start=1)
            }

            # Process results as they complete
            for future in as_completed(futures):
                idx, url = futures[future]
                processed_count += 1
                try:
                    was_successful = future.result() # True if saved, False if skipped or error
                    if was_successful:
                        saved_count += 1
                    else:
                        # Logged reason is inside the worker function
                        error_or_skipped_count += 1

                except Exception as e:
                    # This catches errors *during* future.result() call (should be rare)
                    # or exceptions raised explicitly by the worker that weren't caught inside
                    print_colored(f"Critical error retrieving result for URL #{idx}: {e}", "red")
                    error_or_skipped_count += 1

                # Optional: Progress indicator
                # print(f"\rProcessed: {processed_count}/{len(m3u_urls)} | Saved: {saved_count} | Skipped/Errors: {error_or_skipped_count}", end="")

    except Exception as e:
         # Catch errors related to ThreadPoolExecutor creation or management
         import traceback
         print_colored(f"\nFatal error during thread pool execution: {type(e).__name__} - {e}\n{traceback.format_exc()}", "red")
         # Set counts based on progress before the error
         error_or_skipped_count = len(m3u_urls) - saved_count


    # Clear progress line if used
    # print()

    end_time = time.time()
    duration = end_time - start_time

    # Final Summary
    print_colored(f"\n--- Processing Summary ---", "magenta")
    print_colored(f"Total URLs attempted: {len(m3u_urls)}", "cyan")
    print_colored(f"Successfully saved (contained 'Bein'): {saved_count}", "green")
    print_colored(f"Skipped (no 'Bein') or Failed (Error): {error_or_skipped_count}", "red")
    print_colored(f"(Check console logs above for specific reasons for skips/failures)", "yellow")
    print_colored(f"Total processing time: {duration:.2f} seconds", "cyan")
    print_colored(f"--------------------------", "magenta")

# --- Entry Point ---
if __name__ == "__main__":
    # Basic check for Python version
    if sys.version_info < (3, 7): # Recommend 3.7+ for future compatibility and features
        print_colored("Error: This script requires Python 3.7 or higher.", "red")
        sys.exit(1)

    # Add handler for graceful exit on Ctrl+C
    import signal
    def signal_handler(sig, frame):
        print_colored('\nCtrl+C detected. Shutting down gracefully (may take a moment)...', 'yellow')
        # Ideally, we'd signal the executor to shutdown, but ThreadPoolExecutor lacks easy cancellation.
        # We'll just exit; ongoing tasks might complete or error out abruptly.
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    main()
