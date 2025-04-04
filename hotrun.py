import requests
import time
import os
import shutil
import re # Import regular expressions for parsing
import io  # Import for handling bytes in memory
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Helper Function for Colored Output (No changes needed) ---
def print_colored(text: str, color: str) -> None:
    """Prints colored text to the console."""
    colors = {
        "green": "\033[92m", "red": "\033[91m", "yellow": "\033[93m",
        "cyan": "\033[96m", "magenta": "\033[95m", "white": "\033[97m"
    }
    print(f"{colors.get(color.lower(), '')}{text}\033[0m")

# --- Function to Read M3U URLs from File (No changes needed) ---
def get_m3u_urls_from_file(file_path: str) -> List[str]:
    """Reads M3U URLs from a file, ignoring comments and empty lines."""
    m3u_urls = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith('#'):
                    m3u_urls.append(line)
        print_colored(f"Successfully read {len(m3u_urls)} URLs from {file_path}", "green")
    except FileNotFoundError:
        print_colored(f"Error: Input file '{file_path}' not found.", "red")
    except Exception as e:
        print_colored(f"Error reading file '{file_path}': {e}", "red")
    return m3u_urls

# --- Group Sorting Function (Provided by User) ---
def sort_groups(group_names: List[str]) -> List[str]:
    """
    Sort groups based on specific priority rules:
    1. First priority (exact order):
        - Groups containing 'iran' (case insensitive)
        - Then groups containing 'persian'
        - Then groups containing 'ir' (but not as part of 'iraq' or 'ireland')
    2. Second priority (exact order):
        - Groups containing 'bein' # This rule seems contradictory to skipping 'bein', but we follow the function as given
                                   # The skipping logic will override saving if 'bein' is present anywhere.
        - Then 'sport'
        - Then 'spor'
        - Then 'canal+'
        - Then 'dazn'
        - Then 'paramount'
    3. All other groups alphabetically
       (Modified slightly from original 'original order' to provide deterministic sorting for remaining groups)

    Args:
        group_names: List of group names to sort
    Returns:
        Sorted list of group names
    """
    # Normalize group names for case-insensitive comparison
    normalized_map = {name.lower(): name for name in group_names}
    lower_groups_unique = list(normalized_map.keys())

    priority1_lower = []
    priority2_lower = []
    processed_lower = set()

    # Define search terms
    p1_terms = [('iran', lambda g: 'iran' in g),
                ('persian', lambda g: 'persian' in g),
                ('ir', lambda g: 'ir' in g and 'iraq' not in g and 'ireland' not in g)]

    p2_terms = [('bein', lambda g: 'bein' in g), # Keep in sort logic as per function
                ('sport', lambda g: 'sport' in g),
                ('spor', lambda g: 'spor' in g),
                ('canal+', lambda g: 'canal+' in g),
                ('dazn', lambda g: 'dazn' in g),
                ('paramount', lambda g: 'paramount' in g)]

    # Populate Priority 1
    for _, condition in p1_terms:
        for group_lower in lower_groups_unique:
            if condition(group_lower) and group_lower not in processed_lower:
                priority1_lower.append(group_lower)
                processed_lower.add(group_lower)

    # Populate Priority 2
    for _, condition in p2_terms:
        for group_lower in lower_groups_unique:
            if condition(group_lower) and group_lower not in processed_lower:
                priority2_lower.append(group_lower)
                processed_lower.add(group_lower)

    # Other groups (sorted alphabetically for consistency)
    other_groups_lower = sorted([
        g for g in lower_groups_unique if g not in processed_lower
    ])

    # Map back to original casing
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

            # Basic parsing of EXTINF line
            match_extinf = re.match(r'#EXTINF:(?P<duration>-?\d+)(?P<attributes_str>.*?),\s*(?P<name>.*)', line)
            if match_extinf:
                name = match_extinf.group('name').strip()
                attributes_str = match_extinf.group('attributes_str').strip()

                # Parse attributes (key="value" pairs)
                attr_matches = re.findall(r'([a-zA-Z0-9-]+)="([^"]*)"', attributes_str)
                attributes = dict(attr_matches)
                group_title = attributes.get('group-title', "General") # Get group title

                # Check for 'Bein' group (case-insensitive)
                if "bein" in group_title.lower():
                    found_bein = True
                
                group_titles.add(group_title) # Add to set of unique groups

            # Find the URL on the next non-comment, non-empty line(s)
            url = ""
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if next_line and not next_line.startswith('#'):
                    url = next_line
                    i = j # Advance main loop counter past the URL
                    break
                elif next_line.startswith('#EXT'): # Stop if we hit the next channel entry
                     break
                j += 1

            if name and url:
                channels.append({
                    'name': name,
                    'attributes': attributes, # Store all attributes
                    'url': url,
                    'group_title': group_title, # Store extracted group title
                    'raw_extinf': raw_extinf # Store original EXTINF line if needed
                })
            elif raw_extinf:
                 # Handle cases where EXTINF might not have a URL immediately after (less common)
                 print_colored(f"Warning: Could not find URL for {raw_extinf}", "yellow")

        i += 1 # Move to the next line

    return channels, list(group_titles), found_bein


# --- Modified Download Function with Parsing, Skipping, and Sorting ---
def download_process_and_save_m3u(m3u_url: str, file_index: int, output_folder: str) -> bool:
    """
    Downloads, parses, checks for 'Bein', sorts by group, and saves an M3U file.
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

    print_colored(f"Attempting Download & Process URL #{file_index}: {m3u_url}", "cyan")

    # 1. Download content into memory (BytesIO)
    try:
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept-Encoding': 'gzip, deflate'}
        response = requests.get(m3u_url, timeout=30, headers=headers, stream=True)
        response.raise_for_status()

        content_length_str = response.headers.get('Content-Length')
        if content_length_str:
            try:
                expected_size = int(content_length_str)
                print_colored(f"  https://www.ibm.com/docs/en/filenet-p8-platform/5.6.0?topic=failures-identifying-object-indexing Expected size: {expected_size / 1024 / 1024:.2f} MB", "yellow")
            except ValueError:
                expected_size = None

        # Read content into a BytesIO buffer
        content_buffer = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                content_buffer.write(chunk)
        
        m3u_content_bytes = content_buffer.getvalue()
        downloaded_size = len(m3u_content_bytes)
        content_buffer.close()

        if expected_size is not None and downloaded_size != expected_size:
            print_colored(f"  https://www.ibm.com/docs/en/filenet-p8-platform/5.6.0?topic=failures-identifying-object-indexing Warning: Download size mismatch ({downloaded_size} vs {expected_size} expected). Processing anyway.", "yellow")
            # Decide if mismatch should be fatal? For now, we process it.
            # raise ValueError("Download size mismatch") # Uncomment to make it fatal

        if downloaded_size == 0:
             raise ValueError("Downloaded content is empty.")

        print_colored(f"  https://www.ibm.com/docs/en/filenet-p8-platform/5.6.0?topic=failures-identifying-object-indexing Downloaded {downloaded_size / 1024 / 1024:.2f} MB successfully.", "cyan")

    except requests.exceptions.Timeout:
        print_colored(f"Error downloading URL #{file_index}: Request timed out.", "red")
        return False
    except requests.exceptions.RequestException as e:
        print_colored(f"Error downloading URL #{file_index}: {type(e).__name__} - {e}", "red")
        return False
    except ValueError as e:
         print_colored(f"Error during download URL #{file_index}: {e}", "red")
         return False
    except Exception as e:
        print_colored(f"Unexpected download error URL #{file_index}: {type(e).__name__} - {e}", "red")
        return False

    # 2. Parse the downloaded content and Check for 'Bein'
    try:
        m3u_text_content = m3u_content_bytes.decode('utf-8', errors='ignore')
        
        # Check for #EXTM3U header
        if not m3u_text_content.strip().startswith('#EXTM3U'):
            print_colored(f"Error: File from URL #{file_index} is not a valid M3U file (missing #EXTM3U header). Skipping.", "red")
            return False
            
        channels, unique_groups, found_bein = parse_m3u_content(m3u_text_content)

        if not channels:
             print_colored(f"Warning: No channels found in M3U file from URL #{file_index}. Skipping save.", "yellow")
             return False # Treat as failure if no channels parsed

        if found_bein:
            print_colored(f"Skipping save for URL #{file_index}: Contains 'Bein' group.", "magenta")
            return False # Indicate failure as it's skipped

    except Exception as e:
        print_colored(f"Error parsing M3U content for URL #{file_index}: {type(e).__name__} - {e}", "red")
        return False

    # 3. Sort Groups (if not skipped)
    try:
        print_colored(f"  https://www.ibm.com/docs/en/filenet-p8-platform/5.6.0?topic=failures-identifying-object-indexing Found {len(unique_groups)} unique groups. Sorting...", "cyan")
        sorted_group_names = sort_groups(unique_groups)
        # print_colored(f"  https://www.ibm.com/docs/en/filenet-p8-platform/5.6.0?topic=failures-identifying-object-indexing Sorted Group Order: {', '.join(sorted_group_names[:5])}...", "cyan") # Optional: Log sorted order

    except Exception as e:
        print_colored(f"Error sorting groups for URL #{file_index}: {type(e).__name__} - {e}", "red")
        return False

    # 4. Reconstruct M3U and Save
    try:
        os.makedirs(output_folder, exist_ok=True)
        
        # Use binary write mode again for saving to avoid encoding issues
        with open(output_filepath, 'wb') as f: 
            f.write(b'#EXTM3U\n') # Write header as bytes

            # Write channels in the sorted group order
            channels_written = 0
            for group_name in sorted_group_names:
                for channel in channels:
                    if channel['group_title'] == group_name:
                        # Reconstruct EXTINF line carefully
                        # Start with basic duration and name
                        extinf_line = f"#EXTINF:-1" 
                        # Add attributes back, ensuring group-title is correct
                        channel['attributes']['group-title'] = group_name # Ensure consistent group title
                        for key, value in channel['attributes'].items():
                             extinf_line += f' {key}="{value}"'
                        extinf_line += f",{channel['name']}"
                        
                        f.write(extinf_line.encode('utf-8') + b'\n')
                        f.write(channel['url'].encode('utf-8') + b'\n')
                        channels_written += 1
            
            # Optional: Write channels that might have been missed (e.g., parsing errors)
            if channels_written != len(channels):
                 print_colored(f"Warning: Mismatch in channel count for URL #{file_index}. Written: {channels_written}, Parsed: {len(channels)}", "yellow")

        # Final size verification after saving
        final_size = os.path.getsize(output_filepath)
        print_colored(f"Successfully Processed & Saved {output_filename} ({final_size / 1024 / 1024:.2f} MB).", "green")
        success = True

    except IOError as e:
        print_colored(f"Error saving final file {output_filename}: {e}", "red")
        success = False
    except Exception as e:
        print_colored(f"Unexpected error saving file {output_filename}: {type(e).__name__} - {e}", "red")
        success = False

    # Clean up file if saving failed
    if not success and os.path.exists(output_filepath):
        try:
            os.remove(output_filepath)
        except OSError:
            pass # Ignore error during cleanup

    return success


# --- Main Function (Adjusted Concurrency) ---
def main() -> None:
    """Main function to read URLs, download, process, and save M3U files."""
    input_file = "fixm3u.txt"
    output_folder = "specialiptvs"
    # Keep concurrency low for stability during download AND processing
    max_concurrent_workers = 500 # Adjusted concurrency

    try:
        print_colored(f"Starting M3U file download & processing from '{input_file}'...", "magenta")
        print_colored(f"Output folder: '{output_folder}'", "magenta")
        print_colored(f"Max concurrent workers: {max_concurrent_workers}", "magenta")

        m3u_urls = get_m3u_urls_from_file(input_file)
        if not m3u_urls:
            print_colored("No valid M3U URLs found. Exiting.", "red")
            return

        if os.path.exists(output_folder):
             print_colored(f"Removing existing output folder: {output_folder}...", "yellow")
             shutil.rmtree(output_folder, ignore_errors=True)
             time.sleep(0.5)
        try:
            os.makedirs(output_folder, exist_ok=True)
            print_colored(f"Ensured output folder '{output_folder}' exists.", "cyan")
        except OSError as e:
             print_colored(f"Fatal Error: Could not create output folder '{output_folder}': {e}", "red")
             return

        print_colored(f"Starting parallel download & processing of {len(m3u_urls)} M3U files...", "magenta")

        processed_count = 0
        saved_count = 0
        skipped_bein_count = 0
        error_count = 0

        with ThreadPoolExecutor(max_workers=max_concurrent_workers) as executor:
            futures = {
                executor.submit(download_process_and_save_m3u, m3u_url, idx, output_folder): (idx, m3u_url)
                for idx, m3u_url in enumerate(m3u_urls, start=1)
            }

            for future in as_completed(futures):
                idx, url = futures[future]
                processed_count += 1
                try:
                    was_successful = future.result() # Returns True if saved, False if skipped or error
                    if was_successful:
                        saved_count += 1
                    # We need to differentiate between skipped 'bein' and other errors
                    # The function prints the reason for False, so we just count general errors here
                    elif not was_successful:
                         # A bit simplistic, we rely on logs to know *why* it failed/was skipped
                         error_count +=1 # Count all non-saves as errors/skipped
                         # Note: The function already logs if it was skipped due to 'Bein'

                except Exception as e:
                    print_colored(f"Critical error retrieving result for URL #{idx}: {e}", "red")
                    error_count += 1

        # Adjust summary based on logs (since 'error_count' includes 'bein' skips)
        print_colored(f"\n--- Processing Summary ---", "magenta")
        print_colored(f"Total URLs processed: {processed_count}", "cyan")
        print_colored(f"Successfully saved files: {saved_count}", "green")
        print_colored(f"Files skipped or failed: {error_count}", "red")
        print_colored(f"(Check logs for reasons - includes 'Bein' skips and errors)", "yellow")
        print_colored(f"--------------------------", "magenta")

    except KeyboardInterrupt:
        print_colored("\nProcess interrupted by user.", "yellow")
    except Exception as e:
        print_colored(f"A fatal error occurred in the main process: {type(e).__name__} - {str(e)}", "red")

if __name__ == "__main__":
    # Make sure necessary modules are imported
    import sys
    if sys.version_info < (3, 6):
        print_colored("Error: This script requires Python 3.6 or higher.", "red")
        sys.exit(1)
    main()
