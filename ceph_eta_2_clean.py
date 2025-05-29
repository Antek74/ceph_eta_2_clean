#!/usr/bin/python

import subprocess
import time
import re
import argparse
import sys
from datetime import datetime, timedelta

def get_ceph_object_stats():
    """
    Runs 'ceph -s' and parses it to find degraded, misplaced, and total object counts.
    """
    try:
        # Run ceph -s once
        result = subprocess.run("ceph -s", shell=True, capture_output=True, text=True, check=True)
        output = result.stdout
        # print(f"DEBUG: ceph -s output:\n{output}") # Uncomment for debugging ceph -s output

        degraded_objects = 0
        misplaced_objects = 0
        total_objects_from_degraded = None
        total_objects_from_misplaced = None
        
        # Regex for "X/Y objects degraded"
        degraded_match = re.search(r'(\d+)/(\d+) objects degraded', output)
        if degraded_match:
            degraded_objects = int(degraded_match.group(1))
            total_objects_from_degraded = int(degraded_match.group(2))
            # print(f"DEBUG: Found degraded: {degraded_objects}/{total_objects_from_degraded}")

        # Regex for "A/B objects misplaced"
        misplaced_match = re.search(r'(\d+)/(\d+) objects misplaced', output)
        if misplaced_match:
            misplaced_objects = int(misplaced_match.group(1))
            total_objects_from_misplaced = int(misplaced_match.group(2))
            # print(f"DEBUG: Found misplaced: {misplaced_objects}/{total_objects_from_misplaced}")

        # Determine final total_objects
        # Priority: degraded line total, then misplaced line total
        final_total_objects = None
        if total_objects_from_degraded is not None:
            final_total_objects = total_objects_from_degraded
        elif total_objects_from_misplaced is not None:
            final_total_objects = total_objects_from_misplaced
        else:
            # Fallback: try to parse from summary if no degraded/misplaced lines with totals
            # Common formats: "objects:   NUM objects" or "num_objects: NUM"
            summary_match_objects_line = re.search(r'\s+objects:\s+(\d+)\s+objects', output)
            summary_match_num_objects = re.search(r'num_objects:\s*(\d+)', output) # Newer Ceph versions
            
            if summary_match_objects_line:
                final_total_objects = int(summary_match_objects_line.group(1))
                # print(f"DEBUG: Found total from summary (objects line): {final_total_objects}")
            elif summary_match_num_objects:
                final_total_objects = int(summary_match_num_objects.group(1))
                # print(f"DEBUG: Found total from summary (num_objects): {final_total_objects}")
            else:
                # If there are degraded/misplaced objects but we couldn't find total, it's an issue.
                if degraded_objects > 0 or misplaced_objects > 0:
                    print("Warning: Degraded/misplaced objects exist, but couldn't determine total objects from 'ceph -s'.")
                    return None, None, None # Indicate error
                else:
                    # No degraded, no misplaced, and no total found from specific lines.
                    # This could be an empty or very healthy cluster. Assume 0 total objects.
                    # print("DEBUG: No degraded/misplaced, and no total found in summary. Assuming 0 total.")
                    final_total_objects = 0
        
        if final_total_objects is None: # Should not happen if logic above is complete
            print("Error: Could not determine total number of objects.")
            return None, None, None

        return degraded_objects, misplaced_objects, final_total_objects

    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to run 'ceph -s'. Return code: {e.returncode}")
        if e.stdout: print(f"Stdout: {e.stdout.strip()}")
        if e.stderr: print(f"Stderr: {e.stderr.strip()}")
        return None, None, None
    except Exception as e:
        print(f"Error parsing 'ceph -s' output: {e}")
        return None, None, None

def calculate_eta(initial_count, current_count, elapsed_time_seconds):
    if elapsed_time_seconds == 0: # Avoid division by zero at the very start
        return float('inf')
    
    processed_count = initial_count - current_count
    
    if processed_count <= 0: # No progress or getting worse
        if current_count == 0 : # Already done
            return 0
        return float('inf') # No progress or getting worse, ETA is infinite

    rate_per_second = processed_count / elapsed_time_seconds
    
    if rate_per_second == 0 : # Should be caught by processed_count <=0, but defensive
         return float('inf')

    remaining_seconds = current_count / rate_per_second
    return remaining_seconds

def format_eta(eta_seconds):
    if eta_seconds == float('inf'):
        return "infinite (no progress or worsening)"
    if eta_seconds < 0: # Should ideally not happen with new calculate_eta logic
        return "N/A (getting worse)"
    if eta_seconds == 0:
        return "00:00:00 (completed)"
        
    eta_seconds = int(eta_seconds)
    days, remainder = divmod(eta_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if days > 0:
        return f"{days}d {hours:02}:{minutes:02}:{seconds:02}"
    else:
        return f"{hours:02}:{minutes:02}:{seconds:02}"

def get_local_time_from_utc_delta(delta_seconds):
    if delta_seconds == float('inf') or delta_seconds < 0:
        return "N/A"
    completion_time_utc = datetime.utcnow() + timedelta(seconds=delta_seconds)
    # Use date command for reliable local time conversion including timezone
    utc_time_str = completion_time_utc.strftime('%Y-%m-%d %H:%M:%S')
    try:
        result = subprocess.run(f"date -d '{utc_time_str} UTC' +'%Y-%m-%d %H:%M:%S %Z'", 
                                shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not convert UTC to local time using 'date' command: {e}. Falling back to simple UTC display.")
        return f"{completion_time_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC"


def main(sleep_interval):
    print(f"Ceph Recovery Estimator. Checking status every {sleep_interval} seconds.")
    print("Fetching initial Ceph status...")

    initial_degraded, initial_misplaced, initial_total_objects = get_ceph_object_stats()

    if initial_degraded is None: # Indicates an error from get_ceph_object_stats
        print("Could not get initial Ceph status. Exiting.")
        sys.exit(1)

    if initial_total_objects == 0 and (initial_degraded > 0 or initial_misplaced > 0) :
        print(f"Warning: Initial total objects is 0, but found {initial_degraded} degraded / {initial_misplaced} misplaced.")
        print("Percentage calculation might be misleading. Continuing...")
    
    print(f"Initial state: Degraded: {initial_degraded}, Misplaced: {initial_misplaced}, Total Objects: {initial_total_objects}")
    
    if initial_degraded == 0 and initial_misplaced == 0:
        print("Cluster is healthy. No degraded or misplaced objects found initially. Exiting.")
        sys.exit(0)

    start_time = time.time()
    last_check_time = start_time # For calculating ETA based on progress since last check

    # Store the initial values for ETA calculation relative to the absolute start
    # This avoids ETAs jumping around too much if recovery speed is inconsistent
    # but means the ETA is based on average speed since script start.
    abs_initial_degraded = initial_degraded
    abs_initial_misplaced = initial_misplaced

    print("Starting estimation loop. Press Ctrl+C to stop.")
    print("-" * 60)

    try:
        while True:
            time.sleep(sleep_interval)
            
            current_loop_time = time.time()
            # Elapsed time since the script started
            elapsed_since_start = current_loop_time - start_time

            current_degraded, current_misplaced, current_total_objects = get_ceph_object_stats()

            if current_degraded is None: # Error fetching current status
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Error fetching current Ceph status. Retrying next interval...")
                continue
            
            # Although total_objects can change, for % calculation, use the initial total for consistency
            # unless it was zero and now it's not (e.g. cluster was empty and objects are being added)
            # For simplicity, we'll use initial_total_objects for percent.
            # If current_total_objects is significantly different, the user might need to restart the script.
            # For this script's purpose, initial_total_objects is the most stable reference for percentage recovered.

            # --- Degraded Objects ---
            eta_seconds_degraded = float('inf')
            if abs_initial_degraded > 0 : # Only calculate ETA if there was something to recover initially
                 eta_seconds_degraded = calculate_eta(abs_initial_degraded, current_degraded, elapsed_since_start)
            elif current_degraded == 0: # Was 0 initially and still 0
                 eta_seconds_degraded = 0


            eta_formatted_degraded = format_eta(eta_seconds_degraded)
            completion_time_degraded_local = get_local_time_from_utc_delta(eta_seconds_degraded)
            percent_degraded = (current_degraded / initial_total_objects) * 100 if initial_total_objects > 0 else 0.0

            # --- Misplaced Objects ---
            eta_seconds_misplaced = float('inf')
            if abs_initial_misplaced > 0 : # Only calculate ETA if there was something to recover initially
                eta_seconds_misplaced = calculate_eta(abs_initial_misplaced, current_misplaced, elapsed_since_start)
            elif current_misplaced == 0: # Was 0 initially and still 0
                eta_seconds_misplaced = 0

            eta_formatted_misplaced = format_eta(eta_seconds_misplaced)
            completion_time_misplaced_local = get_local_time_from_utc_delta(eta_seconds_misplaced)
            percent_misplaced = (current_misplaced / initial_total_objects) * 100 if initial_total_objects > 0 else 0.0

            # --- Output ---
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}]")
            print(f"  Degraded : {current_degraded:>7} / {initial_total_objects:<7} ({percent_degraded:>6.2f}%) "
                  f"ETA: {eta_formatted_degraded} (at {completion_time_degraded_local})")
            print(f"  Misplaced: {current_misplaced:>7} / {initial_total_objects:<7} ({percent_misplaced:>6.2f}%) "
                  f"ETA: {eta_formatted_misplaced} (at {completion_time_misplaced_local})")
            print("-" * 60)

            if current_degraded == 0 and current_misplaced == 0:
                print("Recovery complete: 0 degraded and 0 misplaced objects.")
                break
            
            # Update last_check_time and counts for next iteration's differential ETA (if we were to use it)
            # last_check_time = current_loop_time
            # prev_degraded = current_degraded 
            # prev_misplaced = current_misplaced

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    finally:
        print("Script finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Script to estimate ETA for Ceph recovery (degraded and misplaced objects).',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('sleep', type=int, nargs='?', default=60,
                        help='Time interval in seconds between Ceph status checks.')

    args = parser.parse_args()

    if args.sleep < 5:
        print("Warning: Sleep interval is very short. Consider using 30 seconds or more.")

    main(args.sleep)
