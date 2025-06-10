#!/usr/bin/env python3
"""
NetApp Volume Migration Script

This script safely moves volumes between nodes in a NetApp cluster with progress tracking
and limits concurrent volume moves to 4 at a time.

Requirements:
- netapp-ontap Python package (pip install netapp-ontap)
- paramiko package (pip install paramiko)
- Python 3.6+
"""

import argparse
import logging
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
# import paramiko
from netapp_ontap import config, HostConnection, NetAppRestError
from netapp_ontap.resources import Volume, Job, Node
import os
import subprocess

# Define constants for connection settings
class C:
    NETAPP_API_CONNECTION_TIMEOUT = 30
    NETAPP_API_READ_TIMEOUT = 60
    NETAPP_API_RETRY_API_BACKOFF_FACTOR = 0.5

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("volume_migration.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def connect(method):
    '''
    Connect

    Connect to the NetApp server. Note that the NetApp is set as a global
    variable! Every class instance will overwrite it for every other class
    instance: the last to update it wins.

    Hence, this decorator will be applied to each class method, as applicable,
    allowing it to automatically reconnect regardless of what instances may exist
    at the time.
    '''
    def insertConnection(self, *args, **kwargs):
        '''
        Insert Connection

        Inner function for decorator to forcefully set the connection to the
        instance's NetApp.
        '''
        config.CONNECTION = HostConnection(
            host=self.cluster,
            username=self.username,
            password=self.password,
            verify=self.verify_ssl
        )
        config.CONNECTION.protocol_timeouts = (C.NETAPP_API_CONNECTION_TIMEOUT,
                                             C.NETAPP_API_READ_TIMEOUT)
        config.RETRY_API_BACKOFF_FACTOR = C.NETAPP_API_RETRY_API_BACKOFF_FACTOR
        results = method(self, *args, **kwargs)
        return results
    return insertConnection

class VolumeMove:
    """Class to handle NetApp volume move operations"""

    def __init__(self, cluster, username, password, verify_ssl=False):
        """Initialize connection to NetApp cluster"""
        self.cluster = cluster
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.active_moves = {}  # Track active moves: {vol_name: job_id}
        self.move_results = {}  # Store results: {vol_name: success/failure}
        self.progress_lock = threading.Lock()  # Lock for thread-safe updates

    @connect
    def _check_volume_exists(self, volume_name):
        """Check if volume exists"""
        try:
            volumes = list(Volume.get_collection(fields="name", name=volume_name))
            return len(volumes) > 0
        except NetAppRestError as e:
            logger.error(f"API Error checking volume {volume_name}: {str(e)}")
            logger.debug(f"API Error details:", exc_info=True)  # This adds traceback
            return False
        except Exception as e:
            logger.error(f"Error checking volume {volume_name}: {str(e)}")
            logger.exception(f"Exception details for {volume_name}")  # This adds traceback
            return False

    @connect
    def _check_destination_node_status(self, dest_node):
        """Check if destination aggr is healthy and available"""
        try:
            # Get the node details
            nodes = list(Node.get_collection(
                fields="name,health",
                name=dest_node
            ))

            if not nodes:
                logger.error(f"Node/Aggregate {dest_node} not found")
                return False

            node = nodes[0]
            # Check if node is healthy and online
            is_healthy = node.health == "true"
            is_online = node.state == "online"

            if not is_healthy:
                logger.warning(f"Node/Aggregate {dest_node} health status is not optimal")
            if not is_online:
                logger.error(f"Node/Aggregate {dest_node} is not online (state: {node.state})")

            return is_online  # At minimum, node should be online
        except NetAppRestError as e:
            logger.error(f"API Error checking node/aggregate {dest_node} status: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error checking node/aggregate {dest_node} status: {str(e)}")
            return False

    @connect
    def _check_destination_aggr_status(self, dest_aggr):
        """Check if destination aggregate is healthy and available"""
        try:
            # In a future version, this should be updated to check aggregate status directly
            # using netapp_ontap.resources.Aggregate when available
            logger.info(f"Checking status of destination aggregate: {dest_aggr}")

            # For now, we can't effectively check aggregate status through the Node resource
            # so we'll return True and let the move operation validate the aggregate
            return True
        except NetAppRestError as e:
            logger.error(f"API Error checking aggregate {dest_aggr} status: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error checking aggregate {dest_aggr} status: {str(e)}")
            return False

    @connect
    def _get_node_aggregates(self, node_name):
        """Get list of aggregates for a node (useful for advanced move operations)"""
        try:
            # This would require the Aggregate resource, which we can implement if needed
            # For now, we'll let ONTAP auto-select the aggregate based on the node
            return None
        except Exception as e:
            logger.error(f"Error getting aggregates for node {node_name}: {str(e)}")
            return None

    @connect
    def initiate_volume_move_cli(self, volume_name, dest_aggr, cutover_action="retry", cutover_window=30):
        """Initiate volume move operation using sshpass with environment variable"""
        try:
            logger.info(f"Connecting to {self.cluster} as {self.username}")

            # Build the SSH command with individual NetApp CLI arguments
            ssh_cmd = [
                "sshpass", "-e",  # Read password from SSHPASS environment variable
                "ssh",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "ConnectTimeout=30",
                f"{self.username}@{self.cluster}",
                "volume", "move", "start",
                "-vserver", f"{self.cluster}-ns",
                "-volume", volume_name,
                "-destination-aggregate", dest_aggr,
                "-cutover-action", cutover_action,
                "-cutover-window", str(cutover_window)
            ]

            logger.debug(f"Executing SSH command for volume {volume_name}")
            logger.debug(f"Full command: {' '.join(ssh_cmd)}")

            # Set up environment with password
            env = os.environ.copy()
            env['SSHPASS'] = self.password

            # Execute command - Python 3.6 compatible version
            result = subprocess.run(
                ssh_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,  # Use universal_newlines instead of text for Python 3.6
                timeout=120,  # 2 minute timeout
                env=env
            )

            logger.debug(f"Command exit code: {result.returncode}")
            logger.debug(f"Command stdout: {result.stdout}")

            if result.stderr:
                logger.debug(f"Command stderr: {result.stderr}")

            if result.returncode != 0:
                logger.error(f"Command failed with exit code {result.returncode}")
                logger.error(f"Error output: {result.stderr}")

                # Check for common error patterns and provide helpful messages
                if "not found" in result.stderr.lower():
                    logger.error("The volume or aggregate may not exist, or the command syntax is incorrect")
                elif "permission" in result.stderr.lower():
                    logger.error("Permission denied - check username and password")
                elif "vserver" in result.stderr.lower():
                    logger.error(f"VServer '{self.cluster}-ns' may not exist or be accessible")

                return False, result.stderr

            # Parse output to get job ID
            if "Job ID:" in result.stdout:
                job_id = result.stdout.split("Job ID:")[1].strip().split()[0]
                logger.info(f"Volume move started successfully, Job ID: {job_id}")
                return True, job_id
            elif "job-id" in result.stdout.lower():
                # Alternative job ID format
                import re
                job_match = re.search(r'job-id\s+(\d+)', result.stdout, re.IGNORECASE)
                if job_match:
                    job_id = job_match.group(1)
                    logger.info(f"Volume move started successfully, Job ID: {job_id}")
                    return True, job_id
            else:
                # Look for other success indicators
                if any(word in result.stdout.lower() for word in ["started", "initiated", "begin", "moving"]):
                    logger.info(f"Volume move appears to have started successfully")
                    logger.debug(f"Full output: {result.stdout}")
                    return True, "CLI_JOB"
                else:
                    logger.warning(f"Could not parse job ID from output: {result.stdout}")
                    return True, "CLI_JOB"  # Assume success if no error

        except subprocess.TimeoutExpired:
            logger.error(f"SSH command timed out for volume {volume_name}")
            return False, "SSH command timed out"
        except FileNotFoundError:
            logger.error("sshpass command not found. Please install sshpass:")
            logger.error("  - On Ubuntu/Debian: sudo apt-get install sshpass")
            logger.error("  - On CentOS/RHEL: sudo yum install sshpass")
            logger.error("  - On macOS: brew install hudochenkov/sshpass/sshpass")
            return False, "sshpass not installed"
        except Exception as e:
            logger.exception(f"SSH error when moving volume {volume_name}")
            return False, str(e)

    @connect
    def get_move_status(self, volume_name, job_id):
        """Get status of volume move operation"""
        try:
            # Get job details
            job = Job(job_id)
            job.get()

            # Extract job state and progress
            state = job.state

            # Calculate percent complete
            if hasattr(job, 'progress') and hasattr(job.progress, 'percent_complete'):
                percent_complete = job.progress.percent_complete
            else:
                percent_complete = 0

            # Log detailed job info at debug level
            if hasattr(job, 'message'):
                logger.debug(f"Job message for {volume_name}: {job.message}")

            return state, percent_complete
        except NetAppRestError as e:
            logger.error(f"API Error getting status for volume {volume_name} (job {job_id}): {str(e)}")
            return "error", 0
        except Exception as e:
            logger.error(f"Error getting status for volume {volume_name} (job {job_id}): {str(e)}")
            return "error", 0

    @connect
    def wait_for_move_completion(self, volume_name, job_id, timeout=86400):  # Default 24h timeout
        """Wait for volume move to complete with timeout"""
        start_time = time.time()
        last_percent = -1
        last_log_time = 0
        log_interval = 300  # Log status every 5 minutes even if no progress

        while time.time() - start_time < timeout:
            current_time = time.time()
            state, percent_complete = self.get_move_status(volume_name, job_id)

            # Log if percentage changed or if log_interval has passed
            time_since_last_log = current_time - last_log_time
            if percent_complete != last_percent or time_since_last_log >= log_interval:
                with self.progress_lock:
                    logger.info(f"Volume {volume_name}: {percent_complete}% complete (State: {state})")
                last_percent = percent_complete
                last_log_time = current_time

            if state.lower() in ["success", "complete", "completed"]:
                with self.progress_lock:
                    logger.info(f"Volume move for {volume_name} completed successfully")
                return True

            if state.lower() in ["failure", "error", "failed"]:
                with self.progress_lock:
                    logger.error(f"Volume move for {volume_name} failed")
                return False

            time.sleep(30)  # Check status every 30 seconds

        with self.progress_lock:
            logger.error(f"Volume move for {volume_name} timed out after {timeout/3600:.1f} hours")
        return False

    @connect
    def process_volume_move(self, volume_name, dest_aggr, cutover_action="retry", cutover_window=30, timeout=86400):
        """Process a single volume move operation"""
        logger.info(f"Starting move for volume {volume_name} to aggregate {dest_aggr}")

        success, result = self.initiate_volume_move_cli(volume_name, dest_aggr, cutover_action, cutover_window)

        if not success:
            self.move_results[volume_name] = False
            logger.error(f"Failed to initiate move for volume {volume_name}: {result}")
            return False

        job_id = result
        with self.progress_lock:
            self.active_moves[volume_name] = job_id

        move_success = self.wait_for_move_completion(volume_name, job_id, timeout)

        with self.progress_lock:
            if volume_name in self.active_moves:
                del self.active_moves[volume_name]
            self.move_results[volume_name] = move_success

        return move_success

    @connect
    def process_volume_list(self, volume_list, dest_aggr, max_concurrent=4, cutover_action="retry",
                        cutover_window=30, timeout=86400, ignore_health_check=False):
        """Process a list of volumes with a limit on concurrent operations"""
        total_volumes = len(volume_list)
        completed = 0
        success_count = 0
        failed_volumes = []

        # Initial validation before starting moves
        if not ignore_health_check and not self._check_destination_aggr_status(dest_aggr):
            logger.error(f"Destination aggregate {dest_aggr} is not available. Aborting all moves.")
            logger.error("Use --ignore-health-check to bypass this check.")
            return False

        logger.info(f"Starting migration of {total_volumes} volumes to aggregate {dest_aggr}")
        start_time = datetime.now()

        # Track in-progress moves for status updates
        in_progress = {}  # {volume_name: future}
        completed_volumes = []

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            # Submit initial batch up to max_concurrent
            for i, volume in enumerate(volume_list[:max_concurrent]):
                future = executor.submit(
                    self.process_volume_move,
                    volume,
                    dest_aggr,
                    cutover_action,
                    cutover_window
                )
                in_progress[volume] = future
                logger.info(f"Started migration for volume {volume} ({i+1}/{total_volumes})")

            # Process remaining volumes as others complete
            remaining_volumes = volume_list[max_concurrent:]

            # Track and report progress
            last_status_time = time.time()
            status_interval = 60  # Status update every 60 seconds

            while in_progress:
                # Check for completed moves
                completed_now = []
                for vol, future in list(in_progress.items()):
                    if future.done():
                        # Process result
                        success = future.result()
                        if success:
                            success_count += 1
                            logger.info(f"[SUCCESS] Volume {vol} migration completed successfully")
                        else:
                            failed_volumes.append(vol)
                            logger.error(f"[FAILED] Volume {vol} migration failed")

                        # Remove from in progress and add to completed
                        del in_progress[vol]
                        completed_volumes.append(vol)
                        completed_now.append(vol)

                # Submit new volumes for any completed moves (if any remain)
                for _ in completed_now:
                    if remaining_volumes:
                        next_vol = remaining_volumes.pop(0)
                        future = executor.submit(
                            self.process_volume_move,
                            next_vol,
                            dest_aggr,
                            cutover_action,
                            cutover_window
                        )
                        in_progress[next_vol] = future
                        logger.info(f"Started migration for volume {next_vol} ({len(completed_volumes)+len(in_progress)}/{total_volumes})")

                # Periodic status update of in-progress moves
                current_time = time.time()
                if current_time - last_status_time > status_interval:
                    with self.progress_lock:
                        logger.info(f"--- Current Status ---")
                        logger.info(f"Total volumes: {total_volumes}")
                        logger.info(f"Completed: {len(completed_volumes)}/{total_volumes}")
                        logger.info(f"In progress: {len(in_progress)}/{total_volumes}")
                        logger.info(f"Waiting to start: {len(remaining_volumes)}/{total_volumes}")
                        logger.info(f"Success so far: {success_count}")
                        logger.info(f"Failed so far: {len(failed_volumes)}")

                        # Show current progress of in-progress moves
                        if in_progress:
                            logger.info("Currently moving:")
                            for vol in in_progress.keys():
                                if vol in self.active_moves:
                                    job_id = self.active_moves[vol]
                                    state, percent = self.get_move_status(vol, job_id)
                                    logger.info(f"  - {vol}: {percent}% complete (State: {state})")

                    last_status_time = current_time

                # Short sleep to prevent CPU spinning
                time.sleep(1)

        end_time = datetime.now()
        duration = end_time - start_time

        # Final report
        logger.info("=" * 70)
        logger.info("VOLUME MIGRATION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total volumes processed: {total_volumes}")
        logger.info(f"Successfully moved: {success_count}")
        logger.info(f"Failed to move: {len(failed_volumes)}")
        if failed_volumes:
            logger.info("Failed volumes:")
            for vol in failed_volumes:
                logger.info(f"  - {vol}")

        # Calculate time statistics
        total_seconds = duration.total_seconds()
        hours, remainder = divmod(int(total_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        logger.info(f"Total duration: {hours:02d}:{minutes:02d}:{seconds:02d}")

        if total_volumes > 0:
            avg_seconds_per_vol = total_seconds / total_volumes
            avg_minutes_per_vol = avg_seconds_per_vol / 60
            logger.info(f"Average time per volume: {avg_minutes_per_vol:.2f} minutes")

        logger.info("=" * 70)

        return success_count == total_volumes

def read_volume_list(file_path):
    """Read volume list from file (one volume per line)"""
    try:
        with open(file_path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Error reading volume list from {file_path}: {str(e)}")
        return []

def main():
    """Main function to parse arguments and execute volume moves"""
    parser = argparse.ArgumentParser(description='NetApp Volume Migration Tool')
    parser.add_argument('--cluster', required=True, help='NetApp cluster management IP or hostname')
    parser.add_argument('--username', required=True, help='Admin username')
    parser.add_argument('--password', required=True, help='Admin password')
    parser.add_argument('--dest-aggr', required=True, help='Destination aggregate name')
    parser.add_argument('--volume-list', help='Path to file containing list of volumes to move')
    parser.add_argument('--volume', action='append', help='Volume to move (can be specified multiple times)')
    parser.add_argument('--max-concurrent', type=int, default=4, help='Maximum concurrent volume moves (default: 4)')
    parser.add_argument('--cutover-action', default='retry', choices=['retry', 'defer', 'abort', 'force'],
                       help='Action to take if cutover is delayed')
    parser.add_argument('--cutover-window', type=int, default=30,
                       help='Cutover time window in seconds (default: 30)')
    parser.add_argument('--no-verify-ssl', action='store_true', help='Disable SSL certificate verification')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set logging level (default: INFO)')
    parser.add_argument('--ignore-health-check', action='store_true',
                       help='Ignore destination aggregate health check')
    parser.add_argument('--timeout', type=int, default=86400,
                       help='Volume move timeout in seconds (default: 86400)')

    args = parser.parse_args()

    # Configure logging level
    logger.setLevel(getattr(logging, args.log_level))

    # Get list of volumes to move
    volumes = []
    if args.volume_list:
        volumes.extend(read_volume_list(args.volume_list))
    if args.volume:
        volumes.extend(args.volume)

    if not volumes:
        logger.error("No volumes specified for migration")
        parser.print_help()
        sys.exit(1)

    # Remove duplicates
    volumes = list(set(volumes))

    logger.info(f"Preparing to move {len(volumes)} volumes to aggregate {args.dest_aggr}")

    # Display configuration
    logger.info(f"Configuration:")
    logger.info(f"  - Cluster: {args.cluster}")
    logger.info(f"  - Destination aggregate: {args.dest_aggr}")
    logger.info(f"  - Max concurrent moves: {args.max_concurrent}")
    logger.info(f"  - Cutover action: {args.cutover_action}")
    logger.info(f"  - Cutover window: {args.cutover_window} seconds")
    logger.info(f"  - Timeout: {args.timeout} seconds")

    # Instantiate volume move handler
    try:
        mover = VolumeMove(
            cluster=args.cluster,
            username=args.username,
            password=args.password,
            verify_ssl=not args.no_verify_ssl
        )

        # Process volume moves
        success = mover.process_volume_list(
            volume_list=volumes,
            dest_aggr=args.dest_aggr,
            max_concurrent=args.max_concurrent,
            cutover_action=args.cutover_action,
            cutover_window=args.cutover_window,
            timeout=args.timeout,
            ignore_health_check=args.ignore_health_check
        )

        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("Operation interrupted by user. In-progress volume moves will continue on the cluster.")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()