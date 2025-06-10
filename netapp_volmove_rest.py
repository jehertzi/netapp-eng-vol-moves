#!/usr/bin/env python3
"""
NetApp Volume Move Script - REST API Version
Translates PowerShell volume move script to Python using direct REST API calls
"""

import time
import getpass
import requests
import json
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
from requests.auth import HTTPBasicAuth

# Suppress SSL warnings for demo purposes
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

class NetAppVolumeMoverREST:
    def __init__(self):
        self.cluster_name = None
        self.base_url = None
        self.auth = None
        self.session = None
        self.source_aggr = None
        self.dest_aggr = None
        self.volume_list = None
        
    def read_config_files(self):
        """Read configuration from text files"""
        try:
            with open('Cluster.txt', 'r') as f:
                self.cluster_name = f.read().strip()
                self.base_url = f"https://{self.cluster_name}/api"
            
            with open('SAggr.txt', 'r') as f:
                self.source_aggr = f.read().strip()
                
            with open('DAggr.txt', 'r') as f:
                self.dest_aggr = f.read().strip()
                
            with open('Vol.txt', 'r') as f:
                self.volume_list = [line.strip() for line in f.readlines() if line.strip()]
                
        except FileNotFoundError as e:
            print(f"Configuration file not found: {e}")
            raise
            
    def connect_netapp(self):
        """Establish connection to NetApp cluster"""
        username = input("Enter NetApp username: ")
        password = getpass.getpass("Enter NetApp password: ")
        
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.verify = False  # Set to True in production with proper certificates
        
        # Test connection
        try:
            response = self.session.get(f"{self.base_url}/cluster")
            if response.status_code == 200:
                print("Successfully connected to NetApp cluster")
            else:
                raise Exception(f"Connection failed with status code: {response.status_code}")
        except Exception as e:
            print(f"Failed to connect to NetApp cluster: {e}")
            raise
            
    def get_vol_moves(self):
        """Get current volume moves and return counters"""
        try:
            # Get all volume moves
            response = self.session.get(f"{self.base_url}/storage/volume-moves")
            
            if response.status_code != 200:
                print(f"Error getting volume moves: {response.status_code}")
                return 0, 0
                
            vol_moves_data = response.json()
            counter = 0
            multi_counter = 0
            
            for vol_move in vol_moves_data.get('records', []):
                if vol_move.get('state') == 'healthy':
                    counter += 1
                    
                    # Get detailed information for each volume move
                    vol_move_detail_response = self.session.get(
                        f"{self.base_url}/storage/volume-moves/{vol_move['uuid']}"
                    )
                    
                    if vol_move_detail_response.status_code == 200:
                        vol_move_detail = vol_move_detail_response.json()
                        
                        # Check if source aggregate matches our source
                        source_aggr_name = vol_move_detail.get('source_aggregate', {}).get('name', '')
                        if source_aggr_name == self.source_aggr:
                            multi_counter += 1
                        
                        volume_name = vol_move_detail.get('volume', {}).get('name', 'Unknown')
                        dest_aggr_name = vol_move_detail.get('destination_aggregate', {}).get('name', 'Unknown')
                        percent_complete = vol_move_detail.get('percent_complete', 0)
                        
                        print(f"{volume_name} is still moving to {dest_aggr_name} "
                              f"- Percent Complete = {percent_complete}%")
                    
            return counter, multi_counter
            
        except Exception as e:
            print(f"Error getting volume moves: {e}")
            return 0, 0
            
    def get_volume_info(self, volume_name):
        """Get volume information including SVM"""
        try:
            response = self.session.get(
                f"{self.base_url}/storage/volumes",
                params={'name': volume_name, 'fields': 'svm,name,uuid'}
            )
            
            if response.status_code == 200:
                volumes_data = response.json()
                if volumes_data.get('records'):
                    return volumes_data['records'][0]
            
        except Exception as e:
            print(f"Error getting volume info for {volume_name}: {e}")
        return None
        
    def check_existing_vol_move(self, volume_name):
        """Check if volume already has an active move"""
        try:
            response = self.session.get(f"{self.base_url}/storage/volume-moves")
            
            if response.status_code == 200:
                vol_moves_data = response.json()
                for vol_move in vol_moves_data.get('records', []):
                    # Get detailed information
                    detail_response = self.session.get(
                        f"{self.base_url}/storage/volume-moves/{vol_move['uuid']}"
                    )
                    if detail_response.status_code == 200:
                        detail = detail_response.json()
                        if detail.get('volume', {}).get('name') == volume_name:
                            return True
                            
        except Exception as e:
            print(f"Error checking existing volume move: {e}")
        return False
        
    def get_aggregate_uuid(self, aggr_name):
        """Get aggregate UUID by name"""
        try:
            response = self.session.get(
                f"{self.base_url}/storage/aggregates",
                params={'name': aggr_name, 'fields': 'uuid,name'}
            )
            
            if response.status_code == 200:
                aggr_data = response.json()
                if aggr_data.get('records'):
                    return aggr_data['records'][0]['uuid']
                    
        except Exception as e:
            print(f"Error getting aggregate UUID for {aggr_name}: {e}")
        return None
        
    def start_volume_move(self, volume_info):
        """Start a volume move"""
        try:
            # Get destination aggregate UUID
            dest_aggr_uuid = self.get_aggregate_uuid(self.dest_aggr)
            if not dest_aggr_uuid:
                print(f"Could not find destination aggregate: {self.dest_aggr}")
                return
                
            # Prepare volume move request
            vol_move_data = {
                "destination_aggregate": {
                    "uuid": dest_aggr_uuid
                },
                "volume": {
                    "uuid": volume_info["uuid"]
                }
            }
            
            response = self.session.post(
                f"{self.base_url}/storage/volume-moves",
                json=vol_move_data,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [201, 202]:
                print(f"{volume_info['name']} is now moving")
            else:
                print(f"Error starting volume move for {volume_info['name']}: "
                      f"Status {response.status_code}, Response: {response.text}")
                
        except Exception as e:
            print(f"Error starting volume move for {volume_info['name']}: {e}")
            
    def run(self):
        """Main execution function"""
        print("Starting NetApp Volume Move Script (REST API)")
        
        # Read configuration
        self.read_config_files()
        print(f"Cluster: {self.cluster_name}")
        print(f"Source Aggregate: {self.source_aggr}")
        print(f"Destination Aggregate: {self.dest_aggr}")
        print(f"Volumes to move: {len(self.volume_list)}")
        
        # Connect to NetApp
        self.connect_netapp()
        
        # Process each volume
        for vol_name in self.volume_list:
            print(f"\nProcessing volume: {vol_name}")
            
            # Get volume information
            volume_info = self.get_volume_info(vol_name)
            if not volume_info:
                print(f"Volume {vol_name} not found, skipping...")
                continue
                
            # Check if volume already has an active move
            if self.check_existing_vol_move(vol_name):
                print(f"Volume {vol_name} already has an active move, skipping...")
                continue
                
            # Get current volume move status
            counter, multi_counter = self.get_vol_moves()
            
            # Wait if too many concurrent moves from source aggregate
            while multi_counter >= 2:
                current_time = datetime.now().strftime("%H:%M:%S")
                print(f"{current_time} - Vol move counter is greater than 2, sleeping 1 min...")
                time.sleep(60)
                counter, multi_counter = self.get_vol_moves()
                
            # Start volume move if conditions are met
            if counter < 2 and multi_counter < 2:
                print(f"Vol move counter is = {counter}")
                self.start_volume_move(volume_info)
            else:
                print(f"Skipping {vol_name} - too many active moves (counter: {counter}, multi_counter: {multi_counter})")

if __name__ == "__main__":
    try:
        mover = NetAppVolumeMoverREST()
        mover.run()
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
    except Exception as e:
        print(f"Script failed: {e}")
