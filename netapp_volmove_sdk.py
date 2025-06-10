#!/usr/bin/env python3
"""
NetApp Volume Move Script - Python SDK Version
Translates PowerShell volume move script to Python using NetApp ONTAP SDK
"""

import time
import getpass
from datetime import datetime
from netapp_ontap import NetAppRestError
from netapp_ontap.config import HostConnection
from netapp_ontap.resources import Volume, VolMove, Aggregate, Svm

class NetAppVolumeMover:
    def __init__(self):
        self.cluster_name = None
        self.connection = None
        self.source_aggr = None 
        self.dest_aggr = None
        self.volume_list = None
        
    def read_config_files(self):
        """Read configuration from text files"""
        try:
            with open('Cluster.txt', 'r') as f:
                self.cluster_name = f.read().strip()
            
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
        
        try:
            self.connection = HostConnection(
                host=self.cluster_name,
                username=username,
                password=password,
                verify=False  # Set to True in production with proper certificates
            )
        except Exception as e:
            print(f"Failed to connect to NetApp cluster: {e}")
            raise
            
    def get_vol_moves(self):
        """Get current volume moves and return counters"""
        try:
            # Get all active volume moves
            vol_moves = VolMove.get_collection()
            
            counter = 0
            multi_counter = 0
            
            for vol_move in vol_moves:
                if vol_move.state == "healthy":
                    counter += 1
                    
                    # Check if source aggregate matches our source
                    if vol_move.source_aggregate and vol_move.source_aggregate.name == self.source_aggr:
                        multi_counter += 1
                    
                    print(f"{vol_move.volume.name} is still moving to {vol_move.destination_aggregate.name} "
                          f"- Percent Complete = {vol_move.percent_complete}%")
                    
            return counter, multi_counter
            
        except NetAppRestError as e:
            print(f"Error getting volume moves: {e}")
            return 0, 0
            
    def start_volume_move(self, volume_name, vserver_name):
        """Start a volume move"""
        try:
            # Create volume move job
            vol_move = VolMove()
            vol_move.volume = {"name": volume_name, "svm": {"name": vserver_name}}
            vol_move.destination_aggregate = {"name": self.dest_aggr}
            
            vol_move.post()
            print(f"{volume_name} is now moving")
            
        except NetAppRestError as e:
            print(f"Error starting volume move for {volume_name}: {e}")
            
    def get_volume_info(self, volume_name):
        """Get volume information including vserver"""
        try:
            volumes = Volume.get_collection(name=volume_name)
            for volume in volumes:
                volume.get()  # Populate all fields
                return volume
        except NetAppRestError as e:
            print(f"Error getting volume info for {volume_name}: {e}")
        return None
        
    def check_existing_vol_move(self, volume_name):
        """Check if volume already has an active move"""
        try:
            vol_moves = VolMove.get_collection()
            for vol_move in vol_moves:
                if vol_move.volume.name == volume_name:
                    return True
        except NetAppRestError as e:
            print(f"Error checking existing volume move: {e}")
        return False
        
    def run(self):
        """Main execution function"""
        print("Starting NetApp Volume Move Script")
        
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
                self.start_volume_move(vol_name, volume_info.svm.name)
            else:
                print(f"Skipping {vol_name} - too many active moves (counter: {counter}, multi_counter: {multi_counter})")

if __name__ == "__main__":
    try:
        mover = NetAppVolumeMover()
        mover.run()
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
    except Exception as e:
        print(f"Script failed: {e}")
