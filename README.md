# Move volumes from a file list
python netapp_volume_mover.py --cluster cluster.example.com --username admin \
  --password 'your_password' --dest-node node2 --volume-list volumes.txt

# Specify volumes directly on the command line
python netapp_volume_mover.py --cluster cluster.example.com --username admin \
  --password 'your_password' --dest-node node2 --volume vol1 --volume vol2

# Example run
python main.py --cluster svlngen4-c04-san --username storage.gen --password St2fl86f#1y6 --volume-list volList.txt --dest-aggr dAggr.txt