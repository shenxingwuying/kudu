import json
import sys
from collections import OrderedDict

base_file='manifest.json'
delta_file='manifest_delta.json'
final_file='manifest.json'
if (len(sys.argv) >= 4):
  base_file = sys.argv[1]
  delta_file = sys.argv[2]
  final_file = sys.argv[3]
elif (len(sys.argv) >= 3):
  base_file = sys.argv[1]
  delta_file = sys.argv[2]
elif (len(sys.argv) >= 2):
  base_file = sys.argv[1]

with open(base_file, 'r') as f:
  base_data = json.load(f, object_pairs_hook = OrderedDict)

with open(delta_file, 'r') as f:
  delta_data = json.load(f, object_pairs_hook = OrderedDict)

data={}
data['lastUpdated'] = delta_data['lastUpdated']
data['parcels'] = base_data['parcels'] + delta_data['parcels']
parcel_count = len(data['parcels'])
start_index = 0 if parcel_count <= 10 else parcel_count - 10
data['parcels'] = data['parcels'][start_index:parcel_count]

with open(final_file, 'w') as f:
  json.dump(data, f, indent = 4)
