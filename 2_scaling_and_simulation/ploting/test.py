start_longitude_left = -130
start_longitude_right = -120

count = 0
for llcrnlon in range(-130, -60, 10):
    urcrnrlon = llcrnlon+10
    print(llcrnlon, urcrnrlon)
    print("*")
    for llcrnrlat in range(25, 50, 5):
        urcrnrlat = llcrnrlat+5
        print(llcrnrlat, urcrnrlat)
        count += 1
    print("------------")
    

print(count)
import json
import os, shutil

output_filename = 'simulated_data_10_04_2019'
dirpath = os.path.join('', output_filename)
if os.path.exists(dirpath) and os.path.isdir(dirpath):
    shutil.rmtree(dirpath)
os.mkdir(output_filename)

all_routes = []
with open(output_filename+'/-130_25_-120_30.json', 'w') as outfile:
    json.dump(all_routes, outfile)