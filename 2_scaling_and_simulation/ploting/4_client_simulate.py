# To run an example of this script use this command

# The state of Colorado
# python 4_client_simulate.py --input_folder=simulated-data-2019-04-10 --north="42" --south="36" --east="-101" --west="-110" --start_datetime="2019-04-10 18:00:00" --end_datetime="2019-04-10 18:30:00" --output_filename="4_Colorado-2019-04-10-from-180000-to-183000" --keep_snapchat_images="0" --gif_duration=1

# -> The State of New York
# python 4_client_simulate.py --input_folder=simulated-data-2019-04-10 --north="46" --south="40" --east="-73" --west="-80" --start_datetime="2019-04-10 18:00:00" --end_datetime="2019-04-10 18:30:00" --output_filename="4_Newyork-2019-04-10-from-180000-to-183000" --keep_snapchat_images="0" --gif_duration=1

# -> All USA
# python 4_client_simulate.py --input_folder=simulated-data-2019-04-10 --north="50" --south="23" --east="-65" --west="-127" --start_datetime="2019-04-10 18:00:00" --end_datetime="2019-04-10 18:30:00" --output_filename="4_USA-2019-04-10-from-180000-to-183000" --keep_snapchat_images="0" --gif_duration=1

from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt
from datetime import datetime
import matplotlib.patches as mpatches
import argparse, sys
import imageio
import json
import time
import os
import shutil
import imageio
from cv2 import cv2
import os

# Handle and process user input
parser = argparse.ArgumentParser()
parser.add_argument('--input_folder', help='Enter the name of your input simulation data folder which is generated using pyspark, for example (simulated_data_10_04_2019)')
parser.add_argument('--start_datetime', help='Enter the start datetime of the simulation, for example (2019-05-25 18:00:00)')
parser.add_argument('--end_datetime', help='Enter the end datetime of the simulation, for example (2019-05-25 18:00:00)')
parser.add_argument('--output_filename', help='Enter the folder name where you would like to save your simulation, for example (simulation_1)')
parser.add_argument('--keep_snapchat_images', help='Choice wheather or not to delete the snapchat images of the simulation or not, for example (1) to keep and (0) to delete')
parser.add_argument('--gif_duration', help='Enter the frame duration of each frame in the gif simulation, for example (0.5)')
parser.add_argument('--north', help='Enter the latitude of the upper right courner of the target area, for example (41)')
parser.add_argument('--south', help='Enter the latitude of the lower left courner of the target area, for example (37)')
parser.add_argument('--east', help='Enter the longitude of the upper right courner of the target area, for example (-102)')
parser.add_argument('--west', help='Enter the longitude of the lower left courner of the target area, for example (-109)')
args = parser.parse_args()

output_filename = args.output_filename
start_time = round(time.mktime(datetime.strptime(args.start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())/60)
end_time = round(time.mktime(datetime.strptime(args.end_datetime, '%Y-%m-%d %H:%M:%S').timetuple())/60)
gif_duration = args.gif_duration
plot_filenames = []

# Prepare output folder, delete if exists in order to update
dirpath = os.path.join('', output_filename)
if os.path.exists(dirpath) and os.path.isdir(dirpath):
    shutil.rmtree(dirpath)
os.mkdir(output_filename) 

with open("../"+args.input_folder+"/route_information.json", 'r') as file:
    route_information = json.load(file)

with open("../"+args.input_folder+"/position_information.json", 'r') as file:
    position_information = json.load(file)

target_urcrnrlat = float(args.north) # North limit
target_llcrnrlat = float(args.south) # South limit
target_urcrnrlon = float(args.east) # East limit
target_llcrnrlon = float(args.west) # West limit

m = Basemap(llcrnrlon=(target_llcrnrlon),llcrnrlat=(target_llcrnrlat),urcrnrlon=(target_urcrnrlon),urcrnrlat=(target_urcrnrlat), lat_ts=0, resolution='h')


for time in range(start_time, end_time+1, 1):
    flights = position_information[str(time)]
    fig = plt.figure(figsize = (15,8))
    m.drawmapboundary(fill_color='#5D9BFF')
    m.fillcontinents(color='white',lake_color='blue')
    m.drawcountries(color='#585858',linewidth=1)
    m.drawstates(linewidth = 0.2)
    m.drawcoastlines()
    
    for flight in flights:
        flight_lon = flight['longitude']
        flight_lat = flight['latitude']
        if flight_lon >= target_llcrnrlon and flight_lon <= target_urcrnrlon \
            and flight_lat >= target_llcrnrlat and flight_lat <= target_urcrnrlat:
            
            flight_information = route_information[str(flight['route_scaling_id'])]
            origin_lon = flight_information['origin_lon']
            origin_lat = flight_information['origin_lat']
            dest_lon = flight_information['destination_lon']
            dest_lat = flight_information['destination_lat']
            #flight_tag = flight_information['origin_city'] + "->" + flight_information['dest_city']
            flight_tag = flight_information['origin_airport'] + "->" + flight_information['destination_airport']
            
            # drawing inside routes - Black
            if (dest_lon <= target_urcrnrlon and dest_lon >= target_llcrnrlon and dest_lat <= target_urcrnrlat and dest_lat >= target_llcrnrlat) and \
                (origin_lon <= target_urcrnrlon and origin_lon >= target_llcrnrlon and origin_lat <= target_urcrnrlat and origin_lat >= target_llcrnrlat):
                    m.scatter([flight_lon],[flight_lat],zorder=5,s=50,color="#000000",marker="o")
                    plt.text(flight_lon, flight_lat, flight_tag, fontsize='smaller', color="#000000")

            # drawing incoming routes - Green
            if (dest_lon <= target_urcrnrlon and dest_lon >= target_llcrnrlon and dest_lat <= target_urcrnrlat and dest_lat >= target_llcrnrlat) and \
                (origin_lon >= target_urcrnrlon or origin_lon <= target_llcrnrlon or origin_lat >= target_urcrnrlat or origin_lat <= target_llcrnrlat):
                    m.scatter([flight_lon],[flight_lat],zorder=5,s=50,color="#006b1d",marker="o")
                    plt.text(flight_lon, flight_lat, flight_tag, fontsize='smaller', color="#006b1d")
                                
            # drawing outgoing routes - Red
            if (dest_lon >= target_urcrnrlon or dest_lon <= target_llcrnrlon or dest_lat >= target_urcrnrlat or dest_lat <= target_llcrnrlat) and \
                (origin_lon <= target_urcrnrlon and origin_lon >= target_llcrnrlon and origin_lat <= target_urcrnrlat and origin_lat >= target_llcrnrlat):
                    m.scatter([flight_lon],[flight_lat],zorder=5,s=50,color="#ff0000",marker="o")
                    plt.text(flight_lon, flight_lat, flight_tag, fontsize='smaller', color="#ff0000")

            # drawing passing routes - Blue
            if (dest_lon >= target_urcrnrlon or dest_lon <= target_llcrnrlon or dest_lat >= target_urcrnrlat or dest_lat <= target_llcrnrlat) and \
                (origin_lon >= target_urcrnrlon or origin_lon <= target_llcrnrlon or origin_lat >= target_urcrnrlat or origin_lat <= target_llcrnrlat):
                    m.scatter([flight_lon],[flight_lat],zorder=5,s=50,color="#0000FF",marker="o")
                    plt.text(flight_lon, flight_lat, flight_tag, fontsize='smaller', color="#0000FF")

    normal_datetime = datetime.fromtimestamp(time*60).strftime('%Y-%m-%d %H:%M:%S')
    output_path = output_filename+'/'+normal_datetime.replace(":","_").replace("-","_").replace(" ","_")+'.png'
    inside_patch = mpatches.Patch(color='#000000', label='Inside Routes')
    incoming_patch = mpatches.Patch(color='#006b1d', label='Incoming Routes')
    outgoing_patch = mpatches.Patch(color='#ff0000', label='Outgoing Routes')
    passing_patch = mpatches.Patch(color='#0000FF', label='Passing Routes')
    plt.legend(handles=[inside_patch, incoming_patch, outgoing_patch, passing_patch])
    plt.savefig(output_path)
    plt.close()
    plot_filenames.append(output_path)    

# Making a .gif simulation
images = []
for filename in plot_filenames:
    images.append(imageio.imread(filename))
imageio.mimsave(output_filename+'/'+'simulation.gif', images, duration=gif_duration)

# Making a video .avi simulation
image_folder = output_filename
video_name = output_filename+'/'+'simulation_video.avi'
images = [img for img in os.listdir(image_folder) if img.endswith(".png")]
frame = cv2.imread(os.path.join(image_folder, images[0]))
height, width, layers = frame.shape
video = cv2.VideoWriter(video_name, 0, 1, (width,height))
for image in images:
    video.write(cv2.imread(os.path.join(image_folder, image)))
cv2.destroyAllWindows()
video.release()

# Removing images if wanted by user
if args.keep_snapchat_images == "0":
    folder_path = (output_filename)
    images = os.listdir(folder_path)
    for image in images:
        if image.endswith(".png"):
            os.remove(os.path.join(folder_path, image))
