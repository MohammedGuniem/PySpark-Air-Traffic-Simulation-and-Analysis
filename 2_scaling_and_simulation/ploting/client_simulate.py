# To run an example of this script use this command
# python client_simulate.py --input_folder=simulated_data_10_04_2019 --north="41" --south="37" --east="-102" --west="-109" --start_datetime="2019-04-10 15:10:00" --end_datetime="2019-04-10 15:20:00" --output_filename="simulation_over_colorado_between_151000_and_152000_at_10042019" --increase_time_intervall="15" --keep_snapchat_images="0" --gif_duration=0.5

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
parser.add_argument('--increase_time_intervall', help='Enter the increase rate in seconds for the gif simulation, for example (1)')
parser.add_argument('--keep_snapchat_images', help='Choice wheather or not to delete the snapchat images of the simulation or not, for example (1) to keep and (0) to delete')
parser.add_argument('--gif_duration', help='Enter the frame duration of each frame in the gif simulation, for example (0.5)')
parser.add_argument('--north', help='Enter the latitude of the upper right courner of the target area, for example (41)')
parser.add_argument('--south', help='Enter the latitude of the lower left courner of the target area, for example (37)')
parser.add_argument('--east', help='Enter the longitude of the upper right courner of the target area, for example (-102)')
parser.add_argument('--west', help='Enter the longitude of the lower left courner of the target area, for example (-109)')
args = parser.parse_args()

output_filename = args.output_filename
increase_time_intervall = int(args.increase_time_intervall)
current_unix_time = time.mktime(datetime.strptime(args.start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())
gif_duration = args.gif_duration
plot_filenames = []

# Prepare output folder, delete if exists in order to update
dirpath = os.path.join('', output_filename)
if os.path.exists(dirpath) and os.path.isdir(dirpath):
    shutil.rmtree(dirpath)
os.mkdir(output_filename) 

# Function to check if a given point is in an area
def is_point_in_area(point_latitude, point_longitude, area_east_limit, area_west_limit, area_north_limit, area_south_limit):
    if point_longitude <= area_east_limit and point_longitude >= area_west_limit and point_latitude <= area_north_limit and point_latitude >= area_south_limit:
        return True
    return False

# Colorado state coordinates
target_urcrnrlat = float(args.north) # North limit
target_llcrnrlat = float(args.south) # South limit
target_urcrnrlon = float(args.east) # East limit
target_llcrnrlon = float(args.west) # West limit

# Determine which file to load
filenames = []
for llcrnrlon in range(-130, -60, 10):
    urcrnrlon = llcrnrlon+10
    for llcrnrlat in range(25, 50, 5):
        urcrnrlat = llcrnrlat+5
        if is_point_in_area(target_urcrnrlat, target_urcrnrlon, urcrnrlon, llcrnrlon, urcrnrlat, llcrnrlat) \
           or is_point_in_area(target_llcrnrlat, target_llcrnrlon, urcrnrlon, llcrnrlon, urcrnrlat, llcrnrlat) \
           or is_point_in_area(target_urcrnrlat, target_llcrnrlon, urcrnrlon, llcrnrlon, urcrnrlat, llcrnrlat) \
           or is_point_in_area(target_llcrnrlat, target_urcrnrlon, urcrnrlon, llcrnrlon, urcrnrlat, llcrnrlat):
            filename = str(llcrnrlon).replace("-","minus")+"_"+str(llcrnrlat).replace("-","minus")+"_"+str(urcrnrlon).replace("-","minus")+"_"+str(urcrnrlat).replace("-","minus")+'.json'
            filenames.append(filename)

# Plot flights
for filename in filenames:
    with open("../"+args.input_folder+"/"+filename, 'r') as file:
        data = json.load(file)
        while current_unix_time <= time.mktime(datetime.strptime(args.end_datetime, '%Y-%m-%d %H:%M:%S').timetuple()):
            fig = plt.figure(figsize = (15,8))
            m = Basemap(llcrnrlon=(target_llcrnrlon-1),llcrnrlat=(target_llcrnrlat-1),urcrnrlon=(target_urcrnrlon+1),urcrnrlat=(target_urcrnrlat+1), lat_ts=0, resolution='l')
            m.drawmapboundary(fill_color='#5D9BFF')
            m.fillcontinents(color='white',lake_color='blue')
            m.drawcountries(color='#585858',linewidth=1)
            m.drawstates(linewidth = 0.2)
            m.drawcoastlines()
            entry_times = []
            exit_times = []
            unix_datetime = current_unix_time
            normal_datetime = datetime.fromtimestamp(current_unix_time).strftime('%Y-%m-%d %H:%M:%S')

            for route in data:
                if route['is_in_area'] == "INSIDE" and unix_datetime <= route['exit_time'] and unix_datetime >= route['entry_time']:
                    Points = {"Source":(route['dest_lat'],route['dest_lon']),"Destination":(route['origin_lat'],route['origin_lon'])}
                    Lon = [Points[key][0] for key in Points]
                    Lat = [Points[key][1] for key in Points]
                    X, Y = m(Lat,Lon)
                    
                    route_name = route['route']
                    origin_lat = route['origin_lat']
                    origin_lon = route['origin_lon']
                    dest_lat = route['dest_lat']
                    dest_lon = route['dest_lon']
                    longs, lats = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],100)
                    
                    entry_times.append(route['entry_time'])
                    exit_times.append(route['exit_time'])

                    access_points = {"Source":(route['entry_lat'],route['entry_lon']),"Destination":(route['exit_lat'],route['exit_lon'])}
                    access_Lon = [access_points[key][0] for key in Points]
                    access_Lat = [access_points[key][1] for key in Points]
                    access_X, access_Y = m(access_Lat,access_Lon)
                    duration_in_seconds = int(route['exit_time']-route['entry_time'])
                    access_longs, access_lats = m.gcpoints(access_Lat[0],access_Lon[0],access_Lat[1],access_Lon[1],duration_in_seconds)
                    current_point_index = int(unix_datetime-route['entry_time'])-1
                    
                    current_point_longtiude = access_longs[current_point_index]
                    current_point_latitude = access_lats[current_point_index]

                    if current_point_longtiude >= target_llcrnrlon and current_point_longtiude <= target_urcrnrlon and \
                        current_point_latitude >= target_llcrnrlat and current_point_latitude <= target_urcrnrlat:

                        # drawing inside Colorado routes - Black
                        if (dest_lon <= target_urcrnrlon and dest_lon >= target_llcrnrlon and dest_lat <= target_urcrnrlat and dest_lat >= target_llcrnrlat) and \
                        (origin_lon <= target_urcrnrlon and origin_lon >= target_llcrnrlon and origin_lat <= target_urcrnrlat and origin_lat >= target_llcrnrlat):
                            m.scatter([current_point_longtiude],[current_point_latitude],zorder=5,s=50,color="#000000",marker="o")
                            plt.text(current_point_longtiude+0.1, current_point_latitude, route_name, color="#000000")

                        # drawing incoming routes - Green
                        if (dest_lon <= target_urcrnrlon and dest_lon >= target_llcrnrlon and dest_lat <= target_urcrnrlat and dest_lat >= target_llcrnrlat) and \
                        (origin_lon >= target_urcrnrlon or origin_lon <= target_llcrnrlon or origin_lat >= target_urcrnrlat or origin_lat <= target_llcrnrlat):
                            m.scatter([current_point_longtiude],[current_point_latitude],zorder=5,s=50,color="#006b1d",marker="o")
                            plt.text(current_point_longtiude+0.1, current_point_latitude, route_name, color="#006b1d")
                            
                        # drawing outgoing routes - Red
                        if (dest_lon >= target_urcrnrlon or dest_lon <= target_llcrnrlon or dest_lat >= target_urcrnrlat or dest_lat <= target_llcrnrlat) and \
                        (origin_lon <= target_urcrnrlon and origin_lon >= target_llcrnrlon and origin_lat <= target_urcrnrlat and origin_lat >= target_llcrnrlat):
                            m.scatter([current_point_longtiude],[current_point_latitude],zorder=5,s=50,color="#ff0000",marker="o")
                            plt.text(current_point_longtiude+0.1, current_point_latitude, route_name, color="#ff0000")

                        # drawing passing routes - Blue
                        if (dest_lon >= target_urcrnrlon or dest_lon <= target_llcrnrlon or dest_lat >= target_urcrnrlat or dest_lat <= target_llcrnrlat) and \
                        (origin_lon >= target_urcrnrlon or origin_lon <= target_llcrnrlon or origin_lat >= target_urcrnrlat or origin_lat <= target_llcrnrlat):
                            m.scatter([current_point_longtiude],[current_point_latitude],zorder=5,s=50,color="#0000FF",marker="o")
                            plt.text(current_point_longtiude+0.1, current_point_latitude, route_name, color="#0000FF")

            output_path = output_filename+'/'+normal_datetime.replace(":","_").replace("-","_").replace(" ","_")+'.png'
            
            inside_patch = mpatches.Patch(color='#000000', label='Inside Routes')
            incoming_patch = mpatches.Patch(color='#006b1d', label='Incoming Routes')
            outgoing_patch = mpatches.Patch(color='#ff0000', label='Outgoing Routes')
            passing_patch = mpatches.Patch(color='#0000FF', label='Passing Routes')
            plt.legend(handles=[inside_patch, incoming_patch, outgoing_patch, passing_patch])
            plt.savefig(output_path)
            plt.close()
            plot_filenames.append(output_path)

            current_unix_time += increase_time_intervall
            #print(current_unix_time)

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