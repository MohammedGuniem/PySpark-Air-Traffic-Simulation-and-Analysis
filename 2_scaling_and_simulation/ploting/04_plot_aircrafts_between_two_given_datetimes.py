# To run an example of this script use this command
# python 04_plot_aircrafts_between_two_given_datetimes.py --input_filename=simulated_data_2019_05_25.json --start_datetime="2019-05-25 15:10:00" --end_datetime="2019-05-25 15:20:00" --output_filename="simulation_over_colorado_between_151000_and_152000_at_25052019" --increase_time_intervall="15" --keep_snapchat_images="0" --gif_duration=0.5
# python 04_plot_aircrafts_between_two_given_datetimes.py --input_filename=s_d_2019_04_10.json --start_datetime="2019-05-25 15:10:00" --end_datetime="2019-05-25 15:20:00" --output_filename="simulation_over_colorado_between_151000_and_152000_at_10042019" --increase_time_intervall="15" --keep_snapchat_images="0" --gif_duration=0.5

from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt
from datetime import datetime
import argparse, sys
import imageio
import json
import time
import os
import shutil

parser = argparse.ArgumentParser()
parser.add_argument('--input_filename', help='Enter the name of your input file which generated using pyspark, for example (simulated_data_2019_05_25.json)')
parser.add_argument('--start_datetime', help='Enter the start datetime of the simulation, for example (2019-05-25 18:00:00)')
parser.add_argument('--end_datetime', help='Enter the end datetime of the simulation, for example (2019-05-25 18:00:00)')
parser.add_argument('--output_filename', help='Enter the folder name where you would like to save your simulation, for example (simulation_1)')
parser.add_argument('--increase_time_intervall', help='Enter the increase rate in seconds for the gif simulation, for example (1)')
parser.add_argument('--keep_snapchat_images', help='Choice wheather or not to delete the snapchat images of the simulation or not, for example (1) to keep and (0) to delete')
parser.add_argument('--gif_duration', help='Enter the frame duration of each frame in the gif simulation, for example (0.5)')
args = parser.parse_args()

output_filename = args.output_filename
increase_time_intervall = int(args.increase_time_intervall)
current_unix_time = time.mktime(datetime.strptime(args.start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())
gif_duration = args.gif_duration
plot_filenames = []

dirpath = os.path.join('', output_filename)
if os.path.exists(dirpath) and os.path.isdir(dirpath):
    shutil.rmtree(dirpath)
os.mkdir(output_filename) 

with open("../"+args.input_filename, 'r') as file:
    data = json.load(file)
    while current_unix_time <= time.mktime(datetime.strptime(args.end_datetime, '%Y-%m-%d %H:%M:%S').timetuple()):
        fig = plt.figure(figsize = (15,8))
        #m = Basemap(llcrnrlon=-170,llcrnrlat=10,urcrnrlon=-60,urcrnrlat=70, lat_ts=0, resolution='l')
        m = Basemap(llcrnrlon=-112,llcrnrlat=36,urcrnrlon=-100,urcrnrlat=42, lat_ts=0, resolution='l')
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

                # drawing the current point
                # m.scatter([access_longs[current_point_index]],[access_lats[current_point_index]],zorder=5,s=50,color="#4287f5",marker="o")

                # drawing inside Colorado routes - orange
                if (dest_lon < -102 and dest_lon > -109 and dest_lat < 41 and dest_lat > 37) and (origin_lon < -102 and origin_lon > -109 and origin_lat < 41 and origin_lat > 37):
                    # drawing entry points - green
                    # m.scatter([route['entry_lon']],[route['entry_lat']],zorder=5,s=50,color="#00FF00",marker="o")
                    
                    # drawing exit point - red
                    # m.scatter([route['exit_lon']],[route['exit_lat']],zorder=5,s=50,color="#FF0000",marker="o")
                    
                    # drawing full route - orange
                    # m.scatter(X,Y,zorder=5,s=50,color="#FF7F00",marker="^")
                    # plt.plot(longs,lats,color="#FF7F00",linewidth=2)

                    # drawing the route over Colorado - black
                    # plt.plot(access_longs,access_lats,color="#000000",linewidth=2)

                    # drawing the current point - orange
                    m.scatter([access_longs[current_point_index]],[access_lats[current_point_index]],zorder=5,s=50,color="#FF7F00",marker="o")

                # drawing incoming routes - yellow
                if (dest_lon < -102 and dest_lon > -109 and dest_lat < 41 and dest_lat > 37) and (origin_lon > -102 or origin_lon < -109 or origin_lat > 41 or origin_lat < 37):
                    # drawing entry points - green
                    # m.scatter([route['entry_lon']],[route['entry_lat']],zorder=5,s=50,color="#00FF00",marker="o")
                    
                    # drawing exit point - red
                    # m.scatter([route['exit_lon']],[route['exit_lat']],zorder=5,s=50,color="#FF0000",marker="o")
                    
                    # drawing full route - yellow
                    # m.scatter(X,Y,zorder=5,s=50,color="#FFFF00",marker="^")
                    # plt.plot(longs,lats,color="#FFFF00",linewidth=2)

                    # drawing the route over Colorado - black
                    # plt.plot(access_longs,access_lats,color="#000000",linewidth=2)

                    # drawing the current point - yellow
                    m.scatter([access_longs[current_point_index]],[access_lats[current_point_index]],zorder=5,s=50,color="#FFFF00",marker="o")
                    
                # drawing outgoing routes - violet
                if (dest_lon > -102 or dest_lon < -109 or dest_lat > 41 or dest_lat < 37) and (origin_lon < -102 and origin_lon > -109 and origin_lat < 41 and origin_lat > 37):
                    # drawing entry points - green
                    # m.scatter([route['entry_lon']],[route['entry_lat']],zorder=5,s=50,color="#00FF00",marker="o")
                    
                    # drawing exit point - red
                    # m.scatter([route['exit_lon']],[route['exit_lat']],zorder=5,s=50,color="#FF0000",marker="o")
                    
                    # drawing full route - violet
                    # m.scatter(X,Y,zorder=5,s=50,color="#8B00FF",marker="^")
                    # plt.plot(longs,lats,color="#8B00FF",linewidth=2)

                    # drawing the route over Colorado - black
                    # plt.plot(access_longs,access_lats,color="#000000",linewidth=2)

                    # drawing the current point - violet
                    m.scatter([access_longs[current_point_index]],[access_lats[current_point_index]],zorder=5,s=50,color="#8B00FF",marker="o")

                # drawing passing routes - Blue
                if (dest_lon > -102 or dest_lon < -109 or dest_lat > 41 or dest_lat < 37) and (origin_lon > -102 or origin_lon < -109 or origin_lat > 41 or origin_lat < 37):
                    # drawing entry points - green
                    # m.scatter([route['entry_lon']],[route['entry_lat']],zorder=5,s=50,color="#00FF00",marker="o")
                    
                    # drawing exit point - red
                    # m.scatter([route['exit_lon']],[route['exit_lat']],zorder=5,s=50,color="#FF0000",marker="o")
                    
                    # drawing full route - Blue
                    # m.scatter(X,Y,zorder=5,s=50,color="#0000FF",marker="^")
                    # plt.plot(longs,lats,color="#0000FF",linewidth=1)

                    # drawing the route over Colorado
                    # plt.plot(access_longs,access_lats,color="#000000",linewidth=2)

                    # drawing the current point - Blue
                    m.scatter([access_longs[current_point_index]],[access_lats[current_point_index]],zorder=5,s=50,color="#0000FF",marker="o")
        
        output_path = output_filename+'/'+normal_datetime.replace(":","_").replace("-","_").replace(" ","_")+'.png'
        plt.savefig(output_path)
        plt.close()
        plot_filenames.append(output_path)

        current_unix_time += increase_time_intervall
        print(current_unix_time)

import imageio
images = []
for filename in plot_filenames:
    images.append(imageio.imread(filename))
imageio.mimsave(output_filename+'/'+'simulation.gif', images, duration=gif_duration)

if args.keep_snapchat_images == "0":
    folder_path = (output_filename)
    images = os.listdir(folder_path)
    for image in images:
        if image.endswith(".png"):
            os.remove(os.path.join(folder_path, image))