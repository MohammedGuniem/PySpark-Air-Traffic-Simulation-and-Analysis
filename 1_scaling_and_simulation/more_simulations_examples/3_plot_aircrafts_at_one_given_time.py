# To run an example of this script use this command
# python 3_plot_aircrafts_at_one_given_time.py --input_folder=scaled_data_2019_04_10 --input_datetime="2019-04-10 15:49:00"

from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt
from datetime import datetime
import argparse, sys
import json
import time

parser = argparse.ArgumentParser()
parser.add_argument('--input_folder', help='Enter the name of your input file which generated using pyspark, for example (scaled_data_2019_04_10)')
parser.add_argument('--input_datetime', help='Enter one datetime in the simulation, for example (2019-05-25 18:00:00)')
args = parser.parse_args()

input_folder = args.input_folder
time = round(time.mktime(datetime.strptime(args.input_datetime, '%Y-%m-%d %H:%M:%S').timetuple())/60)

plt.figure(figsize = (15,8))
m = Basemap(llcrnrlon=-170,llcrnrlat=10,urcrnrlon=-60,urcrnrlat=70, lat_ts=0, resolution='l')
m.drawmapboundary(fill_color='#5D9BFF')
m.fillcontinents(color='white',lake_color='blue')
m.drawcountries(color='#585858',linewidth=1)
m.drawstates(linewidth = 0.2)
m.drawcoastlines()

with open("../"+input_folder+"/position_information.json", 'r') as file:
    route_positions = json.load(file)[str(time)]

    for flight_position in route_positions:
        m.scatter([flight_position['longitude']],[flight_position['latitude']],zorder=5,s=1,color="#0000FF",marker="^")

plt.savefig("plots/3_all_routes_on_"+args.input_datetime.replace(" ","_").replace(":","")+".png")
plt.close()