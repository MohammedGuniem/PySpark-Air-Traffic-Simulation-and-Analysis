# To run an example of this script use this command
# python 1_plot_routes.py --input_folder=simulated-data-2019-04-10

from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt
from datetime import datetime
import argparse, sys
import json

parser = argparse.ArgumentParser()
parser.add_argument('--input_folder', help='Enter the name of your input folder which generated using pyspark, for example (simulated-data-2019-04-10)')
args = parser.parse_args()
input_folder = args.input_folder

with open("../"+input_folder+"/route_information.json", 'r') as file:
    route_information = json.load(file)

    plt.figure(figsize = (15,8))
    m = Basemap(llcrnrlon=-170,llcrnrlat=10,urcrnrlon=-60,urcrnrlat=70, lat_ts=0, resolution='l')
    m.drawmapboundary(fill_color='#5D9BFF')
    m.fillcontinents(color='white',lake_color='blue')
    m.drawcountries(color='#585858',linewidth=1)
    m.drawstates(linewidth = 0.2)
    m.drawcoastlines()

    for flight_id, flight_information in route_information.items():
        Points = {"Source":(flight_information['origin_lat'],flight_information['origin_lon']),"Destination":(flight_information['destination_lat'],flight_information['destination_lon'])}
        Lon = [Points[key][0] for key in Points]
        Lat = [Points[key][1] for key in Points]
        X, Y = m(Lat,Lon)
        m.scatter(X,Y,zorder=5,s=1,color="#FF7F00",marker="^")
        longs, lats = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],flight_information['airtime_in_minutes'])
        plt.plot(longs,lats,color="#0000FF",linewidth=0.1)

date = input_folder.split("-")
date = date[2]+"-"+date[3]+"-"+date[4]
plt.savefig("plots/plot_images/1_all-routes-on-"+date+".png")
plt.close()
