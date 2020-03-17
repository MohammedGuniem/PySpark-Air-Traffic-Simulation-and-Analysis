from mpl_toolkits.basemap import Basemap
from math import sin, cos, sqrt, atan2, radians
from matplotlib import pyplot as plt
import os
import csv

plt.figure(figsize = (15,8))
m = Basemap(llcrnrlon=-130,llcrnrlat=10,urcrnrlon=-60,urcrnrlat=70, lat_ts=0, resolution='l')
m.drawmapboundary(fill_color='#5D9BFF')
m.fillcontinents(color='white',lake_color='blue')
m.drawcountries(color='#585858',linewidth=1)
m.drawstates(linewidth = 0.2)
m.drawcoastlines()

data_file = None 
directory = os.path.join("d:\\","Data-intensive systemer/dat500_project/scaling/scale_model_2019_03_08_1720_1840/")
for root,dirs,files in os.walk(directory):
    for file in files:
       if file.endswith(".csv"):
          data_file = "d:\\Data-intensive systemer/dat500_project/scaling/scale_model_2019_03_08_1720_1840/" + file

with open(data_file, 'r') as file:
    reader = csv.reader(file)
    headers = next(reader, None)
    for row in reader:
        origin_lat = float(row[46])
        origin_lon = float(row[47])
        destination_lat = float(row[48])
        destination_lon = float(row[49])
        Points = {"Source":(origin_lat,origin_lon),"Destination":(destination_lat,destination_lon)}
        Lon = [Points[key][0] for key in Points]
        Lat = [Points[key][1] for key in Points]
        X, Y = m(Lat,Lon)
        # Draw outside routes as black routes
        if row[50] == "OUTSIDE":
          m.scatter(X,Y,zorder=5,s=50,color="#de1dcb",marker="o")
          x, y = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],500)
          plt.plot(x,y,color="#000000",linewidth=2)

plt.show()