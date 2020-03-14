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
        r = row[36].split("|")
        origin_lat = float(r[0])
        origin_lon = float(r[1])
        destination_lat = float(r[2])
        destination_lon = float(r[3])
        Points = {"Source":(origin_lat,origin_lon),"Destination":(destination_lat,destination_lon)}
        Lon = [Points[key][0] for key in Points]
        Lat = [Points[key][1] for key in Points]
        X, Y = m(Lat,Lon)
        # Draw passover routes as blue routes
        if r[4] == "INSIDE" and row[12] != "Colorado" and row[9] != "Colorado":
          m.scatter(X,Y,zorder=5,s=50,color="#de1dcb",marker="o")
          x, y = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],500)
          plt.plot(x,y,color="#201dde",linewidth=2)
        # Draw landing routes as green routes
        if r[4] == "INSIDE" and row[12] == "Colorado" and row[9] != "Colorado":
          m.scatter(X,Y,zorder=5,s=50,color="#de1dcb",marker="o")
          x, y = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],500)
          plt.plot(x,y,color="#1dde37",linewidth=2)
        # Draw takeoff routes as red routes
        if r[4] == "INSIDE" and row[12] != "Colorado" and row[9] == "Colorado":
          m.scatter(X,Y,zorder=5,s=50,color="#de1dcb",marker="o")
          x, y = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],500)
          plt.plot(x,y,color="#DE1D1D",linewidth=2)

plt.show()