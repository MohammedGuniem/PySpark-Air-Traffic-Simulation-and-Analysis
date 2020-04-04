# To run an example of this script use this command
# python plot_zone_rute.py

from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt

plt.figure(figsize = (15,8))
m = Basemap(llcrnrlon=-140,llcrnrlat=15,urcrnrlon=-40,urcrnrlat=60, lat_ts=0, resolution='l')
m.drawmapboundary(fill_color='#5D9BFF')
m.fillcontinents(color='white',lake_color='blue')
m.drawcountries(color='#585858',linewidth=1)
m.drawstates(linewidth = 0.1)

for line_longitude in range(-130, -50, 10):
    Points = {"Source":(25,line_longitude),"Destination":(50,line_longitude)}
    Lon = [Points[key][0] for key in Points]
    Lat = [Points[key][1] for key in Points]
    X, Y = m(Lat,Lon)
    longs, lats = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],2)
    m.scatter(X,Y,zorder=5,s=50,color="#FF0000",marker="o")
    plt.plot(longs,lats,color="#000000",linewidth=2)

for line_latitude in range(25, 55, 5):
    Points = {"Source":(line_latitude,-130),"Destination":(line_latitude,-60)}
    Lon = [Points[key][0] for key in Points]
    Lat = [Points[key][1] for key in Points]
    X, Y = m(Lat,Lon)
    longs, lats = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],2)
    m.scatter(X,Y,zorder=5,s=50,color="#FF0000",marker="o")
    plt.plot(longs,lats,color="#000000",linewidth=2)

for llcrnlon in range(-130, -60, 10):
    urcrnrlon = llcrnlon+10
    for llcrnrlat in range(25, 50, 5):
        urcrnrlat = llcrnrlat+5

        coordinates_string = "("+str(llcrnlon)+", "+str(llcrnrlat)+")"
        m.scatter(llcrnlon, llcrnrlat, zorder=5, s=50, color="#0000FF", marker="o")
        plt.text(llcrnlon-6.5, llcrnrlat+1, coordinates_string, color="#a83232")
        
        coordinates_string = "("+str(urcrnrlon)+", "+str(urcrnrlat)+")"
        m.scatter(urcrnrlon, urcrnrlat, zorder=5, s=50, color="#0000FF", marker="o")
        plt.text(urcrnrlon-6.5, urcrnrlat+1, coordinates_string, color="#a83232")

plt.savefig("plot_images/zone_rute.png")
plt.show()