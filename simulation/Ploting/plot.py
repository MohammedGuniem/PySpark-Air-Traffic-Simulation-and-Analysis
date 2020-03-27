from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt
import json

plt.figure(figsize = (15,8))
m = Basemap(llcrnrlon=-170,llcrnrlat=10,urcrnrlon=-60,urcrnrlat=70, lat_ts=0, resolution='l')
m.drawmapboundary(fill_color='#5D9BFF')
m.fillcontinents(color='white',lake_color='blue')
m.drawcountries(color='#585858',linewidth=1)
m.drawstates(linewidth = 0.2)
m.drawcoastlines()

with open("../simulated_data.json", 'r') as file:
    data = json.load(file)
    for route in data:
        if route['is_in_area'] == "INSIDE":
            Points = {"Source":(route['dest_lat'],route['dest_lon']),"Destination":(route['origin_lat'],route['origin_lon'])}
            Lon = [Points[key][0] for key in Points]
            Lat = [Points[key][1] for key in Points]
            X, Y = m(Lat,Lon)

            origin_lat = route['origin_lat']
            origin_lon = route['origin_lon']
            dest_lat = route['dest_lat']
            dest_lon = route['dest_lon']
            longs, lats = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],100)
            
            # drawing inside Colorado routes - orange
            if (dest_lon < -102 and dest_lon > -109 and dest_lat < 41 and dest_lat > 37) and (origin_lon < -102 and origin_lon > -109 and origin_lat < 41 and origin_lat > 37):
                # drawing entry points - green
                m.scatter([route['entry_lon']],[route['entry_lat']],zorder=5,s=50,color="#00FF00",marker="o")
                # drawing exit point - red
                m.scatter([route['exit_lon']],[route['exit_lat']],zorder=5,s=50,color="#FF0000",marker="o")
                m.scatter(X,Y,zorder=5,s=50,color="#FF7F00",marker="^")
                plt.plot(longs,lats,color="#FF7F00",linewidth=2)
            # drawing incoming routes - yellow
            if (dest_lon < -102 and dest_lon > -109 and dest_lat < 41 and dest_lat > 37) and (origin_lon > -102 or origin_lon < -109 or origin_lat > 41 or origin_lat < 37):
                # drawing entry points - green
                m.scatter([route['entry_lon']],[route['entry_lat']],zorder=5,s=50,color="#00FF00",marker="o")
                # drawing exit point - red
                m.scatter([route['exit_lon']],[route['exit_lat']],zorder=5,s=50,color="#FF0000",marker="o")
                m.scatter(X,Y,zorder=5,s=50,color="#FFFF00",marker="^")
                plt.plot(longs,lats,color="#FFFF00",linewidth=2)
            # drawing outgoing routes - violet
            if (dest_lon > -102 or dest_lon < -109 or dest_lat > 41 or dest_lat < 37) and (origin_lon < -102 and origin_lon > -109 and origin_lat < 41 and origin_lat > 37):
                # drawing entry points - green
                m.scatter([route['entry_lon']],[route['entry_lat']],zorder=5,s=50,color="#00FF00",marker="o")
                # drawing exit point - red
                m.scatter([route['exit_lon']],[route['exit_lat']],zorder=5,s=50,color="#FF0000",marker="o")
                m.scatter(X,Y,zorder=5,s=50,color="#8B00FF",marker="^")
                plt.plot(longs,lats,color="#8B00FF",linewidth=2)
            # drawing passing routes - Blue
            if (dest_lon > -102 or dest_lon < -109 or dest_lat > 41 or dest_lat < 37) and (origin_lon > -102 or origin_lon < -109 or origin_lat > 41 or origin_lat < 37):
                # drawing entry points - green
                m.scatter([route['entry_lon']],[route['entry_lat']],zorder=5,s=50,color="#00FF00",marker="o")
                # drawing exit point - red
                m.scatter([route['exit_lon']],[route['exit_lat']],zorder=5,s=50,color="#FF0000",marker="o")
                m.scatter(X,Y,zorder=5,s=50,color="#0000FF",marker="^")
                plt.plot(longs,lats,color="#0000FF",linewidth=2)

print("The number of flights over target area: ", len(data))
plt.show()
