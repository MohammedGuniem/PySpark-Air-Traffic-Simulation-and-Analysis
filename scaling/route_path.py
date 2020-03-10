from mpl_toolkits.basemap import Basemap

m = Basemap(llcrnrlon=-180,llcrnrlat=10,urcrnrlon=-50,urcrnrlat=70, lat_ts=0, resolution='l')

Points = {"New York":(40.7,-74),"San Francisco":(37.8,-122.4)}
Lon = [Points[key][0] for key in Points]
Lat = [Points[key][1] for key in Points]
Longs, Lats = m(Lat,Lon)

print("Route Longs: " + str(Longs))
print("Route Lats: " + str(Lats))
Longs, Lats = m.gcpoints(Lat[0],Lon[0],Lat[1],Lon[1],3)
print("Longs: " + str(Longs))
print("Lats: " + str(Lats))