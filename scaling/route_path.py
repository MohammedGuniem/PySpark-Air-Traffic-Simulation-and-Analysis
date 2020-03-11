class Path_finder:

     def __init__(self):
          from mpl_toolkits.basemap import Basemap
          self.map = Basemap(llcrnrlon=-180,llcrnrlat=10,urcrnrlon=-50,urcrnrlat=70, lat_ts=0, resolution='l')


     def find_route(self, source_lat, source_lon, target_lat, target_lon, distance_in_miles):
          Longs, Lats = self.map.gcpoints(source_lon, source_lat, target_lon, target_lat, distance_in_miles/40)
          return Longs, Lats

t = Path_finder()

print(t.find_route(40.63980103, -73.77890015, 37.61899948120117, -122.375, 2572))
