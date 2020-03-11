from mpl_toolkits.basemap import Basemap
from math import sin, cos, sqrt, atan2, radians

class Path_finder:

     def __init__(self):
          self.map = Basemap(llcrnrlon=-180,llcrnrlat=10,urcrnrlon=-50,urcrnrlat=70, lat_ts=0, resolution='l')

     def find_route(self, source_lat, source_lon, target_lat, target_lon):
          distance_in_miles = self.calculate_distance(source_lat, source_lon, target_lat, target_lon)
          Longs, Lats = self.map.gcpoints(source_lon, source_lat, target_lon, target_lat, distance_in_miles/40)
          return Longs, Lats, len(Longs), len(Lats)

     def calculate_distance(self, source_lat, source_lon, target_lat, target_lon):       
          #approximate radius of earth in km
          R = 6373.0

          lat1 = radians(source_lat)
          lon1 = radians(source_lon)
          lat2 = radians(target_lat)
          lon2 = radians(target_lon)
          dlon = lon2 - lon1
          dlat = lat2 - lat1
          a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
          c = 2 * atan2(sqrt(a), sqrt(1 - a))
          distance = R * c        
          return distance

t = Path_finder()
print(t.find_route(40.63980103, -73.77890015, 37.61899948120117, -122.375))
