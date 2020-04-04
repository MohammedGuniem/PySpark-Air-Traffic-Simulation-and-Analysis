start_longitude_left = -130
start_longitude_right = -120

count = 0
for llcrnlon in range(-130, -60, 10):
    urcrnrlon = llcrnlon+10
    print(llcrnlon, urcrnrlon)
    print("*")
    for llcrnrlat in range(25, 50, 5):
        urcrnrlat = llcrnrlat+5
        print(llcrnrlat, urcrnrlat)
        count += 1
    print("------------")
    

print(count)
import sys
sys.exit()
print("-------------")
for line_longitude in range(-130, -50, 10):
    for line_latitude in range(25, 55, 5):
         print(line_longitude, line_latitude)