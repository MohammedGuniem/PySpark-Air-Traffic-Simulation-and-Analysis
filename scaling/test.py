import time

mins = time.time()/60

print("Mins since epoch =", mins)

import time

date_time = '29.08.2011 11:05:02'
pattern = '%d.%m.%Y %H:%M:%S'
epoch = int(time.mktime(time.strptime(date_time, pattern)))
print(epoch)
