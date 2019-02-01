import os
import sys
import time

path = "to_return/"

now = time.time()

for f in os.listdir(path):
    if os.stat(os.path.join(path,f)).st_mtime < now - 86400:
        print("File name: {}".format(f))
        os.remove(path + f)