import glob
import os
import random

log_files = glob.glob("./latencies/giza_c/6a/latFileWrite*")

latencies = []
# Convert regional latencies into a 2D list and count how many operations were recorded in each region.
for log_file in log_files:
    with open(log_file) as f:
        ops = f.readlines()
        lats = [float(op.split(" ")[1]) for op in ops]
        for lat in lats:
            if lat >= 300:
                latencies.append(lat)

print(sorted(latencies)[len(latencies)//2])
print(max(latencies))
