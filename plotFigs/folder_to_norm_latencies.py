import glob
import os
import random
import math


def extract_norm_latencies(folder, is_for_reads):
    if is_for_reads:
        log_files = glob.glob(os.path.join(folder, "latFileRead*"))
    else:
        log_files = glob.glob(os.path.join(folder, "latency.*"))

    # print(os.path.join(folder, "latFileRead*"))
    # print(log_files)

    norm_latencies = []
    regional_latencies = []
    regional_latency_counts = []
    last_start = 0
    first_end = float('inf')

    # Convert regional latencies into a 2D list and count how many operations were recorded in each region.
    for log_file in log_files:
        with open(log_file) as f:
            ops = [op.split(" ", 2) for op in f.readlines()]
            ops = [[int(op[0]), float(op[1])] for op in ops]
            # print("first five ops")
            # print(ops[:5])
            # print("%d ops in this region" % len(ops))
            regional_latencies.append(ops)

            start = ops[0][0]
            end = ops[-1][0]
            if start > last_start:
                last_start = start
            if end < first_end:
                first_end = end

    for i in range(len(regional_latencies)):
        ops = regional_latencies[i]
        first_index = 0
        while True :

            if ops[first_index][0] >= last_start:
                first_index += 5
                break
            first_index += 1
        last_index = -1
       # print("first_end: ", first_end)
        while True :
           # print(last_index)
           # print("first_end: ", first_end)
            if ops[last_index][0] <= first_end:
                last_index -= 5
                break
            last_index -= 1

        ops = ops[first_index:last_index]
        
        print(first_index, last_index, len(ops))
        regional_latencies[i] = ops
        regional_latency_counts.append(len(ops))

    print(regional_latency_counts)
    print("Just finished printing ", folder, "\n\n")
    latencies_to_take = min(regional_latency_counts)
    # print("taking %d ops from each region" % latencies_to_take)

    # Sample an equal amount of latencies from each region to "normalize" the data. Only extract the latency field.
    for latencies_in_region in regional_latencies:
        sample = random.sample(latencies_in_region, latencies_to_take)
        # print("first 5 samples that are being added")
        # print(sample[:5])
        latencies_to_add = [op[1] for op in sample]
        # print("first 5 latencies that are being added")
        # print(latencies_to_add[:5])
        norm_latencies += latencies_to_add
    # print("%d latencies collected" % len(norm_latencies))
    # print("first 5 latencies")
    # print(norm_latencies[:5])

    return norm_latencies
