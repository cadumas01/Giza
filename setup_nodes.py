# Call the commnads setup_giza many for each node remotely (as done in Neo's)

# Make another script similar to Neo's setup_network_delays that will setup network delays (using same util package) 
# based on delays described in config. Make sure to add network delay section to giza/cassandra.yaml and interpret similarly to Neo's?
# yaml vs json

# To Do
#
# 1. Setup nodes
#   - How much is specified by geni-lib vs. how much do we need to specify?
#   - Use remote_util.py
#   - Use same config so that remote function calls can still work
# 
#   - Use servers.txt for ssh command. First ssh into node 1 and then run all of the commands on node1 through node 5

#
# 2. Setup server delays
#    - Add part to config to allow for server delays
# 
# 3. build.sh on each node
#
# 4. ./giza ...  on each node

# Todo: fix remote ip addresses. setup_giza.sh ... Cut control machine for setup? 

import json
from utils.remote_util import *

CONFIG_PATH = "config.json"

f = open(CONFIG_PATH)

config = json.load(f)
f.close()

server_names = config['server_names']


# Going through each server and crunning setup_giza
for i in range(len(server_names)):
    server_url = get_machine_url(config, server_names[i])

    
    # When running remote commands, we are currently in root
    # Must pass argument with setup_giza.sh [1 to 5] for [california to japan]
    run_remote_command_sync("bash ./Giza/setup_giza.sh " + str(i + 1), server_url) # Not sure if works of if need to change directories or sudo su first
    # run_remote_command_sync("mkdir ./Giza/testfile2", server_url)

