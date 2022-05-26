# Call the commnads setup_giza many for each node remotely (as done in Neo's)

# Make another script similar to Neo's setup_network_delays that will setup network delays (using same util package) 
# based on delays described in config. Make sure to add network delay section to giza/cassandra.yaml and interpret similarly to Neo's?
# yaml vs json

# To Do
#
# 1. Setup nodes
#   - How much is specified by geni-lib vs. how much do we need to specify?
#   - Use remote_util.py
#       - rework json lookups into yaml to python dictionary and then lookup that object
#   - Rework cassandra.yaml to have necessary elemeents for get_machine_url
#
# 2. Setup server delays
#    - Add part to config to allow for server delays
# 
# 3. build.sh on each node
#
# 4. ./giza ...  on each node