"""Giza on Cassandra Test Profile
"""

import geni.portal as portal
import geni.rspec.pg as rspec
import geni.rspec.emulab as emulab

pc = portal.Context()

# Describe the parameter(s) this profile script can accept.
pc.defineParameter("n", "Number of PCs", portal.ParameterType.INTEGER, 5)
# pc.defineParameter("lat", "Desired RTT between any 2 nodes (in ms)", portal.ParameterType.INTEGER, 200)

# Retrieve the values the user specifies during instantiation.
params = pc.bindParameters()

request = pc.makeRequestRSpec()

if params.n < 1 or params.n > 8:
    pc.reportError(portal.ParameterError("You must choose at least 1 and no more than 8 VMs.", ["n"]))

# if params.lat < 0:
 #   pc.reportError(portal.ParameterError("You must choose at least 0.", ["lat"]))

pc.verifyParameters()

# lan = request.LAN()

node_list = []

for i in range(params.n):
    si = str(i + 1)
    node = request.RawPC("node" + si)
    node.disk_image = "urn:publicid:IDN+utah.cloudlab.us+image+hyflowtm-PG0:giza-cassandra-test:1"

    iface = node.addInterface()

    # Specify the component id and the IPv4 address
    iface.component_id = "eth1"
    iface.addAddress(rspec.IPv4Address("192.168.1." + si, "255.255.255.0"))

    node_list.append(node)
   #  lan.addInterface(iface)

# lan.bandwidth = 0 # For some reason, the below does not apply without this line.
#lan.latency = params.lat // 4 # Set the latency of each link in the LAN.


node_codes = {
    1: "california",
    2: "ireland",
    3: "japan",
    4: "oregon",
    5: "virginia"
}

server_ping_latencies = {
    "california": {
        "california": 0,
        "ireland": 151,
        "japan": 113,
        "oregon": 59,
        "virginia": 72
    },
    "ireland": {
        "california": 151,
        "ireland": 0,
        "japan": 220,
        "oregon": 145,
        "virginia": 88
    },
    "japan": {
        "california": 113,
        "ireland": 220,
        "japan": 0,
        "oregon": 121,
        "virginia": 162
    },
    "oregon": {
        "california": 59,
        "ireland": 145,
        "japan": 121,
        "oregon": 0,
        "virginia": 93
    },
    "virginia": {
        "california": 72,
        "ireland": 88,
        "japan": 162,
        "oregon": 93,
        "virginia": 0
    }
}

# links node_i with node_j
for i in range(len(node_list)):
    for j in range(0,i + 1):
        link = request.Link(members = [node_list[i], node_list[j]])

        code_i = node_codes[i + 1]
        code_j = node_codes[j + 1]

        link.bandwidth = 0
        link.latency = server_ping_latencies[code_i][code_j] / 4




pc.printRequestRSpec()

