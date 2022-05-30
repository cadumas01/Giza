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

lan = request.LAN()

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
    lan.addInterface(iface)

# lan.bandwidth = 0 # For some reason, the below does not apply without this line.
# lan.latency = params.lat // 4 # Set the latency of each link in the LAN.






pc.printRequestRSpec()

