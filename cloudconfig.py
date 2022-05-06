"""Giza on Cassandra Test Profile
"""

import geni.portal as portal
import geni.rspec.pg as rspec
import geni.rspec.emulab as emulab

pc = portal.Context()

# Describe the parameter(s) this profile script can accept.
pc.defineParameter("n", "Number of VMs", portal.ParameterType.INTEGER, 2)

# Retrieve the values the user specifies during instantiation.
params = pc.bindParameters()

request = pc.makeRequestRSpec()

if params.n < 1 or params.n > 8:
    pc.reportError(portal.ParameterError("You must choose at least 1 and no more than 8 VMs.", ["n"]))

pc.verifyParameters()

link = request.BridgedLink("link")
link.latency = 100
link.bridge.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops:FBSD121-64-STD"

for i in range(params.n):
    si = str(i + 1)
    node = request.RawPC("node" + si)
    node.disk_image = "urn:publicid:IDN+utah.cloudlab.us+image+hyflowtm-PG0:giza-cassandra-test:1"

    iface = node.addInterface("if" + si)

    # Specify the component id and the IPv4 address
    iface.component_id = "eth1"
    iface.addAddress(rspec.IPv4Address("192.168.1." + si, "255.255.255.0"))

    link.addInterface(iface)

pc.printRequestRSpec()
