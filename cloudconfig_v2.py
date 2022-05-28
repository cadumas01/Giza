"""Based on zhouaea's gus-automoation setup. Setup for Gus replication experiments. Copies the disk image across all machines, not just the control machine."""

# Note : currently setup so the cloudlab experiment must be called "test"

import ast
# Import the Portal object.
import geni.portal as portal
import geni.rspec.pg as rspec
import geni.rspec.emulab as emulab

# Disk image from pantherman594's original "cloudconfig.py" script
DISK_IMAGE = "urn:publicid:IDN+utah.cloudlab.us+image+hyflowtm-PG0:giza-cassandra-test:1"


# Create a portal object,
pc = portal.Context()

portal.context.defineParameter("replicas", "List of Replica Names", portal.ParameterType.STRING, "['california', 'virginia', 'ireland', 'oregon', 'japan']")
portal.context.defineParameter("replica_type", "Replica Hardware Type", portal.ParameterType.NODETYPE, "m510")
portal.context.defineParameter("client_type", "Client Hardware Type", portal.ParameterType.NODETYPE, "c6525-25g")
portal.context.defineParameter("control_machine", "Use Control Machine?", portal.ParameterType.BOOLEAN, True)
portal.context.defineParameter("control_type", "Control Hardware Type", portal.ParameterType.NODETYPE, "m510")
portal.context.defineParameter("control_disk_image", "Control Machine Disk Image", portal.ParameterType.IMAGE, DISK_IMAGE)

params = portal.context.bindParameters()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()
replicas = ast.literal_eval(params.replicas)

lan_list = []
# Instantiate server machines.
for i in range(len(replicas)):
    si = str(i + 1)

    replica = request.RawPC(replicas[i])
    iface = replica.addInterface()

    
    replica.disk_image = DISK_IMAGE


    #iface.component_id = "eth1"

    # Specify the component id and the IPv4 address
    iface.component_id = "eth1"
    iface.addAddress(rspec.IPv4Address("192.168.1." + si, "255.255.255.0"))

    lan_list.append(replica)
    replica.hardware_type = params.replica_type

# Instantiate client machine
# client = request.RawPC('client')
# lan_list.append(client)
# client.hardware_type = params.client_type

# # Instantiate control machine
# if params.control_machine:
#     control = request.RawPC('control')
#     lan_list.append(control)
#     control.hardware_type = params.control_type
#     control.disk_image = params.control_disk_image

lan = request.Link(members=lan_list)

# Print the generated rspec
pc.printRequestRSpec(request)