import logging

import algorithm.mip
import algorithm.mip_pathbased_lin
import constants
import model.demand
import model.input
import model.topology

logging.basicConfig(level=logging.DEBUG)

# First generate optical nodes
FIBER_CAPACITY = 4

mytopo = model.topology.Topology(
    name="MyTestTopology",
    parameter={
        constants.KEY_IP_LIGHTPATH_CAPACITY: 2
    }
)

for _ in range(5):
    opt_node = model.topology.OpticalNode(nid=model.topology.NodeIdGenerator.get_next_id())
    mytopo.add_node(opt_node)
for i in [1, 4]:
    mytopo.add_node(
        model.topology.IPNode(
            nid=10 + i,
            parent=mytopo.get_node_by_id(i),
            num_transceiver=4
        )
    )

for i, j in [
    (mytopo.get_node_by_id(1), mytopo.get_node_by_id(2)),
    (mytopo.get_node_by_id(2), mytopo.get_node_by_id(3)),
    (mytopo.get_node_by_id(3), mytopo.get_node_by_id(4)),
    (mytopo.get_node_by_id(1), mytopo.get_node_by_id(5)),
    (mytopo.get_node_by_id(3), mytopo.get_node_by_id(5))
]:
    mytopo.add_edge(
        model.topology.OpticalLink(i, j, capacity=FIBER_CAPACITY)
    )
    mytopo.add_edge(
        model.topology.OpticalLink(j, i, capacity=FIBER_CAPACITY)
    )

mydemandset = model.demand.DemandSet()

mydemandset.append(
    model.demand.Hypergiant(
        name="alpha",
        peering_nodes=[
            model.demand.PeeringNode(nid=model.topology.NodeIdGenerator.get_next_id(), parent=mytopo.get_node_by_id(14),
                                     capacity=6)
        ],
        user_nodes=[
            model.demand.EndUserNode(nid=model.topology.NodeIdGenerator.get_next_id(), parent=mytopo.get_node_by_id(11),
                                     demand_volume=3)
        ]
    )
)

myinput = model.input.InputInstance(
    topology=mytopo,
    demandset=mydemandset
)

mymodelbuilder = algorithm.mip_pathbased_lin.PathMixedIntegerProgram(myinput,
                                                                     algorithm.mip_pathbased_lin.PathMixedIntegerProgram.MODEL_IMPLEMENTOR_CPLEX)
mymodelbuilder.build()

mymodelbuilder.solve()

mymodelbuilder.print_solution()
