"""
Simple example of MIP with a line optical topology and a single CDN.
"""
import algorithm.mip_pathbased_lin
import model.topology
import model.input
import model.demand
import algorithm.mip
import constants
import logging

logging.basicConfig(level=logging.DEBUG)

# First generate optical nodes
FIBER_CAPACITY = 4
LIGHTPATH_CAPACITY = 2


def build_topology():
    mytopo = model.topology.Topology(
        name="MyTestTopology",
        parameter={
            constants.KEY_IP_LIGHTPATH_CAPACITY: LIGHTPATH_CAPACITY
        }
    )

    for _ in range(4):
        opt_node = model.topology.OpticalNode(nid=model.topology.NodeIdGenerator.get_next_id())
        mytopo.add_node(opt_node)
    for n in mytopo.opt_nodes:
        mytopo.add_node(
            model.topology.IPNode(
                nid=model.topology.NodeIdGenerator.get_next_id(),
                parent=n,
                num_transceiver=4
            )
        )

    for i, j in [
        (mytopo.get_node_by_id(1), mytopo.get_node_by_id(2)),
        (mytopo.get_node_by_id(2), mytopo.get_node_by_id(3)),
        (mytopo.get_node_by_id(3), mytopo.get_node_by_id(4))
    ]:
        mytopo.add_edge(
            model.topology.OpticalLink(i, j, capacity=FIBER_CAPACITY)
        )
        mytopo.add_edge(
            model.topology.OpticalLink(j, i, capacity=FIBER_CAPACITY)
        )
    return mytopo


if __name__ == '__main__':
    mytopo = build_topology()
    mydemandset = model.demand.DemandSet()
    mydemandset.append(
        model.demand.Hypergiant(
            name="alpha",
            peering_nodes=[
                model.demand.PeeringNode(nid=model.topology.NodeIdGenerator.get_next_id(),
                                             parent=mytopo.get_node_by_id(8), capacity=6)
            ],
            user_nodes=[
                model.demand.EndUserNode(nid=model.topology.NodeIdGenerator.get_next_id(),
                                             parent=mytopo.get_node_by_id(6), demand_volume=3),
                model.demand.EndUserNode(nid=model.topology.NodeIdGenerator.get_next_id(),
                                             parent=mytopo.get_node_by_id(5), demand_volume=3)
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
