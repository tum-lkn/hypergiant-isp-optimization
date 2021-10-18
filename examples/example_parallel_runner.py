import logging

import algorithm.mip_pathbased_lin
import generator.demand_generator
import generator.topology_generator
import algorithm.mip
import scenario
import constants
import src.model.fixed_layers
import control

logging.basicConfig(level=logging.DEBUG)

topo_cfgs = [
    generator.topology_generator.ComposedTopologyGeneratorConfiguration(
        opt_topo_gen_config=generator.topology_generator.HardCodedOpticalTopologyGeneratorConfiguration(
            nodes=['O-A', 'O-B', 'O-C', 'O-D'],
            links=[('O-A', 'O-B'), ('O-B', 'O-C'), ('O-C', 'O-D')],
            fiber_capacity=6
        ),
        ip_topo_gen_config=generator.topology_generator.IPNodesFromCSVGeneratorConfiguration(
            fname='example_data/line_ip_nodes.csv',
            num_transceiver=4
        ),
        parameter={
            constants.KEY_IP_LIGHTPATH_CAPACITY: lp_cap
        }
    ) for lp_cap in [3, 2]
]

demand_cfg = generator.demand_generator.DemandSetGeneratorFromCSVConfiguration(
    fname_demand='example_data/demand_line.csv',
    fname_peering='example_data/peering_line.csv',
    parameter={}
)

algo_cfg = algorithm.mip_pathbased_lin.PathBasedMixedIntegerProgramConfiguration(
    model_implementor=algorithm.mip_pathbased_lin.PathMixedIntegerProgram.MODEL_IMPLEMENTOR_CPLEX
)

scens = [scenario.ScenarioConfiguration(
    topology_configuration=topo_cfg,
    demand_configuration=demand_cfg,
    algorithm_configuration=algo_cfg,
    fixed_layers=src.model.fixed_layers.HardCodedFixedLayersConfiguration(ip_links={
        'B<->A': {'num_trunks': 2, 'opt_links': [('O-B', 'O-A', 2)]},
        'C<->B': {'num_trunks': 2, 'opt_links': [('O-C', 'O-B', 2)]},
        'D<->C': {'num_trunks': 2, 'opt_links': [('O-D', 'O-C', 2)]}
    }
    ),
    outputs=list()
) for topo_cfg in topo_cfgs]

control.ParallelRunner(scens, num_jobs=2).run_all()
