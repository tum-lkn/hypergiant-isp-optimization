import logging

import algorithm.mip_pathbased_lin
import generator.demand_generator
import generator.topology_generator
import algorithm.mip
import model.fixed_layers
import scenario
import constants
import output.file_writer
import control

logging.basicConfig(level=logging.DEBUG)

topo_cfgs = [
    generator.topology_generator.ComposedTopologyGeneratorConfiguration(
        opt_topo_gen_config=generator.topology_generator.SimpleOpticalTopologyGeneratorConfiguration(
            fiber_capacity=5),
        ip_topo_gen_config=generator.topology_generator.IPNodesFromCSVGeneratorConfiguration(
            fname='example_data/ip_nodes.csv',
            num_transceiver=4
        ),
        parameter={
            constants.KEY_IP_LIGHTPATH_CAPACITY: lp_cap,
            constants.KEY_IP_LINK_UTILIZATION: 0.3
        }
    ) for lp_cap in [100, 50]
]

demand_cfg = generator.demand_generator.DemandSetGeneratorFromCSVConfiguration(
    fname_demand='example_data/demand_simple.csv',
    fname_peering='example_data/peering_simple.csv',
    parameter={}
)

algo_cfg = algorithm.mip_pathbased_lin.PathBasedMixedIntegerProgramConfiguration(
    model_implementor=algorithm.mip.AbstractMixedIntegerProgram.MODEL_IMPLEMENTOR_CBC
)

fixed_layers = model.fixed_layers.HardCodedFixedLayersConfiguration()

scens = [
    scenario.ScenarioConfiguration(
        topology_configuration=topo_cfg,
        demand_configuration=demand_cfg,
        algorithm_configuration=algo_cfg,
        fixed_layers=fixed_layers,
        outputs=[output.file_writer.JsonWriterConfiguration(base_path="example_output")]
    ) for topo_cfg in topo_cfgs]

control.SequentialRunner(scens).run_all()
