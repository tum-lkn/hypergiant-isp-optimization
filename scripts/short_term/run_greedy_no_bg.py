import logging
import argparse
import itertools


import output.file_writer

import scenario
import constants
import model.fixed_layers
import control

import config
from scripts import helpers

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s - %(asctime)s - %(threadName)s - %(name)s  - %(message)s'
                        )

    parser = argparse.ArgumentParser()
    parser.add_argument("--time_agg", type=int, default=4)

    args = parser.parse_args()

    BASE_IN_FOLDER = f"{config.BASE_PATH}/input_{args.time_agg}h"
    BASE_OUT_FOLDER = f"{config.BASE_PATH}/output_{args.time_agg}h"

    algo_cfgs = [
        config.ALGO_CONFIG_GREEDY
    ]
    fixed_layer_cfgs = [
        model.fixed_layers.HardCodedFixedLayersConfiguration()
    ]

    topo_demand_tuples = []

    for folder_suffix in config.FOLDER_SUFFIX_DAY:
        fname_tuples = helpers.get_input_file_tuples(f"{BASE_IN_FOLDER}_{folder_suffix}").values()
        print("Found {} input tuples".format(len(fname_tuples)))

        topo_demand_tuples += helpers.create_demand_and_topo_configs(
            fname_tuples,
            opt_topo_config=helpers.get_fiber_topology(capacity=config.FIBER_CAPACITY),
            topo_parameter={
                constants.KEY_IP_LIGHTPATH_CAPACITY: config.IP_LINK_CAPACITY,
                constants.KEY_IP_LINK_UTILIZATION: config.IP_LINK_UTIL
            },
            ip_node_default_num_transceiver=config.NUM_TRANSCEIVERS,
            demand_parameter={},
            bg_demand_config=None
        )

    scenario_cfgs = list()
    for (topo, dem), algo, fixed_layer in itertools.product(
            topo_demand_tuples,
            algo_cfgs,
            fixed_layer_cfgs
    ):
        scenario_cfgs.append(
            scenario.ScenarioConfiguration(
                topology_configuration=topo,
                demand_configuration=dem,
                algorithm_configuration=algo,
                fixed_layers=fixed_layer,
                outputs=[output.file_writer.JsonWriterConfiguration(BASE_OUT_FOLDER)]
            )
        )
    print("Created {} scenario configurations".format(len(scenario_cfgs)))
    control.ParallelRunner(scenario_cfgs, num_jobs=1).run_all()
