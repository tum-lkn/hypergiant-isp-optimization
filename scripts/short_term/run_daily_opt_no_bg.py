import logging
import argparse

import output.file_writer

import scenario
import constants
import model.fixed_layers
import control

import config
from scripts import helpers

"""
Is it sufficient to optimize once per day? Take the max solution of optimization every 1h and check if it can be 
used for the whole day.
"""

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
        # (Algo configuration, bg_demand_config, path to solution file)
        (config.ALGO_CONFIG_MIP, None,
         [
             # Day 1
             "solution_jomip_day1.json",
             # Day 2
             "solution_jomip_day2.json",
             # Day 3
             "solution_jomip_day3.json",
             # Day 4
             "solution_jomip_day4.json",
             # Day 5
             "solution_jomip_day5.json",
             # Day 6
             "solution_jomip_day6.json",
             # Day 7
             "solution_jomip_day7.json",
         ]
         ),
        (config.ALGO_CONFIG_GREEDY, None,
         [
             # Day 1
             "solution_greedy_day1.json",
             # Day 2
             "solution_greedy_day2.json",
             # Day 3
             "solution_greedy_day3.json",
             # Day 4
             "solution_greedy_day4.json",
             # Day 5
             "solution_greedy_day5.json",
             # Day 6
             "solution_greedy_day6.json",
             # Day 7
             "solution_greedy_day7.json",
         ]
         ),
        (config.ALGO_CONFIG_MIP, "fixed_cdn",
         [
             # Day 1
             "solution_isponly_day1.json",
             # Day 2
             "solution_isponly_day2.json",
             # Day 3
             "solution_isponly_day3.json",
             # Day 4
             "solution_isponly_day4.json",
             # Day 5
             "solution_isponly_day5.json",
             # Day 6
             "solution_isponly_day6.json",
             # Day 7
             "solution_isponly_day7.json",
        ])

    ]

    scenario_cfgs = list()
    for folder_suffix in config.FOLDER_SUFFIX_DAY:
        for algo, demand_type, solution_fname in algo_cfgs:
            fname_tuples = helpers.get_input_file_tuples(f"{BASE_IN_FOLDER}_{folder_suffix}").values()
            print("Found {} input tuples".format(len(fname_tuples)))

            topo_demand_tuples = helpers.create_demand_and_topo_configs(
                fname_tuples,
                opt_topo_config=helpers.get_fiber_topology(capacity=config.FIBER_CAPACITY),
                topo_parameter={
                    constants.KEY_IP_LIGHTPATH_CAPACITY: config.IP_LINK_CAPACITY,
                    constants.KEY_IP_LINK_UTILIZATION: config.IP_LINK_UTIL
                },
                ip_node_default_num_transceiver=config.NUM_TRANSCEIVERS,
                demand_parameter={},
                bg_demand_config=demand_type
            )

            for sol in solution_fname:
                fixed_layer = model.fixed_layers.FromSolutionFileFixedLayersConfiguration(
                    path_to_file=f"{BASE_OUT_FOLDER}/{sol}",
                    ip_links=True,
                    ip_connectivity=False,
                    cdn_assignment=False,
                    strict=True
                )
                for (topo, dem) in topo_demand_tuples:
                    scenario_cfgs.append(
                        scenario.ScenarioConfiguration(
                            topology_configuration=topo,
                            demand_configuration=dem,
                            algorithm_configuration=algo,
                            fixed_layers=fixed_layer,
                            outputs=[output.file_writer.JsonWriterConfiguration(BASE_OUT_FOLDER)],
                            comment="daily_opt_check"
                        )
                    )
    print("Created {} scenario configurations".format(len(scenario_cfgs)))
    control.ParallelRunner(scenario_cfgs, num_jobs=3).run_all()
