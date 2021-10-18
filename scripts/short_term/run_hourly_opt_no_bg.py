import logging

import config
import constants
import control
import model.fixed_layers
import output.file_writer
import scenario
from scripts import helpers

"""
Optimize every 2hours. Take the max solution of optimization every 2h and check if it can be 
used for the whole 2-hour time window.
"""

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s - %(asctime)s - %(threadName)s - %(name)s  - %(message)s'
                        )

    BASE_IN_FOLDER = f"{config.BASE_PATH}/input_1h"
    BASE_OUT_FOLDER = f"{config.BASE_PATH}/output_1h"

    BASE_FOLDER_SOL = f"{config.BASE_PATH}/output_"

    algo_cfgs = [
        # Algo, bg_demand_config, path to solution file
        (config.ALGO_CONFIG_MIP, None,
         [
             f"2h/solution_jomip_{ts}.json" for ts in [
                 # 2h
                 0, 7200, 14400, 21600, 28800, 36000, 43200, 50400, 57600, 64800, 72000, 79200, 86400
             ]
         ]
         ),
        (config.ALGO_CONFIG_GREEDY, None,
         [
             f"2h/solution_greedy_{ts}.json" for ts in [
                 # 2h
                 0, 7200, 14400, 21600, 28800, 36000, 43200, 50400, 57600, 64800, 72000, 79200, 86400
             ]
         ]
         ),
        (config.ALGO_CONFIG_MIP, "fixed_cdn",
         [
             f"2h/solution_isponly_{ts}.json" for ts in [
                 0, 7200, 14400, 21600, 28800, 36000, 43200, 50400, 57600, 64800, 72000, 79200, 86400
             ]
         ]
         )

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
                tw = sol.split("/")[0]
                fixed_layer = model.fixed_layers.FromSolutionFileFixedLayersConfiguration(
                    path_to_file=f"{BASE_FOLDER_SOL}{sol}",
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
                            comment=f"{tw}_opt_check"
                        )
                    )
    print("Created {} scenario configurations".format(len(scenario_cfgs)))
    control.ParallelRunner(scenario_cfgs, num_jobs=1).run_all()
