import logging
import itertools

import algorithm.greedy
import algorithm.mip_pathbased_lin
import generator.topology_generator
import config

from scripts import helpers

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--rclimit", type=float, default=0)
    parser.add_argument("--util", type=float, default=0.3)
    parser.add_argument("--ts", type=str, default='0')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s - %(asctime)s - %(threadName)s - %(name)s  - %(message)s'
                        )

    algo_cfgs = [
        algorithm.greedy.GreedyCDNAssignmentAlgorithmConfiguration(
            algorithm.mip_pathbased_lin.PathBasedMixedIntegerProgramConfiguration(
                model_implementor=algorithm.mip_pathbased_lin.PathMixedIntegerProgram.MODEL_IMPLEMENTOR_CPLEX,
                num_threads=4,
                time_limit=3600
            )
        )
    ]

    COMMENT = ""
    BASE_IN_FOLDER = f"{config.BASE_PATH}/input_4h_{config.FAILURE_DAY_SUFFIX}/"
    BASE_OUT_FOLDER = f"{config.BASE_PATH}/output_4h/"
    FIX_LAYERS = None

    # -------- END OF ORIGINAL PARAMETERS ------------

    RESTORATION_RC_LIMIT = args.rclimit
    ALLOWED_LINK_CAP = args.util
    CDN_LIMIT = None

    fname_tuples = helpers.get_input_file_tuples(BASE_IN_FOLDER)
    print("Found {} input tuples".format(len(fname_tuples)))
    if args.ts != '0':
        print(fname_tuples.keys())
        print(f"Use only Timestamp {args.ts}")
        fname_tuples = [fname_tuples[args.ts]]
    else:
        fname_tuples = fname_tuples.values()

    topo_demand_tuples = helpers.create_demand_and_topo_configs(
        fname_tuples,
        opt_topo_config=generator.topology_generator.SimpleOpticalTopologyGeneratorConfiguration(
            fiber_capacity=config.FIBER_CAPACITY),
        topo_parameter=config.BASE_TOPO_PARAMS,
        ip_node_default_num_transceiver=config.NUM_TRANSCEIVER,
        demand_parameter={},
        bg_demand_config=None
    )

    failed_links = list()
    scenario_cfgs = list()
    config_ids = list()
    for (orig_topo, dem), algo in itertools.product(
            topo_demand_tuples,
            algo_cfgs
    ):
        this_scn_cfgs, this_config_ids, this_failed_links = helpers.generate_link_failure_scenarios_for_single_configuration(
            BASE_OUT_FOLDER,
            orig_topo,
            dem,
            algo,
            FIX_LAYERS,
            COMMENT,
            RESTORATION_RC_LIMIT,
            ALLOWED_LINK_CAP,
            CDN_LIMIT
        )
        scenario_cfgs += this_scn_cfgs
        config_ids += this_config_ids
        failed_links += this_failed_links

    helpers.run_and_dump_failure_scenarios(
        base_out_folder=BASE_OUT_FOLDER,
        fname_infix=f"iplinkfailures_{RESTORATION_RC_LIMIT}_{ALLOWED_LINK_CAP}",
        scenario_configs=scenario_cfgs,
        failed_links=failed_links,
        config_ids=config_ids,
        num_workers=1
    )
