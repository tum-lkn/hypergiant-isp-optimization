import logging

import constants
from scripts import helpers
import config

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--init_ts", type=int, default=0)
    parser.add_argument("--suffix", type=str)
    parser.add_argument("--rclimit", type=float)
    parser.add_argument("--time_agg", type=int, default=4)

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s - %(asctime)s - %(threadName)s - %(name)s  - %(message)s'
                        )

    OFFSET = args.time_agg * 3600
    BASE_IN_FOLDER = f"{config.BASE_PATH}/input_{args.time_agg}h"
    BASE_OUT_FOLDER = f"{config.BASE_PATH}/output_{args.time_agg}h"

    FIX_LAYERS = (args.rclimit, False, False)

    helpers.run_fix_from_beginning_all_in_folder(
        f"{BASE_IN_FOLDER}_{args.suffix}",
        BASE_OUT_FOLDER,
        args.init_ts,
        helpers.get_fiber_topology(config.FIBER_CAPACITY),
        {
            constants.KEY_IP_LIGHTPATH_CAPACITY: config.IP_LINK_CAPACITY,
            constants.KEY_IP_LINK_UTILIZATION: config.IP_LINK_UTIL
        },
        None,
        config.ALGO_CONFIG_MIP,
        config.NUM_TRANSCEIVERS,
        FIX_LAYERS,
        OFFSET
    )
