import argparse
import numpy as np
import os
import shutil

import constants
import generator.demand_generator
import generator.topology_generator
from scripts import helpers

FIBER_CAPACITY = 100
IP_LINK_CAPACITY = 100
NUM_TRANSCEIVERS = 100
IP_LINK_UTIL = 0.5


def randomize_timestamp(folder_in, folder_out, shuffle_peerings, shuffle_timings, seed):
    input_tuples = helpers.get_input_file_tuples(folder_in)

    os.makedirs(folder_out, exist_ok=True)

    if shuffle_timings:
        shuffled_timestamps = list(input_tuples.keys())
        rng = np.random.RandomState(seed=seed)
        rng.shuffle(shuffled_timestamps)

    for timestamp, files in input_tuples.items():
        orig_timestamp = timestamp
        if shuffle_timings:
            timestamp = shuffled_timestamps.pop()

        # Create topology
        opt_topo_config = helpers.get_fiber_topology(capacity=FIBER_CAPACITY)
        topo_parameter = {
                             constants.KEY_IP_LIGHTPATH_CAPACITY: IP_LINK_CAPACITY,
                             constants.KEY_IP_LINK_UTILIZATION: IP_LINK_UTIL
                         }
        ip_node_default_num_transceiver = NUM_TRANSCEIVERS

        topo = generator.topology_generator.ComposedTopologyGeneratorConfiguration(
            opt_topo_gen_config=opt_topo_config,
            ip_topo_gen_config=generator.topology_generator.IPNodesFromCSVGeneratorConfiguration(
                fname=files["ip_nodes"],
                num_transceiver=ip_node_default_num_transceiver
            ),
            parameter=topo_parameter
        ).produce().generate()

        rng = np.random.RandomState(seed=seed)

        demand_set, _ = generator.demand_generator.ShuffledFixedCDNDemandGeneratorFromCSVConfiguration(
            fname_demand=files["demand"],
            fname_peering=files["peering"],
            parameter={},
            seed=seed,
            shuffle_peering=shuffle_peerings
        ).produce(topo, rng=rng).generate()

        fname_demand_out = os.path.join(folder_out, files["demand"].split("/")[-1])
        fname_peering_out = os.path.join(folder_out, files["peering"].split("/")[-1])
        fname_demand_out = fname_demand_out.replace(f"{orig_timestamp}", f"{timestamp}")
        fname_peering_out = fname_peering_out.replace(f"{orig_timestamp}", f"{timestamp}")

        generator.demand_generator.DemandSetCSVParser.write_demand_data(
            fname_demand=fname_demand_out,
            demandset=demand_set,
            timestamp=timestamp
        )

        generator.demand_generator.DemandSetCSVParser.write_peering_data(
            fname_peering=fname_peering_out,
            demandset=demand_set,
            timestamp=timestamp
        )

        _, demand_matrix = generator.demand_generator.ShuffledDemandMatrixFromCSVGeneratorConfiguration(
            fname_demand=files["background"],
            seed=seed
        ).produce(topo, rng=rng).generate()

        fname_ip_nodes_out = os.path.join(folder_out, files["ip_nodes"].split("/")[-1])
        fname_ip_nodes_out = fname_ip_nodes_out
        shutil.copy2(files["ip_nodes"], fname_ip_nodes_out)

        fname_demand_matrix_out = os.path.join(folder_out, files["background"].split("/")[-1])
        fname_demand_matrix_out = fname_demand_matrix_out.replace(f"{orig_timestamp}", f"{timestamp}")
        generator.demand_generator.DemandMatrixCSVParser.write_demand_data(
            fname_matrix=fname_demand_matrix_out,
            demandset=demand_matrix,
            timestamp=timestamp
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=str)
    parser.add_argument("--inpath", type=str)
    parser.add_argument("--outpath", type=str)
    parser.add_argument("--peering", default=False, const=True, action="store_const")
    parser.add_argument("--times", default=False, const=True, action="store_const")
    parser.add_argument("--seed", type=int)

    args = parser.parse_args()

    infolder = os.path.join(args.base, args.inpath)
    outfolder = os.path.join(args.base, args.outpath)

    shuffle_peerings = args.peering
    seed = args.seed

    randomize_timestamp(
        folder_in=infolder, folder_out=outfolder, shuffle_peerings=shuffle_peerings, shuffle_timings=args.times,
        seed=seed
    )
    print("Done")
