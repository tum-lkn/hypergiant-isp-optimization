import pandas as pd
import os
import json
import csv

from scripts import get_reconfiguration_metrics as reconf_module
from scripts import aggregate_raw_solutions as aggmetrics_module


def load_solution_ip_nodes_configuration(solution_fname):
    with open(solution_fname, "r") as fd:
        solution = json.load(fd)

    with open(solution_fname.replace("solution", "configuration"), "r") as fd:
        json_config = json.load(fd)
    ip_nodes_fname = json_config["topology"]["ip_topo"]["fname"]

    ip_nodes = set()
    with open(ip_nodes_fname, "r") as ip_nodes_file:
        read_csv = csv.reader(ip_nodes_file, delimiter=',')
        for row in read_csv:
            ip_nodes.add(row[0])
    ip_nodes = sorted(list(ip_nodes))
    return solution, ip_nodes, json_config


def get_reconfigurations_for_failures(path_to_solutions, failure_type):
    """
    Calculates the reconfigurations applied to restore from failures
    """
    reconf_metrics = []

    for folder in filter(
            lambda x: f"{'link_' if failure_type == 'link' else 'fiber'}failure_analysis_" in x,
            os.listdir(path_to_solutions)
    ):
        try:
            print(folder)
            # Extract timestamp and config id
            timestamp = folder.split("_")[-2]
            orig_config_id = folder.split("_")[-1]
            if len(folder.split("_")) > 5:
                num_fail_cases = int(folder.split("_")[-3]) if failure_type == "link" else -1
            else:
                num_fail_cases = 0

            # Load original solution
            orig_solution_fname = next(filter(
                lambda x: "solution" in x and orig_config_id in x,
                os.listdir(path_to_solutions)
            ))
            print(orig_solution_fname)

            original_solution, ip_nodes, json_config = load_solution_ip_nodes_configuration(
                os.path.join(path_to_solutions, orig_solution_fname)
            )

            adj_matrix_original_solution, _, _ = reconf_module.build_adj_matrix_from_solution(
                original_solution, ip_nodes, link_type=None
            )
            routing_matrix_original_solution = reconf_module.build_routing_matrix_from_solution(
                original_solution
            )
            cdn_matrix_original_solution, _, _ = reconf_module.build_cdn_pop_matrix_from_solution(
                original_solution, unodes=None, peering_nodes=None
            )

            # Iterate over all failures
            for fail_event in filter(
                    lambda x: "solution" in x,
                    os.listdir(os.path.join(path_to_solutions, folder))
            ):
                solution_fname = os.path.join(path_to_solutions, folder, fail_event)
                try:
                    fail_event_solution, ip_nodes, json_config = load_solution_ip_nodes_configuration(
                        solution_fname
                    )
                    failed_link = json_config["topology"]["opt_topo"]["failed_links"] if failure_type != "link" else ''
                    restoration_rclimit = json_config["fixed_layers"]["fix_ip_links"] if failure_type == "link" else json_config["fixed_layers"]["fix_ip_links_w_opt"]
                    restoration_link_util = json_config["topology"]["parameter"]["IP_LINK_UTILIZATION"]
                    restoration_cdn_limit = ("CDN_ASSIGNMENT" in json_config["fixed_layers"])

                    result = {
                        'failed_links': failed_link,
                        'restoration_reconf_limit': restoration_rclimit,
                        'restoration_link_util': restoration_link_util,
                        'fixed_cdns': restoration_cdn_limit,
                        'orig_config_id': orig_config_id,
                        'num_fail_cases': num_fail_cases
                    }
                    result.update(
                        aggmetrics_module.get_metrics_single_solution(
                            solution_file=fail_event,
                            base_path=os.path.join(path_to_solutions, folder)+'/'
                        )
                    )
                    for k in result.keys():
                        result[k] = [result[k]]

                    if len(fail_event_solution["ip_links"]) > 0:
                        # Restoration was possible. Calculate amount of reconfigurations
                        adj_matrix_fail_event_solution, _, _ = reconf_module.build_adj_matrix_from_solution(
                            fail_event_solution, ip_nodes, link_type=None
                        )

                        this_reconfs = reconf_module.calculate_reconfiguration_metrics(
                            [
                                (timestamp, adj_matrix_original_solution),
                                (timestamp, adj_matrix_fail_event_solution)]
                        )
                        result.update(this_reconfs)

                        # Routing changes
                        routing_fail_event_solution = reconf_module.build_routing_matrix_from_solution(
                            fail_event_solution
                        )

                        this_reconfs = reconf_module.calculate_reconfiguration_metrics_routing(
                            [
                                (timestamp, routing_matrix_original_solution),
                                (timestamp, routing_fail_event_solution)]
                        )
                        result.update(this_reconfs)

                        # CDN Assignment changes
                        cdn_fail_event_solution, _, _ = reconf_module.build_cdn_pop_matrix_from_solution(
                            fail_event_solution, unodes=None, peering_nodes=None
                        )
                        this_reconfs = reconf_module.calculate_reconfiguration_metrics_cdn(
                            [
                                (timestamp, cdn_matrix_original_solution),
                                (timestamp, cdn_fail_event_solution)]
                        )
                        result.update(this_reconfs)
                    else:
                        # Could not restore.
                        result.update(
                            {
                                'input_timestamp': [timestamp],
                                'num_links_added': [-1],
                                'num_links_removed': [-1],
                                'num_trunks_increased': [-1],
                                'num_trunks_decreased': [-1],
                                'num_ip_nodes': [0],
                                'changed_cdn_assignments': [-1],
                                'total_cdn_assignments': [-1],
                                'routing_changes_cdn': [-1],
                                'routing_changes_e2e': [-1],
                                'total_cdn_routes': [-1],
                                'total_e2e_routes': [-1],
                                'routing_changes_cdn_samepop': [-1]
                            }
                        )
                    reconf_metrics.append(pd.DataFrame(result))
                except Exception as e:
                    print(e)
                    continue
        except Exception as e:
            print(e)
            continue
    return reconf_metrics


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Calculate reconfigurations for failure cases')
    parser.add_argument('--path', help="base path", type=str)
    parser.add_argument('--suffix', help="suffix of path", type=str)
    parser.add_argument('--ftype', type=str, default='')
    args = parser.parse_args()

    BASE_PATH_SOL = args.path + "/output_" + args.suffix + "/"

    df = get_reconfigurations_for_failures(BASE_PATH_SOL, args.ftype)
    pd.concat(df).to_hdf(
        os.path.join(args.path, f"{args.ftype}failure_reconf_metrics_{args.suffix}.h5"),
        key='failure_reconf_metrics'
    )
