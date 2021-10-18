import numpy as np
import pandas as pd
import os
import json
import csv
import re
import scipy.spatial.distance as sspd

CDN_FIXED_PREFIX = "fixed_cdn"


def get_config_hashes(agg_metrics, algo_name, demand_type, num_transceiver, ip_link_util,
                      fiber_capacity, fixed_layer, comment, timelimit):
    """
    Returns a series of config hashes that matches to the specified parameters
    """
    this_configs = agg_metrics.loc[
        algo_name, demand_type, num_transceiver, ip_link_util, fiber_capacity, fixed_layer, comment, timelimit
    ]["config_id"].sort_index(inplace=False)

    if np.sum(this_configs.index.duplicated()) > 0:
        raise RuntimeError("Multiple solutions for one timestamp")
    return this_configs


def load_file(config_hash, timestamp, demand_type, base_sol_path, ftype="solution"):
    """
    Returns json content of the specified solution file. Constructs proper fname before.
    :return:
    """
    if demand_type == 'DemandSetGeneratorFromCSVConfiguration':
        infix = f'single_{timestamp}_{config_hash}.json'
    elif demand_type == 'DemandMatrixFromCSVGeneratorConfiguration':
        infix = f'{CDN_FIXED_PREFIX}_single_{timestamp}_{config_hash}.json'
    elif demand_type == 'CombinedDemandGeneratorConfiguration':
        infix = f'single_{timestamp}_{config_hash}.json'
    elif demand_type == 'FixedCDNDemandGeneratorFromCSVConfiguration':
        infix = f'single_{timestamp}_{config_hash}.json'
    elif demand_type == ('DemandSetGeneratorFromCSVConfiguration', 'DemandMatrixFromCSVGeneratorConfiguration'):
        infix = f'single_{timestamp}_{config_hash}.json'
    else:
        raise RuntimeError(f"Demand type {demand_type} not known")
    fname_cfg = os.path.join(base_sol_path, f"{ftype}_link_util_{infix}")

    if not os.path.exists(fname_cfg):
        raise RuntimeError(f"{ftype} file {fname_cfg} does not exist")

    with open(fname_cfg, "r") as fd:
        json_config = json.load(fd)
    return json_config


MATRIX_LINK_TYPE_CDN_E2E = "cdn_e2e"
MATRIX_LINK_TYPE_CDN = "cdn"
MATRIX_LINK_TYPE_E2E = "e2e"


def build_adj_matrix_from_solution(solution, ip_nodes, link_type=None, links_cdn=None, links_e2e=None):
    matrix = np.zeros(shape=(len(ip_nodes), len(ip_nodes)))
    if link_type is None:
        for iplink in solution["ip_links"]:
            node1 = iplink["node1"]
            node2 = iplink["node2"]

            try:
                idx1 = ip_nodes.index(node1)
            except ValueError:
                idx1 = ip_nodes.index(re.sub('\d', '',  node1))
            try:
                idx2 = ip_nodes.index(node2)
            except ValueError:
                idx2 = ip_nodes.index(re.sub('\d', '',  node2))
            matrix[idx1, idx2] += iplink["num_trunks"]
        return matrix, None, None

    # Extract links by type if necessary
    if links_cdn is None:
        links_cdn = set()
        for cdn in solution["cdn_assignment"]:
            for unode in cdn["user_nodes"]:
                for routes in unode["routes"]:
                    links_cdn.add((routes[0], routes[1]))
    if links_e2e is None:
        links_e2e = set()
        for demand in solution["e2e_routing"]:
            for route in demand["paths"]:
                links_e2e.add((route[0][0], route[0][1]))

    if link_type == MATRIX_LINK_TYPE_CDN_E2E:
        this_links = links_e2e.intersection(links_cdn)
    elif link_type == MATRIX_LINK_TYPE_CDN:
        this_links = links_cdn.difference(links_e2e)
    elif link_type == MATRIX_LINK_TYPE_E2E:
        this_links = links_e2e.difference(links_cdn)
    else:
        raise RuntimeError("Matrix link type unknown")

    for iplink in this_links:
        node1 = iplink[0]
        node2 = iplink[1]
        try:
            idx1 = ip_nodes.index(node1)
        except ValueError:
            idx1 = ip_nodes.index(re.sub('\d', '',  node1))
        try:
            idx2 = ip_nodes.index(node2)
        except ValueError:
            idx2 = ip_nodes.index(re.sub('\d', '',  node2))

        matrix[idx1, idx2] = 1
    return matrix, links_cdn, links_e2e


def build_link_load_matrix_from_solution(solution, ip_nodes, demandset):
    matrix = np.zeros(shape=(len(ip_nodes), len(ip_nodes)))
    for cdn in solution["cdn_assignment"]:
        for unode in cdn["user_nodes"]:
            for route in unode["routes"]:
                node1 = route[0]
                node2 = route[1]
                try:
                    idx1 = ip_nodes.index(node1)
                except ValueError:
                    idx1 = ip_nodes.index(re.sub('\d', '', node1))
                try:
                    idx2 = ip_nodes.index(node2)
                except ValueError:
                    idx2 = ip_nodes.index(re.sub('\d', '', node2))
                matrix[idx1, idx2] += demandset[unode["node_id"]] * route[2]
    return matrix


def build_adj_matrix_list_over_time_from_config_hashes(config_hashes, demand_type, base_sol_path):
    if np.sum(config_hashes.index.duplicated()) > 0:
        raise RuntimeError("Multiple solutions for one timestamp")
    min_timestamp = np.inf
    max_timestamp = 0

    # Get set of ip links
    ip_nodes = set()
    for timestamp, config_id in config_hashes.iteritems():
        ftimestamp = timestamp
        if timestamp == 0:
            ftimestamp = 'average'
        min_timestamp = min(min_timestamp, timestamp)
        max_timestamp = max(max_timestamp, timestamp)

        json_config = load_file(config_id, ftimestamp, demand_type, base_sol_path, ftype="configuration")
        ip_nodes_fname = json_config["topology"]["ip_topo"]["fname"]

        with open(ip_nodes_fname, "r") as ip_nodes_file:
            read_csv = csv.reader(ip_nodes_file, delimiter=',')
            for row in read_csv:
                ip_nodes.add(row[0])
    ip_nodes = sorted(list(ip_nodes))

    matrices_over_time = list()
    links_cdn_e2e = list()
    link_load_matrices = list()
    demandsets = list()
    for timestamp, config_id in config_hashes.iteritems():
        ftimestamp = timestamp
        if timestamp == 0:
            ftimestamp = 'average'

        solution = load_file(config_id, ftimestamp, demand_type, base_sol_path, ftype="solution")
        if "ip_links" not in solution:
            print("No solution. Stop")
            break

        matrix, _, _ = build_adj_matrix_from_solution(solution, ip_nodes, link_type=None)
        matrices_over_time.append((timestamp, matrix))

        matrix_cdne2e, links_cdn, links_e2e = build_adj_matrix_from_solution(
            solution, ip_nodes, link_type=MATRIX_LINK_TYPE_CDN_E2E
        )

        matrix_cdn, _, _ = build_adj_matrix_from_solution(
            solution, ip_nodes, link_type=MATRIX_LINK_TYPE_CDN, links_cdn=links_cdn, links_e2e=links_e2e
        )

        matrix_e2e, _, _ = build_adj_matrix_from_solution(
            solution, ip_nodes, link_type=MATRIX_LINK_TYPE_E2E, links_cdn=links_cdn, links_e2e=links_e2e
        )
        links_cdn_e2e.append((matrix_cdne2e, matrix_cdn, matrix_e2e))

        json_config = load_file(config_id, ftimestamp, demand_type, base_sol_path, ftype="configuration")
        if json_config["demand"]["name"] in ["DemandSetGeneratorFromCSVConfiguration", "FixedCDNDemandGeneratorFromCSVConfiguration"]:
            demandset = dict()
            fname_demand = json_config["demand"]["fname_demand"]
            with open(fname_demand) as csvfile:
                read_csv = csv.reader(csvfile, delimiter=',')
                for row in read_csv:
                    cdn = row[1]
                    cdnrouter = row[2]
                    enduser = row[3]
                    rate = float(row[4])
                    demandset[f"{enduser}-{cdn}-{cdnrouter}"] = rate

            matrix_link_load = build_link_load_matrix_from_solution(
                solution, ip_nodes, demandset
            )
        else:
            print("Link not defined for E2E traffic")
            matrix_link_load = np.zeros(shape=(1,1))
            demandset = None
        link_load_matrices.append((timestamp, matrix_link_load))
        demandsets.append((timestamp, demandset))

    return matrices_over_time, ip_nodes, min_timestamp, max_timestamp, links_cdn_e2e, link_load_matrices, demandsets


def build_cdn_pop_matrix_from_solution(solution, unodes=None, peering_nodes=None):
    if "cdn_assignment" not in solution:
        print("No solution. Go to next")
        return
    if unodes is None:
        unodes = set()
    if peering_nodes is None:
        peering_nodes = set()
    assignment = dict()
    for cdn in solution['cdn_assignment']:
        for unode in cdn['user_nodes']:
            unode_id = f"{unode['node_id'].split('-')[0]}-{unode['node_id'].split('-')[1]}"
            unodes.add(unode_id)
            assignment[(unode_id, cdn["name"])] = list()
            for pnode in unode['peering_nodes']:
                pnode_id = pnode[0]
                peering_nodes.add(pnode_id)
                assignment[(unode_id, cdn["name"])].append(pnode)
            assignment[(unode_id, cdn["name"])] = sorted(assignment[(unode_id, cdn["name"])])
    return assignment, unodes, peering_nodes


def build_cdn_pop_assignment_matrix_list_over_time_from_config_hashes(config_hashes, demand_type, base_sol_path):
    unodes = None
    peering_nodes = None
    assignment_all = list()
    for timestamp, config_id in config_hashes.iteritems():
        ftimestamp = timestamp
        if timestamp == 0:
            ftimestamp = 'average'

        solution = load_file(config_id, ftimestamp, demand_type, base_sol_path, ftype="solution")
        assignment, unodes, peering_nodes = build_cdn_pop_matrix_from_solution(solution, unodes, peering_nodes)
        assignment_all.append((timestamp, assignment))
    return assignment_all


def build_routing_matrix_from_solution(solution):
    routing = dict()
    routing["cdn"] = dict()
    if "cdn_assignment" in solution:
        for cdn in solution['cdn_assignment']:
            for unode in cdn['user_nodes']:
                unode_id = f"{unode['node_id'].split('-')[0]}-{unode['node_id'].split('-')[1]}"
                routing["cdn"][(cdn["name"], unode_id)] = (unode["peering_nodes"][0], sorted(unode["routes"]))
    routing["e2e"] = dict()
    if "e2e_routing":
        for e2e in solution['e2e_routing']:
            routing["e2e"][(e2e['node1'], e2e['node2'])] = sorted(e2e['paths'])
    return routing


def build_routing_matrix_list_over_time_from_config_hashes(config_hashes, demand_type, base_sol_path):
    matrices_over_time = list()
    for timestamp, config_id in config_hashes.iteritems():
        ftimestamp = timestamp
        if timestamp == 0:
            ftimestamp = 'average'
        solution = load_file(config_id, ftimestamp, demand_type, base_sol_path, ftype="solution")

        routing = build_routing_matrix_from_solution(solution)
        matrices_over_time.append((timestamp, routing))

    return matrices_over_time


RC_TYPE_UNCHANGED_CON = -1
RC_TYPE_ADD = 1
RC_TYPE_REM = 2
RC_TYPE_INC = 3
RC_TYPE_DEC = 4


def calculate_reconfiguration_metrics(matrix_list):
    reconf_metrics = {
        'input_timestamp': list(),
        'num_links_added': list(),  # No. new adjacencies in matrix
        'num_links_removed': list(),  # No. removed adjacencies in matrix
        'num_trunks_increased': list(),  # No. adjacencies with increased capacities
        'num_trunks_decreased': list(),  # No. adjacencies with decreased capacities
        'num_ip_nodes': list(),  # No. of IP nodes in topology
        'rc_type': list(),  # Reconfiguration type per link
        'old_cap': list(),  # Old capacity per link
        'abs_cap_diff': list(),  # Absolute difference in capacity per link
        'cos_similarity_connectivity': list(),
        'cos_similarity_capacity': list(),
        'num_unchanged_links': list(),
        'num_ip_links_from_adj': list(),
        'ip_links_detailed': list(),
        'ip_trunks_detailed': list(),
        'unchanged_links_detailed': list()
    }
    for (new_ts, new_matrix), (old_ts, old_matrix) in zip(matrix_list[1:], matrix_list[:-1]):
        reconf_metrics["input_timestamp"].append(new_ts)
        new_links = new_matrix > 0
        old_links = old_matrix > 0
        diff_links = new_links.astype(int) - old_links.astype(int)
        reconf_metrics["num_links_added"].append(np.sum(diff_links > 0))
        reconf_metrics["num_links_removed"].append(np.sum(diff_links < 0))

        reconf_metrics["num_trunks_increased"].append(np.sum((old_matrix > 0) & (new_matrix > old_matrix)))
        reconf_metrics["num_trunks_decreased"].append(np.sum((new_matrix > 0) & (new_matrix < old_matrix)))
        reconf_metrics["num_ip_nodes"].append(len(new_matrix))
        reconf_metrics["num_unchanged_links"].append(np.sum((new_links == old_links) & (old_links > 0)))
        reconf_metrics["num_ip_links_from_adj"].append(np.sum(new_matrix>0))
        reconf_metrics["ip_links_detailed"].append(new_links.flatten().tolist())
        reconf_metrics["ip_trunks_detailed"].append(new_matrix.flatten().tolist())

        # Add detailed view on reconfigurations
        reconf_metrics["unchanged_links_detailed"].append(
             (((new_links == old_links) & (old_links > 0)) * RC_TYPE_UNCHANGED_CON).flatten().tolist()
        )
        reconf_metrics["rc_type"].append(
            (
             ((new_matrix > 0) & (new_matrix < old_matrix)) * RC_TYPE_DEC +
             ((old_matrix > 0) & (new_matrix > old_matrix)) * RC_TYPE_INC +
             (diff_links > 0) * RC_TYPE_ADD +
             (diff_links < 0) * RC_TYPE_REM).flatten().tolist()
        )
        reconf_metrics["old_cap"].append(
            old_matrix.flatten().tolist()
        )
        reconf_metrics["abs_cap_diff"].append(
            np.abs(new_matrix - old_matrix).flatten().tolist()
        )
        reconf_metrics["cos_similarity_connectivity"].append(
            sspd.cosine(new_links.flatten(), old_links.flatten())
        )
        reconf_metrics["cos_similarity_capacity"].append(
            sspd.cosine(new_matrix.flatten(), old_matrix.flatten())
        )

    return reconf_metrics


def calculate_reconfiguration_metrics_cdn_e2e(matrix_list, links_cdn_e2e_list):
    reconf_metrics = {
        'input_timestamp': list(),
        'num_links_cdn_e2e_added': list(),  # No. new adjacencies in matrix
        'num_links_cdn_e2e_removed': list(),  # No. removed adjacencies in matrix
        'num_trunks_cdn_e2e_increased': list(),  # No. adjacencies with increased capacities
        'num_trunks_cdn_e2e_decreased': list(),  # No. adjacencies with decreased capacities
        'num_links_e2e_added': list(),  # No. new adjacencies in matrix
        'num_links_e2e_removed': list(),  # No. removed adjacencies in matrix
        'num_trunks_e2e_increased': list(),  # No. adjacencies with increased capacities
        'num_trunks_e2e_decreased': list(),  # No. adjacencies with decreased capacities
        'num_links_cdn_added': list(),  # No. new adjacencies in matrix
        'num_links_cdn_removed': list(),  # No. removed adjacencies in matrix
        'num_trunks_cdn_increased': list(),  # No. adjacencies with increased capacities
        'num_trunks_cdn_decreased': list(),  # No. adjacencies with decreased capacities
    }
    for (new_ts, new_matrix), (old_ts, old_matrix), links_cdn_e2e, old_links_cdn_e2e in zip(matrix_list[1:],
                                                                                            matrix_list[:-1],
                                                                                            links_cdn_e2e_list[1:],
                                                                                            links_cdn_e2e_list[:-1]):
        reconf_metrics["input_timestamp"].append(new_ts)
        new_links = (new_matrix > 0) & (links_cdn_e2e[0] > 0)
        old_links = (old_matrix > 0) & (old_links_cdn_e2e[0] > 0)
        diff_links = new_links.astype(int) - old_links.astype(int)
        reconf_metrics["num_links_cdn_e2e_added"].append(np.sum(diff_links > 0))
        reconf_metrics["num_links_cdn_e2e_removed"].append(np.sum(diff_links < 0))
        reconf_metrics["num_trunks_cdn_e2e_increased"].append(
            np.sum((old_matrix > 0) & (new_matrix > old_matrix) & (links_cdn_e2e[0] > 0)))
        reconf_metrics["num_trunks_cdn_e2e_decreased"].append(
            np.sum((new_matrix > 0) & (new_matrix < old_matrix) & (links_cdn_e2e[0] > 0)))

        new_links = (new_matrix > 0) & (links_cdn_e2e[1] > 0)
        old_links = (old_matrix > 0) & (old_links_cdn_e2e[1] > 0)
        diff_links = new_links.astype(int) - old_links.astype(int)
        reconf_metrics["num_links_cdn_added"].append(np.sum(diff_links > 0))
        reconf_metrics["num_links_cdn_removed"].append(np.sum(diff_links < 0))
        reconf_metrics["num_trunks_cdn_increased"].append(
            np.sum((old_matrix > 0) & (new_matrix > old_matrix) & (links_cdn_e2e[1] > 0)))
        reconf_metrics["num_trunks_cdn_decreased"].append(
            np.sum((new_matrix > 0) & (new_matrix < old_matrix) & (links_cdn_e2e[1] > 0)))

        new_links = (new_matrix > 0) & (links_cdn_e2e[2] > 0)
        old_links = (old_matrix > 0) & (old_links_cdn_e2e[2] > 0)
        diff_links = new_links.astype(int) - old_links.astype(int)
        reconf_metrics["num_links_e2e_added"].append(np.sum(diff_links > 0))
        reconf_metrics["num_links_e2e_removed"].append(np.sum(diff_links < 0))
        reconf_metrics["num_trunks_e2e_increased"].append(
            np.sum((old_matrix > 0) & (new_matrix > old_matrix) & (links_cdn_e2e[2] > 0)))
        reconf_metrics["num_trunks_e2e_decreased"].append(
            np.sum((new_matrix > 0) & (new_matrix < old_matrix) & (links_cdn_e2e[2] > 0)))

    return reconf_metrics


def calculate_reconfiguration_metrics_cdn(assign_matrix_list):
    reconf_metrics = {
        'input_timestamp': list(),
        'changed_cdn_assignments': list(),
        'total_cdn_assignments': list(),
        'changed_cdn_assignments_per_cdn': list()
    }
    for (new_ts, new_matrix), (old_ts, old_matrix) in zip(assign_matrix_list[1:], assign_matrix_list[:-1]):
        reconf_metrics["input_timestamp"].append(new_ts)
        diff_assign = 0
        total = 0
        changes_per_hg = dict()
        for k, value in new_matrix.items():
            total += 1
            if k not in old_matrix:
                continue
            if value != old_matrix[k]:
                diff_assign += 1
                if k[1] not in changes_per_hg:
                    changes_per_hg[k[1]] = 0
                changes_per_hg[k[1]] += 1

        reconf_metrics["changed_cdn_assignments"].append(np.sum(diff_assign))
        reconf_metrics["total_cdn_assignments"].append(total)
        reconf_metrics["changed_cdn_assignments_per_cdn"].append(changes_per_hg)
    return reconf_metrics


def calculate_reconfiguration_metrics_routing(routing_matrix_list, demandsets=None):
    reconf_metrics = {
        'input_timestamp': list(),
        'routing_changes_cdn': list(),
        'routing_changes_e2e': list(),
        'total_cdn_routes': list(),
        'total_e2e_routes': list(),
        'routing_changes_cdn_samepop': list(),
        'routing_changes_cdn_per_cdn': list(),
        'routing_changes_cdn_samepop_per_cdn': list(),
        'total_cdn_routes_per_cdn': list(),
        "flowsizes_changes_cdn": list(),
        "flowsizes_changes_cdn_samepop": list(),
        "flowsizes_nochanges_cdn": list()
    }
    for idx, ((new_ts, new_routing), (old_ts, old_routing)) in enumerate(
            zip(routing_matrix_list[1:], routing_matrix_list[:-1])):
        reconf_metrics["input_timestamp"].append(new_ts)
        routing_changes_cdn = 0
        routing_change_nopop_change = 0

        routing_changes_per_hg = dict()
        routing_changes_nopop_change_per_hg = dict()
        routes_per_hg = dict()
        total_volume_per_hg = dict()

        sizes_changed = list()
        sizes_nopop_changed = list()
        sizes_unchanged = list()

        if demandsets is not None:
            demandset = demandsets[idx][1]
        else:
            demandset = None
        for sd, routing in new_routing["cdn"].items():
            try:
                demand_idx = f"{sd[1]}-{sd[0]}-{routing[0][0].split('-')[0]}-{routing[0][0].split('-')[1]}"
                if sd[0] not in routes_per_hg:
                    routes_per_hg[sd[0]] = 1
                    total_volume_per_hg[sd[0]] = routing
                else:
                    routes_per_hg[sd[0]] += 1
                if sd not in old_routing["cdn"]:
                    continue

                if routing[1] != old_routing["cdn"][sd][1]:
                    routing_changes_cdn += 1
                    if sd[0] not in routing_changes_per_hg:
                        routing_changes_per_hg[sd[0]] = 1
                    else:
                        routing_changes_per_hg[sd[0]] += 1

                if routing[0] != old_routing["cdn"][sd][0] and demandset is not None:
                    # PoPs different
                    sizes_changed.append(routing[1][0][2] * demandset[demand_idx])

                if routing[0] == old_routing["cdn"][sd][0] and routing[1] != old_routing["cdn"][sd][1]:
                    routing_change_nopop_change += 1
                    if sd[0] not in routing_changes_nopop_change_per_hg:
                        routing_changes_nopop_change_per_hg[sd[0]] = 1
                    else:
                        routing_changes_nopop_change_per_hg[sd[0]] += 1
                    if demandset is not None:
                        sizes_nopop_changed.append(routing[1][0][2] * demandset[demand_idx])
                else:
                    if demandset is not None:
                        sizes_unchanged.append(routing[1][0][2] * demandset[demand_idx])
            except KeyError as e:
                print(demand_idx)
                continue

        reconf_metrics["routing_changes_cdn"].append(routing_changes_cdn)
        reconf_metrics["routing_changes_cdn_samepop"].append(routing_change_nopop_change)
        reconf_metrics["routing_changes_cdn_per_cdn"].append(routing_changes_per_hg)
        reconf_metrics["routing_changes_cdn_samepop_per_cdn"].append(routing_changes_nopop_change_per_hg)

        reconf_metrics["total_cdn_routes"].append(len(new_routing["cdn"]))
        reconf_metrics["total_cdn_routes_per_cdn"].append(routes_per_hg)

        reconf_metrics["flowsizes_changes_cdn"].append(sizes_changed)
        reconf_metrics["flowsizes_changes_cdn_samepop"].append(sizes_nopop_changed)
        reconf_metrics["flowsizes_nochanges_cdn"].append(sizes_unchanged)

        routing_changes_e2e = 0
        for sd, routing in new_routing["e2e"].items():
            if sd not in old_routing["e2e"]:
                continue
            routing_changes_e2e += routing != old_routing["e2e"][sd]
        reconf_metrics["routing_changes_e2e"].append(routing_changes_e2e)
        reconf_metrics["total_e2e_routes"].append(len(new_routing["e2e"]))

    return reconf_metrics


def calculate_reconf_link_load_matrix(link_load_matrix_list):
    reconf_metrics = {
        'ip_link_load_detailed': list(),
        'input_timestamp': list()
    }
    for ts, matrix in link_load_matrix_list:
        reconf_metrics['input_timestamp'].append(ts)
        reconf_metrics['ip_link_load_detailed'].append(
            matrix.flatten().tolist()
        )
    return reconf_metrics


DF_INDICES = ["algo_name", "demand_type", "num_transceiver", "ip_link_util",
              "fiber_capacity", "used_fixed_layers", "comment", "algo_timelimit", "input_timestamp"]


def get_reconfiguration_metrics(algo_names, demand_types, ip_link_utils, num_transceivers, fiber_capacities,
                                fixed_layers, comments, timelimits,
                                base_sol_path, agg_metrics_path):
    reconfigurations = list()
    reconfigurations_cdn = list()
    reconfigurations_routing = list()
    reconfigurations_cdn_e2e = list()
    reconfigurations_link_load = list()
    df_agg = pd.read_hdf(
        agg_metrics_path,
        key='agg_metrics')
    df_agg["used_fixed_layers"] = df_agg["fixed_layers"].apply(
        lambda x: (x["fix_ip_links"], x["fix_ip_connectivity"], x["fix_cdn_assignment"]) if len(x) > 0 else (
        False, False, False)
    )
    df_agg = df_agg.reset_index().set_index(DF_INDICES).sort_index(inplace=False)

    for algo_name, demand_type, num_txs, ip_link_util, fiber_cap, fixed_layer, comment, timelimit in itertools.product(
            algo_names, demand_types, num_transceivers, ip_link_utils, fiber_capacities, fixed_layers, comments,
            timelimits
    ):
        try:
            config_hashes = get_config_hashes(
                df_agg,
                algo_name,
                demand_type,
                num_txs,
                ip_link_util,
                fiber_cap,
                fixed_layer,
                comment,
                timelimit
            )

            adj_matrix_list, ip_nodes, mints, maxts, links_cdn_e2e, link_loads, demandsets = \
                build_adj_matrix_list_over_time_from_config_hashes(
                    config_hashes, demand_type, base_sol_path
                )

            print("calculate adj changes")
            rec_raw = calculate_reconfiguration_metrics(adj_matrix_list)
            num_samples = len(rec_raw["input_timestamp"])
            rec_raw["algo_name"] = [algo_name] * num_samples
            rec_raw["demand_type"] = [demand_type] * num_samples
            rec_raw["ip_link_util"] = [ip_link_util] * num_samples
            rec_raw["num_transceiver"] = [num_txs] * num_samples
            rec_raw["fiber_capacity"] = [fiber_cap] * num_samples
            rec_raw["used_fixed_layers"] = [fixed_layer] * num_samples
            rec_raw["comment"] = [comment] * num_samples
            rec_raw["algo_timelimit"] = [timelimit] * num_samples
            reconfigurations.append(
                pd.DataFrame(rec_raw).set_index(
                    DF_INDICES
                )
            )

            rec_raw = calculate_reconfiguration_metrics_cdn_e2e(adj_matrix_list, links_cdn_e2e)
            num_samples = len(rec_raw["input_timestamp"])
            rec_raw["algo_name"] = [algo_name] * num_samples
            rec_raw["demand_type"] = [demand_type] * num_samples
            rec_raw["ip_link_util"] = [ip_link_util] * num_samples
            rec_raw["num_transceiver"] = [num_txs] * num_samples
            rec_raw["fiber_capacity"] = [fiber_cap] * num_samples
            rec_raw["used_fixed_layers"] = [fixed_layer] * num_samples
            rec_raw["comment"] = [comment] * num_samples
            rec_raw["algo_timelimit"] = [timelimit] * num_samples
            reconfigurations_cdn_e2e.append(
                pd.DataFrame(rec_raw).set_index(
                    DF_INDICES
                )
            )

            rec_raw = calculate_reconf_link_load_matrix(
                link_loads
            )
            num_samples = len(rec_raw["input_timestamp"])
            rec_raw["algo_name"] = [algo_name] * num_samples
            rec_raw["demand_type"] = [demand_type] * num_samples
            rec_raw["ip_link_util"] = [ip_link_util] * num_samples
            rec_raw["num_transceiver"] = [num_txs] * num_samples
            rec_raw["fiber_capacity"] = [fiber_cap] * num_samples
            rec_raw["used_fixed_layers"] = [fixed_layer] * num_samples
            rec_raw["comment"] = [comment] * num_samples
            rec_raw["algo_timelimit"] = [timelimit] * num_samples
            reconfigurations_link_load.append(
                pd.DataFrame(rec_raw).set_index(
                    DF_INDICES
                )
            )

            assign_matrix_list = build_cdn_pop_assignment_matrix_list_over_time_from_config_hashes(
                config_hashes, demand_type, base_sol_path
            )
            print("calculate cdn changes")
            rec_raw = calculate_reconfiguration_metrics_cdn(assign_matrix_list)
            num_samples = len(rec_raw["input_timestamp"])
            rec_raw["algo_name"] = [algo_name] * num_samples
            rec_raw["demand_type"] = [demand_type] * num_samples
            rec_raw["num_transceiver"] = [num_txs] * num_samples
            rec_raw["ip_link_util"] = [ip_link_util] * num_samples
            rec_raw["fiber_capacity"] = [fiber_cap] * num_samples
            rec_raw["used_fixed_layers"] = [fixed_layer] * num_samples
            rec_raw["comment"] = [comment] * num_samples
            rec_raw["algo_timelimit"] = [timelimit] * num_samples
            reconfigurations_cdn.append(
                pd.DataFrame(rec_raw).set_index(
                    DF_INDICES
                )
            )

            routing_matrix_list = build_routing_matrix_list_over_time_from_config_hashes(
                config_hashes, demand_type, base_sol_path
            )
            print("calculate routing changes")
            rec_raw = calculate_reconfiguration_metrics_routing(routing_matrix_list, demandsets)
            num_samples = len(rec_raw["input_timestamp"])
            rec_raw["algo_name"] = [algo_name] * num_samples
            rec_raw["demand_type"] = [demand_type] * num_samples
            rec_raw["num_transceiver"] = [num_txs] * num_samples
            rec_raw["ip_link_util"] = [ip_link_util] * num_samples
            rec_raw["fiber_capacity"] = [fiber_cap] * num_samples
            rec_raw["used_fixed_layers"] = [fixed_layer] * num_samples
            rec_raw["comment"] = [comment] * num_samples
            rec_raw["algo_timelimit"] = [timelimit] * num_samples
            print("Appending routing rc to df ")
            reconfigurations_routing.append(
                pd.DataFrame(rec_raw).set_index(
                    DF_INDICES
                )
            )
        except Exception as e:
            print("Error: ", e)
            print(demand_type, algo_name, num_txs, fiber_cap, fixed_layer)
    if len(reconfigurations) == 0:
        print("no metrics calculated.")
        return
    df_agg_rc = pd.concat(reconfigurations)
    df_agg = df_agg.merge(df_agg_rc, left_index=True, right_index=True)
    df_agg_rc = pd.concat(reconfigurations_cdn_e2e)
    df_agg = df_agg.merge(df_agg_rc, left_index=True, right_index=True)
    df_agg_rc = pd.concat(reconfigurations_link_load)
    df_agg = df_agg.merge(df_agg_rc, left_index=True, right_index=True)
    df_agg_rc = pd.concat(reconfigurations_cdn)
    df_agg = df_agg.merge(df_agg_rc, left_index=True, right_index=True)
    df_agg_rc = pd.concat(reconfigurations_routing)
    df_agg = df_agg.merge(df_agg_rc, left_index=True, right_index=True)
    print("Saving back to file")
    df_agg.to_hdf(
        agg_metrics_path.replace(".h5", "_w_reconf.h5"),
        key='agg_metrics_with_rc',
        mode='a'
    )


if __name__ == '__main__':
    import itertools
    import argparse

    parser = argparse.ArgumentParser(
        description='Calculate reconfigurations using configs from agg metrics and save to hdf file'
    )
    parser.add_argument('--path', help="base path", type=str)
    parser.add_argument('--suffix', help="suffix of path", type=str)
    parser.add_argument('--sub', help="Sub-folder, e.g., for seeds", default="", type=str)
    args = parser.parse_args()

    BASE_PATH_SOL = args.path + "/output_" + args.suffix + "/"
    if len(args.sub) > 0:
        AGG_METRICS = args.path + "/agg_metrics_" + args.suffix + f"_{args.sub}" + ".h5"
        BASE_PATH_SOL = os.path.join(BASE_PATH_SOL, args.sub)
    else:

        AGG_METRICS = args.path + "/agg_metrics_" + args.suffix + ".h5"

    get_reconfiguration_metrics(
        demand_types=[
            "DemandSetGeneratorFromCSVConfiguration",
            "ShuffledDemandSetGeneratorFromCSVConfiguration",
            "DemandMatrixFromCSVGeneratorConfiguration",
            "FixedCDNDemandGeneratorFromCSVConfiguration",
            "ShuffledFixedCDNDemandGeneratorFromCSVConfiguration",
            ('DemandSetGeneratorFromCSVConfiguration', 'DemandMatrixFromCSVGeneratorConfiguration')
        ],
        algo_names=[
            "PathBasedMixedIntegerProgramConfiguration",
            "PathBasedMixedIntegerProgramExtObjectiveConfiguration",
            "GreedyCDNAssignmentAlgorithmConfiguration"
        ],
        num_transceivers=[100],
        ip_link_utils=[0.5],
        fiber_capacities=[100],
        fixed_layers=[
            (False, False, False),
            (0.15, False, False),
        ],
        comments=["", "fix_from_beginning", "-"],
        timelimits=[
            3600
        ],
        base_sol_path=BASE_PATH_SOL,
        agg_metrics_path=AGG_METRICS
    )
