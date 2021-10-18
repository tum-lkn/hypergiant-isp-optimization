import os
import json
import csv
import collections

import pandas as pd
import numpy as np

import model.metrics
from generator.topology_generator import SimpleOpticalTopologyGenerator


def get_metrics_single_solution(solution_file, base_path):
    with open(os.path.join(base_path, solution_file), "r") as fd:
        sol_dict = json.load(fd)
    metrics = sol_dict['metrics']
    if len(sol_dict["ip_links"]) > 0:
        for k, metric_func in model.metrics.AVAILABLE_METRICS.items():
            if k not in metrics or "path_length" in k:
                metrics.update(metric_func(sol_dict))

    # Additional metric: Total traffic volume
    fname_cfg = os.path.join(base_path, solution_file.replace("solution", "configuration"))

    with open(fname_cfg, "r") as fd:
        json_config = json.load(fd)
    total_traffic = 0

    fname_cdn_demand = None
    if json_config["demand"]["name"] in [
        "DemandSetGeneratorFromCSVConfiguration",
        "FixedCDNDemandGeneratorFromCSVConfiguration"
    ]:
        fname_cdn_demand = json_config["demand"]["fname_demand"]
    elif json_config["demand"]["name"] == "CombinedDemandGeneratorConfiguration":
        fname_cdn_demand = json_config["demand"]["cdn_demand"]["fname_demand"]

    if fname_cdn_demand:
        demandset = dict()
        with open(fname_cdn_demand) as csvfile:
            read_csv = csv.reader(csvfile, delimiter=',')
            for row in read_csv:
                cdn = row[1]
                enduser = row[3]
                cdnrouter = row[2]
                rate = float(row[4])
                demandset[f"{enduser}-{cdn}-{cdnrouter}"] = rate
        for cdn in sol_dict["cdn_assignment"]:
            for unode in cdn["user_nodes"]:
                for routes in unode["routes"]:
                    total_traffic += demandset[unode["node_id"]] * routes[2]

    if json_config["demand"]["name"] in [
        "DemandMatrixFromCSVGeneratorConfiguration",
        "CombinedDemandGeneratorConfiguration"
    ]:
        for demand in sol_dict["e2e_routing"]:
            for route in demand["paths"]:
                total_traffic += route[1]
    metrics.update({'total_traffic': total_traffic})

    # Additional metric: Num links with CDN and BG traffic
    links_cdn = set()
    for cdn in sol_dict["cdn_assignment"]:
        for unode in cdn["user_nodes"]:
            for routes in unode["routes"]:
                links_cdn.add((routes[0], routes[1]))
                links_cdn.add((routes[1], routes[0]))

    links_e2e = set()
    for demand in sol_dict["e2e_routing"]:
        for route in demand["paths"]:
            links_e2e.add((route[0][0], route[0][1]))
            links_e2e.add((route[0][1], route[0][0]))
    links_cdn_e2e = links_e2e.intersection(links_cdn)
    num_links_cdn_e2e = len(links_cdn_e2e)

    metrics.update({
        'num_links_cdn_e2e': num_links_cdn_e2e,
        'num_links_cdn': len(links_cdn.difference(links_e2e)),
        'num_links_e2e': len(links_e2e.difference(links_cdn))
    })

    # Node degree distribution
    degree = collections.defaultdict(int)
    unode_degree = collections.defaultdict(int)
    for iplink in sol_dict["ip_links"]:
        degree[iplink['node1']] += 1
        degree[iplink['node2']] += 1
        if "-S" in iplink['node1']:
            unode_degree[iplink['node1']] += 1
        if "-S" in iplink['node2']:
            unode_degree[iplink['node2']] += 1
    metrics.update(
        {
            'node_degrees': list(degree.values()),
            'unode_degrees': list(unode_degree.values())
        }
    )

    # Path lengths in KM
    oedges_lengths = SimpleOpticalTopologyGenerator.OPT_EDGE_WEIGHTS
    iplinks_lengths = dict()
    for iplink in sol_dict["ip_links"]:
        len_this = 0
        for olink in iplink["opt_links"]:
            if olink[0] == olink[1]:
                continue
            try:
                len_this += oedges_lengths[(olink[0], olink[1])]
            except KeyError:
                len_this += oedges_lengths[(olink[1], olink[0])]
        iplinks_lengths[(iplink["node1"], iplink["node2"])] = len_this

    cdn_path_lengths_km = list()
    path_lengths_km_per_hg = list()
    num_hops_per_hg = list()
    for cdn in sol_dict["cdn_assignment"]:
        this_hg_pl_km = list()
        this_hg_hops = list()
        for unode in cdn["user_nodes"]:
            route_length = 0
            this_un_hg_hops = 0
            this_un_hg_km = 0
            for routes in unode["routes"]:
                if routes[0] == routes[1]:
                    continue
                route_length += iplinks_lengths[(routes[0], routes[1])]
                this_un_hg_km += iplinks_lengths[(routes[0], routes[1])]
                this_un_hg_hops += 1
            cdn_path_lengths_km.append(route_length)
            this_hg_pl_km.append(this_un_hg_km)
            this_hg_hops.append(this_un_hg_hops)
        path_lengths_km_per_hg.append(float(np.mean(this_hg_pl_km)))
        num_hops_per_hg.append(float(np.mean(this_hg_hops)))

    e2e_pl_km = list()
    for demand in sol_dict["e2e_routing"]:
        route_len = 0
        for route in demand["paths"]:
            if route[0][0] == route[0][1]:
                continue
            try:
                route_len += iplinks_lengths[(route[0][0], route[0][1])]
            except KeyError:
                print(routes)
                print(iplinks_lengths[(routes[0][0], routes[0][1])])
        e2e_pl_km.append(route_len)

    metrics.update({
        'mean_path_length_km_cdn': float(np.mean(cdn_path_lengths_km)),
        'mean_path_length_km_e2e': float(np.mean(e2e_pl_km)),
        'std_path_length_km_cdn': float(np.std(cdn_path_lengths_km)),
        'std_path_length_km_e2e': float(np.std(e2e_pl_km)),
        'mean_path_length_km_cdn_e2e': float(np.mean(cdn_path_lengths_km + e2e_pl_km)),
        'std_path_length_km_cdn_e2e': float(np.std(cdn_path_lengths_km + e2e_pl_km)),
        'mean_path_length_km_per_cdn': path_lengths_km_per_hg,
        'mean_num_hops_per_cdn': num_hops_per_hg
    })

    # Num. PoPs used and Load distribution
    peering_pops = set()
    peering_loads = collections.defaultdict(float)
    cdnrouter_per_hg = collections.defaultdict(set)
    frac_used_peerings_per_unode_hg = list()
    used_peerings_per_unode = collections.defaultdict(set)
    if json_config["demand"]["name"] in ["DemandSetGeneratorFromCSVConfiguration",
                                         "FixedCDNDemandGeneratorFromCSVConfiguration",
                                         "CombinedDemandGeneratorConfiguration"]:
        demandset = dict()
        try:
            fname_demand = json_config["demand"]["fname_demand"]
        except KeyError:
            fname_demand = json_config["demand"]["cdn_demand"]["fname_demand"]

        with open(fname_demand) as csvfile:
            read_csv = csv.reader(csvfile, delimiter=',')
            for row in read_csv:
                cdn = row[1]
                enduser = row[3]
                cdnrouter = row[2]
                rate = float(row[4])
                demandset[f"{enduser}-{cdn}-{cdnrouter}"] = rate
                cdnrouter_per_hg[cdn].add(cdnrouter)
        for cdn in sol_dict["cdn_assignment"]:
            peerings_per_unode = collections.defaultdict(set)
            for unode in cdn["user_nodes"]:
                act_unode = unode["node_id"].split("-")[:2]
                for pnode in unode["peering_nodes"]:
                    pnode_id = f"{pnode[0].split('-')[0]}-{pnode[0].split('-')[1]}"
                    peering_pops.add(pnode_id)
                    peerings_per_unode[str(act_unode)].add(pnode_id)
                    peering_loads[pnode_id] += demandset[unode["node_id"]]
                    used_peerings_per_unode[str(act_unode)].add(pnode_id)
            frac_used_peerings_per_unode_hg += [len(v)/len(cdnrouter_per_hg[cdn['name']]) for _, v in peerings_per_unode.items()]
    if len(frac_used_peerings_per_unode_hg) == 0:
        frac_used_peerings_per_unode_hg = [-1]

    peering_loads = np.array(list(peering_loads.values()))
    if len(peering_loads) == 0:
        peering_loads = np.array([-1])
    total_load = np.sum(peering_loads)
    peering_loads = peering_loads / total_load
    metrics.update({
        'num_used_peering_pops': len(peering_pops),
        'mean_peering_load': float(np.mean(peering_loads)),
        'std_peering_load': float(np.std(peering_loads)),
        'min_peering_load': float(np.min(peering_loads)),
        'max_peering_load': float(np.max(peering_loads)),
        'peering_load': peering_loads.tolist(),
        'entropy_peering_load': float(-np.sum(peering_loads * np.log(peering_loads))),
        'used_peerings_per_unode_hg': frac_used_peerings_per_unode_hg,
        'mean_used_peerings_per_unode_hg': float(np.mean(frac_used_peerings_per_unode_hg)),
        'std_used_peerings_per_unode_hg': float(np.std(frac_used_peerings_per_unode_hg)),
        'min_used_peerings_per_unode_hg': float(np.min(frac_used_peerings_per_unode_hg)),
        'max_used_peerings_per_unode_hg': float(np.max(frac_used_peerings_per_unode_hg)),
        'used_peerings_per_unode': [len(v) for v in used_peerings_per_unode.values()]
    })

    # IP Links per fiber
    links_per_fiber = dict()
    for iplink in sol_dict["ip_links"]:
        for olink in iplink["opt_links"]:
            if olink[0] == olink[1]:
                continue
            if (olink[0], olink[1]) in links_per_fiber:
                links_per_fiber[(olink[0], olink[1])] += 1
            elif (olink[1], olink[0]) in links_per_fiber:
                links_per_fiber[(olink[1], olink[0])] += 1
            else:
                links_per_fiber[(olink[0], olink[1])] = 1
    metrics.update({
        'links_per_fiber': list(links_per_fiber.values())
    })

    # IP link utilization detailed
    link_loads = dict()
    for iplink in sol_dict["ip_links"]:
        link_loads[(iplink['node1'], iplink['node2'])] = [0, 100.0 * iplink["num_trunks"]]

    for demand in sol_dict["e2e_routing"]:
        for used_links in demand["paths"]:
                link_loads[tuple(used_links[0])][0] += used_links[1]

    if json_config["demand"]["name"] in ["DemandSetGeneratorFromCSVConfiguration",
                                         "FixedCDNDemandGeneratorFromCSVConfiguration",
                                         "CombinedDemandGeneratorConfiguration"]:
        for cdn in sol_dict["cdn_assignment"]:
            for unode in cdn["user_nodes"]:
                for routes in unode["routes"]:
                    # routes[2] is only fraction of allocated demand not absolute value...
                    link_loads[(routes[0], routes[1])][0] += routes[2] * demandset[unode["node_id"]]

    link_loads_rel = list()
    total_rate_in_net = 0
    total_cap_in_net = 0
    for iplink in link_loads.keys():
        link_loads_rel.append(1.0 * link_loads[iplink][0] / link_loads[iplink][1])
        total_rate_in_net += link_loads[iplink][0]
        total_cap_in_net +=  link_loads[iplink][1]
    metrics.update({
        'ip_link_loads': link_loads_rel,
        'total_ip_link_util': 1.0*total_rate_in_net / total_cap_in_net if total_cap_in_net > 0 else 0
    })

    with open(os.path.join(base_path, solution_file.replace("solution", "configuration")), "r") as fd:
        json_config = json.load(fd)

    # Get demand type
    demtype = json_config['demand']['name']

    if demtype == "CombinedDemandGeneratorConfiguration":
        demtype = (
            json_config['demand']['cdn_demand']['name'],
            json_config['demand']['e2e_demand']['name']
        )

    config = {
        'algo_name': json_config['algorithm']['name'],
        'input_timestamp': int(solution_file.split('_')[-2].replace("average", "0")),
        'demand_type': demtype,
        'fiber_capacity': json_config['topology']['opt_topo']['fiber_capacity'],
        'num_transceiver': json_config['topology']['ip_topo']['num_transceiver'],
        'ip_link_util': json_config['topology']['parameter']['IP_LINK_UTILIZATION'],
        'fixed_layers': json_config['fixed_layers'],
        'comment': json_config.get("comment", "-"),
        'algo_timelimit': json_config['algorithm']["time_limit"] if "time_limit" in json_config["algorithm"] else
        json_config['algorithm']['mip_config']['time_limit']
    }
    config.update(metrics)
    return config


def get_agg_metrics(fname, base_path):
    df = pd.DataFrame(
        columns=["config_id", 'algo_name', 'input_timestamp', 'solver_time', 'deployed_ip_trunks', 'date',
                 'demand_type']
    ).set_index(["algo_name", 'input_timestamp'])
    if os.path.exists(fname):
        df = pd.read_hdf(fname, key='agg_metrics')

    solutions_list = list()
    for solution_file in filter(lambda x: "solution_" in x, sorted(os.listdir(base_path))):
        print(solution_file)
        config_id = solution_file.split('_')[-1].rstrip('.json')
        if config_id in df["config_id"].values.tolist():
            print("skipping")
            continue
        config = get_metrics_single_solution(solution_file, base_path)
        config['config_id'] = config_id

        solutions_list.append(config)

    if len(solutions_list) > 0:
        new_df = pd.DataFrame(solutions_list)
        new_df["date"] = pd.to_datetime(new_df["input_timestamp"], unit='s')
        new_df = new_df.set_index(['algo_name', 'input_timestamp'])
        df = pd.concat(
            [df, new_df], sort=False
        )

        df.to_hdf(fname, key='agg_metrics')
    return df


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Aggregate raw results to hdf file')
    parser.add_argument('--path', help="base path", type=str)
    parser.add_argument('--suffix', help="suffix of path", type=str)
    parser.add_argument('--sub', help="Sub-folder, e.g., for seeds", default="", type=str)
    args = parser.parse_args()

    BASE_PATH = args.path + "/output_" + args.suffix + "/"
    if len(args.sub) > 0:
        AGG_METRICS_NAME = args.path + "/agg_metrics_" + args.suffix + f"_{args.sub}" + ".h5"
        BASE_PATH = os.path.join(BASE_PATH, args.sub)
    else:
        AGG_METRICS_NAME = args.path + "/agg_metrics_" + args.suffix + ".h5"
    
    df = get_agg_metrics(AGG_METRICS_NAME, BASE_PATH)
