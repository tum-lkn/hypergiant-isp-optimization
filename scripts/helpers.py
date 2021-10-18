import os
import json
import collections
import multiprocessing

import scenario
import generator.topology_generator
import generator.demand_generator
import constants
import output.file_writer
import model.fixed_layers


def get_input_file_tuples(folder):
    """
    Returns a dict of tuples with the paths to demand, peering and ip node csv files, key is timestamp.
    Filenames contain timestamp so that grouping is possible
    :param folder:
    :return:
    """
    if not os.path.exists(folder):
        raise RuntimeError("Folder does not exist: {}".format(folder))
    timestamps = dict()
    num_files_not_handled = 0
    for fname in filter(lambda x: ".bak" not in x, os.listdir(folder)):
        # Extract timestamp
        if 'single' in fname:
            ts = fname.split("_single_")[-1].replace(".csv", "")
            content = fname.split("_single_")[0]
            if ts not in timestamps:
                timestamps[ts] = {}
            timestamps[ts][content] = os.path.join(folder, fname)
        else:
            num_files_not_handled += 1
    print("{} files could not be handled".format(num_files_not_handled))
    return timestamps


def create_demand_and_topo_configs(input_file_tuples, opt_topo_config, topo_parameter, bg_demand_config,
                                   ip_node_default_num_transceiver,
                                   demand_parameter):
    """
    Iterates over file tuples and returns list of topology and demand tuples (configuration objects).
    Type of demand is distinguished with value of bg_demand_config:
        - None: only CDN
        - file: CDN + Background
        - fixed_cdn: only CDN with fixed ingress PoPs
        - fixed: CDN + Background with fixed ingress PoPs
        - bg_only: Background
    :param input_file_tuples:
    :param opt_topo_config:
    :param topo_parameter:
    :param bg_demand_config:
    :param ip_node_default_num_transceiver:
    :param demand_parameter:
    :return:
    """

    out_tuples = list()

    for in_tuple in input_file_tuples:
        print(in_tuple)
        try:
            topo_cfg = generator.topology_generator.ComposedTopologyGeneratorConfiguration(
                opt_topo_gen_config=opt_topo_config,
                ip_topo_gen_config=generator.topology_generator.IPNodesFromCSVGeneratorConfiguration(
                    fname=in_tuple["ip_nodes"],
                    num_transceiver=ip_node_default_num_transceiver
                ),
                parameter=topo_parameter
            )
            demand_cfg = generator.demand_generator.DemandSetGeneratorFromCSVConfiguration(
                fname_demand=in_tuple["demand"],
                fname_peering=in_tuple["peering"],
                parameter=demand_parameter
            )
            if isinstance(bg_demand_config, generator.demand_generator.AbstractDemandGeneratorConfiguration):
                demand_cfg = generator.demand_generator.CombinedDemandGeneratorConfiguration(
                    cdn_demand_config=demand_cfg,
                    bg_demand_config=bg_demand_config
                )
            elif bg_demand_config == "file":
                demand_matrix = generator.demand_generator.DemandMatrixFromCSVGeneratorConfiguration(
                    fname_demand=in_tuple["background"]
                )
                demand_cfg = generator.demand_generator.CombinedDemandGeneratorConfiguration(
                    cdn_demand_config=demand_cfg,
                    bg_demand_config=demand_matrix
                )
            elif bg_demand_config == "fixed_cdn":
                demand_cfg = generator.demand_generator.FixedCDNDemandGeneratorFromCSVConfiguration(
                    fname_demand=in_tuple["demand"],
                    fname_peering=in_tuple["peering"],
                    parameter=demand_parameter
                )
            elif bg_demand_config == "fixed":
                demand_cfg = generator.demand_generator.FixedCDNDemandGeneratorFromCSVConfiguration(
                    fname_demand=in_tuple["demand"],
                    fname_peering=in_tuple["peering"],
                    parameter=demand_parameter
                )
                demand_matrix = generator.demand_generator.DemandMatrixFromCSVGeneratorConfiguration(
                    fname_demand=in_tuple["background"]
                )
                demand_cfg = generator.demand_generator.CombinedDemandGeneratorConfiguration(
                    cdn_demand_config=demand_cfg,
                    bg_demand_config=demand_matrix
                )
            elif bg_demand_config == "bg_only":
                demand_cfg = generator.demand_generator.DemandMatrixFromCSVGeneratorConfiguration(
                    fname_demand=in_tuple["background"]
                )
        except KeyError as e:
            print("Error. Skipping.", e)
            continue

        out_tuples.append(
            (topo_cfg, demand_cfg)
        )
    return out_tuples


def find_previous_solution_file(topo, demand, algo, fixed_layer, comment, path, offset):
    if "average" in topo.ip_topo_gen_config.fname:
        raise RuntimeError("No previous solution for average input")
    previous_timestamp = int(topo.ip_topo_gen_config.fname.split("_")[-1].replace(".csv", "")) - offset
    print(f"Timestamp previous solution: {previous_timestamp}")

    for cfg_fname in filter(
            lambda x: "configuration_" in x and str(previous_timestamp) in x,
            os.listdir(path)
    ):
        abs_cfg_fname = os.path.join(path, cfg_fname)
        with open(abs_cfg_fname, "r") as fd:
            config_from_file = json.load(fd)
        if topo.to_dict()["parameter"] != config_from_file["topology"]["parameter"]:
            if topo.to_dict()["name"] != config_from_file["topology"]["name"] or \
                    topo.to_dict()["opt_topo"]["fiber_capacity"] != config_from_file["topology"]["opt_topo"][
                "fiber_capacity"] or \
                    topo.to_dict()["parameter"]["IP_LINK_UTILIZATION"] != config_from_file["topology"][
                "parameter"]["IP_LINK_UTILIZATION"] or \
                    "failed_links" in config_from_file["topology"]["parameter"]:
                continue
        if algo.to_dict() != config_from_file["algorithm"]:
            continue
        if config_from_file["demand"]["name"] != demand.__class__.__name__:
            continue
        if fixed_layer is not None and "fixed_layers" not in config_from_file:
            continue
        elif fixed_layer is None and "fixed_layers" in config_from_file:
            if config_from_file["fixed_layers"] != {}:
                continue
        elif fixed_layer is not None and "fixed_layers" in config_from_file and len(
                config_from_file["fixed_layers"]) > 0 and \
                (config_from_file["fixed_layers"]["fix_ip_links"] != fixed_layer[0] or
                 config_from_file["fixed_layers"]["fix_ip_connectivity"] != fixed_layer[1] or
                 config_from_file["fixed_layers"]["fix_cdn_assignment"] != fixed_layer[2]):
            continue
        if comment is not None and "comment" not in config_from_file:
            continue
        elif comment is None and "comment" in config_from_file:
            continue
        elif comment is not None and "comment" in config_from_file and config_from_file["comment"] != comment:
            continue
        return abs_cfg_fname.replace("configuration", "solution")
    raise RuntimeError(
        f"Could not find solution file for {topo.to_dict()}\n {demand.to_dict()} \n {algo.to_dict()} \n {fixed_layer}")


def get_links_from_solution_file(solution_fname):
    links = dict()

    with open(solution_fname, "r") as fd:
        sol_dict = json.load(fd)

    for link in sol_dict["ip_links"]:
        links[f"{link['node1']}<->{link['node2']}"] = link["num_trunks"]

    return links


def get_cdn_assignment_from_solution_file(solution_fname):
    assignment = dict()

    with open(solution_fname, "r") as fd:
        sol_dict = json.load(fd)

    for cdn in sol_dict["cdn_assignment"]:
        assignment[cdn["name"]] = dict()
        for unode in cdn["user_nodes"]:
            assignment[cdn["name"]][unode["node_id"]] = list()
            for pnode in unode["peering_nodes"]:
                assignment[cdn["name"]][unode["node_id"]].append((pnode[0], pnode[1]))
    return assignment


def get_fiber_topology(capacity=100):
    return generator.topology_generator.SimpleOpticalTopologyGeneratorConfiguration(fiber_capacity=capacity)


def run_failure_scenarios(config_tuple):
    """
    Runs a single failure scenario and returns the failed link, if restoration was possible and the config id of the
    initial solution
    :param config_tuple: tuple of scenario configuration object, failed link and config id
        (the latter two are just forwarded as return value
    :return:
    """
    config, failed_link, config_id = config_tuple[0], config_tuple[1], config_tuple[2]
    try:
        print(f"Running fail case with {failed_link}")
        sol = config.produce().run()
        if len(sol.ip_links) == 0:
            return failed_link, False, config_id
        else:
            return failed_link, True, config_id
    except Exception as e:
        print(e)
        return failed_link, False, config_id


def generate_link_failure_scenarios_for_single_configuration(
        base_out_folder,
        original_opt_topology_configuration,
        demand_configuration,
        algorithm_configuration,
        fix_layers_configuration,
        comment,
        restoration_reconf_limit,
        restoration_ip_link_utilization,
        restoration_cdn_reconf_limit=None,
        time_offset=0,
        num_fail_cases=50
):
    try:
        prev_solution_file = find_previous_solution_file(
            original_opt_topology_configuration,
            demand_configuration,
            algorithm_configuration,
            fix_layers_configuration,
            comment,
            base_out_folder,
            time_offset
        )
    except RuntimeError as e:
        print(e)
        print(f"Could not find solution file")
        return [], [], []

    all_links = get_links_from_solution_file(prev_solution_file)

    if num_fail_cases > 0:
        links_sorted_by_size = sorted(all_links.keys(), key=lambda x: all_links[x], reverse=True)
        links_to_fail = links_sorted_by_size[:num_fail_cases]
    else:
        links_to_fail = all_links.keys()
    print(links_to_fail)
    config_id = prev_solution_file.split("_")[-1].replace(".json", "")
    config_time = prev_solution_file.split("_")[-2]

    failed_links = list()
    scenario_cfgs = list()
    config_ids = list()

    # Create folder for output
    new_outpath = os.path.join(base_out_folder, f"link_failure_analysis_{num_fail_cases}_{config_time}_{config_id}")
    os.makedirs(new_outpath, exist_ok=True)

    for l in links_to_fail:
        reduced_links = dict(all_links)
        reduced_links[l] = 0
        l_split = l.split("<->")
        reduced_links[f"{l_split[1]}<->{l_split[0]}"] = 0
        print(len(reduced_links))

        topo = original_opt_topology_configuration
        topo.parameter[constants.KEY_IP_LINK_UTILIZATION] = restoration_ip_link_utilization
        failed_links.append(l)
        config_ids.append(config_id)  # save the uuid of the config

        cdn_assignment = None
        limit_cdn = 0.0
        if restoration_cdn_reconf_limit is not None:
            cdn_assignment = get_cdn_assignment_from_solution_file(prev_solution_file)
            limit_cdn = restoration_cdn_reconf_limit

        if fix_layers_configuration is not None:
            fixed_layer = model.fixed_layers.HardCodedFixedLayersLimitedReconfigurationConfiguration(
                ip_links=reduced_links,
                limit_links=fix_layers_configuration[0]
            )
        elif restoration_reconf_limit is not None:
            fixed_layer = model.fixed_layers.HardCodedFixedLayersLimitedReconfigurationConfiguration(
                ip_links=reduced_links,
                limit_links=restoration_reconf_limit,
                cdn_assignment=cdn_assignment,
                limit_cdn=limit_cdn
            )
        else:
            fixed_layer = None

        scenario_cfgs.append(
            scenario.ScenarioConfiguration(
                topology_configuration=topo,
                demand_configuration=demand_configuration,
                algorithm_configuration=algorithm_configuration,
                fixed_layers=fixed_layer,
                outputs=[output.file_writer.JsonWriterConfiguration(new_outpath)],
                comment=comment
            )
        )
    return scenario_cfgs, config_ids, failed_links


def run_and_dump_failure_scenarios(
        base_out_folder,
        fname_infix,
        scenario_configs,
        failed_links,
        config_ids,
        num_workers=1
):
    worker_pool = multiprocessing.Pool(num_workers)
    results = worker_pool.map(run_failure_scenarios, zip(scenario_configs, failed_links, config_ids))

    # Gather results
    failed_links = collections.defaultdict(list)
    successful_links = collections.defaultdict(list)
    configs = list()
    for link, res, config_id in results:
        if res:
            successful_links[config_id].append(link)
        else:
            failed_links[config_id].append(link)
        if config_id not in configs:
            configs.append(config_id)

    for config_id in configs:
        with open(
                f"{base_out_folder}/single_{fname_infix}_{config_id}.json", "w"
        ) as fd:
            json.dump(
                {
                    'failed_links': failed_links[config_id],
                    'successful_links': successful_links[config_id],
                    'num_failed': len(failed_links[config_id]),
                    'num_successful': len(successful_links[config_id]),
                    'num_total_links': len(failed_links[config_id]) + len(successful_links[config_id])
                },
                fd
            )


def run_with_reconf_single_ts(topo_config, demand_config, algo_config,
                              fix_layer=None, base_out_folder=None, offset=None, comment=None, bypass=False):
    fix_layer_prev = fix_layer
    if bypass:
        fix_layer_prev = None

    if fix_layer is not None and all([type(f) == bool for f in fix_layer]):
        fixed_layer = model.fixed_layers.FromSolutionFileFixedLayersConfiguration(
            find_previous_solution_file(topo_config, demand_config, algo_config,
                                        fix_layer_prev, comment, base_out_folder, offset),
            ip_links=fix_layer[0],
            ip_connectivity=fix_layer[1],
            cdn_assignment=fix_layer[2]
        )
    elif fix_layer is not None and any([type(f) == float for f in fix_layer]):
        fixed_layer = model.fixed_layers.FromSolutionFileLimitedReconfigurationConfiguration(
            find_previous_solution_file(topo_config, demand_config, algo_config,
                                        fix_layer, comment, base_out_folder, offset),
            ip_links=fix_layer[0],
            ip_connectivity=fix_layer[1],
            cdn_assignment=fix_layer[2]
        )
    else:
        fixed_layer = model.fixed_layers.HardCodedFixedLayersConfiguration()

    scenario.ScenarioConfiguration(
        topology_configuration=topo_config,
        demand_configuration=demand_config,
        algorithm_configuration=algo_config,
        fixed_layers=fixed_layer,
        outputs=[output.file_writer.JsonWriterConfiguration(base_out_folder)],
        comment=comment
    ).produce().run()


def run_fix_from_beginning_all_in_folder(
        base_in_folder, base_out_folder, initial_ts, opt_topo, topo_parameter, bg_demand,
        algo, num_transceiver, fix_layers, offset
):
    comment = "fix_from_beginning"
    fname_tuples = get_input_file_tuples(base_in_folder)
    print("Found {} input tuples".format(len(fname_tuples)))
    old_ts = initial_ts
    for ts, fname_tuple in [(k, fname_tuples[k]) for k in sorted(fname_tuples.keys())]:
        print(ts)
        print(fname_tuple)
        topo_demand_tuples = create_demand_and_topo_configs(
            [fname_tuple],
            opt_topo_config=opt_topo,
            topo_parameter=topo_parameter,
            ip_node_default_num_transceiver=num_transceiver,
            demand_parameter={},
            bg_demand_config=bg_demand
        )
        assert len(topo_demand_tuples) == 1
        topo, dem = topo_demand_tuples[0]

        if int(ts) < initial_ts:
            continue
        elif int(ts) == initial_ts:
            run_with_reconf_single_ts(topo, dem, algo, comment=comment, base_out_folder=base_out_folder)
        elif int(ts) == initial_ts + offset:
            run_with_reconf_single_ts(topo, dem, algo, fix_layer=fix_layers, base_out_folder=base_out_folder,
                                      offset=offset,
                                      comment=comment, bypass=True)
        elif int(ts) > initial_ts and int(ts) >= old_ts + offset:
            run_with_reconf_single_ts(topo, dem, algo, fix_layer=fix_layers, base_out_folder=base_out_folder,
                                      offset=offset,
                                      comment=comment)
        elif int(ts) > initial_ts and int(ts) < old_ts + offset:
            continue
        old_ts = int(ts)
