import json
import os

import pandas as pd


def aggregate(fname, base_path):
    df = pd.DataFrame(
        columns=["config_id", "timestamp", "restoration_reconf_limit", "restoration_link_util", "date"]
    ).set_index(["restoration_reconf_limit", "restoration_link_util", "timestamp", "config_id"])
    if os.path.exists(fname):
        df = pd.read_hdf(fname, key='failure_metrics')
    
    solutions_list = list()
    for solution_file in filter(lambda x: "single_fiberfailures_" in x, sorted(os.listdir(base_path))):
        print(solution_file)
        config_id = solution_file.split('_')[-1].rstrip('.json')
        reconf_limit = float(solution_file.split('_')[-3])
        link_util = float(solution_file.split('_')[-2])

        # Get timestamp
        fa_folder_name = list(filter(lambda x: "failure_analysis_" in x and config_id in x, sorted(os.listdir(base_path))))[0]
        timestamp = int(fa_folder_name.split('_')[-2])

        if (reconf_limit, link_util, timestamp, config_id) in df.index.values.tolist():
            print("skipping")
            continue

        with open(f"{base_path}/{solution_file}", "r") as fd:
            data = json.load(fd)

        data.update(
                {
                    "config_id": config_id,
                    "restoration_reconf_limit": reconf_limit,
                    "restoration_link_util": link_util,
                    "timestamp": timestamp
                }
        )
        solutions_list.append(data)

    if len(solutions_list) > 0:
        new_df = pd.DataFrame(solutions_list)
        new_df["date"] = pd.to_datetime(new_df["timestamp"], unit='s')
        new_df = new_df.set_index(["restoration_reconf_limit", "restoration_link_util", "timestamp", "config_id"])
        df = pd.concat(
                [df, new_df], sort=False
        )
        df.to_hdf(fname, key='failure_metrics')


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Aggregate raw results to hdf file')
    parser.add_argument('--path', help="base path", type=str)
    parser.add_argument('--suffix', help="suffix of path", type=str)

    args = parser.parse_args()
    
    BASE_PATH = args.path + "/output_" + args.suffix + "/"
    AGG_METRICS_NAME = args.path + "/failure_metrics_" + args.suffix + ".h5"

    aggregate(AGG_METRICS_NAME, BASE_PATH)
