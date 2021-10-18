#!/bin/bash

# No background 1,2,4,8h time windows; the days for Corona study are also run with these commands (via FOLDER_SUFFIX_DAY)
PYTHONPATH=../.. python3 run_isponly_no_bg.py --time_agg 1
PYTHONPATH=../.. python3 run_isponly_no_bg.py --time_agg 2
PYTHONPATH=../.. python3 run_isponly_no_bg.py --time_agg 4
PYTHONPATH=../.. python3 run_isponly_no_bg.py --time_agg 8

PYTHONPATH=../.. python3 run_jomip_no_bg.py --time_agg 1
PYTHONPATH=../.. python3 run_jomip_no_bg.py --time_agg 2
PYTHONPATH=../.. python3 run_jomip_no_bg.py --time_agg 4
PYTHONPATH=../.. python3 run_jomip_no_bg.py --time_agg 8

PYTHONPATH=../.. python3 run_greedy_no_bg.py --time_agg 1
PYTHONPATH=../.. python3 run_greedy_no_bg.py --time_agg 2
PYTHONPATH=../.. python3 run_greedy_no_bg.py --time_agg 4
PYTHONPATH=../.. python3 run_greedy_no_bg.py --time_agg 8

# With background traffic
PYTHONPATH=../.. python3 run_isponly_wbg.py --time_agg 2
PYTHONPATH=../.. python3 run_jomip_wbg.py --time_agg 2
PYTHONPATH=../.. python3 run_greedy_wbg.py --time_agg 2

# Limited reconfigurations for Jomip
PYTHONPATH=../.. python3 run_jomip_from_beginning_no_bg.py --suffix DAY1 --rclimit 0.15 --init_ts 0



