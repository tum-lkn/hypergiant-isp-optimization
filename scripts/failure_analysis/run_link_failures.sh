#!/bin/bash 

timestamps=(
# REDACTED
)
for i in "${timestamps[@]}"
do
	echo "Run for $i"
	PYTHONPATH=../.. python3 run_link_failures_top30_no_bg.py --rclimit 0.0 --util 0.5 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_no_bg.py --rclimit 0.0 --util 0.6 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_no_bg.py --rclimit 0.0 --util 0.7 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_no_bg.py --rclimit 0.0 --util 0.8 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_no_bg.py --rclimit 0.0 --util 0.9 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_no_bg.py --rclimit 0.0 --util 1.0 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_baseline_no_bg.py --rclimit 0.0 --util 0.5 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_baseline_no_bg.py --rclimit 0.0 --util 0.6 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_baseline_no_bg.py --rclimit 0.0 --util 0.7 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_baseline_no_bg.py --rclimit 0.0 --util 0.8 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_baseline_no_bg.py --rclimit 0.0 --util 0.9 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_baseline_no_bg.py --rclimit 0.0 --util 1.0 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_greedy_no_bg.py --rclimit 0.0 --util 0.5 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_greedy_no_bg.py --rclimit 0.0 --util 0.6 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_greedy_no_bg.py --rclimit 0.0 --util 0.7 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_greedy_no_bg.py --rclimit 0.0 --util 0.8 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_greedy_no_bg.py --rclimit 0.0 --util 0.9 --ts $i
	PYTHONPATH=../.. python3 run_link_failures_top30_greedy_no_bg.py --rclimit 0.0 --util 1.0 --ts $i
done

