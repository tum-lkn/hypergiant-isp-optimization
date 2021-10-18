import constants
import algorithm.greedy
import algorithm.mip_pathbased_lin

BASE_PATH = "/home/sim/data/"

FOLDER_SUFFIX_DAY = [
    # Actual days are redacted
    "DAY1",
    "DAY2",
    "DAY3"
]

FIBER_CAPACITY = 100
IP_LINK_CAPACITY = 100
NUM_TRANSCEIVERS = 100
IP_LINK_UTIL = 0.5

TOPO_PARAMETER = {
    constants.KEY_IP_LIGHTPATH_CAPACITY: IP_LINK_CAPACITY,
    constants.KEY_IP_LINK_UTILIZATION: IP_LINK_UTIL
}

SOLVER_RUNTIME = 3600

ALGO_CONFIG_MIP = algorithm.mip_pathbased_lin.PathBasedMixedIntegerProgramConfiguration(
    model_implementor=algorithm.mip_pathbased_lin.PathMixedIntegerProgram.MODEL_IMPLEMENTOR_CPLEX,
    num_threads=4,
    time_limit=SOLVER_RUNTIME
)

ALGO_CONFIG_GREEDY = algorithm.greedy.GreedyCDNAssignmentAlgorithmConfiguration(
    algorithm.mip_pathbased_lin.PathBasedMixedIntegerProgramConfiguration(
        model_implementor=algorithm.mip_pathbased_lin.PathMixedIntegerProgram.MODEL_IMPLEMENTOR_CPLEX,
        num_threads=4,
        time_limit=SOLVER_RUNTIME
    )
)
