import subprocess
import sys
import time
import data_generation
import data_gen_utils

PERF_CMD = 'perf stat ./client'
CACHE_GRIND_CMD = 'valgrind --tool=cachegrind --branch-sim=yes ./server'
SERVER_PATH = './server'
CLIENT_PATH = './client'


CLEAR_LOG = 1

import os
import glob


"""
    Clear All database files
"""
def clear_database():
    files = glob.glob('./database/*')
    for f in files:
        os.remove(f)
"""
    Clear cachegrind output
"""
def clear_cachegrind_output():
    files = glob.glob('./cachegrind.out*')
    for f in files:
        os.remove(f)
"""
    Benchmark Experiment
"""
def experiment(args, begin, end, clear=False):
    for i in range(begin,end+1):
        if clear:
            clear_database()
        data_file = open(f'/cs165/staff_test/experiment{i}gen.dsl')
        # ------------
        subprocess.Popen(args, stdout=subprocess.PIPE)
        time.sleep(1)
        subprocess.run([CLIENT_PATH], stdin=data_file)
        # ------------
        data_file.close()
        
    if CLEAR_LOG:
        clear_cachegrind_output()


def m1():
    # Cache 
    experiment(CACHE_GRIND_CMD.split(), 1, 9, clear=True)
    # Time
    experiment(PERF_CMD.split(), 1, 9, clear=True)


def m2_setup():
    experiment(CACHE_GRIND_CMD.split(), 10, 10, clear=True)

def m2(one):
    if one:
        # # Cache 
        experiment(CACHE_GRIND_CMD.split(), 11, 19)
        # # Time
        experiment(PERF_CMD.split(), 11, 19)

        # Cache 
        experiment(CACHE_GRIND_CMD.split(), 20, 28)
        # Time
        experiment(PERF_CMD.split(), 20, 28)
    else:
        # print("\n-------------------------------------\n")
        # Cache 
        # experiment(CACHE_GRIND_CMD.split(), 29, 29)
        # Time
        # experiment(PERF_CMD.split(), 29, 29)

        # print("\n-------------------------------------\n")
        # Cache 
        experiment(CACHE_GRIND_CMD.split(), 30, 30)
        # # Time
        experiment(PERF_CMD.split(), 30, 30)
        # print("\n-------------------------------------\n")

def m4_setup():
    experiment(CACHE_GRIND_CMD.split(), 31, 31, clear=True)

def m4():
    # # Cache 
    experiment(CACHE_GRIND_CMD.split(), 32, 32)
    # # Time
    experiment(PERF_CMD.split(), 32, 32)



def main(argv):
    # m1()
    # m2_setup() 
    # m2(False)

    # m4_setup()
    m4()

if __name__ == "__main__":
	main(sys.argv[1:])

