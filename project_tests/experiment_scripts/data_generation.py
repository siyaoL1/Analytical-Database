#!/usr/bin/python
import sys
import string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd

import data_gen_utils
import math

# note this is the base path where we store the data files we generate
# TEST_BASE_DIR = "/cs165/generated_data"
TEST_BASE_DIR = "../../project_tests/"

# note this is the base path that _POINTS_ to the data files we generate
# DOCKER_TEST_BASE_DIR = "/cs165/staff_test"
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"

# PRECISION FOR AVG OPERATION
PLACES_TO_ROUND = 2


def get_data_file_name(data_size, col_num, id):
    return f'{DOCKER_TEST_BASE_DIR}/{id}_{data_size}_{col_num}_experiment_data.csv'

# M1 Data
def generate_load_data(data_size, col_num, id):
    output_file = f'{TEST_BASE_DIR}/{id}_{data_size}_{col_num}_experiment_data.csv'
    header_line = data_gen_utils.generateHeaderLine(
        'db1', 'tbl_' + id, col_num)
    output_table = pd.DataFrame(np.random.randint(
        0, data_size/4, size=(data_size, col_num)), columns=['col' + str(i+1) for i in range(col_num)])
    output_table.to_csv(output_file, sep=',', index=False,
                        header=header_line, line_terminator='\n')
    return output_table

def generate_load_dsl(test_num, col_num, id, data_file):
    output_file = data_gen_utils.openFileHandles(test_num, TEST_DIR=TEST_BASE_DIR)
    output_file.write('create(db,\"db1\")\n')
    output_file.write(f'create(tbl,\"tbl_{id}\",db1,{col_num})\n')
    for i in range(col_num):
        output_file.write(f'create(col,\"col{i+1}\",db1.tbl_{id})\n')
    output_file.write(f'load(\"{data_file}\")\n')
    output_file.write('shutdown\n')
    # generate expected results
    data_gen_utils.closeFileHandles(output_file)

def m1():
    # Experiment input => (data_size, col_num, id)
    data_sizes = [10000, 50000, 100000, 200000, 300000, 500000, 700000, 800000, 1000000]
    setup_info = [(size, 4, 'm1') for size in data_sizes]

    # Generate data and dsl file
    for i in range(len(setup_info)):
        generate_load_data(*setup_info[i])
        data_file = get_data_file_name(*setup_info[i])
        generate_load_dsl(i+1, setup_info[i][1], setup_info[i][2], data_file)


# M2 Data
def generate_shared_scan_data(data_size, col_num=4, id='m2'):
    output_file = f'{TEST_BASE_DIR}/{id}_{data_size}_{col_num}_experiment_data.csv'
    header_line = data_gen_utils.generateHeaderLine('db1', 'tbl_' + id, col_num)
    output_table = pd.DataFrame(np.random.randint(0, data_size, size=(data_size, 4)), columns =['col1', 'col2', 'col3', 'col4'])
    output_table.to_csv(output_file, sep=',', index=False, header=header_line, line_terminator='\n')
    return output_table

def generate_shared_scan_init_dsl(test_num, col_num=4, id='m2', data_file=''):
    output_file = data_gen_utils.openFileHandles(test_num, TEST_DIR=TEST_BASE_DIR)
    output_file.write('create(db,\"db1\")\n')
    output_file.write(f'create(tbl,\"tbl_{id}\",db1,{col_num})\n')
    for i in range(col_num):
        output_file.write(f'create(col,\"col{i+1}\",db1.tbl_{id})\n')
    output_file.write(f'load(\"{data_file}\")\n')
    output_file.write('shutdown\n')
    # generate expected results
    data_gen_utils.closeFileHandles(output_file)

def generate_shared_scan_dsl(data_size, test_num, query_number, batch=True):
    # prelude and queryDOCKER_TEST_BASE_DIR
    output_file = data_gen_utils.openFileHandles(test_num, TEST_DIR=TEST_BASE_DIR)
    if batch:
        output_file.write('batch_queries()\n')
    for i in range(query_number):
        one = np.random.randint(0, data_size)
        two = np.random.randint(0, data_size)
        left = one if one < two else two
        right = one if one > two else two
        output_file.write('s{}=select(db1.tbl_m2.col4,{},{})\n'.format(i, left, right))
    if batch:
        output_file.write('batch_execute()\n')
    output_file.write('shutdown\n')
    data_gen_utils.closeFileHandles(output_file)

def m2():
    data_size = 30000
    generate_shared_scan_data(data_size)
    data_file = get_data_file_name(data_size, 4, 'm2')
    # 10
    generate_shared_scan_init_dsl(10, data_file=data_file)
    query_numbers = [10, 50, 100, 150, 200, 250, 300, 350, 400]
    index = 11
    # 11 - 19
    for query_number in query_numbers:
        generate_shared_scan_dsl(data_size, index, query_number)
        index += 1
    print(index)
    # 20 - 28
    for query_number in query_numbers:
        generate_shared_scan_dsl(data_size, index, query_number, batch=False)
        index += 1
    # 29
    generate_shared_scan_dsl(data_size, index, 1000)
    index += 1
    # 30
    generate_shared_scan_dsl(data_size, index, 1000, batch=False)
    index += 1


# m4
class ZipfianDistribution:
    def __init__(self,zipfian_param, num_elements):
        self.zipfian_param = zipfian_param
        self.num_elements = num_elements
        self.H_s = ZipfianDistribution.computeHarmonic(zipfian_param, num_elements)

    def computeHarmonic(zipfian_param, num_elements):
        total = 0.0
        for k in range(1,num_elements+1,1):
            total += (1.0/math.pow(k, zipfian_param))
        return total

    def drawRandomSample(self, unifSample):
        total = 0.0
        k = 0
        while (unifSample >= total):
            k += 1
            total += ((1.0/math.pow(k, self.zipfian_param)) / self.H_s)
        return k
    def createRandomNumpyArray(self,arraySize):
        array = np.random.uniform(size=(arraySize))
        vectorized_sample_func = np.vectorize(self.drawRandomSample)
        return vectorized_sample_func(array)

def generate_nested_loop_data(data_size_fact, data_size_dim1, zipfian_param, num_distinct_elements):
    outputFile1 = f'{TEST_BASE_DIR}/m4_nested_fact_experiment_data.csv'
    outputFile2 = f'{TEST_BASE_DIR}/m4_nested_dim1_experiment_data.csv'

    header_line_fact = data_gen_utils.generateHeaderLine('db1', 'tbl5_fact_exp', 2)
    header_line_dim1 = data_gen_utils.generateHeaderLine('db1', 'tbl5_dim1_exp', 2)

    zipfDist = ZipfianDistribution(zipfian_param, num_distinct_elements)

    outputFactTable = pd.DataFrame(np.random.randint(0, data_size_fact/5, size=(data_size_fact, 2)), columns =['col1', 'col2'])
    outputFactTable['col1'] = zipfDist.createRandomNumpyArray(data_size_fact)

    # joinable on col1 with fact table
    outputDimTable1 = pd.DataFrame(np.random.randint(0, data_size_dim1/5, size=(data_size_dim1, 2)), columns =['col1', 'col2'])
    outputDimTable1['col1'] = zipfDist.createRandomNumpyArray(data_size_dim1)

    outputFactTable.to_csv(outputFile1, sep=',', index=False, header=header_line_fact, line_terminator='\n')
    outputDimTable1.to_csv(outputFile2, sep=',', index=False, header=header_line_dim1, line_terminator='\n')
    return outputFactTable, outputDimTable1

def generate_nested_loop_data_dsl():
    # prelude
    output_file = data_gen_utils.openFileHandles(31, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Creates tables for nested join experiment\n')
    output_file.write('create(db,"db1")\n')
    output_file.write('create(tbl,"tbl5_fact_exp",db1,2)\n')
    output_file.write('create(col,"col1",db1.tbl5_fact_exp)\n')
    output_file.write('create(col,"col2",db1.tbl5_fact_exp)\n')
    output_file.write('load("'+DOCKER_TEST_BASE_DIR+'/m4_nested_fact_experiment_data.csv")\n')
    output_file.write('--\n')
    output_file.write('create(tbl,"tbl5_dim1_exp",db1,2)\n')
    output_file.write('create(col,"col1",db1.tbl5_dim1_exp)\n')
    output_file.write('create(col,"col2",db1.tbl5_dim1_exp)\n')
    output_file.write('load("'+DOCKER_TEST_BASE_DIR+'/m4_nested_dim1_experiment_data.csv")\n')
    output_file.write('shutdown\n')
    # no expected results
    data_gen_utils.closeFileHandles(output_file)

def generate_nested_loop_dsl(data_size_fact, data_size_dim1, selectivity_fact, selectivity_dim1):
    output_file = data_gen_utils.openFileHandles(32, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- nested-loop. Select + Join \n')
    output_file.write('p1=select(db1.tbl5_fact_exp.col2,null, {})\n'.format(int(selectivity_fact * (data_size_fact / 5))))
    output_file.write('p2=select(db1.tbl5_dim1_exp.col2,null, {})\n'.format(int((data_size_dim1/5) * selectivity_dim1)))
    output_file.write('f1=fetch(db1.tbl5_fact_exp.col1,p1)\n')
    output_file.write('f2=fetch(db1.tbl5_dim1_exp.col1,p2)\n')
    output_file.write('t1,t2=join(f1,p1,f2,p2,hash)\n')
    output_file.write('shutdown\n')

def m4():
    fact_size = 50000
    dim1_size = 50000
    zipfian_param = 1.0
    num_distinct_elements = 50
    selectivity = 1
    generate_nested_loop_data(fact_size, dim1_size, zipfian_param, num_distinct_elements)
    generate_nested_loop_data_dsl()
    generate_nested_loop_dsl(fact_size, dim1_size, selectivity, selectivity)


# M3
def generateDataMilestone3(dataSize):
    outputFile_ctrl = TEST_BASE_DIR + '/' + 'm3_ctrl_exp.csv'
    outputFile_clustered = TEST_BASE_DIR + '/' + 'm3_clustered_exp.csv'
    outputFile_unclustered = TEST_BASE_DIR + '/' + 'm3_unclustered_exp.csv'
    header_line_ctrl = data_gen_utils.generateHeaderLine('db1', 'tbl4_ctrl', 1)
    header_line_btree = data_gen_utils.generateHeaderLine('db1', 'tbl4_clustered', 1)
    header_line_clustered_btree = data_gen_utils.generateHeaderLine('db1', 'tbl4_unclustered', 1)
    arr = np.arange(start=0, stop=dataSize, step=1)
    np.random.shuffle(arr)
    outputTable = pd.DataFrame(arr, columns =['col1'])
    outputTable.to_csv(outputFile_ctrl, sep=',', index=False, header=header_line_ctrl, line_terminator='\n')
    outputTable.to_csv(outputFile_clustered, sep=',', index=False, header=header_line_btree, line_terminator='\n')
    outputTable.to_csv(outputFile_unclustered, sep=',', index=False, header=header_line_clustered_btree, line_terminator='\n')

def generate_m3_data_dsl():
    # prelude
    output_file = data_gen_utils.openFileHandles(33, TEST_DIR=TEST_BASE_DIR)
    output_file.write('create(db,"db1")\n')

    output_file.write('create(tbl,"tbl4_ctrl",db1,1)\n')
    output_file.write('create(col,"col1",db1.tbl4_ctrl)\n')
    output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/m3_ctrl_exp.csv\")\n')


    output_file.write('create(tbl,"tbl4_clustered",db1,1)\n')
    output_file.write('create(col,"col1",db1.tbl4_clustered)\n')
    output_file.write('create(idx,db1.tbl4_clustered.col1,sorted,clustered)\n')
    output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/m3_clustered_exp.csv\")\n')

    output_file.write('create(tbl,"tbl4_unclustered",db1,1)\n')
    output_file.write('create(col,"col1",db1.tbl4_unclustered)\n')
    output_file.write('create(idx,db1.tbl4_unclustered.col1,sorted,unclustered)\n')
    output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/m3_unclustered_exp.csv\")\n')

    output_file.write('shutdown\n')

def generate_m3_dsl(data_size, selectivity):
    output_file_ctrl = data_gen_utils.openFileHandles(34, TEST_DIR=TEST_BASE_DIR)
    output_file_ctrl.write('p1=select(db1.tbl4_ctrl.col1,0, {})\n'.format(int(data_size * selectivity)))
    output_file_ctrl.write('f2=fetch(db1.tbl4_ctrl.col1,p1)\n')
    output_file_ctrl.write('shutdown\n')
    
    output_file_ctrl = data_gen_utils.openFileHandles(35, TEST_DIR=TEST_BASE_DIR)
    output_file_ctrl.write('p1=select(db1.tbl4_clustered.col1,0, {})\n'.format(int(data_size * selectivity)))
    output_file_ctrl.write('f2=fetch(db1.tbl4_clustered.col1,p1)\n')
    output_file_ctrl.write('shutdown\n')
    
    output_file_ctrl = data_gen_utils.openFileHandles(36, TEST_DIR=TEST_BASE_DIR)
    output_file_ctrl.write('p1=select(db1.tbl4_unclustered.col1,0, {})\n'.format(int(data_size * selectivity)))
    output_file_ctrl.write('f2=fetch(db1.tbl4_unclustered.col1,p1)\n')
    output_file_ctrl.write('shutdown\n')
    
def m3():
    # generateDataMilestone3(30000)
    # generate_m3_data_dsl()
    generate_m3_dsl(30000, 1)

def main(argv):
    global TEST_BASE_DIR
    global DOCKER_TEST_BASE_DIR
    # m1()
    # m2()
    m3()
    # m4()

if __name__ == "__main__":
	main(sys.argv[1:])