import random
import json
import argparse
import pprint
import sys

machinesKey = "machines"
resoucesKey = "resources"
diskKey = "disk"
replicaKey = "replica"
GPPMKey = "global_partitions_per_machine"
MPITKey = "max_partitions_in_task"
stage_prefix = "Stage_"
path = 'dag_sim/CarbyneSim/inputs/'


dag_num = stage_num = resources_dimension = key_num = key_share = replica_num = 0

def generate_config():
    config = {}
    config[GPPMKey] = 2
    config[MPITKey] = 4
    config[machinesKey] = []
    machineObj = {}
    machineObj[diskKey] = 2000
    machineObj[resoucesKey] = [4.0, 16.0]
    #machineObj[resoucesKey] = [5 for i in range(resources_dimension)]
    machineObj[replicaKey] = replica_num
    config[machinesKey].append(machineObj)
    return config

def get_key_size():
	return [key_share for i in range(key_num)]

def get_dependencies():
	dependencies = []
	for i in range(stage_num - 1):
		dependency = {}
		dependency["src"] = stage_prefix + str(i)
		dependency["dst"] = stage_prefix + str(i + 1)
		dependency["pattern"] = "ata"
		dependencies.append(dependency)
	return dependencies

def get_stages():
	stages = []
	for i in range(stage_num):
		stage = {}
		stage["name"] = stage_prefix + str(i)
		stage["duration"] = random.uniform(0.0, 0.2)
		stage["resources"] = [0.1 for i in range(resources_dimension)]
		stage["num_tasks"] = 2 #random.randint(2,15)
		stage["outin_ratio"] = 0.5
		stages.append(stage)
	return stages

def generate_dag_input():
	#need: dag_num, stage_num, resources_dimension, key_num, key_share, replica_num

	dag_input_list = []
	for i in range(dag_num):
		dag_input = {}
		dag_input["name"] = str(i)
		dag_input["dagID"] = i
		dag_input["arrival_time"] = i
		dag_input["quota"] = 1000
		dag_input["key_sizes"] = get_key_size()
		dag_input["stages"] = get_stages()
		dag_input["dependencies"] = get_dependencies()
		dag_input_list.append(dag_input)
	return dag_input_list

def main():
	global dag_num, stage_num, resources_dimension, key_num, key_share, replica_num
	parser = argparse.ArgumentParser()
	parser.add_argument("dag_num", nargs='?', default=1)
	parser.add_argument("stage_num", nargs='?', default=2)
	parser.add_argument("resources_dimension", nargs='?', default=2)
	parser.add_argument("key_num", nargs='?', default=20)
	parser.add_argument("key_share", nargs='?', default=10)
	parser.add_argument("replica_num", nargs='?', default=2)
	dag = None
	config = None
	if len(sys.argv) != 7 and len(sys.argv) != 1:
		print len(sys.argv)
		print ("usage: python generate.py dag_num stage_num resources_dimension key_num key_share replica_num")
		exit(1)
	try:
		args = parser.parse_args()
		dag_num = int(args.dag_num)
		stage_num = int(args.stage_num)
		resources_dimension = int(args.resources_dimension)
		key_num = int(args.key_num)
		key_share = int(args.key_share)
		replica_num = int(args.replica_num)
		print args.dag_num
		print args.stage_num
		print args.resources_dimension
		print args.key_num
		print args.key_share
		print args.replica_num
		dag = generate_dag_input()
		config = generate_config()
	except:
		print ("usage: python generate.py dag_num stage_num resources_dimension key_num key_share replica_num")
		exit(1)
	with open(path+ "config_test.json", 'w') as config_file:
		json.dump(config, config_file)

	with open(path + "dags-input_test.json", 'w') as dags_input_file:
		json.dump(dag, dags_input_file)

main()
