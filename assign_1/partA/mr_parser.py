import json
from pprint import pprint
import matplotlib.pyplot as plt
import glob, os

names = ["12", "21", "50", "71", "85"]
def drawPlot(file, index):
	data = []
	path = "/Users/fuhao/744_big_data_system/partA/new_hdfs_logs/mr/";
	with open(path + file) as f:
	    for line in f:

	    	if line == "\n" or line == "Avro-Json\n":
	    		continue
	    	data.append(json.loads(line))

	map_start = "MAP_ATTEMPT_STARTED"
	reduce_start = "REDUCE_ATTEMPT_STARTED"
	task_finish = "TASK_FINISHED"
	startTime = "startTime"
	finishTime = "finishTime"

	start_entry_key = "org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStarted"
	end_entry_key = "org.apache.hadoop.mapreduce.jobhistory.TaskFinished"
	alive = 0

	map_count = 0
	reduce_count = 0
	add = []
	minus = []

	for line in data:
		type_entry = line["type"]
		if not (type_entry == map_start or type_entry == reduce_start or type_entry == task_finish):
			continue
		if type_entry == map_start or type_entry == reduce_start:
			time_entry = line["event"][start_entry_key][startTime]
			add.append(time_entry)
			if type_entry == map_start:
				map_count += 1
			else:
				reduce_count += 1
		else:
			time_entry = line["event"][end_entry_key][finishTime]
			minus.append(time_entry)

	add.sort()
	minus.sort()
	ptr_add = 0
	ptr_minus = 0
	alive = 0
	arr = []
	for i in range(add[0], minus[-1]):
		while ptr_add < len(add) and i == add[ptr_add]:
			alive += 1
			ptr_add += 1
		while ptr_minus < len(minus) and i == minus[ptr_minus]:
			alive -= 1
			ptr_minus += 1
		arr.append(alive)
	print ("total tasks: " + str(map_count + reduce_count))
	print ("numer of map tasks: " + str(map_count))
	print ("numer of reduce tasks: " + str(reduce_count))
	print ("ratio of reduce versus map: " + str((reduce_count + 0.0) / map_count))

	axes = plt.gca()
	axes.set_ylim([0, 30])
	plt.figure(index)
	plt.plot(arr)
	plt.title(names[index])

def main():
	os.chdir("./new_hdfs_logs/mr")
	i = 0
	for file in glob.glob("*.json"):
		print (names[i])
		drawPlot(file, i)
		i += 1
	plt.show()

if __name__ == "__main__":
	main()