import json
from pprint import pprint
import matplotlib.pyplot as plt
import glob, os

names = ["12", "21", "50", "71", "85"]
def drawPlot(file, index):
	data = []
	path = "/Users/fuhao/744_big_data_system/partA/new_hdfs_logs/tez/";
	with open(path + file) as f:
	    for line in f:
	    	data.append(json.loads(line))

	task_start = "TASK_ATTEMPT_STARTED"
	task_finish = "TASK_FINISHED"
	startTime = "startTime"
	endTime = "endTime"

	hdfs_read = "HDFS_BYTES_READ"
	hdfs_write = "HDFS_BYTES_WRITTEN"

	alive = 0
	add = []
	minus = []
	map_count = 0
	total_count = 0
	reduce_count = 0

	events_type_counter = {}
	for line in data:
		if "events" not in line:
			continue
		type_entry = line["events"][0]

		if not (type_entry["eventtype"] == task_start or type_entry["eventtype"] == task_finish):
			continue

		time_entry = line["otherinfo"]
		cur = type_entry["eventtype"]
		if cur == task_start:
			add.append(type_entry["ts"])
			total_count += 1
		elif cur == task_finish:
			minus.append(time_entry[endTime])
			for group in line["otherinfo"]["counters"]["counterGroups"]:
				if group["counterGroupName"] == "org.apache.tez.common.counters.FileSystemCounter":
					for counter in group["counters"]:
						if counter["counterName"] == hdfs_read:
							map_count += 1

	print ("total count : " + str(total_count))
	print ("map count : " + str(map_count))
	print ("reduce count: " + str(total_count - map_count))
	print ("ratio of reduce versus map: " + str((total_count - map_count + 0.0) / map_count))
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

	axes = plt.gca()
	axes.set_ylim([0, 30])
	plt.figure(index)
	plt.plot(arr)
	plt.title(names[index])


def main():
	os.chdir("./new_hdfs_logs/tez")
	i = 0
	for file in glob.glob("*.json"):
		print ("query " + names[i])
		drawPlot(file, i)
		i += 1
	plt.show()

if __name__ == "__main__":
	main()