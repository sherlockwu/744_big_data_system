import numpy as np
import matplotlib.pyplot as plt

mr_data = [161.243,201.666,369.791,294.58,379.641]
tez_data = [64.081,45.955,281.809,263.225,362.61]
names = ["12", "21", "50", "71", "85"]
x = [1,2,3,4,5]

def compute_disk():
	read_disk_mat = []
	write_disk_mat = []
	with open("./stat/diskstat_mr_new") as f:
		mat = []
		for line in f:
			if line == "\n":
				continue
			arr = line.split();
			if len(arr) != 6:
				continue
			tmp = arr[1:]
			mat.append(tmp)
		read_disk_mat = mat[:6]
		write_disk_mat = mat[6:]

	with open("./stat/diskstat_tez_new") as f:
		mat = []
		for line in f:
			if line == "\n":
				continue
			arr = line.split();
			if len(arr) != 6:
				continue
			tmp = arr[1:]
			mat.append(tmp)
		for i in range(5):
			read_disk_mat.append(mat[i])
		for i in range(5, 10):
			write_disk_mat.append(mat[i])
	# read_disk_mat = np.asarray(read_disk_mat)
	# write_disk_mat = np.asarray(write_disk_mat)
	# read_disk_mat = np.transpose(read_disk_mat)
	# write_disk_mat = np.transpose(write_disk_mat)

	diff_read_mat = [[0 for i in range(5)] for i in range(10)]
	diff_write_mat = [[0 for i in range(5)] for i in range(10)]

	for i in range(1, 11):
		for j in range(5):
			diff_read_mat[i - 1][j] = int(read_disk_mat[i][j]) - int(read_disk_mat[i - 1][j])
			diff_write_mat[i - 1][j] = int(write_disk_mat[i][j]) - int(write_disk_mat[i - 1][j])
	
	query_read_list = []
	query_write_list = []

	for query_index in range(10):
		query_read_total = 0
		query_write_total = 0
		for host_index in range(5):
			query_read_total += diff_read_mat[query_index][host_index] * 0.5 * 0.001
			query_write_total += diff_write_mat[query_index][host_index] * 0.5 * 0.001
		query_read_list.append(query_read_total)
		query_write_list.append(query_write_total)

	for i in range(5):
		throughput_mr_read = (query_read_list[i] + 0.0) / mr_data[i]
		throughput_tez_read = (query_read_list[i + 5] + 0.0) / tez_data[i]
		throughput_mr_write = (query_write_list[i] + 0.0) / mr_data[i]
		throughput_tez_write = (query_write_list[i + 5] + 0.0) / tez_data[i]
		print ("disk, query " + names[i])
		print ("mr read: " + str(throughput_mr_read))
		print ("tez read: " + str(throughput_tez_read))
		print ("mr write: " + str(throughput_mr_write))
		print ("tez write: " + str(throughput_tez_write))

	plt.figure(0)
	plt.ylabel("MB")
	plt.plot(x, query_read_list[0:5], 'r', label="mr_read")
	plt.plot(x, query_write_list[0:5], 'r--', label="mr_write")
	plt.plot(x, query_read_list[5:], 'b', label="tez_read")
	plt.plot(x, query_write_list[5:], 'b--', label="tez_write")
	plt.xticks(x, names)
	plt.title("disk")
	plt.legend()
	# plt.show()

def compute_net():
	read_net_mat = []
	write_net_mat = []
	with open("./stat/netstat_mr_new") as f:
		mat = []
		for line in f:
			if line == "\n":
				continue
			arr = line.split();
			if len(arr) != 6:
				continue
			tmp = arr[1:]
			mat.append(tmp)
		read_net_mat = mat[:6]
		write_net_mat = mat[6:]

	with open("./stat/netstat_tez_new") as f:
		mat = []
		for line in f:
			if line == "\n":
				continue
			arr = line.split();
			if len(arr) != 6:
				continue
			tmp = arr[1:]
			mat.append(tmp)
		for i in range(5):
			read_net_mat.append(mat[i])
		for i in range(5, 10):
			write_net_mat.append(mat[i])
	# read_net_mat = np.asarray(read_net_mat)
	# write_net_mat = np.asarray(write_net_mat)
	# read_net_mat = np.transpose(read_net_mat)
	# write_net_mat = np.transpose(write_net_mat)

	diff_read_mat = [[0 for i in range(5)] for i in range(10)]
	diff_write_mat = [[0 for i in range(5)] for i in range(10)]
	
	for i in range(1, 11):
		for j in range(5):
			diff_read_mat[i - 1][j] = int(read_net_mat[i][j]) - int(read_net_mat[i - 1][j])
			diff_write_mat[i - 1][j] = int(write_net_mat[i][j]) - int(write_net_mat[i - 1][j])
	
	query_read_list = []
	query_write_list = []

	for query_index in range(10):
		query_read_total = 0
		query_write_total = 0
		for host_index in range(5):
			query_read_total += diff_read_mat[query_index][host_index]
			query_write_total += diff_write_mat[query_index][host_index]
		query_read_list.append(query_read_total)
		query_write_list.append(query_write_total)

	for i in range(5):
		throughput_mr_read = (query_read_list[i] + 0.0) / mr_data[i]
		throughput_tez_read = (query_read_list[i + 5] + 0.0) / tez_data[i]
		throughput_mr_write = (query_write_list[i] + 0.0) / mr_data[i]
		throughput_tez_write = (query_write_list[i + 5] + 0.0) / tez_data[i]
		print ("net, query " + names[i])
		print ("mr reveived: " + str(throughput_mr_read))
		print ("tez received: " + str(throughput_tez_read))
		print ("mr transmit: " + str(throughput_mr_write))
		print ("tez transmit: " + str(throughput_tez_write))
	
	plt.figure(1)
	plt.ylabel("bytes")
	plt.plot(x, query_read_list[0:5], 'r', label="mr_received")
	plt.plot(x, query_write_list[0:5], 'r--', label="mr_transmit")
	plt.plot(x, query_read_list[5:], 'b', label="tez_received")
	plt.plot(x, query_write_list[5:], 'b--', label="tez_transmit")
	plt.xticks(x, names)
	plt.title("network")
	plt.legend()
	plt.show()


def main():
	compute_disk()
	compute_net()
if __name__ == "__main__":
	main()