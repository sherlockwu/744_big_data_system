import numpy as np

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
	read_disk_mat = np.asarray(read_disk_mat)
	write_disk_mat = np.asarray(write_disk_mat)
	read_disk_mat = np.transpose(read_disk_mat)
	write_disk_mat = np.transpose(write_disk_mat)

	diff_read_mat = [[0 for i in range(10)] for i in range(5)]
	diff_write_mat = [[0 for i in range(10)] for i in range(5)]
	for i in range(5):
		for j in range(1, 11):
			diff_read_mat[i][j - 1] = int(read_disk_mat[i][j]) - int(read_disk_mat[i][j - 1])
			diff_write_mat[i][j - 1] = int(write_disk_mat[i][j]) - int(write_disk_mat[i][j - 1])
	# print (diff_read_mat)
	# print (diff_write_mat)

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
	read_net_mat = np.asarray(read_net_mat)
	write_net_mat = np.asarray(write_net_mat)
	read_net_mat = np.transpose(read_net_mat)
	write_net_mat = np.transpose(write_net_mat)

	diff_read_mat = [[0 for i in range(10)] for i in range(5)]
	diff_write_mat = [[0 for i in range(10)] for i in range(5)]
	for i in range(5):
		for j in range(1, 11):
			diff_read_mat[i][j - 1] = int(read_net_mat[i][j]) - int(read_net_mat[i][j - 1])
			diff_write_mat[i][j - 1] = int(write_net_mat[i][j]) - int(write_net_mat[i][j - 1])
	print (diff_read_mat)
	print (diff_write_mat)


def main():
	compute_disk()
	compute_net()
if __name__ == "__main__":
	main()