import matplotlib.pyplot as plt
import numpy as np

def main():
	names = ["12", "21", "50", "71", "85"]
	x = [1,2,3,4,5]
	mr_data = [161.243,201.666,369.791,294.58,379.641]
	tez_data = [64.081,45.955,281.809,263.225,362.61]
	plt.bar(np.asarray(x) - 0.2, mr_data, width=0.4, label="MR")
	plt.bar(np.asarray(x) + 0.2, tez_data, width=0.4, label="tez")
	plt.xticks(x, names)
	plt.legend()
	plt.show()

if __name__ == "__main__":
	main()