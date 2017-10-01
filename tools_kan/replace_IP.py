# read a file
import sys
file_name = sys.argv[1]
f_id = open(file_name, 'r')
f_str = f_id.read().replace('MASTER_IP','10.254.0.136')
f_id.close()

f_id = open(file_name, 'wb')

f_id.write(f_str)
f_id.close()

# replace 

