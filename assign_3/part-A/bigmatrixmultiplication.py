import tensorflow as tf
import os
import time
import sys


worker_num = 5
N = 80000 # dimension
d = 30 # # of submatrix in one dimension
M = int(N / d)

def getBlock(i, j):
    return "sub-matrix-"+str(i)+"-"+str(j)

def getTrace(i, j):
    return "inter-"+str(i)+"-"+str(j)

def run():
    g = tf.Graph()

    with g.as_default():
        matrices = {}
        for i in range(0, d):
            for j in range(0, d):
                with tf.device("/job:worker/task:%d" % ((i*(d-1)+j) % worker_num)):
                    matrix_name = getBlock(i, j)
                    matrices[matrix_name] = tf.random_uniform([M, M], name=matrix_name)

        intermediate_traces = {}
        for i in range(0, d):
            for j in range(0, d):
                with tf.device("/job:worker/task:%d" % ((i*(d-1)+j) % worker_num)):
                    A = matrices[getBlock(i, j)]
                    B = matrices[getBlock(j, i)]
                    intermediate_traces[getTrace(i, j)] = tf.trace(tf.matmul(A, B))

        with tf.device("/job:worker/task:0"):
            retval = tf.add_n(intermediate_traces.values())

        config = tf.ConfigProto(log_device_placement=True)
        with tf.Session("grpc://vm-21-3:2222", config=config) as sess:
            result = sess.run(retval)
            sess.close()
            print result


before = time.time()
print('start time'),
print(time.asctime(time.localtime(time.time())))

run()

after = time.time()
print('end time'),
print(time.asctime(time.localtime(time.time())))

print('Duration: '),
print(after - before)
