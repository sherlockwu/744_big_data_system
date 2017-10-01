source /home/ubuntu/run.sh
cd /home/ubuntu/grader/MR_Application/jar
hadoop jar ac.jar AnagramSorter /input /output
cd /home/ubuntu/grader
rm -r ./mr_output
hadoop fs -get /output ./mr_output
