import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AnagramSorter {

    public static class TokenizerMapper     // for read in the file
             extends Mapper<Object, Text, Text, Text>{

             private final static IntWritable one = new IntWritable(1);
             private Text word = new Text();

             public void map(Object key, Text value, Context context
                             ) throws IOException, InterruptedException {
                 StringTokenizer itr = new StringTokenizer(value.toString());
                 while (itr.hasMoreTokens()) {
                     word.set(itr.nextToken());
                     // count what letters this string has?
                     int[] letter_Array = new int[26];
                     String str_word = word.toString();
                     int max = 0;
                     for (char c : str_word.toCharArray()) {
                         if (c-'a' >= letter_Array.length || c-'a' < 0)
                             continue;
                         letter_Array[c-'a']+=1;
                     }
                     int i = 0;
                     String str_bitmap="";
                     while (i < 26) {
                         if (letter_Array[i]>0) {
                             str_bitmap += (char)('a'+i);
                             str_bitmap += Integer.toString(letter_Array[i]);
                         }
                         i++;
                     }
                     context.write(new Text(str_bitmap), word);
                 }
             }
    }

    public static class ClusterMapper    // for read in of the sorter
             extends Mapper<Object, Text, LongWritable, Text>{

             public void map(Object key, Text value, Context context
                             ) throws IOException, InterruptedException {
                 StringTokenizer itr = new StringTokenizer(value.toString());
                 long num = 0;
                 while (itr.hasMoreTokens()) {
                     itr.nextToken();
                     num += 1;
                 }
                 context.write(new LongWritable(num), value);
             }
    }

    public static class SameBitmapReducer    // for cluster all Anagrams
             extends Reducer<Text, Text, Text, Text> {

             public void reduce(Text key, Iterable<Text> values, Context context
                                ) throws IOException, InterruptedException {
                 String all_words = "";
                 for (Text word : values) {
                     all_words += word.toString();
                     all_words += " ";
                 }
                 //context.write(new IntWritable(num), new Text(all_words));
                 context.write(new Text(""), new Text(all_words));
             }
    }

    public static class OutputReducer    // for output
             extends Reducer<LongWritable, Text, Text, Text> {
             
             public void reduce(LongWritable key, Iterable<Text> values, Context context
                                ) throws IOException, InterruptedException {
                 for (Text word : values) {
                     context.write(new Text(""), word);
                 }
             }
    }

    public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
           
            // delete some files
            FileSystem hdfs = FileSystem.get(conf);
            Path output = new Path(args[1]);
            if (hdfs.exists(output)) {
                  hdfs.delete(output, true);
            }
            Path tmp_output = new Path("/tmp_output");
            if (hdfs.exists(tmp_output)) {
                  hdfs.delete(tmp_output, true);
            }
            // Job1: cluster all anagrams
            Job job = Job.getInstance(conf, "AnagramCluster");
            job.setJarByClass(AnagramSorter.class);
            
            job.setMapperClass(TokenizerMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);


            job.setReducerClass(SameBitmapReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextInputFormat.addInputPath(job, new Path(args[0]));
            TextOutputFormat.setOutputPath(job, tmp_output);

            job.waitForCompletion(true);

            // job2: sort the clusters by the number of members
            Job job2 = Job.getInstance(conf, "AnagramSorter");
            job2.setJarByClass(AnagramSorter.class);
            job2.setNumReduceTasks(1);  //to get total sorted list

            job2.setMapperClass(ClusterMapper.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            
            job2.setReducerClass(OutputReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            TextInputFormat.addInputPath(job2, tmp_output);
            TextOutputFormat.setOutputPath(job2, output);
            
            job2.waitForCompletion(true);
            if (hdfs.exists(tmp_output)) {
                  hdfs.delete(tmp_output, true);
            }
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
}
