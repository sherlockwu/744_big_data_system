
package org.myorg.quickstart;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.stream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.windowing.*;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.functions.timestamps.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.triggers.*;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;
/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/quickstart-0.1.jar
 * From the CLI you can then run
 * 		./bin/flink run -c org.myorg.quickstart.StreamingJob target/quickstart-0.1.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class joint {
 
  public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // set data source
		System.out.println("\n=== Streaming with joint window \n");
    DataStream<Tuple4<Integer, Integer, Long, String>> ds = env.addSource(new DataSource());
		// count each window
    //DataStream<Tuple3<String, Long, Integer>> windows = ds
    DataStream<String> windows = ds
          .map( new MapFunction<Tuple4<Integer, Integer, Long, String>, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(Tuple4<Integer, Integer, Long, String> value) throws Exception {
              return new Tuple3<String, Long, Integer>(value.f3, value.f2*1000, 1);
            }
          })
          .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Integer>>() {
              @Override
              public long extractAscendingTimestamp(Tuple3<String, Long, Integer> element) {
                return element.f1;
              }
          })
          .keyBy(0)
          .timeWindow(Time.seconds(60), Time.seconds(1))
          //SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(1)))
          .apply( new MyWindowFunction() )
          ; 
    windows.writeAsText("hdfs://10.254.0.136:8020/C_Q1_joint.output").setParallelism(1);

		// execute program
		env.execute("Streaming with joint window");
	  System.out.println("\n=== Get here! ");
  }

  public static class MyWindowFunction implements WindowFunction<Tuple3<String, Long, Integer>, String, Tuple, TimeWindow> {
    public void apply
      (Tuple key, TimeWindow window, Iterable<Tuple3<String, Long, Integer>> input, Collector<String> out) {
      Integer count = 0;
      for (Tuple3<String, Long, Integer> in: input) {
        count++;
      }
      if (count > 100)
        out.collect("TimeWindow{start=" + window.getStart() + ", end=" + window.getEnd() + "} Count: "+ String.valueOf(count) + " Type: " + key.toString() );
    }
  }
  /**
 * streaming simulation part
 **/
  private static class DataSource extends RichSourceFunction<Tuple4<Integer, Integer, Long, String>> {
    
    private volatile boolean running = true;
    private final String filename = "/home/ubuntu/higgs-activity_time.txt";

    private DataSource() {
      System.out.println("\n===Add stream input===\n");
    }

    public static DataSource create() {
      return new DataSource();
    }

    @Override
    public void run(SourceContext<Tuple4<Integer, Integer, Long, String>> ctx) throws Exception {
      System.out.println("\n===Start generating stream input===\n");

      try{
        final File file = new File(filename);
        final BufferedReader br = new BufferedReader(new FileReader(file));

        String line = "";

        System.out.println("\n===Start read data from \"" + filename + "\"\n");
        long count = 0L;
        while(running && (line = br.readLine()) != null) {
          if ((count++) % 10 == 0) {
            Thread.sleep(1);
          }
          ctx.collect(genTuple(line));
        }

      } catch (Exception e) {
        System.out.println("\n=== facing some errors\n");
        e.printStackTrace();
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    private Tuple4<Integer, Integer, Long, String> genTuple(String line) {
      String[] item = line.split(" ");
      Tuple4<Integer, Integer, Long, String> record = new Tuple4<>();

      record.setField(Integer.parseInt(item[0]), 0);
      record.setField(Integer.parseInt(item[1]), 1);
      record.setField(Long.parseLong(item[2]), 2);
      record.setField(item[3], 3);

      return record;
    }
  }
	
}
