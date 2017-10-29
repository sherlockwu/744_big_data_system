/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.starter;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.apache.storm.StormSubmitter;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;

import org.apache.storm.tuple.Fields;
import org.apache.storm.starter.bolt.IntermediateRankingsBolt;
import org.apache.storm.starter.bolt.RollingCountBolt;
import org.apache.storm.starter.bolt.TotalRankingsBolt;
import org.apache.storm.starter.bolt.TweetsPrinterBolt;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.bolt.PrintRankBolt;
import org.apache.storm.starter.spout.HashtagGenerator;
import org.apache.storm.starter.spout.ThresholdGenerator;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.AuthorizationException;

public class Question2 {   
    static private String consumerKey = "CBsVlpUwS2BM9HbaFOLNxQr5Z"; 
    static private String consumerSecret = "ajzZgzUU6lcuoq7bBSL0o9Yx151RLZ7TuzG9gIVgMNoKoyFs7P"; 
    static private String accessToken = "3033001998-UnxmpGGGVkcJxd10zxwdqTCQPvWgNP3vV1imfWU"; 
    static private String accessTokenSecret ="cI7Jm7wUMUcjskxa0HEy5DY4N8fVesGpQXOpeXsbHXhUW";
    static private StormSubmitter stormSubmitter;
    static private String TOPOLOGY_NAME = "Q2";
    static private String intermediateRanker = "intermediateRanker";
    static private String totalRanker = "totalRanker";
    static private String twitter = "twitter";
    static private String hashtag = "hashtag";
    static private String threshold = "threshold";
    static private String counter = "counter";
    static private String rankPrinter = "rankPrinter";
    private static String printTweets = "printTweets";
    static private final int TOP_N = 10;
    public static void main(String[] args) {
        String[] arguments = {consumerKey, consumerSecret, accessToken, accessTokenSecret};
        String[] keyWords = {};
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(twitter, new TwitterSampleSpout(consumerKey, consumerSecret,accessToken, accessTokenSecret, keyWords));
        builder.setSpout(hashtag, new HashtagGenerator());
        builder.setSpout(threshold, new ThresholdGenerator());
        builder.setBolt(printTweets, new TweetsPrinterBolt()).shuffleGrouping(twitter).shuffleGrouping(hashtag).shuffleGrouping(threshold);
        builder.setBolt(counter, new RollingCountBolt(30, 10), 4).fieldsGrouping(printTweets, new Fields("word"));
        builder.setBolt(intermediateRanker, new IntermediateRankingsBolt(50)).fieldsGrouping(counter, new Fields("obj"));
        builder.setBolt(totalRanker, new TotalRankingsBolt(50)).globalGrouping(intermediateRanker);
        builder.setBolt(rankPrinter, new PrintRankBolt()).globalGrouping(totalRanker);
        if(args[1].equals("local")) {
          run_local(builder, keyWords);
        }
        else{
          run_cluster(builder, keyWords);
        }
    }    
    
    static private void run_cluster(TopologyBuilder builder, String[] keyWords) {
      Config conf = new Config();
      conf.setNumWorkers(5);
      conf.setMaxSpoutPending(5000);
      try{
        stormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
      }
      catch (Exception e){}
    }

    static private void run_local(TopologyBuilder builder, String[] keyWords) {
                
        Config conf = new Config();
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
    }
}
