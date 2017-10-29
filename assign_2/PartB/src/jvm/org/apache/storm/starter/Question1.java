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

import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.AuthorizationException;
public class Question1 {   
    static private String consumerKey = "CBsVlpUwS2BM9HbaFOLNxQr5Z"; 
    static private String consumerSecret = "ajzZgzUU6lcuoq7bBSL0o9Yx151RLZ7TuzG9gIVgMNoKoyFs7P"; 
    static private String accessToken = "3033001998-UnxmpGGGVkcJxd10zxwdqTCQPvWgNP3vV1imfWU"; 
    static private String accessTokenSecret ="cI7Jm7wUMUcjskxa0HEy5DY4N8fVesGpQXOpeXsbHXhUW";
    static private StormSubmitter stormSubmitter;
    static private String TOPOLOGY_NAME = "Q1";
    public static void main(String[] args) {
        String[] arguments = {consumerKey, consumerSecret, accessToken, accessTokenSecret};

        String[] keyWords = getKeyWords();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret,accessToken, accessTokenSecret, keyWords));
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("twitter");

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
    

    private static String[] getKeyWords() {
        String[] allKeyWords = {"google", "facebook", "amazon", "oracle", "nvidia", "amd", "apple", "airbnb", "uber", "ebay"};
        return allKeyWords;
    }
}
