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
package org.apache.storm.starter.bolt;

import java.util.Map;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Arrays;
import twitter4j.HashtagEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import org.apache.storm.tuple.Fields;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus.Client;

public class TweetsPrinterBolt extends BaseRichBolt {
  private static final Logger logger = LoggerFactory.getLogger(PrinterBolt.class); 
  private BufferedWriter bw ;
  private FileWriter fw;
  private int totalCount = 0;
  private String[] hashtags;
  private Set<String> tagsSet;
  private int threshold;
  private static Set<String> stopWords = new HashSet<String>(Arrays.asList(
    new String[]{
          "a",
          "about",
          "above",
          "after",
          "again",
          "against",
          "all",
          "am",
          "an",
          "and",
          "any",
          "are",
          "aren't",
          "as",
          "at",
          "be",
          "because",
          "been",
          "before",
          "being",
          "below",
          "between",
          "both",
          "but",
          "by",
          "can't",
          "cannot",
          "could",
          "couldn't",
          "did",
          "didn't",
          "do",
          "does",
          "doesn't",
          "doing",
          "don't",
          "down",
          "during",
          "each",
          "few",
          "for",
          "from",
          "further",
          "had",
          "hadn't",
          "has",
          "hasn't",
          "have",
          "haven't",
          "having",
          "he",
          "he'd",
          "he'll",
          "he's",
          "her",
          "here",
          "here's",
          "hers",
          "herself",
          "him",
          "himself",
          "his",
          "how",
          "how's",
          "i",
          "i'd",
          "i'll",
          "i'm",
          "i've",
          "if",
          "in",
          "into",
          "is",
          "isn't",
          "it",
          "it's",
          "its",
          "itself",
          "let's",
          "me",
          "more",
          "most",
          "mustn't",
          "my",
          "myself",
          "no",
          "nor",
          "not",
          "of",
          "off",
          "on",
          "once",
          "only",
          "or",
          "other",
          "ought",
          "our",
          "our",
          "ourselves",
          "out",
          "over",
          "own",
          "same",
          "shan't",
          "she",
          "she'd",
          "she'll",
          "she's",
          "should",
          "shouldn't",
          "so",
          "some",
          "such",
          "than",
          "that",
          "that's",
          "the",
          "their",
          "theirs",
          "them",
          "themselves",
          "then",
          "there",
          "there's",
          "these",
          "they",
          "they'd",
          "they'll",
          "they're",
          "they've",
          "this",
          "those",
          "through",
          "to",
          "too",
          "under",
          "until",
          "up",
          "very",
          "was",
          "wasn't",
          "we",
          "we'd",
          "we'll",
          "we're",
          "we've",
          "were",
          "weren't",
          "what",
          "what's",
          "when",
          "when's",
          "where",
          "where's",
          "which",
          "while",
          "who",
          "who's",
          "whom",
          "why",
          "why's",
          "with",
          "won't",
          "would",
          "wouldn't",
          "you",
          "you'd",
          "you'll",
          "you're",
          "you've",
          "your",
          "yours",
          "yourself",
          "yourselves"
          }));
  static final int MAX_COUNT = 200000;
  private OutputCollector collector;
  static final String TOPOLOGY_NAME = "Q2";
  static private String FILENAME = "/home/ubuntu/q2_tweets.txt";

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.threshold = 1;
    this.collector = collector;
    try{
			fw = new FileWriter(FILENAME);
			bw = new BufferedWriter(fw);
    }
    catch(IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void execute(Tuple tuple) {
    if(totalCount > MAX_COUNT) {
        Map conf = Utils.readStormConfig();
        Client client = NimbusClient.getConfiguredClient(conf).getClient();
        KillOptions killOpts = new KillOptions();
        //killOpts.set_wait_secs(waitSeconds); // time to wait before killing
        try {
          client.killTopologyWithOpts(TOPOLOGY_NAME, killOpts); //provide topology name
        }
        catch(Exception e){
        
        }
        return;
    }
    if(tuple.getSourceComponent().equalsIgnoreCase("twitter")) {
      Status status = (Status) tuple.getValueByField("tweet");
      try {
        if(!status.getLang().equals("en"))
          return;
        else if(!belongToHashtags(status)) {
          return;
        }
        else if(status.getFavoriteCount() >= this.threshold) {
          return;
        }
        String currentText = status.getText();
        bw.write(currentText);
        bw.flush();
        String [] words = currentText.split(" ");
        for(String word : words) {
          if(stopWords.contains(word.toLowerCase())) continue;
          this.collector.emit(new Values(word));
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      totalCount++;
    }
    else if(tuple.getSourceComponent().equalsIgnoreCase("hashtag")) {
      List<String> hashtags = (List<String>) tuple.getValueByField("tags");
      this.tagsSet = new HashSet(hashtags);
      //this.tagsSet = new HashSet(Arrays.asList(this.hashtags));
    }
    else if(tuple.getSourceComponent().equalsIgnoreCase("threshold")) {
      this.threshold = (Integer) tuple.getValueByField("threshold");
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
      ofd.declare(new Fields("word"));
  }

  private boolean belongToHashtags(Status status) {
    HashtagEntity[] currentTags = status.getHashtagEntities();
    boolean exist = false;
    for(HashtagEntity entity : currentTags) {
      if(tagsSet.contains(entity.getText())) {
        exist = true;
        break;
      }
    }
    return exist;
  }

}
