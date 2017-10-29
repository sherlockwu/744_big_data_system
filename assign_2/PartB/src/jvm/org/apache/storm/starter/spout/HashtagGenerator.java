package org.apache.storm.starter.spout;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Random;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class HashtagGenerator extends BaseRichSpout {
  private static final int LENGTH = 100;
  private static String[] hashtags = {
          "BTS", "7for7", "giveaway", "EXO", "Mersal", "Nowplaying", "RT", "job",
          "win", "HappyHalloween", "horror", "party", "costume", "ebay", "trickortreat",
          "pumpkin", "Halloween", "ghost", "china", "usa", "trump", "work", "family",
          "happy", "MAGA", "pizza", "piazza", "iphone", "X", "nba", "nfl", "sports", "russia",
          "mexico", "clubs", "fun", "dogs", "cats", "love", "tennis", "oct.", "october", "nov.",
          "november", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"

  };
  SpoutOutputCollector _collector;
  LinkedBlockingQueue<Status> queue = null;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    queue = new LinkedBlockingQueue<Status>(1000);
    _collector = collector;
  }

  @Override
  public void nextTuple() {
      String[] current = getHashTags();
      List currentList = new ArrayList();
      Collections.addAll(currentList, current);

      Random rand = new Random();
      int idx = rand.nextInt(currentList.size());
      currentList.remove(idx);

      this._collector.emit(new Values(currentList));

      Utils.sleep(10000);
  }

  @Override
  public void close() {
  
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config ret = new Config();
    ret.setMaxTaskParallelism(1);
    return ret;
  }

  @Override
  public void ack(Object id) {}

  @Override
  public void fail(Object id) {}

  @Override 
  public void declareOutputFields(OutputFieldsDeclarer declarer){
    declarer.declare(new Fields("tags"));
  }

  private String[] getHashTags() {
    return this.hashtags;
  }

}
