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
import java.util.List;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.FileWriter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.starter.tools.Rankings;
import org.apache.storm.starter.tools.Rankable;

public class PrintRankBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(PrinterBolt.class);
    private BufferedWriter bw ;
    private FileWriter fw;
    private int totalCount = 0;
    static private String FILENAME = "/home/ubuntu/q2_ranking.txt";
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
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
        Rankings rank = (Rankings) tuple.getValueByField("rankings");
        try {
            List<Rankable> rankableList = rank.getRankings();
            for(Rankable ele : rankableList) {
                bw.write((String) ele.getObject());
                bw.write("\n");
            }
            bw.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
