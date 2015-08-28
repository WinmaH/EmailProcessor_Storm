package org.wso2.storm.benchmarks.emailprocessor.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Splitter;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by miyurud on 5/14/15.
 */
public class GlobalMetricsBolt extends BaseBasicBolt {
    private static transient Logger log = Logger.getLogger(GlobalMetricsBolt.class);
    private long emailCounter;
    private long characterCounter;
    private long wordCounter;
    private long paragraphCounter;
    private int LOGGING_WINDOW_IN_MS=5000;//This is in miliseconds
    private File metricsLogFile;
    private String LOG_FILE_PATH = "/home/cep/miyurud/tmp/email-metrics";
    private BufferedWriter bw;
    private long prevTimeStamp;
    private static String COMMA = ",";
    private long startTime = 0;
    private boolean firstFlag = true;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        java.util.Date date= new java.util.Date();
        String tt = (new Timestamp(date.getTime())).toString().replace(' ', '-');
        metricsLogFile = new File(LOG_FILE_PATH + "-" + tt + ".txt");

        try {
            if (!metricsLogFile.exists()) {
                metricsLogFile.createNewFile();
            }

            FileWriter fw = new FileWriter(metricsLogFile.getAbsoluteFile());
            bw = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
        }

        prevTimeStamp = System.currentTimeMillis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        long currTime = System.currentTimeMillis();

        if(firstFlag){
            firstFlag = false;
            startTime = currTime;
        }

        //Count the total number of emails
        emailCounter++;

        characterCounter += tuple.getLongByField("char_count");
        wordCounter += tuple.getLongByField("word_count");
        paragraphCounter += tuple.getLongByField("para_count");

        if((currTime - prevTimeStamp) > LOGGING_WINDOW_IN_MS){
            try {
                bw.write((currTime - startTime) + COMMA + emailCounter + COMMA + characterCounter + COMMA + wordCounter + COMMA + paragraphCounter);
                bw.newLine();
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            prevTimeStamp = currTime;
        }
    }
}
