package org.wso2.storm.benchmarks.emailprocessor.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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
public class MetricsBolt extends BaseBasicBolt {
    private static transient Logger log = Logger.getLogger(MetricsBolt.class);
    private long emailCounter;
    private int LOGGING_WINDOW=1000;
    private File metricsLogFile;
    //private String LOG_FILE_PATH = "/home/miyurud/Projects/CEPStormPerf/tmp/email-metrics";
    private String LOG_FILE_PATH = "/home/cep/miyurud/tmp";
    private BufferedWriter bw;

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
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declareStream("off-path", new Fields("char_count", "word_count", "para_count"));
        outputFieldsDeclarer.declareStream("path", new Fields("iij_timestamp", "from", "to", "cc", "bcc", "subject", "body", "char_count", "word_count", "para_count"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //Count the total number of emails
        //emailCounter++;

        long characterCounter = 0;
        long wordCounter = 0;
        long paragraphCounter = 0;

        String body = tuple.getStringByField("body").toString();

        //Count the number of words and characters
        Splitter splitter = Splitter.on(' ');
        Iterator<String> itr = splitter.split(body).iterator();
        String word = null;

        while (itr.hasNext()) {
            word = itr.next();

            //Note that we are not considering letter 'a' as a word.
            int numChars = word.length();

            if (numChars > 1) {
                wordCounter++;
            }

            characterCounter += numChars;
        }

        //Count the number of paragraphs. Here, we use the simple definition that a paragraph is defined as two new lines.
        //Therefore, even single line, we consider as a paragraph.
        splitter = Splitter.on("\n\n");
        itr = splitter.split(body).iterator();

        while(itr.hasNext()){
            itr.next();
            paragraphCounter++;
        }

        //basicOutputCollector.emit("off-path", new Values(characterCounter, wordCounter, paragraphCounter));
        //tuple.getLong(0), fromAddress, to, cc, bcc, tuple.getStringByField("subject"), body, "(.*)@enron.com"
        basicOutputCollector.emit("path", new Values(tuple.getLong(0), tuple.getStringByField("from"), tuple.getStringByField("to"),
                tuple.getStringByField("cc"), tuple.getStringByField("bcc"), tuple.getStringByField("subject"), tuple.getStringByField("body"), characterCounter, wordCounter, paragraphCounter));
    }
}
