package org.wso2.storm.benchmarks.emailprocessor.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Splitter;
import org.apache.log4j.Logger;
import org.wso2.storm.benchmarks.emailprocessor.spout.EmailSpout;

import javax.mail.internet.MimeUtility;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by miyurud on 5/14/15.
 */
public class ModifyBolt extends BaseBasicBolt {
    private static transient Logger log = Logger.getLogger(ModifyBolt.class);
    private String[] keyPeopleArray = new String[]{"Kenneth Lay","Jeffrey Skilling","Andrew Fastow"};
    private String[] replacementNamesArray = new String[]{"Person1","Person2","Person3"};
    private final String NONE = "NONE";
    private final String COLON = ":";

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("iij_timestamp", "from", "to", "cc", "bcc", "subject", "body"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String body = tuple.getStringByField("body").toString();
        //-------- task 3 ----------------------------------------------------------------------------------------------
        //we need to remove rogue formatting such as dangling newline characters
        //and MIME quoted-printable characters
        //from the email body to restrict the character set to simple ASCII

        //remove dangling new lines
        body = body.trim();

        //Remove any MIME encoded text.
        try {
            body = MimeUtility.decodeText(body);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        //We have to convert all the letters in the email body to lowercase. Otherwise, the String.replace() method may
        //loose different caption combinations of the same word(s).

        body = body.toLowerCase();

        //First we have to find the three key people from the Enron email database and replace the three names with
        //representative names.
        int keyPeopleArrayLen = keyPeopleArray.length;

        for(int i=0; i<keyPeopleArrayLen; i++){
            body = body.replace(keyPeopleArray[i], replacementNamesArray[i]);
        }

        //Second we need to find the most frequent word and prepend it to the subject.
        String mostFrequentWord = getMostFrequentWord(body);
        String subject = tuple.getStringByField("subject").toString();
        subject = mostFrequentWord + COLON + subject;

        Values vt = new Values(tuple.getLong(0), tuple.getStringByField("from"), tuple.getStringByField("to"), tuple.getStringByField("cc"), tuple.getStringByField("bcc"), subject, body);
        basicOutputCollector.emit(vt);
    }

    private String getMostFrequentWord(String body){
        Splitter splitter = Splitter.on(' ');
        Iterator<String> dataStrIterator = splitter.split(body.toLowerCase()).iterator();
        HashMap<String, Integer> index = new HashMap<String, Integer>();
        String mostFrequentWord = NONE;
        int mostFrequentCount = 1;

        while(dataStrIterator.hasNext()){
            String word = dataStrIterator.next().trim();

            if(word.length() > 0) {
                Integer count = index.get(word);

                if (count == null) {
                    index.put(word, 1);
                } else {
                    ++count;
                    if( count.intValue() > mostFrequentCount) {
                        mostFrequentCount = count;
                        mostFrequentWord = word;
                    }
                    index.put(word, count);
                }
            }
        }

        return mostFrequentWord;
    }

    /*
    private String getMostFrequentWord(String body){
        Splitter splitter = Splitter.on(' ');
        Iterator<String> dataStrIterator = splitter.split(body).iterator();
        HashMap<String, Integer> index = new HashMap<String, Integer>();

        while(dataStrIterator.hasNext()){
            String word = dataStrIterator.next().toLowerCase().trim();

            if(word.length() > 0) {
                Integer count = index.get(word);

                if (count == null) {
                    index.put(word, 1);
                } else {
                    index.put(word, ++count);
                }
            }
        }

        Iterator<Map.Entry<String, Integer>> itr = index.entrySet().iterator();

        Map.Entry<String, Integer> mostFrequentItem = new Map.Entry<String, Integer>() {
            public String getKey() {
                return null;
            }

            public Integer getValue() {
                return 0;
            }

            public Integer setValue(Integer value) {
                return null;
            }
        };

        while(itr.hasNext()){
            Map.Entry<String, Integer> item = itr.next();
            if(item.getValue() > mostFrequentItem.getValue()){
                mostFrequentItem = item;
            }
        }

        if(mostFrequentItem.getValue() == 1){
            return NONE;
        }else if(mostFrequentItem.getKey().length() == 1) {
            return NONE;
        }else {
            return mostFrequentItem.getKey();
        }
    }
    */
}
