package org.wso2.storm.benchmarks.emailprocessor.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.uebercomputing.mailrecord.MailRecord;
import org.apache.log4j.Logger;
import org.wso2.storm.benchmarks.emailprocessor.spout.EmailSpout;

import javax.mail.internet.MimeUtility;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;

/**
 * Created by miyurud on 5/14/15.
 */
public class FilterBolt extends BaseBasicBolt {
    private static transient Logger log = Logger.getLogger(EmailSpout.class);
    private String ENRON_DOMAIN = "enron.com";
    private static Splitter splitter = Splitter.on(',');

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("iij_timestamp", "from", "to", "cc", "bcc", "subject", "body"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Values vt = process(tuple);
        if(vt != null) {
            basicOutputCollector.emit(vt);
        }
    }

    public Values process(Tuple tuple) {
        String fromAddress = tuple.getStringByField("from").toString();
        String to = tuple.getStringByField("to").toString();
        String cc = tuple.getStringByField("cc").toString();
        String bcc = tuple.getStringByField("bcc").toString();

        //-------- task 1 ----------------------------------------------------------------------------------------------
        //Drop emails that did not originate within Enron, i.e., email addresses that do not end
        //with @enron.com or with @enron.com>
        if(!(fromAddress.endsWith(ENRON_DOMAIN))){
            return null;
        }

        //-------- task 2 ----------------------------------------------------------------------------------------------
        //remove all
        //email addresses that do not end with @enron.com from the To, CC, and BCC fields.

        //To addresses
        if(to != null){
            Iterator<String> toAddressIterator = splitter.split(to).iterator();

            StringBuilder toAddressesListNew = new StringBuilder();
            String item = null;

            //We have to iterate through the To email addresses and see whether those contain any email address which is
            //outside the "enron.com" domain. If so we have to remove that email address.

            while (toAddressIterator.hasNext()) {
                item = toAddressIterator.next().trim();

                if (item.endsWith(ENRON_DOMAIN)) {
                    toAddressesListNew.append(item);
                }
            }

            to = toAddressesListNew.toString();
        }

        //CC addresses
        if(cc != null){
            Iterator<String> ccAddressIterator = splitter.split(cc).iterator();

            StringBuilder ccAddressesListNew = new StringBuilder();
            String item = null;

            while (ccAddressIterator.hasNext()) {
                item = ccAddressIterator.next();

                if ((((String) item).endsWith(ENRON_DOMAIN))) {
                    ccAddressesListNew.append(item.toString());
                }
            }

            cc = ccAddressesListNew.toString();
        }

        //BCC addresses
        if(bcc != null){
            StringBuilder bccAddressesListNew = new StringBuilder();
            Iterator<String> bccAddressIterator = splitter.split(bcc).iterator();

            String item = null;

            while (bccAddressIterator.hasNext()) {
                item = bccAddressIterator.next();

                if ((((String) item).endsWith(ENRON_DOMAIN))) {
                    bccAddressesListNew.append(item.toString());
                }
            }

            bcc = bccAddressesListNew.toString();
        }

        //-------- task 3 ----------------------------------------------------------------------------------------------
        //we need to remove rogue formatting such as dangling newline characters
        //and MIME quoted-printable characters
        //from the email body to restrict the character set to simple ASCII
        String body = tuple.getStringByField("body").toString();
        //remove dangling new lines
        body = body.trim();

        //Remove any MIME encoded text.
        try {
            body = MimeUtility.decodeText(body);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new Values(tuple.getLong(0), fromAddress, to, cc, bcc, tuple.getStringByField("subject"), body);
    }
}
