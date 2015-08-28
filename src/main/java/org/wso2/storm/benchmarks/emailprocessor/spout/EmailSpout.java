package org.wso2.storm.benchmarks.emailprocessor.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.uebercomputing.mailrecord.MailRecord;
import org.apache.log4j.Logger;
import org.wso2.storm.benchmarks.emailprocessor.util.Constants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;

import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.joda.time.Interval;
import org.joda.time.Instant;


/**
 * Created by miyurud on 5/8/15.
 */
public class EmailSpout extends BaseRichSpout {
    private static transient Logger log = Logger.getLogger(EmailSpout.class);
    private String executionPlanName;
    private int tenantId = -5678;
    private String logPrefix;
    private int eventCount;
    private long batchStartTime;
    private int heartbeatInterval;
    private SpoutOutputCollector spoutOutputCollector = null;
    //private static String filePath = "/home/miyurud/Projects/CEPStormPerf/EmailProcessorBenchmark/datasets/Enron/Avro/enron.avro";
    private static String filePath = "/home/cep/miyurud/data/enron.avro";
    private static long startTime;
    private transient DataFileReader<MailRecord> dataFileReader;
    private boolean firstItemFlag = true;
    private boolean lastItemFlag = false;
    private long differenceFromNTP = 0l; //This is the time drift from the original NTP server.
    private long firstOccurenceOfLastTuple = 0l;
    private int NITR = 10;

    /**
     * Store received events until nextTuple is called. This list has to be synchronized since
     * this is filled by the receiver thread of data bridge and consumed by the nextTuple which
     * runs on the worker thread of spout.
     */
    private ConcurrentLinkedQueue storedEvents = null;

    public EmailSpout(){
        this.logPrefix = "[" + tenantId + ":" + executionPlanName + ":" + "Event Receiver Spout" + "]";
        storedEvents = new ConcurrentLinkedQueue();
        differenceFromNTP = getAverageTimeDifference(NITR);
        System.out.println("differenceFromNTP : " + differenceFromNTP);
    }

    private org.joda.time.DateTime getNTPDate() {
        String[] hosts = new String[]{ "0.jp.pool.ntp.org", "1.jp.pool.ntp.org", "3.sg.pool.ntp.org"};

        NTPUDPClient client = new NTPUDPClient();
        // We want to timeout if a response takes longer than 5 seconds
        client.setDefaultTimeout(5000);

        for (String host : hosts) {

            try {
                InetAddress hostAddr = InetAddress.getByName(host);
                //System.out.println("> " + hostAddr.getHostName() + "/" + hostAddr.getHostAddress());
                TimeInfo info = client.getTime(hostAddr);
                org.joda.time.DateTime date = new org.joda.time.DateTime(info.getReturnTime());
                return date;

            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        client.close();

        return null;
    }

    /**
     * This is the average time that needs to be added to the local time to make it synchronized with the NTP time.
     * @param nitr
     * @return
     */
    private long getAverageTimeDifference(int nitr) {
        long result = 0;
        long ntp = 0l;
        long local = 0l;

        for(int i = 0; i < nitr; i++) {
            ntp = getNTPDate().getMillis();
            local = new Instant().getMillis();

            result+=(ntp-local);
            //result += new Interval(getNTPDate().getMillis(), new Instant().getMillis()).toPeriod().getMillis();
        }

        return (result/nitr);
    }

    private void initiateDataLoading() {
        try {
            DatumReader<MailRecord> userDatumReader = new SpecificDatumReader<MailRecord>(MailRecord.class);
            dataFileReader = new DataFileReader<MailRecord>(new File(filePath), userDatumReader);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        spoutOutputCollector = collector;
    }

    @Override
    public void nextTuple() {
        //Note that loading the email data separately to a LinkedBlockingQueue and then polling the queue for the events
        //does not work. Its because the LinkedBlockingQueue will be full with data and we get memory out of error.
        if(dataFileReader == null){
            initiateDataLoading();
        }

        //The following statement makes the code to iterate over the data set infinitely.
        if(!dataFileReader.hasNext()){
            if(!lastItemFlag) {
                long curTime = System.currentTimeMillis();
                if(firstOccurenceOfLastTuple == 0){
                    firstOccurenceOfLastTuple = curTime;
                }else if((curTime -firstOccurenceOfLastTuple) > 20000){
                    lastItemFlag = true;
                    spoutOutputCollector.emit(new Values(-1l, "miyurud@enron.com", "miyurud@enron.com", "miyurud@enron.com", "miyurud@enron.com", "final-tuple", ""+eventCount));
                }
            }

            return;
        }

        //The very first event injected will be a synthetic one which will help to determine the total end-to-end elapsed time.
        if(firstItemFlag){
            firstItemFlag = false;

            spoutOutputCollector.emit(new Values(-2l, "miyurud@enron.com", "miyurud@enron.com", "miyurud@enron.com", "miyurud@enron.com", "final-tuple", ""+Long.toString(System.currentTimeMillis() + differenceFromNTP)));
            return;
        }

        MailRecord email = dataFileReader.next();
        long cTime = System.currentTimeMillis() + differenceFromNTP;
        ArrayList fromAddresses = new ArrayList();

        Iterator<CharSequence> itr = null;
        StringBuilder sb = new StringBuilder();

        final List<CharSequence> to = email.getTo();
        if(to != null) {
            itr = to.iterator();

            while (itr.hasNext()) {
                sb.append(itr.next());
                if(itr.hasNext()){
                    sb.append(",");
                }
            }
        }

        String toAddresses = sb.toString();
        sb = new StringBuilder();

        final List<CharSequence> cc = email.getCc();
        if(cc != null) {
            itr = cc.iterator();

            while (itr.hasNext()) {
                sb.append(itr.next());
                if(itr.hasNext()){
                    sb.append(",");
                }
            }
        }

        String ccAddresses = sb.toString();
        sb = new StringBuilder();

        final List<CharSequence> bcc = email.getBcc();
        if(bcc != null) {
            itr = bcc.iterator();

            while (itr.hasNext()) {
                sb.append(itr.next());
                if(itr.hasNext()){
                    sb.append(",");
                }
            }
        }

        String bccAddresses = sb.toString();
        String subject = email.getSubject().toString();
        String body = email.getBody().toString();

        eventCount++;
        spoutOutputCollector.emit(new Values(cTime, email.getFrom(), toAddresses, ccAddresses, bccAddresses, subject, body), cTime);
    }

    @Override
    public void ack(Object id) {

    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("iij_timestamp", "from", "to", "cc", "bcc", "subject", "body"));
    }
}
