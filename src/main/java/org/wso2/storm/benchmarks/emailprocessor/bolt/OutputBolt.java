package org.wso2.storm.benchmarks.emailprocessor.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Splitter;
import com.uebercomputing.mailrecord.MailRecord;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.apache.log4j.Logger;
import org.joda.time.Instant;
import org.wso2.storm.benchmarks.emailprocessor.util.PerfStats;

import java.io.*;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Created by miyurud on 5/14/15.
 *
 * There will be only one Output Bolt on the topology.
 */

public class OutputBolt extends BaseBasicBolt {
    private static transient Logger log = Logger.getLogger(OutputBolt.class);
    private OutputCollector _collector;
    private long emailCounter;
    private long wordCounter;
    private long characterCounter;
    //private String AVRO_SCHEMA_FILE = "/home/miyurud/Projects/CEPStormPerf/EmailProcessorBenchmark/Avro/enron.avsc";
    private String AVRO_SCHEMA_FILE = "/home/cep/miyurud/data/enron.avsc";
    private String OUTPUT_FILE_PATH = "/dev/null";
    private transient DataFileWriter<MailRecord> writer;
    private transient GenericDatumWriter<MailRecord> gdw;
    private transient BinaryEncoder be;
    private transient FileOutputStream fout;
    private transient ByteArrayOutputStream out;
    private transient ObjectOutputStream oos;
    private static Splitter splitter = Splitter.on(',');
    private long firstTupleTime = -1;
    private transient PerfStats perfStats = new PerfStats();
    private String logDir = "/home/cep/miyurud/tmp";
    private FileWriter fw = null;
    private BufferedWriter bw = null;
    private StringBuilder stringBuilder = new StringBuilder();
    private static String COMMA = ",";
    private static String CARRIAGERETURN_NEWLINE = "\r\n";
    private static int PERFORMANCE_RECORDING_WINDOW = 10000; //This is the number of events to record.
    private long currentTime = 0l;
    private long differenceFromNTP = 0l; //This is the time drift from the original NTP server.
    private long timeOfFirstEventInjection = 0l;
    private int NITR = 10;

    private void init() {
        perfStats = new PerfStats();
        differenceFromNTP = getAverageTimeDifference(NITR);
        try {
            Schema sch=Schema.parse(new File(AVRO_SCHEMA_FILE));

            //out=new ByteArrayOutputStream();
            fout = new FileOutputStream(new File(OUTPUT_FILE_PATH));
            GZIPOutputStream gz = new GZIPOutputStream(fout);
            oos = new ObjectOutputStream(gz);

            DatumWriter<MailRecord> writer1 = new GenericDatumWriter<MailRecord>(sch);

            //be= EncoderFactory.get().directBinaryEncoder(out, null);
            gdw = new GenericDatumWriter<MailRecord>();
            writer = new DataFileWriter<MailRecord>(gdw).create(sch, new File(OUTPUT_FILE_PATH));

            fw = new FileWriter(new File(logDir + "/output-2-" + getLogFileSequenceNumber() + "-" + (System.currentTimeMillis()+differenceFromNTP) + ".csv").getAbsoluteFile());
            bw = new BufferedWriter(fw);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private org.joda.time.DateTime getNTPDate() {
        String[] hosts = new String[]{ "3.sg.pool.ntp.org", "0.jp.pool.ntp.org", "1.jp.pool.ntp.org"};

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

    /**
     * This method returns a unique integer that can be used as a sequence number for log files.
     */
    private int getLogFileSequenceNumber(){
        int result = -1;
        BufferedReader br = null;

        //read the flag
        try {
            String sCurrentLine;
            br = new BufferedReader(new FileReader(logDir + "/sequence-number.txt"));

            while ((sCurrentLine = br.readLine()) != null) {
                result = Integer.parseInt(sCurrentLine);
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null){
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        //write the new flag
        try {
            if(result == -1){
                result = 0;
            }

            String content = "" + (result+1);//need to increment by one for next round
            File file = new File(logDir + "/sequence-number.txt");

            // if file doesn't exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        init();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        MailRecord email = new MailRecord();
        email.setFrom(tuple.getStringByField("from"));

        String to = tuple.getStringByField("to");
        Iterator<String> toAddressIterator = splitter.split(to).iterator();
        List<CharSequence> toLst = new LinkedList<CharSequence>();

        while (toAddressIterator.hasNext()) {
            toLst.add(toAddressIterator.next());
        }
        email.setTo(toLst);

        String cc = tuple.getStringByField("cc");
        Iterator<String> ccAddressIterator = splitter.split(cc).iterator();
        List<CharSequence> ccLst = new LinkedList<CharSequence>();

        while (ccAddressIterator.hasNext()) {
            ccLst.add(ccAddressIterator.next());
        }
        email.setTo(ccLst);

        String bcc = tuple.getStringByField("bcc");
        Iterator<String> bccAddressIterator = splitter.split(bcc).iterator();
        List<CharSequence> bccLst = new LinkedList<CharSequence>();

        while (bccAddressIterator.hasNext()) {
            bccLst.add(bccAddressIterator.next());
        }
        email.setTo(bccLst);

        email.setSubject(tuple.getStringByField("subject"));
        String body = tuple.getStringByField("body");
        email.setBody(body);
        email.setUuid("");

        //String body = "0";//This is just for the moment
        currentTime = System.currentTimeMillis() + differenceFromNTP;
        if (firstTupleTime == -1) {
            firstTupleTime = currentTime;
        }

        //perfStats.lastEventTime = currentTime;
        long eventOriginationTime = tuple.getLongByField("iij_timestamp");

        //If the event origination time becomes -1, that means we should be done with processing all the events.
        long latency = -1;
        if (eventOriginationTime == -1l) {
            //The following time difference is measured from the output side
            long timeDifferenceFromStart = perfStats.lastEventTime - firstTupleTime; //This first tuple is the very first
            // tuple received at the output bolt. But its not necessarily the first one injected to the topology.
            //Because the very first one injected to the topology may arrive few tuples later. Thats one of the reasons why
            // we need the following time difference. The second reason is there is inherently a delay involved in processing any tuple,
            //so we need to get the time from the place where the tuple was injected.
            long timeDifferenceForEntireExecution = perfStats.lastEventTime - timeOfFirstEventInjection;

            System.out.println("timeDifferenceFromStart:"+timeDifferenceFromStart);
            System.out.println("timeDifferenceForEntireExecution:"+timeDifferenceForEntireExecution);

            try {
                bw.write("throughput (events/second)" + COMMA + "Output data rate(events/second)" + COMMA + "Total elapsed time(s)" + COMMA + "average latency per event(s)" + COMMA + "total number of events sent" + COMMA + "total number of events received (non-atomic)" + COMMA + "total number of events received (atomic)" + CARRIAGERETURN_NEWLINE);
                bw.write((Integer.parseInt(body) * 1000 / timeDifferenceForEntireExecution) + COMMA + (perfStats.count * 1000 / timeDifferenceFromStart) + COMMA + (timeDifferenceForEntireExecution / 1000) + COMMA + (perfStats.totalLatency * 1.0f / (perfStats.count * 1000)) + COMMA + body + COMMA + perfStats.count + COMMA + perfStats.atomicInt.intValue() + CARRIAGERETURN_NEWLINE);
                bw.flush();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                stringBuilder.setLength(0);
            }
        } else if (eventOriginationTime == -2l){
            timeOfFirstEventInjection = Long.parseLong(body);
            currentTime = System.currentTimeMillis() + differenceFromNTP;
            perfStats.lastEventTime = currentTime;
        }else{
            currentTime = System.currentTimeMillis() + differenceFromNTP;
            perfStats.lastEventTime = currentTime;
            latency = currentTime - eventOriginationTime;
            perfStats.totalLatency += latency;
            perfStats.count++;//We take all the events except the first and the last sent
            perfStats.atomicInt.incrementAndGet();
        }

        /*
        try {
            out=new ByteArrayOutputStream();
            be= EncoderFactory.get().directBinaryEncoder(out, be);
            gdw.write(email, be);
            be.flush();
            out.flush();
            oos.write(out.toByteArray());
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */
    }

    /**
     * This is not the best way of doing this.
     */
    public void finalize(){
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}