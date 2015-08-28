package org.wso2.storm.benchmarks.emailprocessor;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.wso2.storm.benchmarks.emailprocessor.bolt.*;
import org.wso2.storm.benchmarks.emailprocessor.spout.EmailSpout;

/**
 * Created by miyurud on 5/8/15.
 */
public class EmailProcessorTopology {

    private static int PARALLELISM = 8;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //There will be only one spout. This spout deserializes the email tuples and creates the input stream
        builder.setSpout("spout", new EmailSpout(), 1);

        /*--------------- Parallelizable region -------------------*/
        builder.setBolt("filter", new FilterBolt(), PARALLELISM).shuffleGrouping("spout");
        builder.setBolt("modify", new ModifyBolt(), PARALLELISM).shuffleGrouping("filter");
        builder.setBolt("metrics", new MetricsBolt(), PARALLELISM).shuffleGrouping("modify");
        /*---------------------------------------------------------*/

        //We use global grouping here
        //path is the stream output by metrics bolt
        builder.setBolt("output", new OutputBolt(), 1).globalGrouping("metrics", "path");

        //parallelism of 1 and global grouping on the “off-path” stream
        //off-path stream is output by the metrics collector bolt
        //builder.setBolt("global-metrics", new GlobalMetricsBolt(), 1).globalGrouping("metrics", "off-path");

        Config conf = new Config();
        conf.setDebug(false);

        conf.setNumWorkers(4);

        StormSubmitter.submitTopologyWithProgressBar("EmailProcessor-Plain", conf, builder.createTopology());
        Utils.sleep(10000);
    }
}
