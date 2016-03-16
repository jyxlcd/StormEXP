package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
//import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.BasicOutputCollector;
//import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.topology.base.BaseBasicBolt;
//import backtype.storm.topology.base.BaseRichSpout;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.Map;
//import java.util.Random;
import storm.starter.bolt.CountBolt;
import storm.starter.bolt.IdentityBolt;
import storm.starter.spout.RandomWordGenSpoutWithMsgId;

public class ThroughputTest {
	public static void main(String[] args) throws Exception {
        /*
         * Args:
         * args[0]: topology name
         * args[1]: size of a val item in the spout
         * args[2]: # of workers
         * args[3]: # of spout executors
         * args[4]: # of bolt executors
         * args[5]: maxPending, not used currently
         */
		if (args.length<=4 || args.length >=6) {
			System.out.println("Incorrent number of input arguments!");
		    return;
		}
    	int size = Integer.parseInt(args[1]);
        int workers = Integer.parseInt(args[2]);
        int spout = Integer.parseInt(args[3]);
        int bolt = Integer.parseInt(args[4]);        
//        int maxPending = Integer.parseInt(args[5]);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",
        		new RandomWordGenSpoutWithMsgId(size),
        		spout);
//        builder.setBolt("count", new CountBolt(), bolt)
//                .fieldsGrouping("bolt", new Fields("id"));
        builder.setBolt("bolt", new IdentityBolt(), bolt)
                .shuffleGrouping("spout");
        builder.setBolt("bolt2", new IdentityBolt(), bolt)
        		.shuffleGrouping("bolt");
//        builder.setBolt("bolt3", new AckBolt(), bolt)
//               .shuffleGrouping("spout");
//        builder.setBolt("count3", new CountBolt(), bolt)
//                .fieldsGrouping("bolt2", new Fields("id"));
        builder.setBolt("count3", new CountBolt(), bolt)
        		.shuffleGrouping("bolt2");
        
        Config conf = new Config();
        conf.setNumWorkers(workers);
        //conf.setMaxSpoutPending(maxPending);
//        conf.setNumAckers(0);
//        conf.setStatsSampleRate(0.0001);
        //topology.executor.receive.buffer.size: 8192 #batched
        //topology.executor.send.buffer.size: 8192 #individual messages
        //topology.transfer.buffer.size: 1024 # batched
        
        //conf.put("topology.executor.send.buffer.size", 1024);
        //conf.put("topology.transfer.buffer.size", 8);
        //conf.put("topology.receiver.buffer.size", 8);
        //conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xdebug -Xrunjdwp:transport=dt_socket,address=1%ID%,server=y,suspend=n");
        
        StormSubmitter.submitTopology(args[0],
        		conf, 
        		builder.createTopology());
    }
}