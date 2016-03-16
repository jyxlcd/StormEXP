package storm.starter;

import backtype.storm.Config;
//import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
//import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.bolt.SingleJoinBolt;
import storm.starter.spout.SelfJoinAgeSpout;
import storm.starter.spout.SelfJoinGenderSpout;

/**
 * Modified by Joseph on October 2, 2015.
 * This program is stream self-join.  
 * 
 * NOTE:
 * You need to input the arguments, see main function for details.
 * No write-back is implemented.
 */

public class SingleJoinExampleExp {
  public static void main(String[] args) throws Exception {
	  
	  /*
	   * arg[0]: topology name
	   * arg[1]: # of workers
	   * arg[2]: # of genderspout (component 1)
	   * arg[3]: # of agespout (component 2)
	   * arg[4]: # of bolt (component 3)
	   * 
	   * The number of tasks inside each component is written already.
	   */
	if (args.length<=3 || args.length >=5) {
		System.out.println("Incorrent number of input arguments!");
	    return;
	}
	
//	int numberOfSpolts = Integer.parseInt(args[4]);
	
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("gender", new SelfJoinGenderSpout(Integer.parseInt(args[2])), Integer.parseInt(args[2]));
    builder.setSpout("age", new SelfJoinAgeSpout(Integer.parseInt(args[2])), Integer.parseInt(args[2]));
    builder.setBolt("join", 
    		new SingleJoinBolt(new Fields("gender", "age")),
    		Integer.parseInt(args[3]))
    		.fieldsGrouping("gender", new Fields("id"))
    		.fieldsGrouping("age", new Fields("id"));

    Config conf = new Config();
    conf.setDebug(true);

    conf.setNumWorkers(Integer.parseInt(args[1]));
    StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

    Utils.sleep(2000);
  }
}
