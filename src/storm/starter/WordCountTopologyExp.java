package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.TopologyContext;
//import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
//import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.spout.RandomSentenceSpout;
import storm.starter.util.ReadWriteGod;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Modified by Joseph on August 25, 2015.
 * This program is streaming word counting with throughput writing.
 * Write-back is implemented to record the word-count result. 
 * 
 * NOTE:
 * You need to input the arguments, see main function for details.
 */


public class WordCountTopologyExp {
  public static class SplitSentence extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
	}
	

	/**
	 * The bolt will only emit the field "word" 
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	/*
    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
    */
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    private PrintWriter pw;
    private String path;
    private boolean append = true;
    
    public WordCount(String path){
    	this.path = path;
    }
    
//    @Overload
    public WordCount(){
    	this.path = "/home/hduser/storm-log/WordCountOutput";
    }
    
    
    @Override
    public void prepare(Map stormConf, TopologyContext context){
//    	API: https://storm.apache.org/apidocs/
//    	backtype/storm/topology/base/BaseBasicBolt.html
    	
//    	The declaration of "prepare" method is slightly different from
//    	the method when implementing IRichBolt
    	
    String completeFileName = path.concat(
    		String.valueOf(context.getThisTaskId()).concat(".txt"));
    	
    try {
		pw = ReadWriteGod.writer(completeFileName, append);
		pw.write("Start Time: " + 
				String.valueOf(System.currentTimeMillis()) + 
				"\n" );
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    	
    }
    
    public void cleanup(){
    	pw.close();
    }
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
      
      pw.write(word + ": " + count + "\n");
      
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }
  

  public static void main(String[] args) throws Exception {
	
	  /*
	   * arg[0]: topology name
	   * arg[1]: # of workers
	   * arg[2]: # of spout (component 1)
	   * arg[3]: # of split (component 2)
	   * arg[4]: # of count (component 3)
	   * 
	   * The number of tasks inside each component is written already.
	   */
	if (args.length<=4 || args.length >=6) {
		System.out.println("Incorrent number of input arguments!");
	    return;
	}
	    
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(true), Integer.parseInt(args[2]));
    /*
     * TwitterSampleSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String[] keyWords)
	 */
    builder.setBolt("split", new SplitSentence(), Integer.parseInt(args[3])).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), Integer.parseInt(args[4])).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(Integer.parseInt(args[1]));
    StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
 
    Utils.sleep(2000);

  }
}
