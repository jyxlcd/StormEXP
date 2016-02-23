package storm.starter.spout;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Random;

import storm.starter.util.ReadWriteGod;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomWordGenSpout extends BaseRichSpout {
    private static final Character[] CHARS = 
    		new Character[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
    	'i','j','k','l','m','n','o','p','q','r','s','t','u','v','w',
    	'x','y','z'};
    

    SpoutOutputCollector collectorObj;
    Random randObj;
    boolean logEnable;
    String emitSavePath;
    String ackSavePath;
    PrintWriter pwEmit;
    PrintWriter pwAck; 
    int outputSize;
    int idLength;
    String msgIdFleid;
    String valueField;
    
    public RandomWordGenSpout(int outputSize,
    		int idLength,
    		Boolean logEnable,
    		String emitSavePath,
    		String ackSavePath) {
    	this.outputSize = outputSize;
    	this.idLength = idLength;
    	this.logEnable = logEnable;
        this.emitSavePath = emitSavePath;
        this.ackSavePath = ackSavePath;
    }
    
    public RandomWordGenSpout(){
    	this.outputSize = 1024*8; //1KB data by default
    	this.idLength = 10; //10B for the id
    	this.logEnable = true;
  	  	this.emitSavePath = "/home/hduser/storm-log/ThroughputSpoutEmitList-";
  	  	this.ackSavePath = "/home/hduser/storm-log/ThroughputAckList-";
    }
    
    public RandomWordGenSpout(int outputSize){
    	this.outputSize = outputSize;
    	this.idLength = 10; //10B for the id
    	this.logEnable = true;
  	  	this.emitSavePath = "/home/hduser/storm-log/ThroughputSpoutEmitList-";
  	  	this.ackSavePath = "/home/hduser/storm-log/ThroughputAckList-";
    }
    
    @Override
    public void open(Map conf, 
    		TopologyContext context, 
    		SpoutOutputCollector collector) {
        
    	collectorObj = collector;
    	randObj = new Random();
//        _id = randString(idLength);
//        _val = randString(outputSize);
    	emitSavePath = emitSavePath + 
    			String.valueOf(context.getThisTaskId()) +
    			".txt";
    	ackSavePath = ackSavePath +
    			String.valueOf(context.getThisTaskId()) + 
    			".txt";
        try {
    		this.pwEmit = ReadWriteGod.writer(this.emitSavePath, true);
    		this.pwAck = ReadWriteGod.writer(this.ackSavePath, true);
    	} catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
        
    }

    @Override
    public void nextTuple() {
    	Utils.sleep(20);
        msgIdFleid = randString(idLength);
        valueField = randString(outputSize);
    	collectorObj.emit(new Values(valueField), msgIdFleid);
        if (logEnable){
        	pwEmit.write(String.valueOf(msgIdFleid) + 
        			" :" + 
        			String.valueOf(System.currentTimeMillis()) + 
        			"\n"
        			);
        }
    }
    
    @Override
    public void ack(Object msgId){
  	  if(logEnable){
		  pwAck.write(String.valueOf(msgId) + 
	    			" :" + 
	    			String.valueOf(System.currentTimeMillis()) + 
	    			"\n"
	    			);
	  }
    }

    private String randString(int size) {
        StringBuffer buf = new StringBuffer();
        for(int i=0; i<size; i++) {
            buf.append(CHARS[randObj.nextInt(CHARS.length)]);
        }
        return buf.toString();
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("item"));
    }
    
    @Override
    public void close(){
  	  pwEmit.close();
  	  pwAck.close();
    }
}

