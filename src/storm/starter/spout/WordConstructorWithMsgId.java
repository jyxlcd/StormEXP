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

public class WordConstructorWithMsgId extends BaseRichSpout{
    private static final Character[] CHARS = 
    	new Character[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
        
    SpoutOutputCollector collectorObj;
    Random randObj;
    boolean logEnable;
    String emitSavePath;
    String ackSavePath;
    PrintWriter pwEmit;
    PrintWriter pwAck;
        
    int sizeOfWord;
    String _val;
        
    public WordConstructorWithMsgId(boolean logEnable, 
      		  String emitSavePath,
      		  String ackSavePath){
      	  this.logEnable = logEnable;
      	  this.emitSavePath = emitSavePath;
      	  this.ackSavePath = ackSavePath;
        }
        
    public WordConstructorWithMsgId(){
      	  this.logEnable = true;
      	  this.emitSavePath = "/home/hduser/storm-log/latencyEmitList.txt";
      	  this.ackSavePath = "/home/hduser/storm-log/latencyAckList.txt";
        } 
        
    @Override
    public void open(Map conf, 
    		TopologyContext context, 
        	SpoutOutputCollector collector) {
            
        collectorObj = collector;
        randObj = new Random();
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
    	Utils.sleep(100);
    	String val = randString(sizeOfWord);
    	
    }
    
    private String randString(int size) {
        StringBuffer buf = new StringBuffer();
        for(int i=0; i<size; i++) {
            buf.append(CHARS[randObj.nextInt(CHARS.length)]);
        }
        return buf.toString();
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}
}
            
