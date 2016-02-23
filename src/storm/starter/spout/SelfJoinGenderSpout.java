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

public class SelfJoinGenderSpout extends BaseRichSpout{
	SpoutOutputCollector collectorObj;
//	Random randObj;
	private PrintWriter pw;
	String path = "/home/hduser/storm-log/SelfJoin";
	
	private int genStart;
	private int genEnd;
	private int genCurrent;
	private String field1 = "id";
	private String field2 = "gender";
	
	private int spoutDivisor;

	
	private void setSelfJoinGenderSpout(int genStart){
		this.genStart = genStart*100000;
		this.genEnd = (genStart+1)*100000-1;
		this.genCurrent = genStart*100000;
	}
	
	public SelfJoinGenderSpout(int spoutDivisor){
		this.spoutDivisor = spoutDivisor;
	}
	
	@Override
	public void nextTuple() {
		genCurrent = genCurrent+1;
		if(genCurrent == genEnd){
			genCurrent = genStart;
		}
		Utils.sleep(100);
//		int id = randObj.nextInt(30);
		String gender;
		int messageLength;
//		if (id % 2 == 0) {
		if (genCurrent % 2 == 0){
	        gender = "male";
	        messageLength = 4+4;
	        //The message length is 32 bits of number plus the word "male".
	      }
	      else {
	        gender = "female";
	        messageLength = 4+6;
	      }
		pw.write(String.valueOf(System.currentTimeMillis())+
				": "+
				String.valueOf(messageLength)+
				"\n" );
		collectorObj.emit(new Values(genCurrent, gender));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collectorObj = collector;
//		this.randObj = new Random();
		setSelfJoinGenderSpout(context.getThisTaskId() % spoutDivisor);
		try {
        	String completeFileName = path+
        			"-Spout-"+
            		String.valueOf(context.getThisTaskId())+
            		".txt";
    		pw = ReadWriteGod.writer(completeFileName, true);
    		pw.write("Start Time: " + 
    				String.valueOf(System.currentTimeMillis()) + 
    				"\n" );
    		
    	} catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
	}

	public void close(){
		pw.close();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(field1, field2));
	}
	

}
