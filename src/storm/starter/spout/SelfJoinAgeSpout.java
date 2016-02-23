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

public class SelfJoinAgeSpout extends BaseRichSpout{
	SpoutOutputCollector collectorObj;
	Random randObj;
	private PrintWriter pw;
	String path = "/home/hduser/storm-log/SelfJoin";
	
	private String field1 = "id";
	private String field2 = "gender";
	
	private int genStart;
	private int genEnd;
	private int genCurrent;
	private int spoutDivisor;

	
	private void setSelfJoinAgeSpout(int genStart){
		this.genStart = genStart*100000;
		this.genEnd = (genStart+1)*100000-1;
		this.genCurrent = genStart*100000;
	}
	
	public SelfJoinAgeSpout(int spoutDivisor){
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
		int age = randObj.nextInt(100);
		int messageLength = 4+4;
		pw.write(String.valueOf(System.currentTimeMillis())+
				": "+
				String.valueOf(messageLength)+
				"\n" );
		collectorObj.emit(new Values(genCurrent, age));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collectorObj = collector;
		this.randObj = new Random();
		setSelfJoinAgeSpout(context.getThisTaskId() % spoutDivisor);
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
