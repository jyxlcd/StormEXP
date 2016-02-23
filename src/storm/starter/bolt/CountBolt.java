package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class CountBolt extends BaseBasicBolt {
    int count;
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String word = tuple.getString(0);
    	int count = word.length();
//    	Utils.sleep(5);
        collector.emit(new Values(count));
    }
}
