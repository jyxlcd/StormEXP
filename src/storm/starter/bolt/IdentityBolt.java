package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class IdentityBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("item"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        int i = 0;
//        Just delay, nothing else
        while (i<20000){
        	i = i + 1;
        }
//    	Utils.sleep(5);
    	collector.emit(tuple.getValues());
    }        
}
