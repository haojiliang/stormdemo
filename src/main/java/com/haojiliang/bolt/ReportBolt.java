package com.haojiliang.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class ReportBolt extends BaseRichBolt {

    private HashMap<String, Long> counts = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // this bolt does not emit anything
    }

    /**
     * Storm 在中止一个 bolt 之前执行 cleanup()，一般用来释放 bolt 占用的资源
     * 注意：当 topology 在 Storm 集群上运行时，cleanup()方法是不可靠的，不能保证会执行
     */
    @Override
    public void cleanup() {
        System.err.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.err.println(key + ":" + this.counts.get(key));
        }
        super.cleanup();
        System.err.println("--------------------");
    }
}
