package com.haojiliang.topology;

import com.haojiliang.bolt.ReportBolt;
import com.haojiliang.bolt.SplitSentenceBolt;
import com.haojiliang.bolt.WordCountBolt;
import com.haojiliang.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) {

        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        // 数据流订阅关系
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
        // 注册一个 SplitSentenceBolt，这个 bolt 订阅 SentenceSpout 发射出来的数据流
        // shuffleGrouping()方法告诉 Storm，要将 SentenceSpout 发射的 tuple 随机均匀的分发给 SplitSentenceBolt 的实例
        // 给 SplitSentenceBolt 设置4个 task 和2个 executor，每个 executor 线程指定4/2个 task 来执行
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);
        // fieldsGrouping()方法保证所有"word"字段值相同的 tuple 会被路由到同一个 WordCountBolt 实例
        // 将 WordCountBolt 运行4个task，每个task由一个 executor 线程执行
        builder.setBolt(COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        // globalGrouping()会将 WordCountBolt 发射的所有 tuple 路由到唯一的 ReportBolt 任务中
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        // 增加分配给一个 topology 的 worker 数量
        config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

}
