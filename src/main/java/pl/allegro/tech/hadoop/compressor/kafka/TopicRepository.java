package pl.allegro.tech.hadoop.compressor.kafka;

import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TopicRepository {

    private final List<ZkUtils> zkUtils;

    public TopicRepository(List<ZkUtils> zkUtils) {
        this.zkUtils = zkUtils;
    }

    public Set<String> topicNames() {
        Set<String> topics = new HashSet<>();
        for (ZkUtils zk: zkUtils) {
            List<String> clusterTopics = JavaConversions.seqAsJavaList(zk.getAllTopics());
            topics.addAll(clusterTopics);
        }
        return topics;
    }

}
