package pl.allegro.tech.hadoop.compressor.kafka;

import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

import java.util.List;

public class TopicRepository {

    private final ZkUtils zkUtils;

    public TopicRepository(ZkUtils zkUtils) {
        this.zkUtils= zkUtils;
    }

    public List<String> topicNames() {
        return JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
    }

}
