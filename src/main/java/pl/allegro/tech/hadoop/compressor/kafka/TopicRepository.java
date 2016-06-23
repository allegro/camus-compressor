package pl.allegro.tech.hadoop.compressor.kafka;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;

import java.util.List;

public class TopicRepository {

    private final ZkClient zkClient;

    public TopicRepository(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public List<String> topicNames() {
        return JavaConversions.asJavaList(ZkUtils.getAllTopics(zkClient));
    }

}
