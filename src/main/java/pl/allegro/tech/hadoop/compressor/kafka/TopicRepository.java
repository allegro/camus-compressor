package pl.allegro.tech.hadoop.compressor.kafka;

import kafka.utils.ZkUtils;
import org.apache.log4j.Logger;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TopicRepository {

    private static final Logger logger = Logger.getLogger(TopicRepository.class);

    private final List<ZkUtils> zkUtils;

    public TopicRepository(List<ZkUtils> zkUtils) {
        this.zkUtils = zkUtils;
    }

    public Set<String> topicNames() {
        Set<String> topics = new HashSet<>();
        for (ZkUtils zk: zkUtils) {
            final Seq<String> allTopics = zk.getAllTopics();
            List<String> clusterTopics = JavaConversions.seqAsJavaList(allTopics);
            logger.info("Cluster topics: " + clusterTopics.toString());
            topics.addAll(clusterTopics);
        }
        return topics;
    }

}
