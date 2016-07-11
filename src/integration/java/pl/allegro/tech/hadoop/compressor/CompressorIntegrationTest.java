package pl.allegro.tech.hadoop.compressor;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.palantir.curatortestrule.SharedZooKeeperRule;
import com.palantir.curatortestrule.ZooKeeperRule;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import pl.allegro.tech.hadoop.compressor.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.DEFAULT)
public class CompressorIntegrationTest {

    private static final String SCHEMAREPO_HOST = "http://localhost:2877/schema-repo";
    private static final int SCHEMAREPO_PORT = 2877;

    private static final long UNCOMPRESSED_JSON_SIZE = 34L;
    private static final long COMPRESSED_JSON_SIZE = 105L;
    private static final long FIRST_AVRO_SIZE = 490L;
    private static final long SECOND_AVRO_SIZE = 494L;
    private static final long COMPRESSED_AVRO_SIZE = 527L;
    private static final String TOPIC_PAYLOAD = "{\"version\":1,\"partitions\":{\"0\":[114,224]}}";

    private File baseDir;
    private String hdfsURI;
    private FileSystem fileSystem;
    private MiniDFSCluster hdfsCluster;
    private String todayPath;
    private String pastDayPath;

    @ClassRule
    public static ZooKeeperRule sharedZookeeper = new SharedZooKeeperRule();

    @Rule
    public WireMockRule wireMock = new WireMockRule(SCHEMAREPO_PORT);
    private CuratorFramework zookeeper;

    @Before
    public void setUpZookeeper() throws Exception {
        zookeeper = sharedZookeeper.getClient();
        final ZkClient zkClient = new ZkClient(zookeeper.getZookeeperClient().getCurrentConnectionString());
        ZkUtils.updatePersistentPath(zkClient, "/brokers/topics/topic1_avro", TOPIC_PAYLOAD);
        ZkUtils.updatePersistentPath(zkClient, "/brokers/topics/topic2_avro", TOPIC_PAYLOAD);
        ZkUtils.updatePersistentPath(zkClient, "/brokers/topics/topic1", TOPIC_PAYLOAD);
        ZkUtils.updatePersistentPath(zkClient, "/brokers/topics/topic2", TOPIC_PAYLOAD);
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.driver.allowMultipleContexts", "true");
        System.setProperty("spark.executor.instances", "1");
        System.setProperty("spark.compressor.avro.schema.repository.class",
                "pl.allegro.tech.hadoop.compressor.schema.SchemaRepoSchemaRepository");
        System.setProperty("spark.compressor.processing.topic-name-retriever.class",
                "pl.allegro.tech.hadoop.compressor.schema.KafkaTopicNameRetriever");
        System.setProperty("spark.compressor.processing.mode", "all");
        System.setProperty("spark.compressor.output.compression", "none");
        System.setProperty("spark.compressor.avro.schema.repository.url", SCHEMAREPO_HOST);
        System.setProperty("spark.compressor.processing.delay", "1");
        System.setProperty("spark.compressor.processing.mode.all.excludes", "base,history,integration");
        System.setProperty("spark.compressor.processing.mode.topic.pattern", "daily/*/*/*,hourly/*/*/*/*");
        System.setProperty("spark.compressor.processing.delay", "1");
        System.setProperty("spark.compressor.processing.calculate.counts", "true");
        System.setProperty("spark.compressor.zookeeper.paths",
                zookeeper.getZookeeperClient().getCurrentConnectionString());
        baseDir = Files.createTempDirectory("hdfs").toFile();
        FileUtil.fullyDelete(baseDir);

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";

        Iterator<Entry<String, String>> i = conf.iterator();
        while (i.hasNext()) {
            Entry<String, String> data = i.next();
            System.setProperty("spark.hadoop." + data.getKey(), data.getValue());
        }
        fileSystem = FileSystemUtils.getFileSystem(conf);
    }

    @After
    public void tearDown() throws Exception {
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    @Test
    public void shouldPerformJsonCompression() throws Exception {
        // given
        System.setProperty("spark.compressor.input.format", "json");
        System.setProperty("spark.compressor.input.path", "/camus_main_dir");
        prepareData();

        // when
        Compressor.main();

        // then
        checkUncompressed("/camus_main_dir/topic1/daily/" + todayPath + "/file1");
        checkUncompressed("/camus_main_dir/topic1/daily/" + todayPath + "/file2");
        checkUncompressed("/camus_main_dir/topic1/daily/" + todayPath + "/file3");
        checkCompressed("/camus_main_dir/topic1/daily/" + pastDayPath + "/*");

        checkUncompressed("/camus_main_dir/topic2/hourly/" + todayPath + "/12/file1");
        checkUncompressed("/camus_main_dir/topic2/hourly/" + todayPath + "/12/file2");
        checkUncompressed("/camus_main_dir/topic2/hourly/" + todayPath + "/12/file3");
        checkCompressed("/camus_main_dir/topic2/hourly/" + pastDayPath + "/12/*");
    }

    @Test
    public void shouldPerformAvroCompression() throws Exception {
        // given
        System.setProperty("spark.compressor.input.format", "avro");
        System.setProperty("spark.compressor.input.path", "/camus_main_avro_dir");
        prepareAvroData();
        final String schemaString = IOUtils.toString(getClass().getClassLoader().getResourceAsStream("twitter.avsc"));
        stubSchemaRepo(schemaString, "topic1");
        stubSchemaRepo(schemaString, "topic2");

        // when
        Compressor.main();

        // then
        checkUncompressed(FIRST_AVRO_SIZE, "/camus_main_avro_dir/topic1/daily/" + todayPath + "/file1.avro");
        checkUncompressed(SECOND_AVRO_SIZE, "/camus_main_avro_dir/topic1/daily/" + todayPath + "/file2.avro");
        checkCompressed(COMPRESSED_AVRO_SIZE, "/camus_main_avro_dir/topic1/daily/" + pastDayPath + "/*");

        checkUncompressed(FIRST_AVRO_SIZE, "/camus_main_avro_dir/topic2/hourly/" + todayPath + "/12/file1.avro");
        checkUncompressed(SECOND_AVRO_SIZE, "/camus_main_avro_dir/topic2/hourly/" + todayPath + "/12/file2.avro");
        checkCompressed(COMPRESSED_AVRO_SIZE, "/camus_main_avro_dir/topic2/hourly/" + pastDayPath + "/12/*");
    }

    private void stubSchemaRepo(String schema, String topicName) throws Exception {
        wireMock.stubFor(get(urlPathEqualTo("/schema-repo/" + topicName + "/latest"))
                .willReturn(aResponse()
                        .withBody(schema)
                        .withStatus(200)));
        wireMock.stubFor(get(urlPathEqualTo("/schema-repo/" + topicName))
                .willReturn(aResponse()
                        .withBody("1\t" + schema)
                        .withStatus(200)));
    }

    private void prepareData() throws Exception {
        fileSystem.mkdirs(new Path("/camus_main_dir"));

        todayPath = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
        pastDayPath = "2015/01/01";

        fillWithData("/camus_main_dir/topic1/daily/"  + todayPath);
        fillWithData("/camus_main_dir/topic1/daily/" + pastDayPath);
        fillWithData("/camus_main_dir/topic2/hourly/" + todayPath + "/12");
        fillWithData("/camus_main_dir/topic2/hourly/" + pastDayPath + "/12");
    }

    private void prepareAvroData() throws Exception {
        fileSystem.mkdirs(new Path("/camus_main_avro_dir"));

        todayPath = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
        pastDayPath = "2015/01/01";

        fillWithAvroData("/camus_main_avro_dir/topic1/daily/" + todayPath);
        fillWithAvroData("/camus_main_avro_dir/topic1/daily/" + pastDayPath);
        fillWithAvroData("/camus_main_avro_dir/topic2/hourly/" + todayPath + "/12");
        fillWithAvroData("/camus_main_avro_dir/topic2/hourly/" + pastDayPath + "/12");
    }

    private void fillWithData(String path) throws Exception {
        fileSystem.mkdirs(new Path(path));
        putSampleData(path + "/file1");
        putSampleData(path + "/file2");
        putSampleData(path + "/file3");
    }

    private void fillWithAvroData(String path) throws Exception {
        fileSystem.mkdirs(new Path(path));
        putAvroFile(path + "/file1.avro", "twitter-1.avro");
        putAvroFile(path + "/file2.avro", "twitter-2.avro");
    }

    private void putAvroFile(String path, String localPath) throws IOException {
        final FSDataOutputStream outputStream = fileSystem.create(new Path(path));
        outputStream.write(IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream(localPath)));
        outputStream.close();
    }

    private void putSampleData(String path) throws Exception {
        FSDataOutputStream os = fileSystem.create(new Path(path));
        os.writeChars("line1\nline2\nline3");
        os.close();
    }

    private void checkCompressed(String path) throws Exception {
        checkCompressed(COMPRESSED_JSON_SIZE, path);
    }

    private void checkCompressed(long size, String path) throws Exception {
        assertEquals(size, fileSystem.globStatus(new Path(path))[0].getLen());
    }

    private void checkUncompressed(String path) throws Exception {
        checkUncompressed(UNCOMPRESSED_JSON_SIZE, path);
    }

    private void checkUncompressed(long size, String path) throws Exception {
        assertEquals(size, fileSystem.globStatus(new Path(path))[0].getLen());
    }
}
