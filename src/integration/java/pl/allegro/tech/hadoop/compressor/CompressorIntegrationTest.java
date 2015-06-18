package pl.allegro.tech.hadoop.compressor;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import pl.allegro.tech.hadoop.compressor.util.FileSystemUtils;

public class CompressorIntegrationTest {

    private static final long UNCOMPRESSED_SIZE = 34L;
    private static final long COMPRESSED_SIZE = 105L;
    private File baseDir;
    private String hdfsURI;
    private FileSystem fileSystem;
    private MiniDFSCluster hdfsCluster;
    private String todayPath;
    private String pastDayPath;

    @Test
    public void shouldPerformCompression() throws Exception {
        // given
        prepareData();

        // when
        Compressor.main(new String[] { "all", hdfsURI + "camus_main_dir", "none"});

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

    @Before
    public void setUp() throws IOException {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.executor.instances", "1");
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
    public void tearDown() {
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
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

    private void fillWithData(String path) throws Exception {
        fileSystem.mkdirs(new Path(path));
        putSampleData(path + "/file1");
        putSampleData(path + "/file2");
        putSampleData(path + "/file3");
    }

    private void putSampleData(String path) throws Exception {
        FSDataOutputStream os = fileSystem.create(new Path(path));
        os.writeChars("line1\nline2\nline3");
        os.close();
    }

    private void checkCompressed(String path) throws Exception {
        assertEquals(COMPRESSED_SIZE, fileSystem.globStatus(new Path(path))[0].getLen());

    }

    private void checkUncompressed(String path) throws Exception {
        assertEquals(UNCOMPRESSED_SIZE, fileSystem.globStatus(new Path(path))[0].getLen());
    }

}
