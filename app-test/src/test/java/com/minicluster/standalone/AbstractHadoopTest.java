package com.minicluster.standalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static com.minicluster.util.HadoopConstants.*;

public abstract class AbstractHadoopTest {
    private static final String HDFS_URI =
            System.getProperty("hdfs.uri",  "hdfs://192.168.1.104:20112");
    final String HDFS_USER  = System.getProperty("hdfs.user", "hp12");

    final String NN_HTTP_HOST = System.getProperty("nn.host", "192.168.1.104");
    final int    NN_HTTP_PORT = Integer.getInteger("nn.http.port", 9870);

    final String THRIFT_HOST = System.getProperty("thrift.host", "localhost");
    final int    THRIFT_PORT = Integer.getInteger("thrift.port", 10000);
    final String HIVE_JDBC_URL =
            System.getProperty("hive.jdbc",
                    "jdbc:hive2://" + THRIFT_HOST + ":" + THRIFT_PORT + "/default;transportMode=binary");

    final String REST_HOST = System.getProperty("rest.host", "localhost");
    final int    REST_PORT = Integer.getInteger("rest.port", 18080);

    static CloseableHttpClient http;

    static Configuration buildConfiguration(java.nio.file.Path hadoopHome){
        System.setProperty(HADOOP_HOME_DIR, hadoopHome.toAbsolutePath().toString());
        System.setProperty(HADOOP_HOME, hadoopHome.toString());

        Configuration configuration = new Configuration();
        configuration.set(FS_DEFAULTFS, HDFS_URI);
        configuration.setBoolean(DSF_CLIENT_USE_DATA_NODE_HOSTNAME, true);

        return configuration;
    }

    static void setWindowsClientConfig(java.nio.file.Path bin) throws IOException {
        // (facultatif) déposer un winutils.exe minimal dans /bin
        try (InputStream in = StandaloneHadoopClusterClientTest.class.getResourceAsStream("/winutils.exe")) {
            if (in != null) {
                Files.copy(in, bin.resolve("winutils.exe"), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

}
