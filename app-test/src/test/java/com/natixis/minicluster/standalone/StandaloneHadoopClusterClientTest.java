package com.natixis.minicluster.standalone;

import com.natixis.minicluster.util.HdfsRecursiveListerTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import static com.natixis.minicluster.standalone.utils.TestHelper.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StandaloneHadoopClusterClientTest extends AbstractHadoopTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StandaloneHadoopClusterClientTest.class);

    private static final Path filePath = new Path("/user/test-file.txt");
    private static final String content = "Hello Hadoop MiniCluster!";

    private static Configuration configuration;
    private static FileSystem fileSystem;
    private static java.nio.file.Path hadoopHome;

    @BeforeAll
    static void setup() throws Exception {
        hadoopHome = Files.createTempDirectory("hadoop-home-client");
        java.nio.file.Path bin = hadoopHome.resolve("bin");
        Files.createDirectories(bin);

        RequestConfig rc = RequestConfig.custom()
                .setConnectTimeout(3000).setSocketTimeout(15000).build();
        http = HttpClients.custom().setDefaultRequestConfig(rc).build();

        // Sous Windows côté client : hadoop.home.dir doit pointer vers un dossier avec bin\winutils.exe
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            setWindowsClientConfig(bin);
        }
        configuration = buildConfiguration(hadoopHome);
        fileSystem = FileSystem.get(configuration);
    }

    @AfterAll
    static void teardown() throws IOException {
        if (hadoopHome != null) {
            // Supprime le répertoire temporaire HADOOP_HOME
            Files.walk(hadoopHome)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(java.io.File::delete);
        }
    }

//-------------------------- HDFS (API Java) -------------------------

    @Test
    @Order(1)
    void hdfsLsShouldList() throws IOException {
        Path path = new Path("/");
        FileStatus[] stats = fileSystem.listStatus(path);
        for (FileStatus stat : stats) {
            LOGGER.info(" - {}", stat.getPath().toString());
        }
        assertNotNull(stats);
    }

    @Test
    @Order(2)
    void testCreateAndWriteFileOnHDFS() throws Exception {
        // Écriture dans HDFS
        try (FSDataOutputStream out = fileSystem.create(filePath)) {
            out.writeUTF(content);
        }
        // Vérifie l'existence du fichier dans le HDFS
        Assertions.assertTrue(fileSystem.exists(filePath));
        LOGGER.info("✅ Fichier créer dans HDFS : {}", filePath);
    }

    @Test
    @Order(3)
    void testShouldOneFileFromHDFS() throws Exception {
        // Vérifie que nous n'avons qu'un seul fichier dans le HDFS
        Assertions.assertEquals(1,
                fileSystem.listStatus(filePath).length
        );
    }
    @Test
    @Order(5)
    void testFolderOfFileInHDFS() throws Exception {
        // Vérifie que le fichier est bien listé dans le répertoire user
        Assertions.assertTrue(Arrays.stream(fileSystem.listStatus(new Path("/user")))
                .anyMatch(s -> s.getPath().getName().equals("test-file.txt"))
        );
    }

    @Test
    @Order(6)
    void testGetAndReadFileFromHDFS() throws Exception {
        String read;
        // Lecture depuis HDFS
        try (FSDataInputStream in = fileSystem.open(filePath)) {
            read = in.readUTF();
        }
        Assertions.assertEquals(content, read);
        LOGGER.info("✅ HDFS fonctionne, contenu lu : {}", read);
    }

    @Test
    @Order(7)
    void hdfsLs() throws Exception {
        Path path = new Path("/");
        HdfsRecursiveListerTool.listAllPaths(fileSystem, path);
    }

// --------------------- WebHDFS (REST natif) ---------------------------------
    @Test
    @Order(8)
    void webHDFSLshhouldReturnJson() throws Exception {
        String path = "/user";
        // Ping NN UI d'abord, sinon skip
        assumeTrue(isReachable("http", NN_HTTP_HOST, NN_HTTP_PORT),
                "WebHDFS non joignable sur " + NN_HTTP_HOST + ":" + NN_HTTP_PORT);

        String url = "http://" + NN_HTTP_HOST + ":" + NN_HTTP_PORT
                + "/webhdfs/v1" + norm(path) + "?op=LISTSTATUS";

        HttpGet get = new HttpGet(url);
        HttpResponse resp = http.execute(get);
        int code = resp.getStatusLine().getStatusCode();
        String body = readAll(resp.getEntity().getContent());

        assertTrue(code >= 200 && code < 400, "HTTP code=" + code + " body=" + body);
        assertNotNull(body);
        assertTrue(body.contains("FileStatuses") || body.contains("RemoteException"),
                "Réponse inattendue: " + body);
    }

// -------------------- JDBC (Spark Thrift Server / HiveServer2) ---------------------

    @Test
    @Order(9)
    void jdbcShouldExecuteQueries() throws Exception {
        String sql = "select 1 as x"; // select current_date() as d
        assumeTrue(isReachable("tcp", THRIFT_HOST, THRIFT_PORT),
                "ThriftServer non joignable sur " + THRIFT_HOST + ":" + THRIFT_PORT);

        // Driver Hive JDBC 2.3.x
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        try (Connection cnx = DriverManager.getConnection(HIVE_JDBC_URL);
             Statement st = cnx.createStatement()) {

            boolean has = st.execute(sql);
            if (has) {
                try (ResultSet rs = st.getResultSet()) {
                    assertTrue(rs.next(), "Pas de ligne retournée");
                    // on lit simplement la première colonne
                    rs.getObject(1);
                }
            } else {
                assertTrue(st.getUpdateCount() >= 0);
            }
        }
    }

// ------------------- API REST (Javalin) ----------------------

    @Test
    @Order(10)
    void restGetShouldRespond() throws Exception {
        String path = "/routes";
        assumeTrue(isReachable("http", REST_HOST, REST_PORT),
                "REST non joignable sur " + REST_HOST + ":" + REST_PORT);

        String url = "http://" + REST_HOST + ":" + REST_PORT + norm(path);
        HttpGet get = new HttpGet(url);
        HttpResponse resp = http.execute(get);
        int code = resp.getStatusLine().getStatusCode();
        String body = resp.getEntity() != null ? readAll(resp.getEntity().getContent()) : "";

        assertEquals(200, code, "HTTP code=" + code);
        assertNotNull(body);
    }

    @Order(10)
    @Test
    void restPostSqlQueryExample() throws Exception {
        assumeTrue(isReachable("http", REST_HOST, REST_PORT),
                "REST non joignable sur " + REST_HOST + ":" + REST_PORT);

        String url = "http://" + REST_HOST + ":" + REST_PORT + "/sql";
        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/json");
        post.setEntity(new StringEntity("{\"query\":\"select 1 as x\"}", "UTF-8"));

        HttpResponse resp = http.execute(post);
        int code = resp.getStatusLine().getStatusCode();
        String body = resp.getEntity() != null ? readAll(resp.getEntity().getContent()) : "";

        assertEquals(200, code, "HTTP code=" + code + " body=" + body);
    }

}
