package com.natixis.minicluster.embedded;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import static com.natixis.minicluster.util.HadoopConstants.SPARK_HADOOP_FS_DEFAULTFS;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class EmbeddedHadoopClusterTest {
    // Configurations Hadoop
    private static final String HADOOP_HOME = "HADOOP_HOME";
    private static final String HADOOP_HOME_DIR = "hadoop.home.dir";
    private static final String DFS_PERMISSIONS = "dfs.permissions.enabled";
    private static final String DF_PERMISSIONS_UMASK = "fs.permissions.umask-mode";
    private static final String JAVA_TMP_DIR = "java.io.tmpdir";

    private static MiniDFSCluster miniDFSCluster;
    private static FileSystem fileSystem;
    // répertoire temporaire jouant le rôle de HADOOP_HOME
    private static java.nio.file.Path hadoopHome;

    private static SparkSession spark;
    private static String hdfsUri;

    // Donnée & Fichier de test dans HDFS
    private final Path filePath = new Path("/test-file.txt");
    private final String content = "Hello Hadoop MiniCluster!";

    @BeforeAll
    static void setup() throws Exception {
        hadoopHome = Files.createTempDirectory("hadoop-home");
        java.nio.file.Path bin = hadoopHome.resolve("bin");
        Files.createDirectories(bin);

        if (System.getProperty("os.name").toLowerCase().contains("win"))
            setUpForWindows(bin);
        setHadoopProperties();

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hadoopHome.resolve("data").toString());
        conf.setBoolean(DFS_PERMISSIONS, false);
        conf.set(DF_PERMISSIONS_UMASK, "000");

        miniDFSCluster = new MiniDFSCluster.Builder(conf)
                .numDataNodes(1)
                .build();
        miniDFSCluster.waitClusterUp();
        fileSystem = miniDFSCluster.getFileSystem();

        setUpForHiveSpark();

    }

    /***
     * Définit les variables système nécessaires pour Hadoop
     */
    static void setHadoopProperties() {
        //️ Configuration des variables Hadoop à la volée
        System.setProperty(HADOOP_HOME_DIR, hadoopHome.toString());
        System.setProperty(HADOOP_HOME, hadoopHome.toString());
        System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, hadoopHome.toString());
        System.setProperty(JAVA_TMP_DIR, hadoopHome.toString());
    }

    /***
     * Configuration Spark/Hive pour utiliser le MiniDFSCluster
     * * Spark / Hive branchés sur le MiniDFS
     * * @throws IOException
     */
    static void setUpForHiveSpark() throws IOException {
        hdfsUri = "hdfs://127.0.0.1:" + miniDFSCluster.getNameNodePort();
        // Préparer le warehouse Hive sur HDFS
        fileSystem.mkdirs(new Path("/user/hive/warehouse"));
        spark = SparkSession.builder()
                .appName("SparkHiveOnMiniHdfs")
                .master("local[2]")
                // Diriger toutes les IO HDFS de Spark vers le MiniDFS
                .config(SPARK_HADOOP_FS_DEFAULTFS, hdfsUri)
                .config("spark.sql.warehouse.dir", hdfsUri + "/user/hive/warehouse")
                .config("spark.sql.catalogImplementation", "hive")

                // Metastore Derby embarqué (mémoire) + warehouse sur HDFS
                .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:memory:metastore_db;create=true")
                .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
                .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")

                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    /***
     * Configuration spécifique pour Windows (winutils.exe + hadoop.dll)
     * * @param bin répertoire bin dans le HADOOP_HOME temporaire
     * * @throws IOException
     */
    static void setUpForWindows(java.nio.file.Path bin) throws IOException {
        try (InputStream in = EmbeddedHadoopClusterTest.class.getResourceAsStream("/winutils.exe")) {
            if (in == null) throw new FileNotFoundException("winutils.exe manquant dans src/test/resources/");
            Files.copy(in, bin.resolve("winutils.exe"), StandardCopyOption.REPLACE_EXISTING);
        }
        try (InputStream in = EmbeddedHadoopClusterTest.class.getResourceAsStream("/hadoop.dll")) {
            if (in == null) throw new FileNotFoundException("hadoop.dll manquant dans src/test/resources/");
            Files.copy(in, bin.resolve("hadoop.dll"), StandardCopyOption.REPLACE_EXISTING);
            System.load(bin.resolve("hadoop.dll").toString());
        }

    }

    @AfterAll
    static void teardown() throws IOException {
        if (spark != null) {
            spark.stop();
        }
        if (fileSystem != null) fileSystem.close();
        if (miniDFSCluster != null) miniDFSCluster.shutdown();
        if (hadoopHome != null) {
            // Supprime le répertoire temporaire HADOOP_HOME
            Files.walk(hadoopHome)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(java.io.File::delete);
        }
    }

    @Test
    @Order(1)
    void testCreateAndWriteFileOnHDFS() throws Exception {

        // Écriture dans HDFS
        try (FSDataOutputStream out = fileSystem.create(filePath)) {
            out.writeUTF(content);
        }

        // Vérifie l'existence du fichier dans le HDFS
        Assertions.assertTrue(fileSystem.exists(filePath));
        System.out.println("✅ Fichier créer dans HDFS : " + filePath);
    }

    @Test
    @Order(2)
    void testGetAndReadFileFromHDFS() throws Exception {
        String read;
        // Lecture depuis HDFS
        try (FSDataInputStream in = fileSystem.open(filePath)) {
            read = in.readUTF();
        }

        // Vérifie le nombre de fichiers dans le HDFS
        Assertions.assertEquals(1,
                fileSystem.getFileStatus(new Path("/test-file.txt")).getReplication()
        );

        // Vérifie que le fichier est bien listé dans le répertoire racine
        Assertions.assertTrue(Arrays.stream(fileSystem.listStatus(new Path("/")))
                .anyMatch(s -> s.getPath().getName().equals("test-file.txt"))
        );
        Assertions.assertEquals(content, read);
        System.out.println("✅ HDFS fonctionne, contenu lu : " + read);
    }

    @Test
    void sparkHive_ddl_dml_on_hdfs() {
        // Création d'une base et d'une table Hive gérée par Spark
        spark.sql("CREATE DATABASE IF NOT EXISTS mytestdb");
        spark.sql("USE mytestdb");
        spark.sql("DROP TABLE IF EXISTS test_table");
        // table gérée par Spark (format Parquet) dans le warehouse HDFS
        spark.sql("CREATE TABLE test_table (i INT, s STRING) USING PARQUET");
        // Insertion de données
        spark.sql("INSERT INTO test_table VALUES (1,'a'),(2,'b'),(3,'c')");
        // Lecture des données
        long n = spark.sql("SELECT COUNT(*) AS c FROM test_table")
                .collectAsList().get(0).getLong(0);
        Assertions.assertEquals(3L, n);

        // Lecture/écriture explicite en HDFS via Spark
        Dataset<Row> df = spark.table("test_table");
        df.write().mode(SaveMode.Overwrite).parquet("hdfs:///tmp/mytest_parquet");
        Dataset<Row> back = spark.read().parquet("hdfs:///tmp/mytest_parquet");
        Assertions.assertEquals(3L, back.count());
    }

    @Test
    void sparkHive_external_csv_on_hdfs() {
        // Écrit un petit CSV sur HDFS
        Dataset<Row> data = spark.sql("SELECT stack(3, 1,'x', 2,'y', 3,'z') as (i, s)");
        data.write().mode(SaveMode.Overwrite).option("header", true).csv("hdfs:///tmp/csv_data_test");

        spark.sql("CREATE DATABASE IF NOT EXISTS mytestdb");
        spark.sql("USE mytestdb");
        spark.sql("DROP TABLE IF EXISTS ext_csv");

        // External table "façon Hive" pointant sur le répertoire HDFS CSV
        spark.sql(
                "CREATE EXTERNAL TABLE ext_csv (i INT, s STRING) " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +
                        "STORED AS TEXTFILE LOCATION 'hdfs:///tmp/csv_data_test'"
        );
        long n = spark.sql("SELECT COUNT(*) FROM ext_csv")
                .collectAsList().get(0).getLong(0);

        // Vérifie que les 3 lignes du CSV sont bien lues (+1 pour header)
        Assertions.assertEquals(4L, n);
    }

    @Test
    void sparkHive_external_csv_on_hdfs_skip_header_csv() {
        Dataset<Row> data = spark.sql("SELECT stack(3, 1,'x', 2,'y', 3,'z') as (i, s)");
        data.write().mode(SaveMode.Overwrite).option("header", false).csv("hdfs:///tmp/csv_data_test");

        spark.sql("CREATE DATABASE IF NOT EXISTS mytestdb");
        spark.sql("USE mytestdb");
        spark.sql("DROP TABLE IF EXISTS ext_csv");
        spark.sql(
                "CREATE EXTERNAL TABLE ext_csv (i INT, s STRING) " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +
                        "STORED AS TEXTFILE LOCATION 'hdfs:///tmp/csv_data_test'"
        );
        long n = spark.sql("SELECT COUNT(*) FROM ext_csv")
                .collectAsList().get(0).getLong(0);
        Assertions.assertEquals(3L, n);
    }
}