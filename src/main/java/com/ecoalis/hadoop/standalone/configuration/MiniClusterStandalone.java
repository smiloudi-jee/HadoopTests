package com.ecoalis.hadoop.standalone.configuration;

import com.ecoalis.hadoop.standalone.tools.PrinterTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;

import static com.ecoalis.hadoop.standalone.configuration.HadoopConfiguration.*;
import static com.ecoalis.hadoop.standalone.tools.StarterUtils.*;

public class MiniClusterStandalone {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniClusterStandalone.class);
    private static final java.util.Properties props = new java.util.Properties();
    private static String sparkEnabled = Boolean.toString(false);
    private static String sparkHiveEnabled = Boolean.toString(false);
    private static String restEnabled = Boolean.toString(false);

    private static MiniDFSCluster cluster;
    private static FileSystem fs;

    // répertoire temporaire jouant le rôle de HADOOP_HOME
    private static java.nio.file.Path workDir;
    private static SparkSession spark;
    private static String HOST = null;
    private static String hdfsUri;

    /**
     * Démarre un MiniDFSCluster autonome.
     *
     * @param hostname L’IP/hostname de la machine qui héberge le MiniDFS (joignable par les autres PC).
     *                 Laisse null pour auto-détecter l’IP locale.
     */
    public static void starter(String hostname) throws Exception {
        loadFileProperties();
        // Auto-détection simple (prend l’hostname puis sa première IP)
        HOST = (hostname == null) ? InetAddress.getLocalHost().getHostAddress() : hostname;

        // Prépare un HADOOP_HOME temporaire + winutils.exe si on est sous Windows
        setupHadoopHomeAndWindowsBinaries();

        // --- Configuration Hadoop pour écoute réseau ---
        Configuration conf = new Configuration();
        setUpConfigurationHadoop(conf);

        // --- Démarrage du MiniDFS ---
        sparkEnabled = props.getProperty(SPARK_ENABLED, "true");
        if (Boolean.parseBoolean(sparkEnabled)) startClusterHDFS(conf);

        // Spark/Hive
        sparkHiveEnabled = props.getProperty(SPARK_HIVE_ENABLED, "true");
        if (Boolean.parseBoolean(sparkHiveEnabled)) startSparkAndHive();

        // REST (optionnel)
        restEnabled = props.getProperty(REST_ENABLED, "true");
        if (Boolean.parseBoolean(restEnabled)) startRestApi();

        // Affichage des informations de connexion
        printAllConfigurationInfo();

        // Arrêt propre sur Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(MiniClusterStandalone::shutdownQuiet));
        LOGGER.info("MiniDFS en cours d’exécution. Presse Ctrl+C pour arrêter.");
        new java.util.concurrent.CountDownLatch(1).await();
    }

    /***
     * Prépare un répertoire temporaire jouant le rôle de HADOOP_HOME
     * et lance la configuration Windows si nécessaire.
     * * @throws IOException
     */
    private static void setupHadoopHomeAndWindowsBinaries() throws IOException {
        String miniHdfsHome = System.getProperty(HADOOP_HDFS_HOME, "mini-hdfs-home");
        workDir = Files.createTempDirectory(miniHdfsHome);
        java.nio.file.Path bin = workDir.resolve("bin");
        Files.createDirectories(bin);

        // Déclare HADOOP_HOME/HOME_DIR/tmp pour Hadoop
        System.setProperty(HADOOP_HOME_DIR, workDir.toString());
        System.setProperty(HADOOP_HOME, workDir.toString());
        System.setProperty(JAVA_TMP_DIR, workDir.toString());

        if (IS_WINDOWS)
            setUpForWindows(bin);
    }

    /***
     * Configuration spécifique pour Windows (winutils.exe + hadoop.dll)
     * * @param bin répertoire bin dans le HADOOP_HOME temporaire
     * * @throws IOException
     */
    private static void setUpForWindows(java.nio.file.Path bin) throws IOException {
        try (InputStream in = MiniClusterStandalone.class.getResourceAsStream("/" + WINUTILS)) {
            if (in == null)
                throw new FileNotFoundException(WINUTILS + " manquant dans src/main/resources/");
            Files.copy(in, bin.resolve(WINUTILS), StandardCopyOption.REPLACE_EXISTING);
        }
        try (InputStream in = MiniClusterStandalone.class.getResourceAsStream("/" + HADOOP_DLL)) {
            if (in == null)
                LOGGER.info(HADOOP_DLL + " manquant dans src/main/resources/, on continue sans le charger.");
            else {
                Files.copy(in, bin.resolve(HADOOP_DLL), StandardCopyOption.REPLACE_EXISTING);
                System.load(bin.resolve(HADOOP_DLL).toString());
            }
        }
    }

    /***
     * Configuration Hadoop pour écoute réseau : Liaison sur toutes interfaces, mais publication sur l’IP LAN
     * * @param conf Configuration Hadoop à modifier
     */
    private static void setUpConfigurationHadoop(Configuration conf) {
        conf.set(HadoopConfiguration.DSF_DATA_NODE_HOST, HOST);

        String dfsPermissions = props.getProperty(HadoopConfiguration.DFS_PERMISSIONS, "false");
        conf.setBoolean(HadoopConfiguration.DFS_PERMISSIONS, Boolean.parseBoolean(dfsPermissions));

        String dfPermissionsUmask = props.getProperty(HadoopConfiguration.DF_PERMISSIONS_UMASK, "000");
        conf.set(HadoopConfiguration.DF_PERMISSIONS_UMASK, dfPermissionsUmask);

        String dsfNameNodeRcpBindHost = props.getProperty(HadoopConfiguration.DSF_NAME_NODE_RPC_BIND_HOST, "0.0.0.0");
        conf.set(HadoopConfiguration.DSF_NAME_NODE_RPC_BIND_HOST, dsfNameNodeRcpBindHost);

        int nnRpcPort = Integer.parseInt(props.getProperty(HadoopConfiguration.NN_RPC_PORT, "20112"));
        conf.set(HadoopConfiguration.DSF_NAME_NODE_RPC_ADRESS, HOST + ":" + nnRpcPort);

        String dsfNameNodeHttpBindHost = props.getProperty(HadoopConfiguration.DSF_NAME_NODE_HTTP_BIND_HOST, "0.0.0.0");
        conf.set(HadoopConfiguration.DSF_NAME_NODE_HTTP_BIND_HOST, dsfNameNodeHttpBindHost);

        int nnHttpPort = Integer.parseInt(props.getProperty(HadoopConfiguration.NN_HTTP_PORT, "9870"));
        conf.set(HadoopConfiguration.DSF_NAME_NODE_HTTP_ADRESS, HOST + ":" + nnHttpPort);

        String dsfDataNodeBindHost = props.getProperty(HadoopConfiguration.DSF_DATA_NODE_BIND_HOST, "0.0.0.0");
        conf.set(HadoopConfiguration.DSF_DATA_NODE_BIND_HOST, dsfDataNodeBindHost);

        int dnXferPort = Integer.parseInt(props.getProperty(HadoopConfiguration.DN_XFER_PORT, "9866"));
        conf.set(HadoopConfiguration.DSF_DATA_NODE_ADDRESS, HOST + ":" + dnXferPort);

        int dnIpcPort = Integer.parseInt(props.getProperty(HadoopConfiguration.DN_IPC_PORT, "9867"));
        conf.set(HadoopConfiguration.DSF_DATA_NODE_IPC_ADDRESS, HOST + ":" + dnIpcPort);

        int dnHttpPort = Integer.parseInt(props.getProperty(HadoopConfiguration.DN_HTTP_PORT, "9864"));
        conf.set(HadoopConfiguration.DSF_DATA_NODE_HTTP_ADDRESS, HOST + ":" + dnHttpPort);

        // Important quand les clients sont sur d’autres machines/sous-réseaux
        conf.setBoolean(HadoopConfiguration.DSF_CLIENT_USE_DATA_NODE_HOSTNAME, true);
    }

    /**
     * Démarre le MiniDFSCluster
     */
    private static void startClusterHDFS(Configuration conf) throws IOException {
        // Base de travail MiniDFS
        java.nio.file.Path baseDir = workDir.resolve("dfs-data");
        Files.createDirectories(baseDir);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());

        int nnRpcPort = Integer.parseInt(props.getProperty(HadoopConfiguration.NN_RPC_PORT, "20112"));
        int nnHttpPort = Integer.parseInt(props.getProperty(HadoopConfiguration.NN_HTTP_PORT, "9870"));

        // Vérification des ports libres
        ensurePortFree(HOST, nnRpcPort, "NameNode RPC");
        ensurePortFree(HOST, nnHttpPort, "NameNode HTTP");

        hdfsUri = String.format("hdfs://%s:%d", HOST, nnRpcPort);

        // --- Lancement du cluster ---
        cluster = new MiniDFSCluster.Builder(conf)
                .nameNodePort(nnRpcPort)
                .nameNodeHttpPort(nnHttpPort)
                .numDataNodes(1)
                .build();
        cluster.waitClusterUp();

        fs = cluster.getFileSystem();
    }

    /**
     * Affiche les informations de configuration du MiniDFSCluster
     */
    private static void printAllConfigurationInfo() {

        PrinterTools.printGlobalConfiguration(hdfsUri, HOST, props);

        if (Boolean.parseBoolean(sparkEnabled) && spark != null) {
            PrinterTools.printSparkConfiguration(spark, props);
        }
        if (Boolean.parseBoolean(sparkHiveEnabled) && spark != null) {
            PrinterTools.printHiveConfiguration(hdfsUri, props, spark);
        }
        if (Boolean.parseBoolean(restEnabled) && spark != null)
            PrinterTools.printRestConfiguration(props);
    }

    /**
     * Vérifie qu’un port est libre
     */
    private static void ensurePortFree(String host, int port, String label) throws IOException {
        try (ServerSocket s = new ServerSocket()) {
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress(host.equals("0.0.0.0") ? "127.0.0.1" : host, port));
        } catch (IOException e) {
            throw new IOException(label + " (" + host + ":" + port + ") déjà occupé", e);
        }
    }

    /**
     * Charge les propriétés depuis le fichier mini-hdfs.properties
     */
    private static void loadFileProperties() {
        try (InputStream input = MiniClusterStandalone.class.getResourceAsStream("/mini-hdfs.properties")) {
            if (input == null) {
                LOGGER.warn("mini-hdfs.properties non trouvé dans src/main/resources/, on continue avec les valeurs par défaut.");
                return;
            }
            props.load(input);
        } catch (IOException ex) {
            LOGGER.error("Erreur lors du chargement de mini-hdfs.properties : {}", ex.getMessage());
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void shutdownQuiet() {
        LOGGER.info("\nArrêt du MiniDFS…");
        try {
            if (spark != null) spark.stop();
        } catch (Exception ignore) {
        }
        try {
            if (fs != null) fs.close();
        } catch (Exception ignore) {
        }
        try {
            if (cluster != null) cluster.shutdown();
        } catch (Exception ignore) {
        }

        // Nettoyage du répertoire de travail
        if (workDir != null) {
            try {
                Files.walk(workDir)
                        .sorted(Comparator.reverseOrder())
                        .map(java.nio.file.Path::toFile)
                        .forEach(File::delete);
            } catch (IOException ignore) {
            }
        }
        LOGGER.info("MiniDFS arrêté proprement.");
    }

    private static void startSparkAndHive() throws IOException {
        // --- Dossiers ---
        // Warehouse HDFS
        final String hiveWarehousePath = props.getProperty(HadoopConfiguration.HIVE_WAREHOUSE_DIR, "/user/hive/warehouse");
        fs.mkdirs(new Path(hiveWarehousePath));

        // Metastore LOCAL (Derby) sous le workDir => <HADOOP_HOME_TEMP>/metastore
        final java.nio.file.Path metaDir = workDir.resolve("metastore");
        final java.nio.file.Path derbyHome = metaDir; // on met derby.system.home = metaDir
        ensureDir(metaDir);

        // Optionnel : nettoyage à froid à chaque run (utile pour tests). Piloté par propriété.
        // mini-hdfs.properties : metastore.cleanOnStart=true/false (défaut false)
        if (Boolean.parseBoolean(props.getProperty("metastore.cleanOnStart", "false"))) {
            deleteRecursively(metaDir);
            ensureDir(metaDir);
        }

        // Chemin JDBC Derby "fichier" (attention au séparateur Windows)
        final String derbyDbPath = metaDir.resolve("metastore_db").toString().replace('\\', '/');

        // Nécessaire pour Derby : tous les fichiers (log/lock) seront sous derby.system.home
        System.setProperty(DERBY_SYSTEM_HOME, derbyHome.toString());

        // --- SparkSession (Hive intégré) ---
        spark = SparkSession.builder()
                .appName(props.getProperty(APP_SPARK_HIVE_NAME, "MiniDP-SparkHive"))
                .master(props.getProperty(SPARK_MASTER, "local[*]"))

                // HDFS côté Spark
                .config(SPARK_HADOOP_FS_DEFAULTFS, hdfsUri)
                .config(SPARK_HADOOP_DFS_CLIENT_USE_DATANODE_HOSTNAME,
                        props.getProperty(SPARK_HADOOP_DFS_CLIENT_USE_DATANODE_HOSTNAME, "true"))

                // Warehouse HDFS (même chemin côté Spark & Hive)
                .config(SPARK_SQL_WAREHOUSE_DIR, hdfsUri + hiveWarehousePath)
                .config("hive.metastore.warehouse.dir", hdfsUri + hiveWarehousePath)

                // Metastore Derby local (JDO)
                .config(JAVAX_JDO_OPTION_CONNECTION_URL, "jdbc:derby:" + derbyDbPath + ";create=true")
                .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")

                .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

                // Auto-création du schéma (évite la vérification stricte au premier run)
                .config(DATANUCLEUS_AUTO_CREATE_SCHEMA, props.getProperty(DATANUCLEUS_AUTO_CREATE_SCHEMA, "true"))
                .config(HIVE_METASTORE_SCHEMA_VERIFICATION, props.getProperty(HIVE_METASTORE_SCHEMA_VERIFICATION, "false"))

                // Divers Hive
                .config(SPARK_SQL_CATALOG_IMPLEMENTATION, props.getProperty(SPARK_SQL_CATALOG_IMPLEMENTATION, "hive"))
                .config("hive.exec.scratchdir", "/tmp/hive")

                .enableHiveSupport()
                .getOrCreate();

        // appelez-la après la création de 'spark'
        registerBuiltInUdfs(spark);

        spark.sparkContext().setLogLevel(props.getProperty(SPARK_LOG_LEVEL, "WARN"));

        // --- Bootstrap du metastore : force l'init et évite le NPE sur databaseExists ---
        try {
            spark.sql("CREATE DATABASE IF NOT EXISTS default");
            spark.sql("CREATE TABLE IF NOT EXISTS default._probe_(id INT)");
            spark.sql("DROP TABLE IF EXISTS default._probe_");
        } catch (Throwable t) {
            LOGGER.warn("Bootstrap Hive a rencontré un souci (on continue) : {}", t.toString());
        }

        // --- Thrift Server JDBC (optionnel) ---
        String thriftEnabled = props.getProperty(HadoopConfiguration.THRIFT_ENABLED, "true");
        if (Boolean.parseBoolean(thriftEnabled)) {
            startThriftServer();

            // Attente que le port soit ouvert
            String host = props.getProperty("advertisedhost", "localhost"); // côté client, tu utilisais localhost
            int port = Integer.parseInt(props.getProperty(THRIFT_PORT, "10000"));
            try {
                waitForPortOpen(host, port, 20_000L);
            } catch (Exception e) {
                LOGGER.warn("Le port JDBC HiveServer2 ({}) ne s'est pas ouvert à temps : {}", port, e.toString());
            }
        }
        buildPermanentUdf();
    }

    private static void buildPermanentUdf() {
        try {
            spark.sql(
                    "CREATE OR REPLACE FUNCTION default.to_upper " +
                            "AS 'com.ecoalis.hadoop.standalone.udf.ToUpperUDF'"
            );
            LOGGER.info("UDF Hive 'to_upper' enregistrée dans le metastore.");
        } catch (Exception e) {
            LOGGER.warn("Impossible d'enregistrer la UDF Hive globale: {}", e.getMessage());
        }
    }

    /** Enregistre les UDFs intégrées (côté serveur)
     * * Fontionne uniquement avec REST SQL (REST API exposé POST /sql/execute)
     * * @param spark SparkSession
     * * @return void
     * */
    private static void registerBuiltInUdfs(SparkSession spark) {
        // Java 8 : UDF1<String,String>
        org.apache.spark.sql.api.java.UDF1<String, String> toUpper =
                s -> s == null ? null : s.toUpperCase();
        spark.udf().register("to_upper", toUpper, org.apache.spark.sql.types.DataTypes.StringType);
    }

    private static void startThriftServer() {
        try {
            // Le port peut être fixé via HADOOP/Hive conf, mais Spark 3.5 lit hive-site. Ici on fixe via sysprops.
            System.setProperty(HIVE_SERVER2_THRIFT_PORT, props.getProperty(THRIFT_PORT, "10000"));
            System.setProperty("hive.server2.thrift.bind.host", "0.0.0.0"); // optionnel
            System.setProperty("hive.server2.transport.mode", "binary");
            org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.startWithContext(spark.sqlContext());
            LOGGER.info("Thrift Server JDBC sur port {}", props.getProperty(THRIFT_PORT, "10000"));
        } catch (Throwable t) {
            LOGGER.error("Thrift Server non démarré: {}", t.getMessage());
        }
    }

    private static void startRestApi() {
        RestJavalinAPIConfig.config(hdfsUri, spark, fs, props);
    }
}