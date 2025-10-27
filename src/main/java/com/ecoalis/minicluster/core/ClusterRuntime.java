package com.ecoalis.minicluster.core;


import com.ecoalis.minicluster.modules.hive.HiveBootstrap;
import com.ecoalis.minicluster.modules.hive.HiveServiceHandle;
import com.ecoalis.minicluster.modules.rest.RestBootstrap;
import com.ecoalis.minicluster.modules.rest.RestServiceHandle;
import com.ecoalis.minicluster.modules.spark.SparkBootstrap;
import com.ecoalis.minicluster.modules.spark.SparkServiceHandle;
import com.ecoalis.minicluster.util.PrinterTools;
import com.ecoalis.minicluster.util.StarterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

import static com.ecoalis.minicluster.util.HadoopConstants.*;

public final class ClusterRuntime implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterRuntime.class);

    private final ClusterRuntimeConfig config;
    private final MiniDFSCluster hdfsCluster;
    private final FileSystem fs;
    private final String hdfsUri;
    private final String host;
    private final Path workDir;

    private final Optional<HiveServiceHandle> hiveHandle;
    private final Optional<SparkServiceHandle> sparkHandle;
    private final Optional<RestServiceHandle> restHandle;

    private ClusterRuntime(ClusterRuntimeConfig config,
                           MiniDFSCluster hdfsCluster,
                           FileSystem fs,
                           String hdfsUri,
                           String host,
                           Path workDir,
                           Optional<HiveServiceHandle> hiveHandle,
                           Optional<SparkServiceHandle> sparkHandle,
                           Optional<RestServiceHandle> restHandle) {
        this.config = config;
        this.hdfsCluster = hdfsCluster;
        this.fs = fs;
        this.hdfsUri = hdfsUri;
        this.host = host;
        this.workDir = workDir;
        this.hiveHandle = hiveHandle;
        this.sparkHandle = sparkHandle;
        this.restHandle = restHandle;
    }

    public static ClusterRuntime start(ClusterRuntimeConfig cfg) throws Exception {
        Properties props = cfg.getProps();

        // 1. host
        String advertisedHost = cfg.getAdvertisedHost();
        String host = (advertisedHost == null || advertisedHost.isEmpty())
                ? InetAddress.getLocalHost().getHostAddress()
                : advertisedHost;

        // 2. work dir
        Path tmpWorkDir = Files.createTempDirectory(
                props.getProperty(HADOOP_HDFS_HOME, "mini-hdfs-home")
        );

        // 3. set up WINDOWS specifics (winutils, hadoop.dll)
        setupHadoopHomeAndWindowsBinaries(tmpWorkDir);

        // 4. start HDFS
        MiniDFSCluster cluster = HdfsBootstrap.startHdfs(cfg, tmpWorkDir, host, props);
        Configuration hadoopConf = cluster.getConfiguration(0);
        FileSystem fs = FileSystem.get(hadoopConf);

        String nnRpcPort = props.getProperty(NN_RPC_PORT, "20112");
        String hdfsUri = "hdfs://" + host + ":" + nnRpcPort;

        // 5. Hive (warehouse, metastore)
        Optional<HiveServiceHandle> hiveHandle = Optional.empty();
        if (cfg.isHiveEnabled()) {
            hiveHandle = Optional.of(
                    HiveBootstrap.startHive(props, hdfsUri, fs, tmpWorkDir)
            );
        }

        // 6. Spark (+Thrift JDBC) : dépend de Hive pour la config
        Optional<SparkServiceHandle> sparkHandle = Optional.empty();
        if (cfg.isSparkEnabled()) {
            sparkHandle = Optional.of(
                    SparkBootstrap.startSpark(
                            props,
                            hdfsUri,
                            tmpWorkDir,
                            hiveHandle,
                            cfg.isThriftEnabled() && Boolean.parseBoolean(props.getProperty(THRIFT_ENABLED, "true"))
                    )
            );
        }

        // 7. REST API
        Optional<RestServiceHandle> restHandle = Optional.empty();
        if (cfg.isRestEnabled()
                && Boolean.parseBoolean(props.getProperty(REST_ENABLED, "true"))) {
            restHandle = Optional.of(
                    RestBootstrap.startRestApi(
                            props,
                            hdfsUri,
                            fs,
                            sparkHandle.map(SparkServiceHandle::sparkSession).orElse(null)
                    )
            );
        }

        // 8. Print config
        PrinterTools.printGlobalConfiguration(hdfsUri, host, props);
        sparkHandle.ifPresent(h ->
                PrinterTools.printSparkConfiguration(h.sparkSession(), props)
        );

        return new ClusterRuntime(
                cfg,
                cluster,
                fs,
                hdfsUri,
                host,
                tmpWorkDir,
                hiveHandle,
                sparkHandle,
                restHandle
        );
    }

    /**
     * Prépare un HADOOP_HOME temporaire et gère les binaires Windows.
     */
    private static void setupHadoopHomeAndWindowsBinaries(Path workDir) throws IOException {
        Path bin = workDir.resolve("bin");
        Files.createDirectories(bin);

        System.setProperty(HADOOP_HOME_DIR, workDir.toString());
        System.setProperty(HADOOP_HOME, workDir.toString());
        System.setProperty(JAVA_TMP_DIR, workDir.toString());

        boolean isWindows = System.getProperty("os.name", "")
                .toLowerCase().contains("win");

        if (isWindows) {
            try (InputStream in = ClusterRuntime.class.getResourceAsStream("/" + WINUTILS)) {
                if (in == null) {
                    throw new FileNotFoundException(WINUTILS + " manquant dans resources/");
                }
                Files.copy(in, bin.resolve(WINUTILS), StandardCopyOption.REPLACE_EXISTING);
            }
            try (InputStream in = ClusterRuntime.class.getResourceAsStream("/" + HADOOP_DLL)) {
                if (in == null) {
                    // possible, pas bloquant
                    LOGGER.info(HADOOP_DLL + " manquant, on continue sans.");
                } else {
                    Files.copy(in, bin.resolve(HADOOP_DLL), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    // Getters pour les tests
    public String getHdfsUri() { return hdfsUri; }
    public FileSystem getFileSystem() { return fs; }
    public Optional<SparkSession> getSparkSession() {
        return sparkHandle.map(SparkServiceHandle::sparkSession);
    }
    public Optional<HiveServiceHandle> getHiveHandle() { return hiveHandle; }
    public Optional<RestServiceHandle> getRestHandle() { return restHandle; }
    public String getHost() { return host; }
    public Path getWorkDir() { return workDir; }

    @Override
    public void close() {
        // arrêt en ordre inverse
        restHandle.ifPresent(RestServiceHandle::stop);
        sparkHandle.ifPresent(SparkServiceHandle::stop);
        hiveHandle.ifPresent(HiveServiceHandle::stop);

        try {
            fs.close();
        } catch (Exception ignore) {}

        try {
            hdfsCluster.shutdown();
        } catch (Exception e) {
            LOGGER.warn("Erreur à l'arrêt du MiniDFSCluster: {}", e.getMessage());
        }

        try {
            StarterUtils.deleteRecursively(workDir);
        } catch (Exception e) {
            LOGGER.warn("Cleanup error: {}", e.getMessage());
        }
    }
}