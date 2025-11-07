package com.minicluster.core;

import com.minicluster.util.StarterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

import static com.minicluster.util.HadoopConstants.*;
import static com.minicluster.util.StarterUtils.ensurePortFree;

public final class HdfsBootstrap {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsBootstrap.class);

    private static String host = null;
    private static String hdfsUri = null;

    private HdfsBootstrap() {}

    public static MiniDFSCluster startHdfs(ClusterRuntimeConfig cfg,
                                           Path workDir,
                                           String hostname,
                                           Properties props) throws IOException {

        // Auto-détection simple (prend l’hostname puis sa première IP)
        host = (hostname == null) ? InetAddress.getLocalHost().getHostAddress() : hostname;
        // Prépare un HADOOP_HOME temporaire + winutils.exe si on est sous Windows
        setupHadoopHomeAndWindowsBinaries(workDir);

        Configuration conf = new Configuration();
        setUpConfigurationHadoop(conf, props);

        java.nio.file.Path baseDir = workDir.resolve("dfs-data");
        Files.createDirectories(baseDir);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());

        int nnRpcPort = Integer.parseInt(props.getProperty(NN_RPC_PORT, "20112"));
        int nnHttpPort = Integer.parseInt(props.getProperty(NN_HTTP_PORT, "9870"));

        // Vérification des ports libres
        ensurePortFree(host, nnRpcPort, "NameNode RPC");
        ensurePortFree(host, nnHttpPort, "NameNode HTTP");

        hdfsUri = String.format("hdfs://%s:%d", host, nnRpcPort);

        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                .nameNodePort(nnRpcPort)
                .nameNodeHttpPort(nnHttpPort)
                .numDataNodes(1)
                .build();

        cluster.waitClusterUp();
        LOGGER.info("MiniDFS en cours d’exécution, hdfsUri : {}", hdfsUri);
        return cluster;
    }

    /***
     * Configuration Hadoop pour écoute réseau : Liaison sur toutes interfaces, mais publication sur l’IP LAN
     * * @param conf Configuration Hadoop à modifier
     */
    private static void setUpConfigurationHadoop(Configuration conf, Properties props) {
        conf.set(DSF_DATA_NODE_HOST, host);

        String dfsPermissions = props.getProperty(DFS_PERMISSIONS, "false");
        conf.setBoolean(DFS_PERMISSIONS, Boolean.parseBoolean(dfsPermissions));

        String dfPermissionsUmask = props.getProperty(DF_PERMISSIONS_UMASK, "000");
        conf.set(DF_PERMISSIONS_UMASK, dfPermissionsUmask);

        String dsfNameNodeRcpBindHost = props.getProperty(DSF_NAME_NODE_RPC_BIND_HOST, "0.0.0.0");
        conf.set(DSF_NAME_NODE_RPC_BIND_HOST, dsfNameNodeRcpBindHost);

        int nnRpcPort = Integer.parseInt(props.getProperty(NN_RPC_PORT, "20112"));
        conf.set(DSF_NAME_NODE_RPC_ADRESS, host + ":" + nnRpcPort);

        String dsfNameNodeHttpBindHost = props.getProperty(DSF_NAME_NODE_HTTP_BIND_HOST, "0.0.0.0");
        conf.set(DSF_NAME_NODE_HTTP_BIND_HOST, dsfNameNodeHttpBindHost);

        int nnHttpPort = Integer.parseInt(props.getProperty(NN_HTTP_PORT, "9870"));
        conf.set(DSF_NAME_NODE_HTTP_ADRESS, host + ":" + nnHttpPort);

        String dsfDataNodeBindHost = props.getProperty(DSF_DATA_NODE_BIND_HOST, "0.0.0.0");
        conf.set(DSF_DATA_NODE_BIND_HOST, dsfDataNodeBindHost);

        int dnXferPort = Integer.parseInt(props.getProperty(DN_XFER_PORT, "9866"));
        conf.set(DSF_DATA_NODE_ADDRESS, host + ":" + dnXferPort);

        int dnIpcPort = Integer.parseInt(props.getProperty(DN_IPC_PORT, "9867"));
        conf.set(DSF_DATA_NODE_IPC_ADDRESS, host + ":" + dnIpcPort);

        int dnHttpPort = Integer.parseInt(props.getProperty(DN_HTTP_PORT, "9864"));
        conf.set(DSF_DATA_NODE_HTTP_ADDRESS, host + ":" + dnHttpPort);

        // Important quand les clients sont sur d’autres machines/sous-réseaux
        conf.setBoolean(DSF_CLIENT_USE_DATA_NODE_HOSTNAME, true);
    }

    /***
     * Prépare un répertoire temporaire jouant le rôle de HADOOP_HOME
     * et lance la configuration Windows si nécessaire.
     * * @throws IOException
     */
    private static void setupHadoopHomeAndWindowsBinaries(Path workDir) throws IOException {
        String miniHdfsHome = System.getProperty(HADOOP_HDFS_HOME, "mini-hdfs-home");
        workDir = Files.createTempDirectory(miniHdfsHome);
        java.nio.file.Path bin = workDir.resolve("bin");
        StarterUtils.ensureDirectory(bin);

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
        try (InputStream in = HdfsBootstrap.class.getResourceAsStream("/" + WINUTILS)) {
            if (in == null)
                throw new FileNotFoundException(WINUTILS + " manquant dans src/main/resources/");
            Files.copy(in, bin.resolve(WINUTILS), StandardCopyOption.REPLACE_EXISTING);
        }
        try (InputStream in = HdfsBootstrap.class.getResourceAsStream("/" + HADOOP_DLL)) {
            if (in == null)
                LOGGER.info(HADOOP_DLL + " manquant dans src/main/resources/, on continue sans le charger.");
            else {
                Files.copy(in, bin.resolve(HADOOP_DLL), StandardCopyOption.REPLACE_EXISTING);
                System.load(bin.resolve(HADOOP_DLL).toString());
            }
        }
    }
}