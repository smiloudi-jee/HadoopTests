package com.ecoalis.minicluster.util;

public interface HadoopConstants {

    // Windows spécifications
    String OS_NAME = System.getProperty("os.name").toLowerCase();
    boolean IS_WINDOWS = OS_NAME.contains("win");
    String WINUTILS              = "winutils.exe";
    String HADOOP_DLL            = "hadoop.dll";


    // répertoires par défaut
    String DEFAULT_BASE_DIR = "target/minicluster-work";
    String DEFAULT_HDFS_DATA_DIR = "dfs-data";
    String DEFAULT_HIVE_WAREHOUSE_DIR = "/user/hive/warehouse";
    String DEFAULT_SPARK_WAREHOUSE_LOCAL = "spark-warehouse";
    String METASTORE_DIR_NAME = "metastore";

    // props fichier mini-hdfs.properties
    String PROP_ADVERTISED_HOST    = "advertisedhost";
    String PROP_HADOOP_HOST        = "hadoop.host";

    String NN_RPC_PORT             = "nn.rpc.port";
    String NN_HTTP_PORT            = "nn.http.port";
    String DN_HTTP_PORT            = "dn.http.port";
    String DN_XFER_PORT            = "dn.xfer.port";
    String DN_IPC_PORT             = "dn.ipc.port";

    String DFS_PERMISSIONS_ENABLED = "dfs.permissions.enabled";
    String FS_PERMISSIONS_UMASK    = "fs.permissions.umask-mode";

    String DFS_NN_RPC_BIND_HOST    = "dfs.namenode.rpc-bind-host";
    String DFS_NN_HTTP_BIND_HOST   = "dfs.namenode.http-bind-host";
    String DFS_DN_BIND_HOST        = "dfs.datanode.bind-host";

    String DFS_CLIENT_USE_DN_HOST  = "dfs.client.use.datanode.hostname";

    // Spark / Hive activation flags
    String SPARK_ENABLED           = "spark.enabled";
    String SPARK_HIVE_ENABLED      = "sparkHive.enabled";
    String APP_SPARK_HIVE_NAME     = "app.spark.hive.name";
    String HIVE_WAREHOUSE_DIR      = "hive.warehouse.dir";
    String SPARK_MASTER            = "spark.master";
    String SPARK_SQL_WAREHOUSE_DIR = "spark.sql.warehouse.dir";
    String METASTORE_CLEAN_ON_START= "metastore.cleanOnStart";

    // Hive Metastore / Spark-Hive config
    String DERBY_SYSTEM_HOME                       = "derby.system.home";
    String JAVAX_JDO_OPTION_CONNECTION_URL         = "javax.jdo.option.ConnectionURL";
    String DATANUCLEUS_AUTO_CREATE_SCHEMA          = "datanucleus.autoCreateSchema";
    String HIVE_METASTORE_SCHEMA_VERIFICATION      = "hive.metastore.schema.verification";
    String SPARK_SQL_CATALOG_IMPLEMENTATION        = "spark.sql.catalogImplementation";
    String SPARK_LOG_LEVEL                         = "spark.log.level";
    String SPARK_HADOOP_DFS_CLIENT_USE_DN_HOSTNAME = "spark.hadoop.dfs.client.use.datanode.hostname";

    // REST API
    String REST_ENABLED          = "rest.enabled";
    String REST_PORT             = "rest.port";
    String REST_CORS             = "rest.cors";

    // Thrift server
    String THRIFT_ENABLED        = "thrift.enabled";
    String THRIFT_PORT           = "thrift.port";
    String HIVE_SERVER2_THRIFT_PORT = "hive.server2.thrift.port";

    // Divers chemins internes (MiniClusterStandalone)
    String HADOOP_HDFS_HOME      = "hadoop.hdfs.home";
    String HADOOP_HOME_DIR       = "hadoop.home.dir";
    String HADOOP_HOME           = "HADOOP_HOME";
    String JAVA_TMP_DIR          = "java.io.tmpdir";

        // Valeurs par défaut utiles
    String DEFAULT_SPARK_APP_NAME = "MiniSparkHive";
    String DEFAULT_SPARK_MASTER   = "local[*]";
    String DEFAULT_SPARK_LOGLEVEL = "WARN";
}
