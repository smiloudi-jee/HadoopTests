package com.ecoalis.minicluster.util;

public interface HadoopConstants {

    // Windows spécifications
    String OS_NAME = System.getProperty("os.name").toLowerCase();
    boolean IS_WINDOWS = OS_NAME.contains("win");
    String WINUTILS              = "winutils.exe";
    String HADOOP_DLL            = "hadoop.dll";

    // Ports Hadoop par défaut
    String HADOOP_HDFS_HOME = "hadoop.hdfs.home"; //C:/hadoop/hdfs
    String HADOOP_HOST = "hadoop.host"; //192.168.1.50
    String NN_RPC_PORT = "nn.rpc.port"; //20112 => RPC NameNode (HDFS URI)
    String NN_HTTP_PORT = "nn.http.port"; //9870 => UI NameNode
    String DN_HTTP_PORT = "dn.http.port"; //9864 => UI DataNode
    String DN_XFER_PORT = "dn.xfer.port"; // 9866 => data transfer port
    String DN_IPC_PORT = "dn.ipc.port"; // 9867 => IPC DataNode

    // Configurations Hadoop
    String HADOOP_HOME = "HADOOP_HOME";
    String HADOOP_HOME_DIR = "hadoop.home.dir";
    String JAVA_TMP_DIR = "java.io.tmpdir";
    String DFS_PERMISSIONS = "dfs.permissions.enabled";
    String DF_PERMISSIONS_UMASK = "fs.permissions.umask-mode";
    String FS_DEFAULTFS = "fs.defaultFS";

    // Configurations spécifiques pour le binding des adresses IP/hostnames
    String DSF_NAME_NODE_RPC_BIND_HOST = "dfs.namenode.rpc-bind-host";
    String DSF_NAME_NODE_RPC_ADRESS = "dfs.namenode.rpc-address";
    String DSF_NAME_NODE_HTTP_BIND_HOST = "dfs.namenode.http-bind-host";
    String DSF_NAME_NODE_HTTP_ADRESS = "dfs.namenode.http-address";

    String DSF_DATA_NODE_HOST = "dfs.datanode.hostname";
    String DSF_DATA_NODE_BIND_HOST = "dfs.datanode.bind-host";
    String DSF_DATA_NODE_ADDRESS = "dfs.datanode.address";
    String DSF_DATA_NODE_IPC_ADDRESS = "dfs.datanode.ipc.address";
    String DSF_DATA_NODE_HTTP_ADDRESS = "dfs.datanode.http.address";

    // Important quand les clients sont sur d’autres machines/sous-réseaux
    String DSF_CLIENT_USE_DATA_NODE_HOSTNAME = "dfs.client.use.datanode.hostname";
    String SPARK_HADOOP_DFS_CLIENT_USE_DATANODE_HOSTNAME = "spark.hadoop.dfs.client.use.datanode.hostname";

    String DERBY_SYSTEM_HOME = "derby.system.home";
    String SPARK_ENABLED = "spark.enabled";
    String SPARK_HIVE_ENABLED = "sparkHive.enabled";
    String REST_ENABLED = "rest.enabled";
    String REST_PORT = "rest.port";
    String REST_CORS = "rest.cors";
    String JAVELIN_REST_SWAGGER = "routes";
    String THRIFT_ENABLED = "thrift.enabled";
    String THRIFT_PORT = "thrift.port";

    String APP_SPARK_HIVE_NAME = "app.spark.hive.name";
    String HIVE_WAREHOUSE_DIR = "hive.warehouse.dir";
    String SPARK_MASTER = "spark.master";
    String SPARK_HADOOP_FS_DEFAULTFS = "spark.hadoop.fs.defaultFS";
    String SPARK_SQL_WAREHOUSE_DIR = "spark.sql.warehouse.dir";
    String JAVAX_JDO_OPTION_CONNECTION_URL = "javax.jdo.option.ConnectionURL";
    String DATANUCLEUS_AUTO_CREATE_SCHEMA = "datanucleus.autoCreateSchema";
    String HIVE_METASTORE_SCHEMA_VERIFICATION = "hive.metastore.schema.verification";
    String SPARK_SQL_CATALOG_IMPLEMENTATION = "spark.sql.catalogImplementation";
    String SPARK_LOG_LEVEL = "spark.log.level";
    String SPARK_UI_PORT = "spark.ui.port";

    String HIVE_SERVER2_THRIFT_PORT = "hive.server2.thrift.port";

    // répertoires par défaut
    String DEFAULT_BASE_DIR = "target/minicluster-work";
    String DEFAULT_HIVE_WAREHOUSE_DIR = "/user/hive/warehouse";
    String METASTORE_DIR_NAME = "metastore";

    String METASTORE_CLEAN_ON_START= "metastore.cleanOnStart";

    // Hive Metastore / Spark-Hive config
    String SPARK_HADOOP_DFS_CLIENT_USE_DN_HOSTNAME = "spark.hadoop.dfs.client.use.datanode.hostname";

    // Valeurs par défaut utiles
    String DEFAULT_SPARK_APP_NAME = "MiniSparkHive";
    String DEFAULT_SPARK_MASTER   = "local[*]";
    String DEFAULT_SPARK_LOGLEVEL = "WARN";

}
