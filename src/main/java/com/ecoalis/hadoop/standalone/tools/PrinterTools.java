package com.ecoalis.hadoop.standalone.tools;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ecoalis.hadoop.standalone.configuration.HadoopConfiguration.*;

public class PrinterTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrinterTools.class);

    public static void printGlobalConfiguration(String hdfsUri, String HOST, java.util.Properties props) {
        LOGGER.info("\n================ MiniDFS démarré ================");
        LOGGER.info("HDFS URI           : {}", hdfsUri);
        LOGGER.info("NameNode UI        : http://{}:{}/", HOST, Integer.parseInt(props.getProperty(NN_HTTP_PORT, "9870")));
        LOGGER.info("WebHDFS (REST natif HDFS) : NameNode UI + {}", "/webhdfs/v1/?op=LISTSTATUS");
        LOGGER.info("DataNode UI        : http://{}:{}/", HOST, Integer.parseInt(props.getProperty(DN_HTTP_PORT, "9864")));
        LOGGER.info("Data transfer port : {}", Integer.parseInt(props.getProperty(DN_XFER_PORT, "9866")));
        LOGGER.info("IPC DN port        : {}", Integer.parseInt(props.getProperty(DN_IPC_PORT, "9867")));
        LOGGER.info("HADOOP_HOME        : {}", System.getProperty(HADOOP_HOME_DIR));
        LOGGER.info("=================================================\n");
    }

    public static void printSparkConfiguration(SparkSession sparkSession, java.util.Properties props) {
        String sparkVersion = sparkSession.version();
        String sparkApp = sparkSession.sparkContext().appName();
        String sparkMaster = sparkSession.sparkContext().master();
        String sparkUiUrl = getSparkUiURL(sparkSession);

        LOGGER.info("\n================ SPARK ================");
        LOGGER.info("Version            : {}", sparkVersion != null ? sparkVersion : "unknown");
        LOGGER.info("App name           : {}", sparkApp != null ?
                sparkApp : props.getProperty(APP_SPARK_HIVE_NAME, "MiniSparkHive"));
        LOGGER.info("Master             : {}", sparkMaster != null ?
                sparkMaster : props.getProperty(SPARK_MASTER, "local[*]"));
        LOGGER.info("Spark UI           : {}", sparkUiUrl != null ?
                sparkUiUrl : "http://" + props.getProperty(ADEVERTISED_HOST, "localhost")
                + ":" +props.getProperty(SPARK_UI_PORT, "4040"));
        LOGGER.info("=======================================\n");

    }

    private static String getSparkUiURL(SparkSession sparkSession){
        scala.Option<String> uiOpt = sparkSession.sparkContext().uiWebUrl();

        String sparkUiUrl = null;
        if (uiOpt != null && !uiOpt.isEmpty()) {
            sparkUiUrl = uiOpt.get();
            if (!sparkUiUrl.startsWith("http")) {
                sparkUiUrl = "http://" + sparkUiUrl;
            }
        }
        return sparkUiUrl;
    }

    // ====== HIVE / SPARK SQL (Hive intégré) ======
    public static void printHiveConfiguration(String hdfsUri, java.util.Properties props, SparkSession sparkSession) {
        String warehouse = hdfsUri + props.getProperty(HIVE_WAREHOUSE_DIR, "/user/hive/warehouse");
        String warehouseSQL = sparkSession.conf().get(SPARK_SQL_WAREHOUSE_DIR) != null ?
                sparkSession.conf().get(SPARK_SQL_WAREHOUSE_DIR) : props.getProperty(SPARK_SQL_WAREHOUSE_DIR, "hdfs:///" + warehouse);
        String hiveVersion = getHiveVersion();
        int thriftPort = Integer.parseInt(props.getProperty("thrift.port", "10000"));
        String jdbcUrl = "jdbc:hive2://" + props.getProperty("advertisedhost", "localhost")
                + ":" + thriftPort + "/default";

        LOGGER.info("\n================ HIVE / SPARK SQL (Hive intégré) ================");
        LOGGER.info("Hive version       : {}", hiveVersion);
        LOGGER.info("Warehouse          : {}", warehouse);
        if (!warehouse.equals(warehouseSQL))
            LOGGER.info("Warehouse SQL      : {}", warehouseSQL);
        LOGGER.info("Thrift/JDBC (HS2)  : {}", jdbcUrl);
        LOGGER.info("Driver JDBC        : org.apache.hive.jdbc.HiveDriver");
        LOGGER.info("===============================================================\n");
    }

    private static String getHiveVersion() {
        String hiveVersion = "intégrée à Spark";

        try {
            Class<?> vi = Class.forName("org.apache.hive.VersionInfo");
            java.lang.reflect.Method mv = vi.getMethod("getVersion");
            Object v = mv.invoke(null);
            if (v != null) hiveVersion = String.valueOf(v);
        } catch (Throwable ignore) {
            LOGGER.info("Version intégrée à Spark");
        }

        return hiveVersion;
    }

    public static void printRestConfiguration(java.util.Properties props) {
        // ====== API REST perso (Javalin) ======
        String restHost = props.getProperty("advertisedhost", "localhost");
        int restPort = Integer.parseInt(props.getProperty("rest.port", "18080"));
        LOGGER.info("\n================ API REST : HDFS + SQL ================");
        LOGGER.info("REST API sur http://{}:{}/{}", restHost, restPort, JAVELIN_REST_SWAGGER);
        LOGGER.info("======================================================\n");
    }
}
