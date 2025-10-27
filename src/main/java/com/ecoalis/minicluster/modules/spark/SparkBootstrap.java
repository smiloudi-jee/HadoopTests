package com.ecoalis.minicluster.modules.spark;
import com.ecoalis.minicluster.modules.hive.HiveServiceHandle;
import com.ecoalis.minicluster.util.HadoopConstants;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

import static com.ecoalis.minicluster.util.HadoopConstants.*;

public final class SparkBootstrap {

    private SparkBootstrap() {}

    public static SparkServiceHandle startSpark(Properties props,
                                                String hdfsUri,
                                                Path workDir,
                                                Optional<HiveServiceHandle> hiveHandle,
                                                boolean thriftEnabled) {

        final String appName   = props.getProperty(APP_SPARK_HIVE_NAME, HadoopConstants.DEFAULT_SPARK_APP_NAME);
        final String master    = props.getProperty(SPARK_MASTER, HadoopConstants.DEFAULT_SPARK_MASTER);
        final String logLevel  = props.getProperty(SPARK_LOG_LEVEL, HadoopConstants.DEFAULT_SPARK_LOGLEVEL);

        // où Spark pense que le warehouse est
        final String hiveWarehousePath = props.getProperty(HIVE_WAREHOUSE_DIR,
                HadoopConstants.DEFAULT_HIVE_WAREHOUSE_DIR);

        // metastore local derby
        final java.nio.file.Path metaDir = workDir.resolve(HadoopConstants.METASTORE_DIR_NAME);
        final String derbyDbPath = metaDir.resolve("metastore_db").toString().replace('\\', '/');

        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName)
                .master(master)

                // HDFS comme FS par défaut
                .config("spark.hadoop.fs.defaultFS", hdfsUri)

                // Hive warehouse / Spark SQL warehouse
                .config(SPARK_SQL_WAREHOUSE_DIR, hdfsUri + hiveWarehousePath)
                .config("hive.metastore.warehouse.dir", hdfsUri + hiveWarehousePath)

                // Metastore Derby local
                .config(JAVAX_JDO_OPTION_CONNECTION_URL, "jdbc:derby:" + derbyDbPath + ";create=true")
                .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")

                // Compat, bootstrap metastore
                .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

                // Auto-création du schéma Hive
                .config(DATANUCLEUS_AUTO_CREATE_SCHEMA,
                        props.getProperty(DATANUCLEUS_AUTO_CREATE_SCHEMA, "true"))
                .config(HIVE_METASTORE_SCHEMA_VERIFICATION,
                        props.getProperty(HIVE_METASTORE_SCHEMA_VERIFICATION, "false"))

                // Catalog Hive
                .config(SPARK_SQL_CATALOG_IMPLEMENTATION,
                        props.getProperty(SPARK_SQL_CATALOG_IMPLEMENTATION, "hive"))

                // scratchdir Hive
                .config("hive.exec.scratchdir", "/tmp/hive")

                // pour les clients cross host
                .config(SPARK_HADOOP_DFS_CLIENT_USE_DN_HOSTNAME,
                        props.getProperty(SPARK_HADOOP_DFS_CLIENT_USE_DN_HOSTNAME, "true"))

                .enableHiveSupport();

        SparkSession spark = builder.getOrCreate();

        // UDF intégrée "to_upper"
        registerBuiltInUdfs(spark);

        spark.sparkContext().setLogLevel(logLevel);

        // Force init metastore (ce que tu fais dans MiniClusterStandalone)
        try {
            spark.sql("CREATE DATABASE IF NOT EXISTS default");
            spark.sql("CREATE TABLE IF NOT EXISTS default._probe_(id INT)");
            spark.sql("DROP TABLE IF EXISTS default._probe_");
        } catch (Exception e) {
            // pas bloquant
        }

        // UDF permanente dans le metastore Hive (optionnel)
        try {
            spark.sql(
                    "CREATE OR REPLACE FUNCTION default.to_upper " +
                            "AS 'com.ecoalis.hadoop.standalone.udf.ToUpperUDF'"
            );
        } catch (Exception ignore) {
        }

        // Thrift Server JDBC, si demandé
        if (thriftEnabled) {
            startThriftServer(props, spark);
        }

        return new SparkServiceHandle(spark);
    }

    private static void registerBuiltInUdfs(SparkSession spark) {
        org.apache.spark.sql.api.java.UDF1<String, String> toUpper =
                s -> s == null ? null : s.toUpperCase();
        spark.udf().register("to_upper",
                toUpper,
                org.apache.spark.sql.types.DataTypes.StringType);
    }

    private static void startThriftServer(Properties props, SparkSession spark) {
        try {
            System.setProperty(HIVE_SERVER2_THRIFT_PORT,
                    props.getProperty(THRIFT_PORT, "10000"));
            System.setProperty("hive.server2.thrift.bind.host", "0.0.0.0");
            System.setProperty("hive.server2.transport.mode", "binary");

            org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
                    .startWithContext(spark.sqlContext());
            System.out.println("Thrift Server JDBC sur port " +
                    props.getProperty(THRIFT_PORT, "10000"));
        } catch (Throwable t) {
            System.err.println("Thrift Server non démarré: " + t.getMessage());
        }
    }
}
