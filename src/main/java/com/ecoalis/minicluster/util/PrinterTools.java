package com.ecoalis.minicluster.util;

import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public final class PrinterTools {

    private PrinterTools() {}

    public static void printGlobalConfiguration(String hdfsUri,
                                                String host,
                                                Properties props) {
        System.out.println("=== MiniCluster Global Configuration ===");
        System.out.println("Advertised Host           : " + host);
        System.out.println("HDFS URI                  : " + hdfsUri);

        if (props != null) {
            System.out.println("--- mini-cluster.properties ---");
            props.forEach((k, v) -> System.out.println("  " + k + " = " + v));
        }
        System.out.println("========================================");
    }

    public static void printSparkConfiguration(SparkSession spark,
                                               Properties props) {
        if (spark == null) return;

        System.out.println("=== Spark Runtime Configuration ===");
        System.out.println("Spark Version : " + spark.version());
        System.out.println("App Name      : " + spark.sparkContext().appName());
        System.out.println("Master        : " + spark.sparkContext().master());

        if (props != null) {
            System.out.println("--- Spark/Hive props ---");
            props.forEach((k, v) -> {
                String key = k.toString().toLowerCase();
                if (key.startsWith("spark.") || key.startsWith("hive.") || key.startsWith("javax.jdo.")) {
                    System.out.println("  " + k + " = " + v);
                }
            });
        }
        System.out.println("====================================");
    }
}