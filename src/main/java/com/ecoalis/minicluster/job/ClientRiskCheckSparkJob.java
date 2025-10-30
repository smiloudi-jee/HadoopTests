package com.ecoalis.minicluster.job;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public final class ClientRiskCheckSparkJob {

    private ClientRiskCheckSparkJob() {}

    /**
     * Client Risk
     * * Lis les positions publiées par le provider (ex : /provider/positions.csv)
     * * Calcule l'exposition totale par desk
     * * Ajoute un label "limit_breached" si le total dépasse un seuil donné
     * * Écrit le résultat dans outputPathHdfs en CSV.
     */
    public static void run(SparkSession spark,
                           String providerInputPathHdfs,
                           String outputPathHdfs,
                           double limitThreshold) {

        // provider dataset (snapshot)
        Dataset<Row> positions = spark.read()
                .option("header", "true")
                .schema("trade_id STRING, desk STRING, notional DOUBLE, currency STRING")
                .csv(providerInputPathHdfs);

        // agrégation par desk
        Dataset<Row> exposureByDesk = positions
                .groupBy(col("desk"))
                .agg(sum("notional").alias("total_notional"))
                .withColumn("limit_breached",
                        col("total_notional").gt(lit(limitThreshold)))
                .orderBy(col("desk"));

        exposureByDesk
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPathHdfs);
    }
}