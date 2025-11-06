package com.natixis.minicluster.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/**
 * Exemple de job Spark "métier".
 * Lit un CSV d'entrée, agrège les montants par pays,
 * écrit le résultat (par pays, total_amount) dans un dossier de sortie.
 */
public final class SalesAggregationJob {

    private SalesAggregationJob() {}

    public static void run(SparkSession spark,
                           String inputPathHdfs,
                           String outputPathHdfs) {

        // Lire les données sources
        Dataset<Row> inputDf = spark.read()
                .option("header", "true")
                .schema("country STRING, amount DOUBLE")
                .csv(inputPathHdfs);

        // Agrégation métier
        Dataset<Row> aggregated = inputDf
                .groupBy(col("country"))
                .agg(sum("amount").alias("total_amount"))
                .orderBy(col("country"));

        // Ecriture du résultat. Ici on choisit CSV header=true,
        // mais tu peux switcher en Parquet si c'est ce que vous faites en vrai.
        aggregated.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPathHdfs);
    }
}