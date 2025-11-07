package com.minicluster.modules.hive;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Exemple de UDF Hive from your code.
 * Convertit une chaîne en MAJ.
 */
public class ToUpperUDF extends UDF {

    public Text evaluate(Text input) {
        if (input == null) {
            return null;
        }
        return new Text(input.toString().toUpperCase());
    }
}