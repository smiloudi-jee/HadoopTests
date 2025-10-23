package com.ecoalis.hadoop.standalone;

import com.ecoalis.hadoop.standalone.configuration.MiniClusterStandalone;

public class Starter {
    public static void main(String[] args) throws Exception {
        System.out.println("Hadoop Standalone Starter");
        MiniClusterStandalone.starter(null);
    }
}
