package com.natixis.minicluster.modules.rest.response;

/**
 * POJO pour désérialiser la réponse JSON de /health
 */
public class RestHealthResponse {

    private String status;
    private String hdfs;
    private String spark;
    private String hiveServer;

    // Constructeur sans argument obligatoire pour Jackson
    public RestHealthResponse() {
    }

    public RestHealthResponse(String status, String hdfs, String spark, String hiveServer) {
        this.status = status;
        this.hdfs = hdfs;
        this.spark = spark;
        this.hiveServer = hiveServer;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getHdfs() {
        return hdfs;
    }

    public void setHdfs(String hdfsUri) {
        this.hdfs = hdfsUri;
    }

    public String getSpark() {
        return spark;
    }

    public void setSpark(String spark) {
        this.spark = spark;
    }

    public String getHiveServer() {
        return hiveServer;
    }

    public void setHiveServer(String hiveServer) {
        this.hiveServer = hiveServer;
    }

    @Override
    public String toString() {
        return "RestHealthResponse{" +
                "status='" + status + '\'' +
                ", hdfs='" + hdfs + '\'' +
                ", spark='" + spark + '\'' +
                ", hiveServer='" + hiveServer + '\'' +
                '}';
    }
}