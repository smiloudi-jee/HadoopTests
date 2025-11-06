package com.natixis.minicluster.modules.rest;

import io.javalin.core.util.RouteOverviewPlugin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static com.natixis.minicluster.util.HadoopConstants.*;

public final class RestBootstrap {

    private static String hdfsUri;
    private static int port;
    private static java.util.Properties props;


    private RestBootstrap() {}

    public static RestServiceHandle startRestApi(Properties properties,
                                                 String uri,
                                                 FileSystem fs,
                                                 SparkSession sparkSession) {

        props = properties;
        hdfsUri = uri;
        port = Integer.parseInt(props.getProperty(REST_PORT, "18080"));
        String cors = props.getProperty(REST_CORS, "*");

        io.javalin.Javalin app = io.javalin.Javalin.create(cfg -> {
            if ("*".equals(cors)) cfg.enableCorsForAllOrigins();
            cfg.enableDevLogging();
            cfg.defaultContentType = "application/json";
            cfg.registerPlugin(new RouteOverviewPlugin("/" + JAVELIN_REST_SWAGGER));
        });

        buildDefaultRoutes(app);
        buildHealthRoute(app, fs, sparkSession);
        buildGetListRoute(app, fs);
        buildCreateFileRoute(app, fs);
        buildGetFileRoute(app, fs);
        buildPostSqlRoutes(app, sparkSession);
        buildPostSqlRoutesWithMapResults(app, sparkSession);
        buildBootstrapRoute(app, sparkSession);

        app.start(port);
        return new RestServiceHandle(app, port);
    }


    // Route: Health check and info
    private static void buildDefaultRoutes(io.javalin.Javalin app) {
        app.get("/info", ctx -> ctx.json(new java.util.HashMap<String, Object>() {{
            put("hdfsUri", hdfsUri);
            put("nnHttp", Integer.parseInt(props.getProperty(NN_HTTP_PORT, "9870")));
            put("warehouse", props.getProperty(SPARK_SQL_WAREHOUSE_DIR, "hdfs:///user/hive/warehouse"));
            put("thriftEnabled", props.getProperty(THRIFT_ENABLED, "true"));
        }}));
    }

    private static void buildHealthRoute(io.javalin.Javalin app, FileSystem fs, SparkSession sparkSession) {
        app.get("/health", ctx -> {
            String hdfs = "DOWN", spark = "DOWN", hive = "DOWN";
            try {
                fs.listStatus(new org.apache.hadoop.fs.Path("/"));
                hdfs = "UP";
            } catch (Throwable ignore) {}

            try {
                sparkSession.range(1).count(); // ping driver/executor
                spark = "UP";
            } catch (Throwable ignore) {}

            try {
                String host = props.getProperty("advertisedhost", "localhost");
                int port = Integer.parseInt(props.getProperty("thrift.port", "10000"));
                java.net.Socket s = new java.net.Socket();
                try {
                    s.connect(new java.net.InetSocketAddress(host, port), 700);
                    hive = "UP";
                } finally {
                    try { s.close(); } catch (Exception ignore) {}
                }
            } catch (Throwable ignore) {}

            java.util.Map<String,Object> resp = new java.util.LinkedHashMap<String,Object>();
            resp.put("hdfs", hdfs);
            resp.put("spark", spark);
            resp.put("status", 200);
            resp.put("hiveServer", hive);
            ctx.json(resp);
        });
    }

    // Route: List files in a given HDFS path
    private static void buildGetListRoute(io.javalin.Javalin app, FileSystem fs){
        app.get("/hdfs/list", ctx -> {
            String path = ctx.queryParam("path");
            java.util.List<java.util.Map<String, Object>> out = new java.util.ArrayList<>();
            for (org.apache.hadoop.fs.FileStatus s : fs.listStatus(new org.apache.hadoop.fs.Path(path))) {
                java.util.Map<String, Object> m = new java.util.HashMap<>();
                m.put("path", s.getPath().toString());
                m.put("isDir", s.isDirectory());
                m.put("len", s.getLen());
                out.add(m);
            }
            ctx.json(out);
        });
    }

    // Route: Upload a file to HDFS
    private static void buildCreateFileRoute(io.javalin.Javalin app, FileSystem fs){
        // Upload simple (texte)
        app.post("/hdfs/create", ctx -> {
            String path = ctx.queryParam("path");
            if (path == null) {
                ctx.status(400).result("path manquant");
                return;
            }
            byte[] body = ctx.bodyAsBytes();
            try (org.apache.hadoop.fs.FSDataOutputStream out = fs.create(new org.apache.hadoop.fs.Path(path), true)) {
                out.write(body);
            }
            ctx.status(201).result("ok");
        });
    }

    // Route: Download a file from HDFS
    private static void buildGetFileRoute(io.javalin.Javalin app, FileSystem fs) {
        // Download simple (texte)
        app.get("/hdfs/get", ctx -> {
            String path = ctx.queryParam("path");
            if (path == null) {
                ctx.status(400).result("path manquant");
                return;
            }
            try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(new org.apache.hadoop.fs.Path(path))) {
                byte[] data = org.apache.commons.io.IOUtils.toByteArray(in);
                ctx.result(new java.io.ByteArrayInputStream(data));
            }
        });
    }

    // Route: Execute SQL query on Spark => POST with JSON body { "sql": "SELECT * FROM table" }
    private static void buildPostSqlRoutes(io.javalin.Javalin app, SparkSession sparkSession) {
        app.post("/sql", ctx -> {
            String sql = (String) ctx.bodyAsClass(java.util.Map.class).get("query");
            if (sql == null) {
                ctx.status(400).result("sql manquant");
                return;
            }
            java.util.List<org.apache.spark.sql.Row> rows = sparkSession.sql(sql).collectAsList();
            java.util.List<java.util.List<Object>> res = new java.util.ArrayList<>();
            for (org.apache.spark.sql.Row r : rows) {
                java.util.List<Object> line = new java.util.ArrayList<>(r.size());
                for (int i = 0; i < r.size(); i++) line.add(r.get(i));
                res.add(line);
            }
            ctx.json(res);
        });
    }

    private static void buildPostSqlRoutesWithMapResults(io.javalin.Javalin app, SparkSession sparkSession) {
        app.post("/sql/execute", ctx -> {
            String sql = ctx.body(); // text/plain
            if (sql != null && sql.trim().isEmpty()) {
                ctx.status(400).result("Empty SQL");
                return;
            }
            try {
                org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = sparkSession.sql(sql);
                java.util.List<org.apache.spark.sql.Row> rows = df.limit(100).collectAsList();
                java.util.List<java.util.List<Object>> data = new java.util.ArrayList<java.util.List<Object>>();
                for (org.apache.spark.sql.Row r : rows) {
                    java.util.List<Object> row = new java.util.ArrayList<Object>(r.size());
                    for (int i = 0; i < r.size(); i++) row.add(r.get(i));
                    data.add(row);
                }
                java.util.Map<String,Object> out = new java.util.LinkedHashMap<String,Object>();
                out.put("sql", sql);
                out.put("columns", java.util.Arrays.asList(df.columns()));
                out.put("count", rows.size());
                out.put("data", data);
                ctx.json(out);
            } catch (Throwable t) {
                ctx.status(500).result("SQL error: " + t.getMessage());
            }
        });
    }

    private static void buildBootstrapRoute(io.javalin.Javalin app, SparkSession sparkSession) {
        app.post("/bootstrap/sample", ctx -> {
            try {
                sparkSession.sql("CREATE DATABASE IF NOT EXISTS demo");
                sparkSession.sql("USE demo");
                sparkSession.sql("CREATE TABLE IF NOT EXISTS people(id INT, name STRING) STORED AS PARQUET");
                sparkSession.sql("INSERT INTO people VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')");
                long cnt = sparkSession.sql("SELECT COUNT(*) FROM people").collectAsList().get(0).getLong(0);
                ctx.json(java.util.Collections.singletonMap("rowcount", cnt));
            } catch (Throwable t) {
                ctx.status(500).result("Bootstrap failed: " + t.getMessage());
            }
        });
    }

}