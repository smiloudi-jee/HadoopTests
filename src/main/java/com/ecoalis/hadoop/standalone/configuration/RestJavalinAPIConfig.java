package com.ecoalis.hadoop.standalone.configuration;

import io.javalin.core.util.RouteOverviewPlugin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ecoalis.hadoop.standalone.configuration.HadoopConfiguration.*;

public class RestJavalinAPIConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestJavalinAPIConfig.class);

    private final String hdfsUri;
    private final java.util.Properties props;
    private static RestJavalinAPIConfig config;

    // Constructeur privé pour singleton
    private RestJavalinAPIConfig(String hdfsUri, SparkSession sparkSession, FileSystem fs, java.util.Properties props) {
        this.hdfsUri = hdfsUri;
        this.props = props;

        int port = Integer.parseInt(props.getProperty(REST_PORT, "18080"));
        String cors = props.getProperty(REST_CORS, "*");

        io.javalin.Javalin app = io.javalin.Javalin.create(cfg -> {
            if ("*".equals(cors)) cfg.enableCorsForAllOrigins();
            cfg.enableDevLogging();
            cfg.defaultContentType = "application/json";
            cfg.registerPlugin(new RouteOverviewPlugin("/" + JAVELIN_REST_SWAGGER));
        }).start(port);

        buildDefaultRoutes(app);
        buildGetListRoute(app, fs);
        buildCreateFileRoute(app, fs);
        buildGetFileRoute(app, fs);
        buildPostSqlRoutes(app, sparkSession);
    }

    // Route: Health check and info
    private void buildDefaultRoutes(io.javalin.Javalin app) {
        app.get("/health", ctx -> ctx.json(java.util.Collections.singletonMap("status", "ok")));
        app.get("/info", ctx -> ctx.json(new java.util.HashMap<String, Object>() {{
            put("hdfsUri", hdfsUri);
            put("nnHttp", Integer.parseInt(props.getProperty(NN_HTTP_PORT, "9870")));
            put("warehouse", props.getProperty(SPARK_SQL_WAREHOUSE_DIR, "hdfs:///user/hive/warehouse"));
            put("thriftEnabled", props.getProperty(THRIFT_ENABLED, "true"));
        }}));
    }

    // Route: List files in a given HDFS path
    private void buildGetListRoute(io.javalin.Javalin app, FileSystem fs){
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
    private void buildCreateFileRoute(io.javalin.Javalin app, FileSystem fs){
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
    private void buildGetFileRoute(io.javalin.Javalin app, FileSystem fs) {
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
    private void buildPostSqlRoutes(io.javalin.Javalin app, SparkSession sparkSession) {
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

    public static void config(String hdfsUri, SparkSession sparkSession, FileSystem fs, java.util.Properties props) {
        if(config == null) {
            config = new RestJavalinAPIConfig(hdfsUri, sparkSession, fs, props);
        } else {
            LOGGER.info("RestJavalinAPIConfig is already configured.");
        }
    }
}
