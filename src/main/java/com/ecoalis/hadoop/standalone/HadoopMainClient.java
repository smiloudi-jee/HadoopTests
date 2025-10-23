package com.ecoalis.hadoop.standalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.net.URI;
import java.sql.*;
import java.util.Properties;

/**
 * Client de démonstration Java 8 pour consommer un MiniCluster :
 * - HDFS via API Java
 * - WebHDFS (REST natif) via HttpClient
 * - Hive / Spark SQL via JDBC (Spark Thrift Server)
 * - API REST perso (Javalin)
 *
 * Usage (exemples) :
 *   java EnvClient hdfs ls /
 *   java EnvClient hdfs put /client-demo/hello.txt "Bonjour depuis le client"
 *   java EnvClient hdfs cat /client-demo/hello.txt
 *
 *   java EnvClient webhdfs ls /
 *
 *   java EnvClient jdbc query "select 1 as x"
 *
 *   java EnvClient rest get /health
 *   java EnvClient rest post /sql/query "{\"sql\":\"select 1\"}"
 */
public class HadoopMainClient {
    // ---- Paramètres par défaut (adapter si besoin) ----
    private static final String HDFS_URI = System.getProperty("env.hdfs.uri", "hdfs://192.168.1.104:20112");
    private static final String HDFS_USER = System.getProperty("env.hdfs.user", "hp12");

    private static final String NN_HTTP_HOST = System.getProperty("env.nn.host", "192.168.1.104");
    private static final int    NN_HTTP_PORT = Integer.getInteger("env.nn.http.port", 9870); // WebHDFS passe par 9870

    private static final String THRIFT_HOST = System.getProperty("env.thrift.host", "localhost");
    private static final int    THRIFT_PORT = Integer.getInteger("env.thrift.port", 10000);
    // Spark Thrift Server par défaut en noSasl/binary dans ton setup
    private static final String HIVE_JDBC_URL =
            System.getProperty("env.hive.jdbc",
                    "jdbc:hive2://" + THRIFT_HOST + ":" + THRIFT_PORT + "/default;transportMode=binary");

    private static final String REST_HOST = System.getProperty("rest.host", "localhost");
    private static final int    REST_PORT = Integer.getInteger("rest.port", 18080);

    // ---- Entrée du programme ----
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printHelp();
            return;
        }
        String area = args[0];

        if ("hdfs".equalsIgnoreCase(area)) {
            runHdfs(args);
        } else if ("webhdfs".equalsIgnoreCase(area)) {
            runWebHdfs(args);
        } else if ("jdbc".equalsIgnoreCase(area)) {
            runJdbc(args);
        } else if ("rest".equalsIgnoreCase(area)) {
            runRest(args);
        } else {
            printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("EnvClient - commandes :\n" +
                "  -------------------- HDFS (API Java) :\n" +
                "  hdfs ls <path>\n" +
                "  hdfs put <path> <text>\n" +
                "  hdfs cat <path>\n" +
                "  -------------------- WEB-HDFS :\n" +
                "  webhdfs ls <path>\n" +
                "  -------------------- JDBC QUERY :\n" +
                "  jdbc query <SQL>\n" +
                "  -------------------- REST API :\n" +
                "  rest get <path>\n" +
                "  rest post <path> <jsonBody>\n"
        );
    }

    // =============== HDFS (API Java) ===============

    private static void runHdfs(String[] args) throws Exception {
        if (args.length < 3) { printHelp(); return; }
        String cmd = args[1];
        String path = args[2];

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", HDFS_URI);
        conf.set("dfs.client.use.datanode.hostname", "true"); // pratique en local/Win
        // Pas d'auth Kerberos ici (SIMPLE)
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), conf, HDFS_USER);

        try {
            if ("ls".equalsIgnoreCase(cmd)) {
                hdfsLs(fs, new Path(path));
            } else if ("put".equalsIgnoreCase(cmd)) {
                if (args.length < 4) { System.err.println("hdfs put <path> <text>"); return; }
                String text = args[3];
                hdfsPutString(fs, new Path(path), text);
                System.out.println("Écrit: " + path);
            } else if ("cat".equalsIgnoreCase(cmd)) {
                hdfsCat(fs, new Path(path));
            } else {
                printHelp();
            }
        } finally {
            fs.close();
        }
    }

    private static void hdfsLs(FileSystem fs, Path p) throws IOException {
        FileStatus[] stats = fs.listStatus(p);
        for (FileStatus st : stats) {
            String type = st.isDirectory() ? "[DIR]" : "[FILE]";
            System.out.println(" - " + st.getPath().toString() + " " + type + " size=" + st.getLen());
        }
    }

    private static void hdfsPutString(FileSystem fs, Path p, String content) throws IOException {
        // crée les dossiers parents si besoin
        Path parent = p.getParent();
        if (parent != null && !fs.exists(parent)) {
            fs.mkdirs(parent);
        }
        try (FSDataOutputStream out = fs.create(p, true);
             OutputStreamWriter w = new OutputStreamWriter(out, "UTF-8")) {
            w.write(content);
        }
    }

    private static void hdfsCat(FileSystem fs, Path p) throws IOException {
        try (FSDataInputStream in = fs.open(p);
             BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"))) {
            String line; while ((line = br.readLine()) != null) System.out.println(line);
        }
    }

    // =============== WebHDFS (REST natif HDFS) ===============

    private static void runWebHdfs(String[] args) throws Exception {
        if (args.length < 3) { printHelp(); return; }
        String cmd = args[1];
        String path = args[2];

        if ("ls".equalsIgnoreCase(cmd)) {
            webhdfsListStatus(path);
        } else {
            printHelp();
        }
    }

    private static void webhdfsListStatus(String path) throws Exception {
        // NameNode HTTP endpoint
        // NB: WebHDFS redirige vers le DataNode. HttpClient suit les redirections par défaut.
        String url = "http://" + NN_HTTP_HOST + ":" + NN_HTTP_PORT +
                "/webhdfs/v1" + norm(path) + "?op=LISTSTATUS";

        RequestConfig rc = RequestConfig.custom()
                .setConnectTimeout(5000).setSocketTimeout(15000).build();

        try (CloseableHttpClient http = HttpClients.custom()
                .setDefaultRequestConfig(rc).build()) {

            HttpGet get = new HttpGet(url);
            HttpResponse resp = http.execute(get);
            int code = resp.getStatusLine().getStatusCode();
            System.out.println("HTTP " + code);
            String body = readAll(resp.getEntity().getContent());
            System.out.println(body);
        }
    }

    // =============== JDBC (Hive / Spark SQL via ThriftServer) ===============

    private static void runJdbc(String[] args) throws Exception {
        if (args.length < 3) { printHelp(); return; }
        String cmd = args[1];
        if (!"query".equalsIgnoreCase(cmd)) { printHelp(); return; }
        String sql = args[2];

        // Charge le driver Hive JDBC 2.3.x
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        try (Connection cnx = DriverManager.getConnection(HIVE_JDBC_URL);
             Statement st = cnx.createStatement()) {

            boolean hasResult = st.execute(sql);
            if (hasResult) {
                try (ResultSet rs = st.getResultSet()) {
                    ResultSetMetaData md = rs.getMetaData();
                    int n = md.getColumnCount();
                    // header
                    StringBuilder head = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) head.append("\t");
                        head.append(md.getColumnLabel(i));
                    }
                    System.out.println(head.toString());
                    // rows
                    while (rs.next()) {
                        StringBuilder row = new StringBuilder();
                        for (int i = 1; i <= n; i++) {
                            if (i > 1) row.append("\t");
                            row.append(String.valueOf(rs.getObject(i)));
                        }
                        System.out.println(row.toString());
                    }
                }
            } else {
                int updated = st.getUpdateCount();
                System.out.println("OK (" + updated + " ligne(s) affectée(s))");
            }
        }
    }

    // =============== API REST perso (Javalin) ===============

    private static void runRest(String[] args) throws Exception {
        if (args.length < 3) { printHelp(); return; }
        String method = args[1];
        String path = args[2];

        if ("get".equalsIgnoreCase(method)) {
            restGet(path);
        } else if ("post".equalsIgnoreCase(method)) {
            if (args.length < 4) { System.err.println("rest post <path> <jsonBody>"); return; }
            restPost(path, args[3]);
        } else {
            printHelp();
        }
    }

    private static void restGet(String path) throws Exception {
        String url = "http://" + REST_HOST + ":" + REST_PORT + norm(path);
        RequestConfig rc = RequestConfig.custom()
                .setConnectTimeout(3000).setSocketTimeout(15000).build();

        try (CloseableHttpClient http = HttpClients.custom()
                .setDefaultRequestConfig(rc).build()) {

            HttpGet get = new HttpGet(url);
            HttpResponse resp = http.execute(get);
            System.out.println("HTTP " + resp.getStatusLine().getStatusCode());
            System.out.println(readAll(resp.getEntity().getContent()));
        }
    }

    private static void restPost(String path, String json) throws Exception {
        String url = "http://" + REST_HOST + ":" + REST_PORT + norm(path);
        RequestConfig rc = RequestConfig.custom()
                .setConnectTimeout(3000).setSocketTimeout(15000).build();

        try (CloseableHttpClient http = HttpClients.custom()
                .setDefaultRequestConfig(rc).build()) {

            HttpPost post = new HttpPost(url);
            post.setHeader("Content-Type", "application/json");
            post.setEntity(new StringEntity(json, "UTF-8"));

            HttpResponse resp = http.execute(post);
            System.out.println("HTTP " + resp.getStatusLine().getStatusCode());
            System.out.println(readAll(resp.getEntity().getContent()));
        }
    }

    // =============== Helpers ===============

    private static String norm(String p) {
        if (p == null || p.isEmpty()) return "/";
        return p.startsWith("/") ? p : "/" + p;
    }

    private static String readAll(InputStream in) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"))) {
            StringBuilder sb = new StringBuilder();
            String line; while ((line = br.readLine()) != null) sb.append(line).append('\n');
            return sb.toString();
        }
    }
}
