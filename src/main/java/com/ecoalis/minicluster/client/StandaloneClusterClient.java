package com.ecoalis.minicluster.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Client for interacting with a standalone Hadoop cluster via WebHDFS, Hive JDBC, and REST API.
 */
public final class StandaloneClusterClient {
    private final String webhdfsBase; // ex: http://HOST:9870/webhdfs/v1
    private final String hiveUrl;     // ex: jdbc:hive2://HOST:10000/default;transportMode=binary
    private final String restBase;    // ex: http://HOST:18080

    public StandaloneClusterClient(String webhdfsBase, String hiveUrl, String restBase) {
        this.webhdfsBase = webhdfsBase;
        this.hiveUrl = hiveUrl;
        this.restBase = restBase;
    }

    // Petit fichier (non chunké)
    public void hdfsPutSmall(String path, String content) throws Exception {
        // 1) CREATE -> 307
        HttpURLConnection c1 = (HttpURLConnection) new URL(webhdfsBase + path + "?op=CREATE&overwrite=true").openConnection();
        c1.setInstanceFollowRedirects(false);
        c1.setRequestMethod("PUT");
        int code = c1.getResponseCode();
        if (code != 307 && code != 201) throw new RuntimeException("CREATE failed: " + code);
        String loc = c1.getHeaderField("Location");
        c1.disconnect();

        // 2) PUT data
        HttpURLConnection c2 = (HttpURLConnection) new URL(loc).openConnection();
        c2.setDoOutput(true);
        c2.setRequestMethod("PUT");
        try (OutputStream os = c2.getOutputStream()) {
            os.write(content.getBytes("UTF-8"));
        }
        int code2 = c2.getResponseCode();
        c2.disconnect();
        if (code2 != 201) throw new RuntimeException("PUT failed: " + code2);
    }

    // Gros fichiers (CHUNKED)
    public void hdfsPutChunked(String destPath, InputStream data, int chunkKB) throws Exception {
        // 1) CREATE -> redirection
        HttpURLConnection c1 = (HttpURLConnection) new URL(webhdfsBase + destPath + "?op=CREATE&overwrite=true").openConnection();
        c1.setInstanceFollowRedirects(false);
        c1.setRequestMethod("PUT");
        int code = c1.getResponseCode();
        if (code != 307 && code != 201) throw new RuntimeException("CREATE failed: " + code);
        String loc = c1.getHeaderField("Location");
        c1.disconnect();

        // 2) PUT chunked
        HttpURLConnection c2 = (HttpURLConnection) new URL(loc).openConnection();
        c2.setDoOutput(true);
        c2.setRequestMethod("PUT");
        c2.setChunkedStreamingMode(Math.max(1024, chunkKB * 1024));
        byte[] buf = new byte[64 * 1024];
        try (OutputStream os = c2.getOutputStream()) {
            int r;
            while ((r = data.read(buf)) != -1) {
                os.write(buf, 0, r);
            }
        }
        int code2 = c2.getResponseCode();
        c2.disconnect();
        if (code2 != 201) throw new RuntimeException("PUT chunked failed: " + code2);
    }

    public void hdfsDownload(String hdfsPath, OutputStream out) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(webhdfsBase + hdfsPath + "?op=OPEN").openConnection();
        c.setRequestMethod("GET");
        try (InputStream in = c.getInputStream()) {
            byte[] buf = new byte[64 * 1024];
            int r;
            while ((r = in.read(buf)) != -1) out.write(buf, 0, r);
        } finally { c.disconnect(); }
    }

    public String hdfsLs(String dir) throws Exception {
        String url = webhdfsBase + dir + "?op=LISTSTATUS";
        java.net.HttpURLConnection c = (java.net.HttpURLConnection) new java.net.URL(url).openConnection();
        c.setRequestMethod("GET");
        try (java.io.InputStream in = c.getInputStream()) {
            return new java.util.Scanner(in, "UTF-8").useDelimiter("\\A").next();
        } finally { c.disconnect(); }
    }

    // --- JDBC Hive (Spark ThriftServer) ---
    public java.sql.Connection openHive() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        java.util.Properties p = new java.util.Properties();
        // auth=noSasl si tu l’utilises ; sinon laisse vide pour auth par défaut
        return java.sql.DriverManager.getConnection(hiveUrl, p);
    }

    // --- REST SQL (ton API si tu exposes /sql/execute) ---
    public String restSql(String sql) throws Exception {
        java.net.URL u = new java.net.URL(restBase + "/sql/execute");
        java.net.HttpURLConnection c = (java.net.HttpURLConnection) u.openConnection();
        c.setRequestMethod("POST");
        c.setDoOutput(true);
        c.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
        try (java.io.OutputStream os = c.getOutputStream()) {
            os.write(sql.getBytes("UTF-8"));
        }
        int code = c.getResponseCode();
        java.io.InputStream in = (code >= 200 && code < 300) ? c.getInputStream() : c.getErrorStream();
        String body = new java.util.Scanner(in, "UTF-8").useDelimiter("\\A").next();
        c.disconnect();
        if (code < 200 || code >= 300) throw new RuntimeException("REST SQL error: " + code + " -> " + body);
        return body;
    }

    public String health() throws Exception {
        java.net.URL u = new java.net.URL(restBase + "/health");
        java.net.HttpURLConnection c = (java.net.HttpURLConnection) u.openConnection();
        c.setRequestMethod("GET");
        try (java.io.InputStream in = c.getInputStream()) {
            return new java.util.Scanner(in, "UTF-8").useDelimiter("\\A").next();
        } finally { c.disconnect(); }
    }
}