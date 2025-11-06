package com.natixis.minicluster.standalone.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class TestHelper {

    private static final String TCP = "tcp";
    private static final String HTTP = "http";
    private static final int TIMEOUT_MS = 1200;
    private static final int READ_TIMEOUT_MS = 1200;
    private static final int HTTP_OK_THRESHOLD = 0; // on veut juste “ça répond”

    public static boolean isReachable(String scheme, String host, int port) {
        if (TCP.equalsIgnoreCase(scheme)) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), TIMEOUT_MS);
                return true;
            } catch (IOException e) {
                return false;
            }
        }
        if (HTTP.equalsIgnoreCase(scheme)) {
            try {
                URL u = new URL(HTTP + "://" + host + ":" + port + "/");
                HttpURLConnection c = (HttpURLConnection) u.openConnection();
                c.setConnectTimeout(TIMEOUT_MS);
                c.setReadTimeout(READ_TIMEOUT_MS);
                c.setRequestMethod("GET");
                int code = c.getResponseCode();
                return code > HTTP_OK_THRESHOLD; // on veut juste “ça répond”
            } catch (IOException e) {
                return false;
            }
        }
        return false;
    }

    public static String norm(String p) {
        if (p == null || p.isEmpty()) return "/";
        return p.startsWith("/") ? p : "/" + p;
    }

    public static String readAll(InputStream in) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line; while ((line = br.readLine()) != null) sb.append(line).append('\n');
            return sb.toString();
        }
    }

}
