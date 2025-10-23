package com.ecoalis.hadoop.standalone.tools;

import java.io.IOException;

public class StarterUtils {
    // Supprime récursivement un répertoire (Java 8)
    public static void deleteRecursively(java.nio.file.Path p) throws IOException {
        if (p == null || !java.nio.file.Files.exists(p)) return;
        java.nio.file.Files.walk(p)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(path -> {
                    try { java.nio.file.Files.delete(path); } catch (IOException ignore) {}
                });
    }

    // S’assure qu’un répertoire existe
    public static void ensureDir(java.nio.file.Path p) throws IOException {
        if (!java.nio.file.Files.exists(p)) {
            java.nio.file.Files.createDirectories(p);
        }
    }

    // Attend qu’un port soit ouvert (serveur prêt)
    public static void waitForPortOpen(String host, int port, long timeoutMs) throws IOException, InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        IOException last = null;
        while (System.currentTimeMillis() < end) {
            try (java.net.Socket s = new java.net.Socket()) {
                s.connect(new java.net.InetSocketAddress(host, port), 500);
                return;
            } catch (IOException e) {
                last = e;
                Thread.sleep(200);
            }
        }
        throw (last != null) ? last : new IOException("Timeout en attente de l’ouverture du port " + host + ":" + port);
    }
}
