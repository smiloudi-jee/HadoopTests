package com.minicluster.util;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public final class StarterUtils {

    private StarterUtils() {}

    public static void ensureDirectory(java.nio.file.Path dir) throws IOException {
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }
    }

    public static void deleteRecursively(java.nio.file.Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public @NotNull FileVisitResult visitFile(java.nio.file.Path file, @NotNull BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public @NotNull FileVisitResult postVisitDirectory(java.nio.file.Path d, IOException exc)
                    throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Check qu'un port est libre sur l'interface HOST.
     */
    public static void ensurePortFree(String host, int port, String label) {
        try (ServerSocket s = new ServerSocket()) {
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress(host.equals("0.0.0.0") ? "127.0.0.1" : host, port));
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Port déjà utilisé pour " + label + " [" + host + ":" + port + "]", e);
        }
    }
}