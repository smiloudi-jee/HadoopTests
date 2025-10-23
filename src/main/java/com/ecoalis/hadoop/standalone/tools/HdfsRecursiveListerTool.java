package com.ecoalis.hadoop.standalone.tools;

import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsRecursiveListerTool {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(HdfsRecursiveListerTool.class);

    public static void listRecursive(FileSystem fs, Path root) throws Exception {
        listRecursive(fs, root, 0);
    }

    // --- Implémentation interne ---
    private static void listRecursive(FileSystem fs, Path dir, int depth) throws Exception {
        // Affiche le répertoire courant
        if (depth == 0) {
            LOGGER.info("{} [ROOT]", dir);
        }

        RemoteIterator<FileStatus> it = fs.listStatusIterator(dir);

        while (it.hasNext()) {
            FileStatus st = it.next();
            Path p = st.getPath();
            String indent = repeat("  ", depth + 1); // indentation lisible

            try {
                if (st.isSymlink()) {
                    // Évite les cycles potentiels via liens symboliques
                    LOGGER.info("{}- {} [SYMLINK -> {}]", indent, p, st.getSymlink());
                    continue;
                }

                if (st.isDirectory()) {
                    LOGGER.info("{}- {} [DIR]", indent, p);
                    // Descente récursive
                    if (depth < Integer.MAX_VALUE) {
                        listRecursive(fs, p, depth + 1);
                    }
                } else {
                    LOGGER.info("{}- {} [FILE] ({} bytes)", indent, p, st.getLen());
                }
            } catch (Exception e) {
                LOGGER.info("{}! Erreur sur {} -> {}", indent, p, e.getMessage());
            }
        }
    }

    // Petit helper pour indentations
    private static String repeat(String s, int n) {
        StringBuilder sb = new StringBuilder(s.length() * n);
        for (int i = 0; i < n; i++) sb.append(s);
        return sb.toString();
    }
}
