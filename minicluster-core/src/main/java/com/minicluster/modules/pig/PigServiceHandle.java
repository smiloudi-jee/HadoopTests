package com.minicluster.modules.pig;

import org.apache.pig.PigServer;

import java.net.URLClassLoader;
import java.util.List;

/**
 * Handle simple pour exécuter des scripts Pig.
 */
public class PigServiceHandle implements AutoCloseable {

    private final PigServer pig;
    private final String hdfsUri;
    private final URLClassLoader injectedCl;   // NEW
    private final ClassLoader previousCl;      // NEW

    public PigServiceHandle(PigServer pig, String hdfsUri) {
        this(pig, hdfsUri, null, null);
    }

    public PigServiceHandle(PigServer pig, String hdfsUri, URLClassLoader injectedCl, ClassLoader previousCl) {
        this.pig = pig;
        this.hdfsUri = hdfsUri;
        this.injectedCl = injectedCl;
        this.previousCl = previousCl;
    }

    public PigServer getPigServer() { return pig; }
    public String getHdfsUri() { return hdfsUri; }

    public void runScript(String script) throws Exception {
        String[] lines = script.split("\\r?\\n");
        for (String l : lines) {
            String t = l.trim();
            if (t.isEmpty() || t.startsWith("--")) continue;
            pig.registerQuery(t);
        }
        pig.executeBatch();
        pig.discardBatch();
    }

    public void runStatements(List<String> statements) throws Exception {
        for (String s : statements) {
            String t = s.trim();
            if (t.isEmpty() || t.startsWith("--")) continue;
            pig.registerQuery(t);
        }
        pig.executeBatch();
        pig.discardBatch();
    }

    @Override
    public void close() {
        try { pig.shutdown(); } catch (Exception ignore) {}
        // Restaure le classloader si on l'a changé
        if (previousCl != null) {
            try { Thread.currentThread().setContextClassLoader(previousCl); } catch (Exception ignore) {}
        }
        if (injectedCl != null) {
            try { injectedCl.close(); } catch (Exception ignore) {}
        }
    }
}