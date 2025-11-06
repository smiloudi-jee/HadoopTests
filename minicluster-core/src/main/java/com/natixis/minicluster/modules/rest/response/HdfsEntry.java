package com.natixis.minicluster.modules.rest.response;

/**
 * Représente une entrée renvoyée par l'endpoint /hdfs/ls
 */
public class HdfsEntry {

    private String path;
    private long len;
    private boolean isDir;

    // Constructeur vide requis par Jackson
    public HdfsEntry() {}

    public HdfsEntry(String path, long len, boolean isDir) {
        this.path = path;
        this.len = len;
        this.isDir = isDir;
    }

    public String getPath() {
        return path;
    }

    public long getLen() {
        return len;
    }

    public boolean getIsDir() {
        return isDir;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setLen(long len) {
        this.len = len;
    }

    public void setIsDir(boolean dir) {
        isDir = dir;
    }

    @Override
    public String toString() {
        return "HdfsEntry{" +
                "path='" + path + '\'' +
                ", len=" + len +
                ", isDir=" + isDir +
                '}';
    }
}
