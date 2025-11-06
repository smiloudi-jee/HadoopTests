package com.natixis.minicluster.util;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class HdfsRecursiveListerTool {

    private HdfsRecursiveListerTool() {}

    public static List<String> listAllPaths(FileSystem fs, Path root) throws IOException {
        List<String> result = new ArrayList<>();
        recurse(fs, root, result);
        return result;
    }

    private static void recurse(FileSystem fs, Path path, List<String> acc) throws IOException {
        if (!fs.exists(path)) {
            return;
        }
        FileStatus[] stats = fs.listStatus(path);
        if (stats == null) return;

        for (FileStatus st : stats) {
            acc.add(st.getPath().toString() + (st.isDirectory() ? "/" : ""));
            if (st.isDirectory()) {
                recurse(fs, st.getPath(), acc);
            }
        }
    }
}