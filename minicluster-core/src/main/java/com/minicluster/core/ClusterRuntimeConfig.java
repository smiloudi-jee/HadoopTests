package com.minicluster.core;

import com.minicluster.util.HadoopConstants;

import java.util.Properties;

public final class ClusterRuntimeConfig {

    private final boolean enableSpark;
    private final boolean enableHive;
    private final boolean enableRest;
    private final boolean enableThrift; // lié à ton thrift.enabled
    private final boolean enableYarn;
    private final boolean enableMapReduce;
    private final boolean enableZookeeper; //
    private final boolean enableHBase;
    private final boolean enablePig;

    private final String baseDir;
    private final String advertisedHost;
    private final Properties props;

    private ClusterRuntimeConfig(Builder b) {
        this.enableSpark = b.enableSpark;
        this.enableHive = b.enableHive;
        this.enableRest = b.enableRest;
        this.enableThrift = b.enableThrift;
        this.enableYarn = b.enableYarn;
        this.enableMapReduce = b.enableMapReduce;
        this.enableZookeeper  = b.enableZookeeper;
        this.enableHBase      = b.enableHBase;
        this.enablePig = b.enablePig;

        this.baseDir = b.baseDir;
        this.advertisedHost = b.advertisedHost;
        this.props = b.props;
    }

    public boolean isSparkEnabled()   { return enableSpark; }
    public boolean isHiveEnabled()    { return enableHive; }
    public boolean isRestEnabled()    { return enableRest; }
    public boolean isThriftEnabled()  { return enableThrift; }
    public boolean isYarnEnabled() { return enableYarn; }
    public boolean isMapReduceEnabled() { return enableMapReduce; }
    public boolean isZookeeperEnabled() { return enableZookeeper; }
    public boolean isHBaseEnabled()     { return enableHBase; }
    public boolean isPigEnabled()     { return enablePig; }

    public String getBaseDir()        { return baseDir; }
    public String getAdvertisedHost() { return advertisedHost; }
    public Properties getProps()      { return props; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private boolean enableSpark = false;
        private boolean enableHive  = false;
        private boolean enableRest  = false;
        private boolean enableThrift= false;
        private boolean enableYarn  = false;
        private boolean enableMapReduce = false;
        private boolean enableZookeeper  = false; //
        private boolean enableHBase      = false;
        private boolean enablePig = false;

        private String baseDir      = HadoopConstants.DEFAULT_BASE_DIR;
        private String advertisedHost = null;
        private Properties props     = new Properties();

        public Builder withSpark() { this.enableSpark = true; return this; }
        public Builder withHive()  { this.enableHive  = true; return this; }
        public Builder withRest()  { this.enableRest  = true; return this; }
        public Builder withThrift(){ this.enableThrift= true; return this; }
        public Builder withYarn()    { this.enableYarn  = true; return this; }
        public Builder withMapReduce()  { this.enableMapReduce = true; return this; }
        // HBase fonctionne avec ZooKeeper.
        public Builder withHBase()      { this.enableHBase      = true; return withZookeeper(); }
        public Builder withZookeeper()  { this.enableZookeeper  = true; return this; }
        public Builder withPig()   { this.enablePig      = true; return this; }

        public Builder baseDir(String dir) {
            this.baseDir = dir;
            return this;
        }

        public Builder advertisedHost(String host) {
            this.advertisedHost = host;
            return this;
        }

        public Builder props(Properties p) {
            this.props = p;
            return this;
        }

        public ClusterRuntimeConfig build() {
            return new ClusterRuntimeConfig(this);
        }
    }
}