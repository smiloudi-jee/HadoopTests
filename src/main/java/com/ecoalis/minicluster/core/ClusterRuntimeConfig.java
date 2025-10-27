package com.ecoalis.minicluster.core;

import java.util.Properties;

public final class ClusterRuntimeConfig {

    private final boolean enableSpark;
    private final boolean enableHive;
    private final boolean enableRest;
    private final boolean enableThrift; // lié à ton thrift.enabled
    private final String baseDir;
    private final String advertisedHost;
    private final Properties props;

    private ClusterRuntimeConfig(Builder b) {
        this.enableSpark = b.enableSpark;
        this.enableHive = b.enableHive;
        this.enableRest = b.enableRest;
        this.enableThrift = b.enableThrift;
        this.baseDir = b.baseDir;
        this.advertisedHost = b.advertisedHost;
        this.props = b.props;
    }

    public boolean isSparkEnabled()   { return enableSpark; }
    public boolean isHiveEnabled()    { return enableHive; }
    public boolean isRestEnabled()    { return enableRest; }
    public boolean isThriftEnabled()  { return enableThrift; }

    public String getBaseDir()        { return baseDir; }
    public String getAdvertisedHost() { return advertisedHost; }
    public Properties getProps()      { return props; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private boolean enableSpark = false;
        private boolean enableHive  = false;
        private boolean enableRest  = false;
        private boolean enableThrift= false;
        private String baseDir      = com.ecoalis.minicluster.util.HadoopConstants.DEFAULT_BASE_DIR;
        private String advertisedHost = null;
        private Properties props     = new Properties();

        public Builder withSpark() {
            this.enableSpark = true;
            return this;
        }
        public Builder withHive()  { this.enableHive  = true; return this; }
        public Builder withRest()  { this.enableRest  = true; return this; }
        public Builder withThrift(){ this.enableThrift= true; return this; }

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