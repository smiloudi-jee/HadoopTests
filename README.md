<h1 align="center">🧩 HadoopTests</h1>

<p align="center">
  <strong>Complete Hadoop mini-ecosystem for development, testing, integration, and learning</strong><br>
  <em>Based on the hadoop-minicluster library</em>
</p>

---

## 🎯 Overview

The **HadoopTests** project allows you to recreate a **complete Hadoop environment** locally to run unit, integration, and functional tests in real conditions, without heavy infrastructure.  
It relies on the **hadoop-minicluster** library and provides several modules to illustrate different use cases.

### Supported Technologies

- **HDFS** - Hadoop Distributed File System
- **YARN** - Resource management
- **MapReduce** - Distributed data processing
- **HBase** - NoSQL database
- **Spark** - Fast data processing engine (v3.5.1)
- **Hive** - SQL-like data warehouse
- **Pig** - High-level data flow scripting
- **REST API** - HTTP interface for cluster interaction

---

## 🧱 Project Structure

| Module | Description |
|--------|--------------|
| 🧩 **minicluster-core** | Core system: manages the MiniCluster lifecycle (HDFS, YARN, Hive, Spark, HBase, etc.) |
| 🚀 **app** | Sample client application consuming the cluster (HDFS ingestion, Hive and Spark queries) |
| 🧪 **app-test** | Unit and integration test suites covering Hadoop components |

---

## ⚙️ Prerequisites

- ☕ **JDK 8** or higher
- 🧰 **Maven 3.6+**
- 💾 At least **4GB RAM** for running embedded clusters
- 💻 **Linux / macOS / Windows**

---

## 📚 Technology Stack

| Technology | Version |
|------------|---------|
| Hadoop | 3.3.6 |
| Spark | 3.5.1 (Scala 2.12) |
| HBase | 2.5.12-hadoop3 |
| Pig | 0.17.0 |
| Javalin | 4.6.4 |
| JUnit | 5.10.2 |

---

## 🚀 Quick Start

### Clone and Build

```bash
# Clone the repository
git clone https://github.com/smiloudi-jee/HadoopTests.git
cd HadoopTests

# Build the entire project
mvn clean install

# Build without running tests
mvn clean install -DskipTests

# Build a specific module
cd minicluster-core
mvn clean install
```

### Run Tests

```bash
# Run all tests
mvn test

# Run integration tests only
mvn verify

# Run a specific test class
mvn test -Dtest=MapReduceIT
```

### Available Integration Tests

| Test Class | Description |
|------------|-------------|
| `ClusterRuntimeHdfsIT` | HDFS operations and file system tests |
| `MapReduceIT` | MapReduce job execution |
| `HBaseIT` | HBase table operations |
| `PigIT` | Pig Latin script execution |
| `YarnIT` | YARN application lifecycle |
| `RestApiIT` | REST API endpoints |
| `HiveUdfIT` | Hive UDF testing |
| `DataFormatsIT` | Various data format handling |

---

## 💻 Usage

### Starting the Mini-Cluster

Run the embedded cluster as a standalone application:

**Unix/Linux/macOS:**
```bash
java -cp minicluster-core/target/minicluster-core-1.0-SNAPSHOT.jar:minicluster-core/target/lib/* \
  com.minicluster.starter.Starter
```

**Windows:**
```cmd
java -cp minicluster-core/target/minicluster-core-1.0-SNAPSHOT.jar;minicluster-core/target/lib/* ^
  com.minicluster.starter.Starter
```

The cluster will start with:
- ✅ HDFS on a random available port
- ✅ REST API (if enabled)
- ✅ Spark, Hive, and other configured services

### Using in Tests

Example of using the embedded cluster in a test:

```java
@Test
public void testHdfsOperations() throws Exception {
    ClusterRuntimeConfig config = ClusterRuntimeConfig.builder()
        .withHive()
        .withSpark()
        .build();
    
    try (ClusterRuntime runtime = ClusterRuntime.start(config)) {
        FileSystem fs = runtime.getFileSystem();
        Path testPath = new Path("/test/file.txt");
        
        // Write to HDFS
        try (FSDataOutputStream out = fs.create(testPath)) {
            out.write("Hello HDFS".getBytes());
        }
        
        // Read from HDFS
        try (FSDataInputStream in = fs.open(testPath)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals("Hello HDFS", content);
        }
    }
}
```

### Sample Jobs

The `app` module includes example jobs:

| Job | Description |
|-----|-------------|
| `WordCountMapReduceJob` | Classic MapReduce word count implementation |
| `SalesAggregationJob` | Spark SQL-based sales data aggregation |
| `ClientRiskCheckSparkJob` | Spark-based risk analysis |

---

## ⚙️ Configuration

The embedded cluster can be configured via properties or programmatically:

```java
Properties props = new Properties();
props.setProperty("hdfs.namenode.port", "9000");
props.setProperty("yarn.resourcemanager.port", "8032");

ClusterRuntimeConfig config = ClusterRuntimeConfig.builder()
    .withHive()
    .withSpark()
    .withRest()
    .props(props)
    .build();
```

### REST API

When REST is enabled, the cluster exposes HTTP endpoints for:
- 📁 File system operations
- 🚀 Job submission and monitoring
- 📊 Cluster status and metrics

Access the REST API at: `http://localhost:<rest-port>/`

---

## 🔧 Troubleshooting

### Out of Memory Errors

Increase JVM heap size:

**Unix/Linux/macOS:**
```bash
export MAVEN_OPTS="-Xmx4g"
mvn test
```

**Windows:**
```cmd
set MAVEN_OPTS=-Xmx4g
mvn test
```

### Port Conflicts

The cluster uses random available ports by default. If you need specific ports, configure them via properties.

### Native Library Warnings

Hadoop may warn about native libraries. These warnings are generally harmless in test environments and can be ignored.

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

---

## 📝 License

This project is intended for testing and development purposes.

---

## 📧 Contact

For questions or issues, please open an issue in the repository.
