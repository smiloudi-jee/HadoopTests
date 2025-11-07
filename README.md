# HadoopTests

A comprehensive testing framework for Apache Hadoop ecosystem components, featuring embedded mini-clusters for local development and testing.

## Overview

HadoopTests provides a lightweight, embedded environment for testing Hadoop applications without requiring a full cluster deployment. It includes support for:

- **HDFS** - Hadoop Distributed File System
- **YARN** - Resource management
- **MapReduce** - Distributed data processing
- **HBase** - NoSQL database
- **Spark** - Fast data processing engine
- **Hive** - SQL-like data warehouse
- **Pig** - High-level data flow scripting
- **REST API** - HTTP interface for cluster interaction

## Project Structure

```
HadoopTests/
├── minicluster-core/    # Core embedded cluster runtime and infrastructure
├── app/                 # Sample applications and jobs (MapReduce, Spark)
├── app-test/           # Integration tests and test utilities
└── pom.xml             # Parent Maven configuration
```

## Technologies

- **Java 8** - Required runtime
- **Maven 3** - Build and dependency management
- **Hadoop 3.3.6** - Hadoop ecosystem
- **Spark 3.5.1** - Data processing
- **HBase 2.5.12-hadoop3** - NoSQL database
- **JUnit 5** - Testing framework

## Prerequisites

- **JDK 8** or higher
- **Maven 3.6+**
- At least **4GB RAM** for running embedded clusters
- **Unix-like OS** recommended (Linux, macOS) for full compatibility

## Building the Project

Build the entire project:

```bash
mvn clean install
```

Build without running tests:

```bash
mvn clean install -DskipTests
```

Build a specific module:

```bash
cd minicluster-core
mvn clean install
```

## Running Tests

Run all tests:

```bash
mvn test
```

Run integration tests only:

```bash
mvn verify
```

Run a specific test class:

```bash
mvn test -Dtest=MapReduceIT
```

### Available Integration Tests

- `ClusterRuntimeHdfsIT` - HDFS operations
- `MapReduceIT` - MapReduce job execution
- `HBaseIT` - HBase table operations
- `PigIT` - Pig Latin script execution
- `YarnIT` - YARN application lifecycle
- `RestApiIT` - REST API endpoints
- `HiveUdfIT` - Hive UDF testing
- `DataFormatsIT` - Various data format handling

## Usage

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
- HDFS on a random available port
- REST API (if enabled)
- Spark, Hive, and other configured services

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

- **WordCountMapReduceJob** - Classic MapReduce word count
- **SalesAggregationJob** - Spark SQL-based sales data aggregation
- **ClientRiskCheckSparkJob** - Spark-based risk analysis

## Configuration

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

## REST API

When REST is enabled, the cluster exposes HTTP endpoints for:
- File system operations
- Job submission and monitoring
- Cluster status and metrics

Access the REST API at: `http://localhost:<rest-port>/`

## Troubleshooting

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

## License

This project is intended for testing and development purposes.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## Contact

For questions or issues, please open an issue in the repository.
