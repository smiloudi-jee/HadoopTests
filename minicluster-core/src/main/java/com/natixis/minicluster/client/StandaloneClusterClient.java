package com.natixis.minicluster.client;

import com.natixis.minicluster.modules.rest.response.HdfsEntry;
import com.natixis.minicluster.modules.rest.response.RestHealthResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Client applicatif Java 8 pour interagir avec le mini-cluster via l'API REST.
 */
public class StandaloneClusterClient {

    private final String baseUrl;
    private final ObjectMapper mapper;
    private final CloseableHttpClient httpClient;

    public StandaloneClusterClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClients.createDefault();
    }

    /**
     * Appelle /health et désérialise la réponse JSON.
     */
    public RestHealthResponse getHealth() throws IOException {
        String url = baseUrl + "/health";
        HttpGet req = new HttpGet(url);

        try (CloseableHttpResponse resp = httpClient.execute(req)) {
            int code = resp.getStatusLine().getStatusCode();
            if (code != 200) {
                throw new IOException("GET /health returned " + code);
            }
            String body = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
            return mapper.readValue(body, RestHealthResponse.class);
        }
    }

    /**
     * Appelle /hdfs/list?path=<path>
     * et retourne la liste d'entrées HDFS telles que renvoyées par l'API.
     */
    public List<HdfsEntry> listHdfs(String path) throws IOException {
        String encoded = URLEncoder.encode(path, "UTF-8");
        String url = baseUrl + "/hdfs/list?path=" + encoded;
        HttpGet req = new HttpGet(url);

        try (CloseableHttpResponse resp = httpClient.execute(req)) {
            int code = resp.getStatusLine().getStatusCode();
            if (code != 200) {
                throw new IOException("GET /hdfs/list returned " + code);
            }
            String body = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);

            HdfsEntry[] arr = mapper.readValue(body, HdfsEntry[].class);
            return Arrays.asList(arr);
        }
    }

    /**
     * Appelle /sql/execute avec le SQL en body (POST).
     * Retourne le texte brut renvoyé (Spark DataFrame.showString()).
     */
    public String executeSql(String sql) throws IOException {
        String url = baseUrl + "/sql/execute";
        HttpPost req = new HttpPost(url);

        req.setEntity(new StringEntity(sql, StandardCharsets.UTF_8));
        req.setHeader("Content-Type", "text/plain; charset=UTF-8");

        try (CloseableHttpResponse resp = httpClient.execute(req)) {
            int code = resp.getStatusLine().getStatusCode();
            if (code != 200) {
                throw new IOException("POST /sql/execute returned " + code);
            }
            HttpEntity entity = resp.getEntity();
            return EntityUtils.toString(entity, StandardCharsets.UTF_8);
        }
    }

    /**
     * Ferme proprement le client HTTP (optionnel dans les tests).
     */
    public void close() {
        try {
            httpClient.close();
        } catch (IOException ignored) {}
    }
}