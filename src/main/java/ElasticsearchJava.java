import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.indices.CreateIndexRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchJava {

    private static RestHighLevelClient client = null;

    private static synchronized RestHighLevelClient makeConnection() {
        final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider
                .setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "password"));

        if (client == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost("localhost", 9200, "http"))
                            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                    httpClientBuilder.disableAuthCaching();
                                    return httpClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
                                }
                            })
            );
        }

        return client;
    }

    public static void main(String[] args) throws IOException {
        client = makeConnection();
        String mappings = "{\n" +
                "  \"properties\": {\n" +
                "    \"id\": {\n" +
                "      \"type\": \"keyword\"\n" +
                "    },\n" +
                "    \"name\": {\n" +
                "      \"type\": \"text\"\n" +
                "    }\n" +                "  }\n" +
                "}";
        System.out.println("mapping is as follows: ");
        System.out.println(mappings);

        try {
            CreateIndexRequest request = new CreateIndexRequest("employees");
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            );

            request.mapping(mappings, XContentType.JSON);
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            System.out.println("response id: " + createIndexResponse.index());
        } catch (Exception e) {
//            e.printStackTrace();
        }

        // Method 1: Write documents into employees index
        IndexRequest request = new IndexRequest("employees");
        request.id("1");
        String jsonString = "{" +
                "\"id\":\"1\"," +
                "\"name\":\"liuxg\"" +
                "}";
        request.source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse.getId());
        System.out.println("response name: "+indexResponse.getResult().name());

        // Method 2: Write documents into employees index
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id", "2");
        jsonMap.put("name", "Nancy");
        IndexRequest indexRequest = new IndexRequest("employees")
                .id("2").source(jsonMap);
        IndexResponse indexResponse2 = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse2.getId());
        System.out.println("response name: "+indexResponse2.getResult().name());

        // Method 3: Write documents into employees index
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("id", "3");
            builder.field("name", "Jason");
        }
        builder.endObject();
        IndexRequest indexRequest3 = new IndexRequest("employees")
                .id("3").source(builder);
        IndexResponse indexResponse3 = client.index(indexRequest3, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse3.getId());
        System.out.println("response name: "+indexResponse3.getResult().name());

        // Method 4: Write documents into employees index
        IndexRequest indexRequest4 = new IndexRequest("employees")
                .id("4")
                .source("id", "4",
                        "name", "Mark");
        IndexResponse indexResponse4 = client.index(indexRequest4, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse4.getId());
        System.out.println("response name: "+indexResponse4.getResult().name());

        //  Method 5: Write documents into employees index
        Employee employee = new Employee("5", "Martin");
        IndexRequest indexRequest5 = new IndexRequest("employees");
        indexRequest.id("5");
        indexRequest.source(new ObjectMapper().writeValueAsString(employee), XContentType.JSON);
        IndexResponse indexResponse5 = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse5.getId());
        System.out.println("response name: "+indexResponse5.getResult().name());
    }
}
