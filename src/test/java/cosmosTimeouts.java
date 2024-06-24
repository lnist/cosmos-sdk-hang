import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.extension.ResponseTransformerV2;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

class CosmosTimeoutTest {
    private static final String CONNECTION_STRING = "TODO";
    private static final String MASTER_KEY = "TODO";
    private final Integer DELAY = 10_000; // or 2_000
    private static final String DATABASE = "life";
    private static final String NON_DEFAULT_PARTITION_KEY_COLLECTION = "CollectionWitNonDefaultPartitionKey";
    private static final String DEFAULT_COLLECTION = "CollectionWithIdAsPartitionKey";

    private WireMockServer wireMockServer;
    private CosmosClient client;
    private CosmosDatabase database;
    private String wireMockUrl;

    public CosmosClientBuilder createCosmosClientBuilder() {
        final var retryOptions = new ThrottlingRetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests( 3 );
        retryOptions.setMaxRetryWaitTime( Duration.ofSeconds( 12 ) );

        var cosmosClientBuilder = new CosmosClientBuilder()
                .endpoint( wireMockUrl )
                .throttlingRetryOptions( retryOptions )
                .consistencyLevel( ConsistencyLevel.SESSION )
                .contentResponseOnWriteEnabled( false )
                .endToEndOperationLatencyPolicyConfig( new CosmosEndToEndOperationLatencyPolicyConfigBuilder( Duration.ofSeconds( 1 ) ).build() );

        cosmosClientBuilder.key( MASTER_KEY );

        cosmosClientBuilder.gatewayMode();

        return cosmosClientBuilder;
    }

    @BeforeEach
    public void beforeEach() {
        System.setProperty( "javax.net.ssl.trustStore", "src/test/resources/wiremock.jks" );
        System.setProperty( "javax.net.ssl.trustStorePassword", "selfsigned" );

        var transformer = new CosmosUrlTransformer();
        wireMockServer = new WireMockServer( options().dynamicHttpsPort().keystorePath( "src/test/resources/wiremock.jks" ).keystorePassword( "selfsigned" ).keyManagerPassword( "selfsigned" ).extensions( transformer ) );
        wireMockServer.start();
        wireMockServer.stubFor( any( urlMatching( ".*" ) )
                .willReturn( aResponse().proxiedFrom( CONNECTION_STRING ).withTransformers( transformer.getName() ) ) );
        wireMockUrl = "https://localhost:" + wireMockServer.httpsPort();
        transformer.setWireMockUrl(wireMockUrl);

        client = createCosmosClientBuilder().buildClient();
        database = client.getDatabase(DATABASE);

        wireMockServer.setGlobalFixedDelay(DELAY);
    }

    @AfterEach
    public void afterEach() {
        if ( wireMockServer != null ) {
            wireMockServer.stop();
        }
        if (client != null) {
            client .close();
        }
    }

    public static class CosmosUrlTransformer implements ResponseTransformerV2 {
        private String wireMockUrl;

        @Override
        public String getName() {
            return "cosmosUrlTransformer";
        }

        @Override
        public Response transform( Response response, ServeEvent serveEvent ) {
            return Response.Builder.like( response ).but().body( response.getBodyAsString().replaceAll( "https://.*/", wireMockUrl ) ).build();
        }

        public void setWireMockUrl( String wireMockUrl ) {
            this.wireMockUrl = wireMockUrl;
        }
    }


    @Test
    void readAllContainers() {
        // CosmosEndToEndOperationLatencyPolicyConfig seems to not be respected for either delay of 2_000 or 10_000
        database.readAllContainers();
    }

    @Test
    void properties() {
        // CosmosEndToEndOperationLatencyPolicyConfig seems to not be respected for either delay of 2_000 or 10_000
       database.read().getProperties().getId();
    }

    private CosmosQueryRequestOptions createQueryRequestOptions() {
        var queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setMaxDegreeOfParallelism( 16 );
        return queryOptions;
    }

    @Test
    @Timeout(60)
    void readNonDefaultPartitionKey() {
        // CosmosEndToEndOperationLatencyPolicyConfig seems to not be respected delay of 10_000, but is respected for 2_000. Tracing seems to indicate it retries 3 after which it should stop, but then it keeps looping.
        var querySpec = new SqlQuerySpec();
        querySpec.setQueryText( "SELECT * FROM c WHERE c.id=@id" );
        var parameters = new ArrayList<SqlParameter>();
        parameters.add( new SqlParameter( "@id", UUID.randomUUID() ) );
        querySpec.setParameters( parameters );
        var ignored = database.getContainer(NON_DEFAULT_PARTITION_KEY_COLLECTION).queryItems( querySpec, createQueryRequestOptions(), JsonNode.class ).stream().toList();
    }

    @Test
    @Timeout(60)
    void count() {
        // CosmosEndToEndOperationLatencyPolicyConfig seems to not be respected delay of 10_000, but is respected for 2_000. Tracing seems to indicate it retries 3 after which it should stop, but then it keeps looping.
        var querySpec = new SqlQuerySpec();
        querySpec.setQueryText( "SELECT VALUE COUNT(1) FROM c" );
        var ignored = database.getContainer(DEFAULT_COLLECTION).queryItems( querySpec, createQueryRequestOptions(), Long.class ).stream().findFirst();
    }

    @Test
    @Timeout(60)
    void readAll() {
        // CosmosEndToEndOperationLatencyPolicyConfig seems to not be respected delay of 10_000, but is respected for 2_000. Tracing seems to indicate it retries 3 after which it should stop, but then it keeps looping.
        var querySpec = new SqlQuerySpec();
        querySpec.setQueryText( "SELECT * FROM c" );
        var ignored = database.getContainer(DEFAULT_COLLECTION).queryItems( querySpec, createQueryRequestOptions(), JsonNode.class ).stream().findFirst();
    }

    @Test
    @Timeout(60)
    void writeBulk() {
        // CosmosEndToEndOperationLatencyPolicyConfig seems to not be respected delay of 10_000, but is respected for 2_000.
        var operations = new ArrayList<CosmosItemOperation>();
        var id = UUID.randomUUID().toString();
        var jsonNode =  new ObjectMapper().createObjectNode().put( "id", id );
        operations.add(CosmosBulkOperations.getUpsertItemOperation( jsonNode,  new PartitionKey( id )));

        var ignored = new ArrayList<CosmosBulkOperationResponse<Object>>(operations.size());
        database.getContainer(DEFAULT_COLLECTION).executeBulkOperations( operations )
                .forEach( ignored::add );
    }
}
