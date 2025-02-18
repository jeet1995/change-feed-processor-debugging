package org.example.oai.memoryleak;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.implementation.TestConfigurations;

import java.util.Arrays;
import java.util.List;

public class CommonUtils {

    public static final String DATABASE_NAME = "DataIngestor";

    public static final List<String> CONTAINER_NAMES = Arrays.asList(
            "ct1",
            "ct2",
            "ct3",
            "ct4",
            "ct5",
            "ct6",
            "ct7",
            "ct8",
            "ct9",
            "ct10");

    public static CosmosClient buildCosmosClient() {
        return new CosmosClientBuilder()
                .endpoint(TestConfigurations.HOST)
                .key(TestConfigurations.MASTER_KEY)
                .buildClient();
    }

    public static CosmosAsyncClient buildCosmosAsyncClient(boolean isContentResponseOnWriteRequired) {
        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
                .endpoint(TestConfigurations.HOST)
                .key(TestConfigurations.MASTER_KEY);

        if (isContentResponseOnWriteRequired) {
            cosmosClientBuilder = cosmosClientBuilder.contentResponseOnWriteEnabled(true);
        }

        return cosmosClientBuilder.buildAsyncClient();
    }

}
