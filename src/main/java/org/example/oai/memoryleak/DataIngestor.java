package org.example.oai.memoryleak;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.ThroughputProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.example.oai.memoryleak.CommonUtils.CONTAINER_NAMES;
import static org.example.oai.memoryleak.CommonUtils.DATABASE_NAME;
import static org.example.oai.memoryleak.CommonUtils.buildCosmosClient;

public class DataIngestor {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestor.class);

    public static void main(String[] args) {
        try (CosmosClient cosmosClient = buildCosmosClient()) {

            cosmosClient.createDatabaseIfNotExists(DATABASE_NAME);

            Thread.sleep(60_000);

            CosmosDatabase cosmosDatabase = cosmosClient.getDatabase(DATABASE_NAME);

            // create 10 containers each with manual provisioned throughput of 180_000 RUs - should create around 20 physical partitions give or take
            for (String containerName : CONTAINER_NAMES) {
                cosmosDatabase.createContainerIfNotExists(containerName, "/id", ThroughputProperties.createManualThroughput(100_000));
                Thread.sleep(60_000);
            }

            // insert 10_000 documents in each container
            // swallow CosmosException - make ingestion best effort
            for (String containerName : CONTAINER_NAMES) {

                try {
                    for (int i = 1; i <= 10_000; i++) {

                        String uuid = UUID.randomUUID().toString();

                        TestItem testItem = new TestItem(uuid, uuid);

                        CosmosContainer container = cosmosDatabase.getContainer(containerName);

                        container.createItem(testItem);
                    }

                    logger.info("Ingestion for container : {} complete.", containerName);
                } catch (CosmosException e) {
                    logger.warn("Error occurred for ingestion for container with id : {} with error : {}", containerName, e.getMessage());
                }
            }

        } catch (Exception e) {
            logger.error("Exception thrown! - {}", e.getMessage());
        }
    }
}
