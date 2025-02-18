package org.example.oai.memoryleak;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.ChangeFeedProcessorState;
import com.azure.cosmos.models.ThroughputProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.example.oai.memoryleak.CommonUtils.CONTAINER_NAMES;
import static org.example.oai.memoryleak.CommonUtils.DATABASE_NAME;
import static org.example.oai.memoryleak.CommonUtils.buildCosmosAsyncClient;

public class DataCapture {

    private static final Logger logger = LoggerFactory.getLogger(DataCapture.class);
    private static final String LEASE_CONTAINER_NAME = "leaseContainer";
    private static final String HOST_NAME = "HOST_0";

    public static void main(String[] args) {

        CosmosAsyncClient cosmosAsyncClient = buildCosmosAsyncClient(true);

        try {

            CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(DATABASE_NAME);

            List<CosmosAsyncContainer> cosmosContainers = new ArrayList<>();
            List<ChangeFeedProcessorHolder> changeFeedProcessorHolders = new ArrayList<>();

            // One time action:
            //  Start 10 CFP instances in LatestVersion mode
            //  Stop them -> this will ensure leases are created
            // Run OAI snippet on a doled out thread

            // Run DataIngestor first so that containers are created
            for (String containerName : CONTAINER_NAMES) {

                CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(containerName);
                cosmosContainers.add(cosmosAsyncContainer);
            }

            cosmosAsyncDatabase
                    .createContainerIfNotExists(
                            LEASE_CONTAINER_NAME,
                            "/id",
                            ThroughputProperties.createManualThroughput(10_000))
                    .block();

            CosmosAsyncContainer leaseContainer = cosmosAsyncDatabase.getContainer(LEASE_CONTAINER_NAME);

            for (CosmosAsyncContainer cosmosContainer : cosmosContainers) {
                ChangeFeedProcessor changeFeedProcessor = new ChangeFeedProcessorBuilder()
                        .feedContainer(cosmosContainer)
                        .leaseContainer(leaseContainer)
                        .handleLatestVersionChanges(changeFeedProcessorItems -> {
                            logger.info("Processing latest version changes : {}", changeFeedProcessorItems.size());
                        })
                        .options(new ChangeFeedProcessorOptions().setMaxItemCount(1))
                        .hostName(HOST_NAME)
                        .buildChangeFeedProcessor();

                changeFeedProcessorHolders.add(new ChangeFeedProcessorHolder(changeFeedProcessor, cosmosContainer.getId(), HOST_NAME));
            }

            for (ChangeFeedProcessorHolder changeFeedProcessorHolder : changeFeedProcessorHolders) {

                ChangeFeedProcessor changeFeedProcessor = changeFeedProcessorHolder.getChangeFeedProcessor();

                changeFeedProcessor.start().block(Duration.ofSeconds(60));
                changeFeedProcessor.stop().block(Duration.ofSeconds(60));
            }

             Thread thread = new Thread(() -> getCurrentState(changeFeedProcessorHolders));
             thread.start();

        } catch (RuntimeException e) {
            logger.error("Exception thrown! - {}", e.getMessage());
        } finally {
            // cosmosAsyncClient.close();
        }
    }

    private static void getCurrentState(List<ChangeFeedProcessorHolder> changeFeedProcessorHolders) {
        logger.info("Starting to get lags estimation on all processors:");
        while (true) {
            long startTimestamp = System.currentTimeMillis();
            try {
                final int LogLagThreshold = 10000;
                synchronized (changeFeedProcessorHolders) {
                    for (ChangeFeedProcessorHolder changeFeedProcessorHolder : changeFeedProcessorHolders) {
                        try {
                            // getCurrentState returns a list of state objects for each monitored
                            // partition
                            List<ChangeFeedProcessorState> states =
                                    changeFeedProcessorHolder.getChangeFeedProcessor().getCurrentState().block();
                            if (states == null) {
                                continue;
                            }

                            int[] maxLag = {0};
                            String[] maxLagPartition = {""};
                            int[] largeLagPartitionCount = {0};
                            for (ChangeFeedProcessorState state : states) {
                                logger.info(
                                        String.format(
                                                "Container:%s, Lease Token: %s, Continuation Token: %s, Estimated lag: %d%n",
                                                changeFeedProcessorHolder.getTargetFeedContainerName(),
                                                state.getLeaseToken(),
                                                state.getContinuationToken(),
                                                state.getEstimatedLag()));
                                if (state.getEstimatedLag() > LogLagThreshold) {
                                    largeLagPartitionCount[0]++;
                                }
                                if (state.getEstimatedLag() > maxLag[0]) {
                                    maxLag[0] = state.getEstimatedLag();
                                    maxLagPartition[0] = state.getLeaseToken();
                                }
                            }

                            logger.info(
                                    String.format(
                                            "Host:%s, Container:%s, Partition: %s has an max estimated lag %d",
                                            changeFeedProcessorHolder.getHostName(),
                                            changeFeedProcessorHolder.getTargetFeedContainerName(),
                                            maxLagPartition[0],
                                            maxLag[0]));
                        } catch (RuntimeException e) {
                            // timeout
                        }
                    }
                }
            } catch (RuntimeException e) {
                // timeout
            }

            long endTimestamp = System.currentTimeMillis();
            try {
                // Sleep for 0.5 minutes minus the time already spent
                Thread.sleep(Math.max(0, 30_000 - (endTimestamp - startTimestamp)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
