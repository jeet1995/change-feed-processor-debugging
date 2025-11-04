package org.example.oai.workload;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnostics;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.ThroughputProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Configurable Change Feed Processor workload that can be run as an executable jar.
 * All configuration is done via environment variables.
 *
 * Required Environment Variables:
 * - COSMOS_ENDPOINT: Cosmos DB account endpoint
 * - COSMOS_KEY: Cosmos DB account key
 * - FEED_CONTAINER: Name of the container to monitor for changes
 * - LEASE_CONTAINER: Name of the container to store leases
 *
 * Optional Environment Variables:
 * - DATABASE_NAME: Database name (default: "CFPWorkloadDB")
 * - PREFERRED_REGIONS: Comma-separated list of preferred regions (default: empty)
 * - CONNECTION_MODE: "GATEWAY" or "DIRECT" (default: "DIRECT")
 * - HOST_NAME: CFP host name (default: "cfp-host-" + currentTimeMillis)
 * - LEASE_PREFIX: Lease prefix (default: "workload")
 * - MAX_ITEM_COUNT: Max items per batch (default: 100)
 * - FEED_POLL_DELAY_MS: Feed poll delay in milliseconds (default: 5000)
 * - STARTUP_DELAY: Startup delay for CFP (default: "PT15S")
 * - WORKLOAD_DURATION_MINUTES: How long to run the workload (default: 60)
 * - LOG_LEVEL: Logging level (default: "INFO")
 */
public class ChangeFeedProcessorWorkload {

    private static final Logger logger = LoggerFactory.getLogger(ChangeFeedProcessorWorkload.class);

    // Required environment variables
    private static final String COSMOS_ENDPOINT = getRequiredEnv("COSMOS_ENDPOINT");
    private static final String COSMOS_KEY = getRequiredEnv("COSMOS_KEY");
    private static final String FEED_CONTAINER = getRequiredEnv("FEED_CONTAINER");
    private static final String LEASE_CONTAINER = getRequiredEnv("LEASE_CONTAINER");

    // Optional environment variables with defaults
    private static final String DATABASE_NAME = getEnvOrDefault("DATABASE_NAME", "CFPWorkloadDB");
    private static final String CONNECTION_MODE = getEnvOrDefault("CONNECTION_MODE", "GATEWAY");
    private static final String HOST_NAME = getEnvOrDefault("HOST_NAME", "cfp-host-" + System.currentTimeMillis());
    private static final String LEASE_PREFIX = getEnvOrDefault("LEASE_PREFIX", "workload");
    private static final int MAX_ITEM_COUNT = Integer.parseInt(getEnvOrDefault("MAX_ITEM_COUNT", "100"));
    private static final int FEED_POLL_DELAY_MS = Integer.parseInt(getEnvOrDefault("FEED_POLL_DELAY_MS", "5000"));
    private static final String STARTUP_DELAY = getEnvOrDefault("STARTUP_DELAY", "PT15S");
    private static final int WORKLOAD_DURATION_MINUTES = Integer.parseInt(getEnvOrDefault("WORKLOAD_DURATION_MINUTES", "600000"));

    private static final List<String> PREFERRED_REGIONS = parsePreferredRegions();

    private static final AtomicLong processedItemCount = new AtomicLong(0);
    private static final AtomicLong batchCount = new AtomicLong(0);

    public static void main(String[] args) {
        logger.info("Starting Change Feed Processor Workload");
        logger.info("Configuration:");
        logger.info("  COSMOS_ENDPOINT: {}", COSMOS_ENDPOINT);
        logger.info("  DATABASE_NAME: {}", DATABASE_NAME);
        logger.info("  FEED_CONTAINER: {}", FEED_CONTAINER);
        logger.info("  LEASE_CONTAINER: {}", LEASE_CONTAINER);
        logger.info("  CONNECTION_MODE: {}", CONNECTION_MODE);
        logger.info("  PREFERRED_REGIONS: {}", PREFERRED_REGIONS);
        logger.info("  HOST_NAME: {}", HOST_NAME);
        logger.info("  LEASE_PREFIX: {}", LEASE_PREFIX);
        logger.info("  MAX_ITEM_COUNT: {}", MAX_ITEM_COUNT);
        logger.info("  FEED_POLL_DELAY_MS: {}", FEED_POLL_DELAY_MS);
        logger.info("  STARTUP_DELAY: {}", STARTUP_DELAY);
        logger.info("  WORKLOAD_DURATION_MINUTES: {}", WORKLOAD_DURATION_MINUTES);

        try {
            runWorkload();
        } catch (Exception e) {
            logger.error("Workload failed", e);
            System.exit(1);
        }
    }

    private static void runWorkload() throws InterruptedException {
        CosmosAsyncClient client = null;
        ChangeFeedProcessor processor = null;

        try {
            // Create Cosmos client
            client = createCosmosClient();
            logger.info("Created Cosmos client successfully");

            // Setup database and containers
            setupDatabaseAndContainers(client);

            // Create Change Feed Processor
            processor = createChangeFeedProcessor(client);
            logger.info("Created Change Feed Processor with host name: {}", HOST_NAME);

            // Start the processor
            logger.info("Starting Change Feed Processor...");
            processor.start()
                .doOnSuccess(v -> logger.info("Change Feed Processor started successfully"))
                .doOnError(e -> logger.error("Failed to start Change Feed Processor", e))
                .block(Duration.ofSeconds(60));

            // Run for specified duration
            logger.info("Change Feed Processor is running. Will stop after {} minutes.", WORKLOAD_DURATION_MINUTES);

            // Setup shutdown hook
            CountDownLatch shutdownLatch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received. Stopping Change Feed Processor...");
                shutdownLatch.countDown();
            }));

            // Wait for specified duration or shutdown signal
            boolean completed = shutdownLatch.await(WORKLOAD_DURATION_MINUTES, java.util.concurrent.TimeUnit.MINUTES);

            if (completed) {
                logger.info("Received shutdown signal");
            } else {
                logger.info("Workload duration completed ({} minutes)", WORKLOAD_DURATION_MINUTES);
            }

        } finally {
            // Stop processor
            if (processor != null) {
                logger.info("Stopping Change Feed Processor...");
                try {
                    processor.stop()
                        .doOnSuccess(v -> logger.info("Change Feed Processor stopped successfully"))
                        .doOnError(e -> logger.error("Error stopping Change Feed Processor", e))
                        .block(Duration.ofSeconds(30));
                } catch (Exception e) {
                    logger.error("Error during processor shutdown", e);
                }
            }

            // Close client
            if (client != null) {
                logger.info("Closing Cosmos client...");
                try {
                    client.close();
                    logger.info("Cosmos client closed successfully");
                } catch (Exception e) {
                    logger.error("Error closing Cosmos client", e);
                }
            }

            logger.info("Workload completed. Processed {} items in {} batches",
                processedItemCount.get(), batchCount.get());
        }
    }

    private static CosmosAsyncClient createCosmosClient() {
        CosmosClientBuilder builder = new CosmosClientBuilder()
            .endpoint(COSMOS_ENDPOINT)
            .key(COSMOS_KEY)
            .consistencyLevel(ConsistencyLevel.SESSION)
            .contentResponseOnWriteEnabled(true);

        // Set connection mode
        if ("GATEWAY".equalsIgnoreCase(CONNECTION_MODE)) {
            builder.gatewayMode();
            logger.info("Using Gateway connection mode");
        } else {
            builder.directMode();
            logger.info("Using Direct connection mode");
        }

        // Set preferred regions if specified
        if (!PREFERRED_REGIONS.isEmpty()) {
            builder.preferredRegions(PREFERRED_REGIONS);
            logger.info("Using preferred regions: {}", PREFERRED_REGIONS);
        }

        return builder.buildAsyncClient();
    }

    private static void setupDatabaseAndContainers(CosmosAsyncClient client) {
        logger.info("Setting up database and containers...");

        try {
            // Create database if not exists
            CosmosDatabaseResponse dbResponse = client.createDatabaseIfNotExists(DATABASE_NAME)
                .block(Duration.ofSeconds(30));

            if (dbResponse != null) {
                logger.info("Database setup complete. Request charge: {} RUs",
                    dbResponse.getRequestCharge());
                logResponseDiagnostics("CreateDatabase", dbResponse.getDiagnostics());
            }

            CosmosAsyncDatabase database = client.getDatabase(DATABASE_NAME);

            // Create feed container if not exists
            CosmosContainerProperties feedContainerProps = new CosmosContainerProperties(FEED_CONTAINER, "/id");
            CosmosContainerResponse feedResponse = database.createContainerIfNotExists(
                feedContainerProps,
                ThroughputProperties.createManualThroughput(400))
                .block(Duration.ofSeconds(30));

            if (feedResponse != null) {
                logger.info("Feed container '{}' setup complete. Request charge: {} RUs",
                    FEED_CONTAINER, feedResponse.getRequestCharge());
                logResponseDiagnostics("CreateFeedContainer", feedResponse.getDiagnostics());
            }

            // Create lease container if not exists
            CosmosContainerProperties leaseContainerProps = new CosmosContainerProperties(LEASE_CONTAINER, "/id");
            CosmosContainerResponse leaseResponse = database.createContainerIfNotExists(
                leaseContainerProps,
                ThroughputProperties.createManualThroughput(400))
                .block(Duration.ofSeconds(30));

            if (leaseResponse != null) {
                logger.info("Lease container '{}' setup complete. Request charge: {} RUs",
                    LEASE_CONTAINER, leaseResponse.getRequestCharge());
                logResponseDiagnostics("CreateLeaseContainer", leaseResponse.getDiagnostics());
            }

        } catch (Exception e) {
            logger.error("Failed to setup database and containers", e);
            throw e;
        }
    }

    private static ChangeFeedProcessor createChangeFeedProcessor(CosmosAsyncClient client) {
        CosmosAsyncDatabase database = client.getDatabase(DATABASE_NAME);
        CosmosAsyncContainer feedContainer = database.getContainer(FEED_CONTAINER);
        CosmosAsyncContainer leaseContainer = database.getContainer(LEASE_CONTAINER);

        ChangeFeedProcessorOptions options = new ChangeFeedProcessorOptions();
        options.setLeasePrefix(LEASE_PREFIX);
        options.setMaxItemCount(MAX_ITEM_COUNT);
        options.setFeedPollDelay(Duration.ofMillis(FEED_POLL_DELAY_MS));
        options.setLeaseAcquireInterval(Duration.ofSeconds(13));
        options.setLeaseExpirationInterval(Duration.ofSeconds(60));
        options.setLeaseRenewInterval(Duration.ofSeconds(17));

        return new ChangeFeedProcessorBuilder()
            .hostName(HOST_NAME)
            .feedContainer(feedContainer)
            .leaseContainer(leaseContainer)
            .options(options)
            .handleLatestVersionChanges((changes) -> {
                long currentBatch = batchCount.incrementAndGet();
                long itemsInBatch = changes.size();
                long totalProcessed = processedItemCount.addAndGet(itemsInBatch);

                logger.info("Batch #{}: Processed {} items (Total: {})",
                        currentBatch, itemsInBatch, totalProcessed);
            })
            .buildChangeFeedProcessor();
    }

    private static void logResponseDiagnostics(String operation, CosmosDiagnostics diagnostics) {
        if (diagnostics != null) {
            logger.debug("{} - Request charge: {} RUs, Duration: {} ms",
                operation,
                diagnostics.getDiagnosticsContext(),
                diagnostics.getDuration().toMillis());

            if (logger.isTraceEnabled()) {
                logger.trace("{} - Full diagnostics: {}", operation, diagnostics.toString());
            }
        }
    }

    private static List<String> parsePreferredRegions() {
        String regionsEnv = getEnvOrDefault("PREFERRED_REGIONS", "");
        if (regionsEnv.trim().isEmpty()) {
            return List.of();
        }
        return Arrays.stream(regionsEnv.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }

    private static String getRequiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Required environment variable '" + name + "' is not set");
        }
        return value.trim();
    }

    private static String getEnvOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.trim().isEmpty()) ? defaultValue : value.trim();
    }
}