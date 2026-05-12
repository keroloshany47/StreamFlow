package com.datamasterylab;

import com.datamasterylab.dto.Transaction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionProducer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);

    private static final String TOPIC              = "financial_transactions";
    private static final String BOOTSTRAP_SERVERS  = "localhost:29092,localhost:39092,localhost:49092";

    private static final int   NUM_THREADS         = Runtime.getRuntime().availableProcessors();
    private static final int   NUM_PARTITIONS      = 16;
    private static final short REPLICATION_FACTOR  = 3;

    private static final AtomicLong totalRecordsSent = new AtomicLong(0);

    public static void main(String[] args) {
        createTopicIfNotExists();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,524288);       
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        props.put(ProducerConfig.ACKS_CONFIG,"1");
        props.put(ProducerConfig.SEND_BUFFER_CONFIG,1048576); 
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG,1048576);      
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,268435456L);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG,         300000);

        logger.info("Starting {} producer threads on {} cores", NUM_THREADS,
                Runtime.getRuntime().availableProcessors());

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            ThreadLocal<ObjectMapper> mapperLocal =
                    ThreadLocal.withInitial(ObjectMapper::new);

            // throughput reporter thread
            executor.submit(() -> {
                long lastCount = 0;
                long lastTime  = System.currentTimeMillis();
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(1000);
                        long now      = System.currentTimeMillis();
                        long current  = totalRecordsSent.get();
                        long delta    = current - lastCount;
                        double elapsed = (now - lastTime) / 1000.0;
                        logger.info("Throughput: {} records/sec | Total: {}",
                                String.format("%,.0f", delta / elapsed),
                                String.format("%,d", current));
                        lastCount = current;
                        lastTime  = now;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });

            // producer threads
            for (int i = 0; i < NUM_THREADS - 1; i++) {
                executor.submit(() -> {
                    ObjectMapper mapper = mapperLocal.get();
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Transaction transaction     = Transaction.randomTransaction();
                            String      transactionJson = mapper.writeValueAsString(transaction);

                            ProducerRecord<String, String> record = new ProducerRecord<>(
                                    TOPIC,
                                    transaction.getTransactionId(),
                                    transactionJson
                            );

                            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                                if (exception != null) {
                                    logger.error("Failed to send: {}", exception.getMessage());
                                } else {
                                    totalRecordsSent.incrementAndGet();
                                }
                            });

                        } catch (Exception e) {
                            logger.error("Error in producer thread", e);
                        }
                    }
                });
            }

            // graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down — flushing remaining messages...");
                executor.shutdownNow();
                producer.flush();
                logger.info("Done. Total sent: {}", String.format("%,d", totalRecordsSent.get()));
            }));

            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void createTopicIfNotExists() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            boolean topicExists = adminClient.listTopics().names().get().contains(TOPIC);
            if (!topicExists) {
                logger.info("Topic '{}' does not exist. Creating...", TOPIC);
                NewTopic newTopic = new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                logger.info("Topic '{}' created successfully.", TOPIC);
            } else {
                logger.info("Topic '{}' already exists.", TOPIC);
            }
        } catch (Exception e) {
            logger.error("Error checking or creating topic", e);
        }
    }
}