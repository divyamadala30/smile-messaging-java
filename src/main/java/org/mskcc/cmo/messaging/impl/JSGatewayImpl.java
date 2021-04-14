package org.mskcc.cmo.messaging.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.common.FileUtil;
import org.mskcc.cmo.common.impl.FileUtilImpl;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.messaging.events.NATSPublisher;
import org.mskcc.cmo.messaging.events.PublishingQueueTask;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class JSGatewayImpl implements Gateway {
    private static final Log LOG = LogFactory.getLog(JSGatewayImpl.class);
    private static final CountDownLatch PUBLISHING_SHUTDOWN_LATCH = new CountDownLatch(1);
    private static volatile Boolean shutdownInitiated = Boolean.FALSE;
    private static volatile BlockingQueue<PublishingQueueTask> PUBLISHING_QUEUE = new LinkedBlockingQueue<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, JetStreamSubscription> subscribers = new HashMap<>();
    private volatile ExecutorService exec = Executors.newSingleThreadExecutor();

    // connection variables
    @Value("${nats.url}")
    public String natsUrl;

    private Connection natsConnection;
    private JetStream jsConnection;

    // publishing logger file variables
    @Value("${metadb.publishing_failures_filepath}")
    private String metadbPubFailuresFilepath;

    private final String PUB_FAILURES_FILE_HEADER = "DATE\tTOPIC\tMESSAGE\n";
    private static final FileUtil fileUtil = new FileUtilImpl();
    private static File publishingLoggerFile;

    @Override
    public void connect() throws Exception {
        connect(natsUrl);
    }

    @Override
    public void connect(String natsUrl) throws Exception {
        this.natsConnection = Nats.connect(natsUrl);
        this.jsConnection = natsConnection.jetStream();
        publishingLoggerFile = fileUtil.getOrCreateFileWithHeader(
                metadbPubFailuresFilepath, PUB_FAILURES_FILE_HEADER);
        exec.execute(NATSPublisher.withDefaultOptions(natsConnection.getOptions()));
    }

    @Override
    public  boolean isConnected() {
        if (natsConnection == null) {
            return Boolean.FALSE;
        }
        return (natsConnection != null && natsConnection.getStatus() != null
                && (natsConnection.getStatus().equals(Connection.Status.CONNECTED)));
    }

    @Override
    public void publish(String stream, String subject, Object message) throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Gateway connection has not been established.");
        }
        if (!shutdownInitiated) {
            PUBLISHING_QUEUE.put(new PublishingQueueTask(subject, message));
        } else {
            LOG.error("Shutdown initiated, not accepting publish request: \n" + message);
            throw new IllegalStateException("Shutdown initiated, not accepting anymore publish requests");
        }
    }

    @Override
    public void subscribe(String stream, String subject, Class messageClass,
            MessageConsumer messageConsumer) throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Gateway connection has not been established.");
        }
        if (!subscribers.containsKey(subject)) {
            Dispatcher dispatcher = natsConnection.createDispatcher();
            //Should we add PushSubscribeOptions to add durable name?
            JetStreamSubscription sub = jsConnection.subscribe(subject, dispatcher,
                msg -> onMessage(msg, messageClass, messageConsumer), false);
            subscribers.put(subject, sub);
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Gateway connection has not been established");
        }
        exec.shutdownNow();
        shutdownInitiated = true;
        PUBLISHING_SHUTDOWN_LATCH.await();
        natsConnection.close();
    }

    /**
     * Configure basic message handler logic.
     * @param msg
     * @param messageClass
     * @param messageConsumer
     */
    public void onMessage(Message msg, Class messageClass, MessageConsumer messageConsumer) {
        String payload = new String(msg.getData(), StandardCharsets.UTF_8);
        Object message = null;

        try {
            message = mapper.readValue(payload, messageClass);
        } catch (JsonProcessingException ex) {
            LOG.error("Error deserializing NATS message: " + payload, ex);
        }
        if (message != null) {
            messageConsumer.onMessage(message);
        }
    }

    /**
     * Writes to publishing logger file.
     * @param subject
     * @param message
     */
    public static void writeToPublishingLoggerFile(String subject, String message) {
        String currentDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        StringBuilder builder = new StringBuilder();
        builder.append(currentDate)
                .append("\t")
                .append(subject)
                .append("\t")
                .append(message)
                .append("\n");

        try {
            fileUtil.writeToFile(publishingLoggerFile,
                    builder.toString());
        } catch (IOException ex) {
            LOG.error("Error during attempt to log publishing task to logger file", ex);
        }
    }

    public static Boolean shutdownInitiated() {
        return shutdownInitiated;
    }

    public static void countdownPublishingShutdownLatch() {
        PUBLISHING_SHUTDOWN_LATCH.countDown();
    }

    public static PublishingQueueTask pollPublishingQueue() throws InterruptedException {
        return PUBLISHING_QUEUE.poll(100, TimeUnit.MILLISECONDS);
    }

    public static Boolean publishingQueueIsEmpty() {
        return PUBLISHING_QUEUE.isEmpty();
    }
}
