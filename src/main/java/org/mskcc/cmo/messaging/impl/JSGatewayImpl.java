package org.mskcc.cmo.messaging.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Connection.Status;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamOptions;
import io.nats.client.JetStreamSubscription;
import io.nats.client.MessageHandler;
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
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.messaging.utils.SSLUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Primary
@Component
public class JSGatewayImpl implements Gateway {
    @Value("${nats.tls_channel:false}")
    private boolean tlsChannel;

    @Value("${nats.url}")
    private String natsURL;

    @Value("${metadb.publishing_failures_filepath}")
    private String metadbPubFailuresFilepath;

    private FileUtil fileUtil;

    private File pubFailuresFile;

    @Autowired
    private void initPubFailuresFile() throws IOException {
        this.pubFailuresFile = fileUtil.getOrCreateFileWithHeader(
                metadbPubFailuresFilepath, PUB_FAILURES_FILE_HEADER);
    }

    @Autowired
    SSLUtils sslUtils;

    private static final String PUB_FAILURES_FILE_HEADER = "DATE\tTOPIC\tMESSAGE\n";
    private Connection natsConnection;
    private JetStream jsConnection;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, JetStreamSubscription> subscribers = new HashMap<String,
            JetStreamSubscription>();
    private volatile boolean shutdownInitiated;
    private final ExecutorService exec = Executors.newSingleThreadExecutor();
    private final CountDownLatch publishingShutdownLatch = new CountDownLatch(1);
    private final BlockingQueue<PublishingQueueTask> publishingQueue =
        new LinkedBlockingQueue<PublishingQueueTask>();
    private final Log LOG = LogFactory.getLog(StanGatewayImpl.class);

    private class PublishingQueueTask {
        String topic;
        Object message;

        PublishingQueueTask(String topic, Object message) {
            this.topic = topic;
            this.message = message;
        }
    }

    private class NATSPublisher implements Runnable {

        Connection natsConn;
        JetStream jetStreamConn;
        boolean interrupted = false;

        NATSPublisher() throws Exception {
            JetStreamOptions jsOptions = new JetStreamOptions.Builder()
                    .build();
            natsConn = Nats.connect(natsConnection.getOptions());
            this.jetStreamConn = natsConn.jetStream(jsOptions);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    PublishingQueueTask task = publishingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        String msg = mapper.writeValueAsString(task.message);
                        try {
                            jetStreamConn.publishAsync(task.topic, msg.getBytes(StandardCharsets.UTF_8));
                        } catch (Exception e) {
                            try {
                                fileUtil.writeToFile(pubFailuresFile,
                                        generatePublishFailureRecord(task.topic, msg));
                            } catch (IOException ex) {
                                LOG.error("Error during attempt to log publishing failure to file: "
                                        + metadbPubFailuresFilepath, e);
                            }
                            if (e instanceof InterruptedException) {
                                interrupted = true;
                            } else {
                                LOG.error("Error during attempt to publish on topic: " + task.topic, e);
                            }
                        }
                    }
                    if ((interrupted || shutdownInitiated) && publishingQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (JsonProcessingException e) {
                    LOG.error("Error parsing JSON from message", e);
                }
            }
            try {
                natsConn.close();
                publishingShutdownLatch.countDown();
            } catch (Exception e) {
                LOG.error("Error closing streaming connection: %s\n" + e.getMessage());
            }
        }
    }

    @Override
    public void connect() throws Exception {
        connect(natsURL);
    }

    @Override
    public void connect(String clusterId, String clientId, String natsUrl) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void connect(String natsUrl) throws Exception {
        natsConnection = Nats.connect(natsUrl);
        jsConnection = natsConnection.jetStream();
        exec.execute(new NATSPublisher());
    }

    @Override
    public boolean isConnected() {
        if (natsConnection == null) {
            return Boolean.FALSE;
        }
        return (natsConnection != null && natsConnection.getStatus() != null
                && (natsConnection.getStatus().CONNECTED.equals(Status.CONNECTED)));
    }

    @Override
    public void publish(String stream, String subject, Object message) throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Gateway connection has not been established");
        }
        if (!shutdownInitiated) {
            PublishingQueueTask task = new PublishingQueueTask(subject, message);
            publishingQueue.put(task);
        } else {
            LOG.error("Shutdown initiated, not accepting publish request: \n" + message);
            throw new IllegalStateException("Shutdown initiated, not accepting anymore publish requests");
        }
    }

    @Override
    public void publish(String topic, Object message) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(String stream, String subject, Class messageClass,
            MessageConsumer messageConsumer) throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Gateway connection has not been established");
        }
        if (!subscribers.containsKey(subject)) {
            Dispatcher dispatcher = natsConnection.createDispatcher();
            MessageHandler handler = new MessageHandler() {
                @Override
                public void onMessage(io.nats.client.Message msg) throws InterruptedException {
                    Object message = null;
                    try {
                        String json = new String(msg.getData(), StandardCharsets.UTF_8);
                        message = mapper.readValue(json, messageClass);
                    } catch (Exception e) {
                        LOG.error("Error deserializing NATS message: \n" + msg);
                        LOG.error("Exception: \n" + e.getMessage());
                    }
                    if (message != null) {
                        messageConsumer.onMessage(message);
                    }
                }
            };
            //Should we add PushSubscribeOptions to add durable name?
            JetStreamSubscription sub = jsConnection.subscribe(subject, dispatcher, handler, false);
            subscribers.put(subject, sub);
        }
    }

    @Override
    public void subscribe(String topic, Class messageClass,
            MessageConsumer messageConsumer) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Gateway connection has not been established");
        }
        exec.shutdownNow();
        shutdownInitiated = true;
        publishingShutdownLatch.await();
        natsConnection.close();
    }

    private String generatePublishFailureRecord(String subject, String message) {
        String currentDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        StringBuilder builder = new StringBuilder();
        builder.append(currentDate)
                .append("\t")
                .append(subject)
                .append("\t")
                .append(message)
                .append("\n");
        return builder.toString();
    }
}
