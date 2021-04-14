package org.mskcc.cmo.messaging.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamOptions;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.impl.JSGatewayImpl;

public class NATSPublisher implements Runnable {
    private final Log LOG = LogFactory.getLog(NATSPublisher.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private Connection natsConnection;
    private JetStream jetStreamConn;
    private boolean interrupted;

    public NATSPublisher() {
        this.interrupted = Boolean.FALSE;
    }

    /**
     * NATSPublisher constructor.
     * @param options
     * @throws IOException
     * @throws InterruptedException
     */
    public NATSPublisher(Options options) throws IOException, InterruptedException {
        this.natsConnection = Nats.connect(options);
        this.jetStreamConn = natsConnection.jetStream(JetStreamOptions.DEFAULT_JS_OPTIONS);
        this.interrupted = Boolean.FALSE;
    }

    public static NATSPublisher withDefaultOptions(Options options) throws IOException, InterruptedException {
        return new NATSPublisher(options);
    }

    @Override
    public void run() {
        while (true) {
            try {
                PublishingQueueTask task = JSGatewayImpl.pollPublishingQueue();
                if (task != null) {
                    String msg = mapper.writeValueAsString(task.getPayload());
                    try {
                        PublishAck ack = jetStreamConn.publish(task.getSubject(), msg.getBytes());
                        if (ack.getError() != null) {
                            JSGatewayImpl.writeToPublishingLoggerFile(task.getSubject(), msg);
                        }
                    } catch (Exception e) {
                        JSGatewayImpl.writeToPublishingLoggerFile(task.getSubject(), msg);
                        if (e instanceof InterruptedException) {
                            interrupted = Boolean.TRUE;
                        } else {
                            LOG.error("Error during attempt to publish on topic: " + task.getSubject(), e);
                        }
                    }
                    if ((interrupted || JSGatewayImpl.shutdownInitiated())
                            && JSGatewayImpl.publishingQueueIsEmpty()) {
                        break;
                    }
                }
            } catch (InterruptedException ex) {
                interrupted = Boolean.TRUE;
            } catch (JsonProcessingException ex) {
                LOG.error("Error parsing JSON from message", ex);
            }
        }
        try {
            // close connection
            natsConnection.close();
        } catch (InterruptedException ex) {
            LOG.error("Error during attempt to close NATS connection", ex);
        }
        JSGatewayImpl.countdownPublishingShutdownLatch();
    }

    public Connection getNatsConnection() {
        return natsConnection;
    }

    public void setNatsConnection(Connection natsConnection) {
        this.natsConnection = natsConnection;
    }

    public JetStream getJetStreamConn() {
        return jetStreamConn;
    }

    public void setJetStreamConn(JetStream jetStreamConn) {
        this.jetStreamConn = jetStreamConn;
    }

    public boolean isInterrupted() {
        return interrupted;
    }

    public void setInterrupted(boolean interrupted) {
        this.interrupted = interrupted;
    }
}
