package org.mskcc.cmo.messaging.impl;

import io.nats.client.Connection;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import org.apache.log4j.Logger;

public class StreamingErrorListener implements ErrorListener {
    private Logger logger = Logger.getLogger(StreamingErrorListener.class);
    
    /**
     * Handles server errors
     */
    @Override
    public void errorOccurred(Connection conn, String error) {
        logger.error("The server notified the client with: " + error);
    }
    
    /**
     * Handles connection errors
     */
    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        logger.error("The connection handled an exception: " + exp.getLocalizedMessage());
    }

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        logger.error("A slow consumer dropped messages: " + consumer.getDroppedCount());

    }
}
