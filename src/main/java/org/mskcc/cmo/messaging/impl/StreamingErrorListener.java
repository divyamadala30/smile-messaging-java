package org.mskcc.cmo.messaging.impl;

import io.nats.client.Connection;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;

class StreamingErrorListener implements ErrorListener {
    @Override
    public void errorOccurred(Connection conn, String error) {
        System.out.println("The server notificed the client with: " + error);
    }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        System.out.println("The connection handled an exception: " + exp.getLocalizedMessage());
    }

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        System.out.println("A slow consumer dropped messages: " + consumer.getDroppedCount());

    }
}
