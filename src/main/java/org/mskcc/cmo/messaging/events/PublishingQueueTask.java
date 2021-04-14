package org.mskcc.cmo.messaging.events;

public class PublishingQueueTask {
    private String subject;
    private Object payload;

    public PublishingQueueTask() {}

    public PublishingQueueTask(String subject, Object payload) {
        this.subject = subject;
        this.payload = payload;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }
}
