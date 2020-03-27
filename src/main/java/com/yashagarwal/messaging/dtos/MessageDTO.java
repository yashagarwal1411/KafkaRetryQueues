package com.yashagarwal.messaging.dtos;

import java.util.Date;

public class MessageDTO<T> {

    public enum MessageType {
        EMAIL
    }

    private Date dateCreated;

    private T payload;

    private MessageType type;

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }
}
