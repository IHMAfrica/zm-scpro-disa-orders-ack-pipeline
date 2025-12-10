package zm.gov.moh.hie.scp.dto;

import java.io.Serializable;
import java.time.LocalDateTime;

public class OrderAck implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private String code;
    private String orderMessageRefId;
    private String message;
    private String rawMessage; // Store the original HL7 message
    private LocalDateTime createdAt; // HL7 message timestamp
    private LocalDateTime receivedAt; // Current timestamp when message is received

    public OrderAck() {}

    public OrderAck(Long id, String code, String orderMessageRefId, String message) {
        this.id = id;
        this.code = code;
        this.orderMessageRefId = orderMessageRefId;
        this.message = message;
    }

    public OrderAck(Long id, String code, String orderMessageRefId, String message, String rawMessage) {
        this.id = id;
        this.code = code;
        this.orderMessageRefId = orderMessageRefId;
        this.message = message;
        this.rawMessage = rawMessage;
    }

    public OrderAck(Long id, String code, String orderMessageRefId, String message, String rawMessage,
                    LocalDateTime createdAt, LocalDateTime receivedAt) {
        this.id = id;
        this.code = code;
        this.orderMessageRefId = orderMessageRefId;
        this.message = message;
        this.rawMessage = rawMessage;
        this.createdAt = createdAt;
        this.receivedAt = receivedAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getOrderMessageRefId() {
        return orderMessageRefId;
    }

    public void setOrderMessageRefId(String orderMessageRefId) {
        this.orderMessageRefId = orderMessageRefId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(String rawMessage) {
        this.rawMessage = rawMessage;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(LocalDateTime receivedAt) {
        this.receivedAt = receivedAt;
    }

    @Override
    public String toString() {
        return "OrderAck{" +
                "id=" + id +
                ", code='" + code + '\'' +
                ", orderMessageRefId='" + orderMessageRefId + '\'' +
                ", message='" + message + '\'' +
                ", createdAt=" + createdAt +
                ", receivedAt=" + receivedAt +
                ", rawMessage='" + (rawMessage != null ? rawMessage.substring(0, Math.min(50, rawMessage.length())) + "..." : null) + '\'' +
                '}';
    }
}