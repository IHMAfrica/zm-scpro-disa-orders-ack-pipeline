package zm.gov.moh.hie.scp.dto;

import java.io.Serializable;

public class MessageAck implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String acknowledgmentCode;
    private String messageControlId;
    private String textMessage;

    public MessageAck() {}

    public MessageAck(String acknowledgmentCode, String messageControlId, String textMessage) {
        this.acknowledgmentCode = acknowledgmentCode;
        this.messageControlId = messageControlId;
        this.textMessage = textMessage;
    }

    public String getAcknowledgmentCode() {
        return acknowledgmentCode;
    }

    public void setAcknowledgmentCode(String acknowledgmentCode) {
        this.acknowledgmentCode = acknowledgmentCode;
    }

    public String getMessageControlId() {
        return messageControlId;
    }

    public void setMessageControlId(String messageControlId) {
        this.messageControlId = messageControlId;
    }

    public String getTextMessage() {
        return textMessage;
    }

    public void setTextMessage(String textMessage) {
        this.textMessage = textMessage;
    }

    @Override
    public String toString() {
        return "MessageAck{" +
                "acknowledgmentCode='" + acknowledgmentCode + '\'' +
                ", messageControlId='" + messageControlId + '\'' +
                ", textMessage='" + textMessage + '\'' +
                '}';
    }
}