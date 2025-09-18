package zm.gov.moh.hie.scp.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Header {
    @JsonProperty("sending_application")
    private String sendingApplication;
    
    @JsonProperty("sending_facility")
    private String sendingFacility;
    
    @JsonProperty("receiving_application")
    private String receivingApplication;
    
    @JsonProperty("receiving_facility")
    private String receivingFacility;
    
    @JsonProperty("date_time_of_message")
    private String dateTimeOfMessage;
    
    @JsonProperty("message_type")
    private String messageType;
    
    @JsonProperty("message_control_id")
    private String messageControlId;
    
    @JsonProperty("processing_id")
    private String processingId;
    
    @JsonProperty("version_id")
    private String versionId;
    
    @JsonProperty("country_code")
    private String countryCode;

    // Default constructor
    public Header() {}

    // Getters and setters
    public String getSendingApplication() {
        return sendingApplication;
    }

    public void setSendingApplication(String sendingApplication) {
        this.sendingApplication = sendingApplication;
    }

    public String getSendingFacility() {
        return sendingFacility;
    }

    public void setSendingFacility(String sendingFacility) {
        this.sendingFacility = sendingFacility;
    }

    public String getReceivingApplication() {
        return receivingApplication;
    }

    public void setReceivingApplication(String receivingApplication) {
        this.receivingApplication = receivingApplication;
    }

    public String getReceivingFacility() {
        return receivingFacility;
    }

    public void setReceivingFacility(String receivingFacility) {
        this.receivingFacility = receivingFacility;
    }

    public String getDateTimeOfMessage() {
        return dateTimeOfMessage;
    }

    public void setDateTimeOfMessage(String dateTimeOfMessage) {
        this.dateTimeOfMessage = dateTimeOfMessage;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getMessageControlId() {
        return messageControlId;
    }

    public void setMessageControlId(String messageControlId) {
        this.messageControlId = messageControlId;
    }

    public String getProcessingId() {
        return processingId;
    }

    public void setProcessingId(String processingId) {
        this.processingId = processingId;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }
}