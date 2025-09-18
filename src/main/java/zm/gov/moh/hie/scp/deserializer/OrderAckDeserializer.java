package zm.gov.moh.hie.scp.deserializer;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.v25.message.ACK;
import ca.uhn.hl7v2.parser.DefaultXMLParser;
import ca.uhn.hl7v2.parser.PipeParser;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.OrderAck;

public class OrderAckDeserializer implements DeserializationSchema<OrderAck> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderAckDeserializer.class);

    private transient PipeParser parser;

    @Override
    public OrderAck deserialize(byte[] bytes) {
        try {
            // Initialize parser if not already done (for serialization compatibility)
            if (parser == null) {
                parser = new PipeParser();
            }

            String hl7Message = new String(bytes);
            LOG.info("Received HL7 message for deserialization: {}",
                    hl7Message.length() > 100 ? hl7Message.substring(0, 100) + "..." : hl7Message);

            String sanitizedMessage = sanitize(hl7Message);

            // Parse the HL7 ACK message
            ACK ackMsg = (ACK) parser.parse(sanitizedMessage);

            if (ackMsg == null) {
                LOG.warn("Parsed ACK message is null");
                return null;
            }

            // Extract MSA segment data
            String acknowledgmentCode = ackMsg.getMSA().getAcknowledgmentCode().getValue();
            String messageControlId = ackMsg.getMSA().getMessageControlID().getValue();
            String textMessage = ackMsg.getMSA().getTextMessage().getValue();

            LOG.info("Successfully parsed OrderAck: code={}, messageControlId={}, textMessage={}",
                    acknowledgmentCode, messageControlId, textMessage);

            // Create OrderAck DTO with raw message
            return new OrderAck(null, acknowledgmentCode, messageControlId, textMessage, hl7Message);

        } catch (HL7Exception | RuntimeException e) {
            LOG.error("Failed to deserialize HL7 message: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(OrderAck nextElement) {
        return false;
    }

    // Sanitize HL7 message similar to the results pipeline
    private String sanitize(String input) {
        input = input.replace("\n", "\r");
        input = input.replace("\r\r", "\r");
        
        // Remove NTE segments that might cause parsing issues
        input = input.replaceAll("(?m)^NTE\\|.*(?:\\r?\\n)?", "");
        
        input = input.replace("\r\r", "\r");
        
        return input;
    }

    @Override
    public TypeInformation<OrderAck> getProducedType() {
        return TypeInformation.of(OrderAck.class);
    }
}