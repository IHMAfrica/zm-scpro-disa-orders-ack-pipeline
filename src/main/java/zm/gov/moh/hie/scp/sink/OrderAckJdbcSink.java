package zm.gov.moh.hie.scp.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.OrderAck;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OrderAckJdbcSink extends RichSinkFunction<OrderAck> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderAckJdbcSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private transient Connection connection;

    public OrderAckJdbcSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static OrderAckJdbcSink getSinkFunction(String jdbcUrl, String username, String password) {
        return new OrderAckJdbcSink(jdbcUrl, username, password);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(true);
        LOG.info("OrderAckJdbcSink connection established to: {}", jdbcUrl);
    }

    @Override
    public void invoke(OrderAck orderAck, Context context) throws Exception {
        LOG.info("Processing OrderAck: code={}, messageControlId={}",
                orderAck.getCode(), orderAck.getOrderMessageRefId());

        try {
            // Step 1: Insert into data.message and get the generated ID
            String insertMessageSql = "INSERT INTO data.message (message_type, processed, data) VALUES (?, ?, ?) RETURNING id";
            long messageId;

            try (PreparedStatement stmt = connection.prepareStatement(insertMessageSql)) {
                stmt.setString(1, "ACK");
                stmt.setBoolean(2, false);
                // Store the original HL7 message
                stmt.setString(3, orderAck.getRawMessage());

                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    messageId = rs.getLong(1);
                    LOG.info("Created message record with ID: {}", messageId);
                } else {
                    throw new SQLException("Failed to get generated message ID");
                }
            }

            // Step 2: Insert into crt.order_ack using the generated message ID
            String insertAckSql = "INSERT INTO crt.order_ack (id, code, order_message_ref_id, message) VALUES (?, ?, ?, ?)";
            try (PreparedStatement stmt = connection.prepareStatement(insertAckSql)) {
                stmt.setLong(1, messageId);
                stmt.setString(2, orderAck.getCode());
                stmt.setString(3, orderAck.getOrderMessageRefId());
                stmt.setString(4, orderAck.getMessage());
                int rowsAffected = stmt.executeUpdate();
                LOG.info("Inserted OrderAck record, rows affected: {}", rowsAffected);
            }

            // Step 3: Mark the message as processed
            String updateProcessedSql = "UPDATE data.message SET processed = true WHERE id = ?";
            try (PreparedStatement stmt = connection.prepareStatement(updateProcessedSql)) {
                stmt.setLong(1, messageId);
                int rowsAffected = stmt.executeUpdate();
                LOG.info("Marked message as processed, rows affected: {}", rowsAffected);
            }

            LOG.info("Successfully processed OrderAck with messageId: {}", messageId);

        } catch (SQLException e) {
            LOG.error("Failed to process OrderAck: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process OrderAck: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("OrderAckJdbcSink connection closed");
        }
        super.close();
    }
}