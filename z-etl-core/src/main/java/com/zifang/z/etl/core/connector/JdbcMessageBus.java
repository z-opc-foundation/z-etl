package com.zifang.z.etl.core.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.spi.MessageBus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 默认消息总线实现：**落库表当队列**（不引任何 MQ 依赖）。
 *
 * <p>表 {@code etl_message_outbox}：发出去的消息落一行，消费时标记 {@code consumed=1}。
 * 跨进程可读（别的服务也能查这张表），可与外部 MQ 打通（宿主注入自己的 {@link MessageBus} 即可替换）。
 *
 * @author zifang
 */
public class JdbcMessageBus implements MessageBus {

    /** 消息表名（与 {@code EtlSchema} 保持一致）。 */
    public static final String TABLE = "etl_message_outbox";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataSource dataSource;

    public JdbcMessageBus(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void publish(String topic, String key, String payload) throws Exception {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("INSERT INTO " + TABLE
                    + " (message_id, topic, msg_key, payload, created_at, consumed) VALUES (?, ?, ?, ?, ?, 0)");
            statement.setString(1, UUID.randomUUID().toString());
            statement.setString(2, topic);
            statement.setString(3, key);
            statement.setString(4, payload);
            statement.setLong(5, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("消息落库失败: " + e.getMessage(), e);
        } finally {
            close(statement);
            close(connection);
        }
    }

    @Override
    public List<Map<String, Object>> poll(String topic, int limit) throws Exception {
        List<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();
        Connection connection = null;
        PreparedStatement select = null;
        PreparedStatement mark = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            select = connection.prepareStatement("SELECT message_id, msg_key, payload, created_at FROM " + TABLE
                    + " WHERE topic = ? AND consumed = 0 ORDER BY created_at LIMIT " + Math.max(1, limit));
            select.setString(1, topic);
            rs = select.executeQuery();
            while (rs.next()) {
                Map<String, Object> message = new LinkedHashMap<String, Object>();
                String payload = rs.getString("payload");
                Map<String, Object> body = parsePayload(payload);
                message.putAll(body);
                message.put("__key", rs.getString("msg_key"));
                message.put("__created_at", rs.getLong("created_at"));
                message.put("__message_id", rs.getString("message_id"));
                messages.add(message);
            }
            rs.close();
            rs = null;
            if (!messages.isEmpty()) {
                mark = connection.prepareStatement("UPDATE " + TABLE + " SET consumed = 1 WHERE message_id = ?");
                for (Map<String, Object> message : messages) {
                    mark.setString(1, String.valueOf(message.get("__message_id")));
                    mark.addBatch();
                }
                mark.executeBatch();
            }
            return messages;
        } catch (SQLException e) {
            throw new IllegalStateException("消息消费失败: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(mark);
            close(select);
            close(connection);
        }
    }

    /** 消息体 JSON → 字段；不是 JSON 就原样塞进 {@code payload}。 */
    private Map<String, Object> parsePayload(String payload) {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        if (payload == null || payload.trim().isEmpty()) {
            return body;
        }
        try {
            JsonNode node = MAPPER.readTree(payload);
            if (node != null && node.isObject()) {
                java.util.Iterator<String> names = node.fieldNames();
                while (names.hasNext()) {
                    String name = names.next();
                    JsonNode value = node.get(name);
                    if (value == null || value.isNull()) {
                        body.put(name, null);
                    } else if (value.isNumber()) {
                        body.put(name, value.numberValue());
                    } else if (value.isBoolean()) {
                        body.put(name, Boolean.valueOf(value.booleanValue()));
                    } else if (value.isObject() || value.isArray()) {
                        body.put(name, value.toString());
                    } else {
                        body.put(name, value.asText());
                    }
                }
                return body;
            }
        } catch (Exception ignore) {
            // 不是 JSON ⇒ 按纯文本处理
        }
        body.put("payload", payload);
        return body;
    }

    /** 建消息表（幂等）。 */
    public static void ensure(DataSource dataSource) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                        + "message_id VARCHAR(64) PRIMARY KEY, "
                        + "topic VARCHAR(128), "
                        + "msg_key VARCHAR(256), "
                        + "payload TEXT, "
                        + "created_at BIGINT, "
                        + "consumed INT)");
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    private void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignore) {
                // 忽略关闭异常
            }
        }
    }
}
