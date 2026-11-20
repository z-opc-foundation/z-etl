package com.zifang.z.etl.core.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.CountingSink;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;
import com.zifang.z.etl.core.spi.MessageBus;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 消息汇（输出类型）：把行发到消息总线（Kafka / RocketMQ / 库表队列）。
 *
 * <p>口径：
 * <ul>
 *   <li>{@code topic} 主题；{@code keyColumn} 指定从行里取哪一列当消息键；</li>
 *   <li>{@code format}：{@code json}（默认，整行 JSON）/ {@code text}（单列文本）；</li>
 *   <li>{@code keyField} 可改消息键在消息体里的字段名（默认 {@code key}）。</li>
 * </ul>
 *
 * @author zifang
 */
public class MessageSink implements CountingSink {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PipelineNode node;
    private final MessageBus messageBus;

    private String topic;
    private String keyColumn;
    private String keyField = "key";
    private String format = "json";
    private long written;

    public MessageSink(PipelineNode node, MessageBus messageBus) {
        this.node = node;
        this.messageBus = messageBus;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        topic = node.string("topic");
        if (topic == null) {
            throw new IllegalArgumentException("message.sink 节点 " + node.getId() + " 缺少 topic");
        }
        keyColumn = node.string("keyColumn");
        keyField = node.string("keyField", "key");
        format = node.string("format", "json").toLowerCase();
        if (messageBus == null) {
            throw new IllegalStateException("message.sink 节点 " + node.getId()
                    + " 需要消息总线：请由宿主注入 MessageBus 实现（Kafka / RocketMQ / 库表队列）");
        }
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        Map<String, Object> row = record.row();
        String key = null;
        if (keyColumn != null) {
            Object value = row.get(keyColumn);
            key = value == null ? null : String.valueOf(value);
        }
        String payload;
        if ("text".equals(format)) {
            if (row.size() != 1) {
                throw new IllegalArgumentException("message.sink 节点 " + node.getId()
                        + " 的 format=text 要求行里只有一列，实际 " + row.size() + " 列");
            }
            payload = String.valueOf(row.values().iterator().next());
        } else {
            Map<String, Object> body = new LinkedHashMap<String, Object>(row);
            if (key != null) {
                body.put(keyField, key);
            }
            payload = MAPPER.writeValueAsString(body);
        }
        messageBus.publish(topic, key, payload);
        written++;
    }

    @Override
    public void processWatermark(long watermark) {
        // 汇不涉水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 汇不涉定时器
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public long written() {
        return written;
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
