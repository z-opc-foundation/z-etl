package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.spi.MessageBus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 消息源（输入类型）：从消息总线消费一条流。
 *
 * <p>默认实现是落库表当队列（{@code JdbcMessageBus}），宿主可注入 Kafka / RocketMQ 实现。
 * 消息体是 JSON 时，字段被摊平进行里（另附 {@code __key} / {@code __created_at} / {@code __message_id}）。
 *
 * <pre>
 * - id: mq
 *   type: message.source
 *   config:
 *     topic: order.created
 *     pollSize: 500
 * </pre>
 *
 * @author zifang
 */
public class MessageSource extends AbstractPullSource {

    private final MessageBus messageBus;
    private String topic;
    private int pollSize = 100;

    private List<Map<String, Object>> buffer = new ArrayList<Map<String, Object>>();
    private int bufferIndex;

    public MessageSource(PipelineNode node, MessageBus messageBus) {
        super(node);
        this.messageBus = messageBus;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        topic = node.string("topic");
        if (topic == null) {
            throw new IllegalArgumentException("message.source 节点 " + node.getId() + " 缺少 topic");
        }
        pollSize = node.integer("pollSize", 100);
        if (messageBus == null) {
            throw new IllegalStateException("message.source 节点 " + node.getId()
                    + " 需要消息总线：请由宿主注入 MessageBus 实现（Kafka / RocketMQ / 库表队列）");
        }
        bufferIndex = 0;
    }

    @Override
    protected Map<String, Object> doNext() throws Exception {
        if (bufferIndex >= buffer.size()) {
            buffer = messageBus.poll(topic, pollSize);
            bufferIndex = 0;
            if (buffer.isEmpty()) {
                return null;
            }
        }
        return buffer.get(bufferIndex++);
    }
}
