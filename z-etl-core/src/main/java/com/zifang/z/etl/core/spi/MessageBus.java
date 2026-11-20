package com.zifang.z.etl.core.spi;

import java.util.List;
import java.util.Map;

/**
 * 消息总线（队列 / 主题）SPI：流水线的**消息类输入与输出**都走它。
 *
 * <p>为什么是 SPI 而不是直接把 Kafka 写死：本仓不引任何 MQ 依赖（镜像体积 + 版本耦合），
 * 由**宿主**注入实现 —— 挂 Kafka / RocketMQ / RabbitMQ 都行；
 * 宿主不注入时用默认实现 {@code JdbcMessageBus}（落库表当队列，跨进程可读）。
 *
 * <p>实现方要点：
 * <ul>
 *   <li>{@link #publish} 幂等性由使用方保证（本仓的落库汇都是幂等键 upsert 口径）；</li>
 *   <li>{@link #poll} 取出即消费（至少一次）；实现方需保证不重复投递同一条消息给同一批消费方。</li>
 * </ul>
 *
 * @author zifang
 */
public interface MessageBus {

    /**
     * 发一条消息。
     *
     * @param topic   主题 / 队列名
     * @param key     消息键（可空；分区 / 幂等键）
     * @param payload 消息体（JSON 文本）
     * @throws Exception 发送失败
     */
    void publish(String topic, String key, String payload) throws Exception;

    /**
     * 消费一批消息。
     *
     * @param topic 主题 / 队列名
     * @param limit 最多取几条
     * @return 消息列表（每条含 {@code payload} / {@code key} 等字段）
     * @throws Exception 消费失败
     */
    List<Map<String, Object>> poll(String topic, int limit) throws Exception;
}
