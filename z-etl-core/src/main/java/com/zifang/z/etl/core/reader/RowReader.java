package com.zifang.z.etl.core.reader;

/**
 * 取数器（reader）。
 *
 * <p>引擎层自己实现的理由（需求 008 notes.md §9.6.1 E3）：{@code DynamicQuery.list()} 一次性装满
 * {@code List}，没有流式消费口，也没有「上次读到哪」的概念 ⇒ reader 必须自带分页游标与水位过滤。
 *
 * @author zifang
 */
public interface RowReader {

    /**
     * 按顺序读出所有行并回调。
     *
     * @param consumer 行消费者
     * @return 读出的行数
     * @throws Exception 读取失败
     */
    long read(RowConsumer consumer) throws Exception;

    /**
     * 源侧行数（对账口径之一）。
     *
     * @return 与本次读取同口径的行数
     * @throws Exception 统计失败
     */
    long count() throws Exception;
}
