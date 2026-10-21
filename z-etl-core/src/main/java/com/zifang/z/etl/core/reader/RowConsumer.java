package com.zifang.z.etl.core.reader;

import java.util.Map;

/**
 * 行消费者（reader → transform → writer 的回调口）。
 *
 * @author zifang
 */
public interface RowConsumer {

    /**
     * 消费一行。
     *
     * @param row 一行数据（列名 → 值）
     * @throws Exception 消费失败（写库失败等），异常向上抛终止本次读取
     */
    void accept(Map<String, Object> row) throws Exception;
}
