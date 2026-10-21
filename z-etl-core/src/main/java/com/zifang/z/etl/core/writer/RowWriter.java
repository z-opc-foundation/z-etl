package com.zifang.z.etl.core.writer;

import java.util.Map;

/**
 * 行写出器（writer）。
 *
 * @author zifang
 */
public interface RowWriter {

    /**
     * 写一行（内部按 batchSize 攒批）。
     *
     * @param row 一行数据（目标列名 → 值）
     * @throws Exception 写失败
     */
    void write(Map<String, Object> row) throws Exception;

    /**
     * 冲刷攒批（把未提交批次落库）。
     *
     * @throws Exception 写失败
     */
    void flush() throws Exception;

    /**
     * 已写出的行数（含被 upsert 更新掉的行）。
     *
     * @return 行数
     */
    long written();
}
