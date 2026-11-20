package com.zifang.z.etl.core.writer;

/**
 * 作业写出器（可关闭的 {@link RowWriter}）。
 *
 * @author zifang
 */
public interface JobWriter extends RowWriter {

    /**
     * 关闭（刷批 / 关连接 / 落文件收尾）。
     *
     * @throws Exception 关闭失败
     */
    void close() throws Exception;
}
