package com.zifang.z.etl.core.reader;

/**
 * 分页取数器（页级断点续传的口子）。
 *
 * <p>JDBC 取数器与**连接器取数器**（文件 / HTTP / 消息 …）都按页吐数据 ——
 * 调用方在「本页写成功」之后才推进水位 ⇒ 崩在中途水位停在上一个成功页。
 *
 * @author zifang
 */
public interface PagedRowReader {

    /**
     * 逐页读出并回调。
     *
     * @param handler 页处理器
     * @return 读出的总行数
     * @throws Exception 读取失败
     */
    long readPages(PageHandler handler) throws Exception;
}
