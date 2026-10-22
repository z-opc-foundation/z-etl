package com.zifang.z.etl.core.reader;

import java.util.List;
import java.util.Map;

/**
 * 页处理器（页级断点续传的口子）。
 *
 * <p>与 {@link RowConsumer} 的差别：回调拿到的是**一页**而不是一行 ⇒ 调用方可以在
 * 「本页写成功」之后推进水位（checkpoint）。进程被 kill 时水位停在上一个成功页，
 * 重启从断点继续，而不是从头再来。
 *
 * @author zifang
 */
public interface PageHandler {

    /**
     * 处理一页。
     *
     * @param page 一页数据（行 → 列名/值）
     * @throws Exception 处理失败（写库失败等），异常向上抛终止本次读取
     */
    void handle(List<Map<String, Object>> page) throws Exception;
}
