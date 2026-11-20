package com.zifang.z.etl.core.reader;

import com.zifang.z.etl.core.runtime.PullSource;
import com.zifang.z.etl.core.runtime.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 连接器取数器：把任意拉取式源（文件 / HTTP / 消息 / 脚本 …）按页吐出来。
 *
 * <p>口径与 JDBC 取数器一致 —— **页级回调**：调用方在「本页写成功」之后推进水位，
 * 崩在中途时水位停在上一个成功页（断点续传语义不变）。
 *
 * @author zifang
 */
public class ConnectorRowReader implements PagedRowReader {

    private final PullSource source;
    private final int pageSize;

    /**
     * @param source   拉取式源（已打开）
     * @param pageSize 页大小
     */
    public ConnectorRowReader(PullSource source, int pageSize) {
        this.source = source;
        this.pageSize = Math.max(1, pageSize);
    }

    @Override
    public long readPages(PageHandler handler) throws Exception {
        long total = 0L;
        List<Map<String, Object>> page = new ArrayList<Map<String, Object>>();
        Record record;
        while ((record = source.next()) != null) {
            page.add(record.row());
            if (page.size() >= pageSize) {
                handler.handle(page);
                total += page.size();
                page = new ArrayList<Map<String, Object>>();
            }
        }
        if (!page.isEmpty()) {
            handler.handle(page);
            total += page.size();
        }
        return total;
    }

    /** 源（取证用）。 */
    public PullSource source() {
        return source;
    }
}
