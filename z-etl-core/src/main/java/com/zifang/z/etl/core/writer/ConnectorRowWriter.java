package com.zifang.z.etl.core.writer;

import com.zifang.z.etl.core.runtime.CountingSink;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.OutputTag;
import com.zifang.z.etl.core.runtime.Record;

import java.util.Map;

/**
 * 连接器写出器：把行交给任意汇算子（文件 / HTTP / 消息 …）。
 *
 * <p>汇算子本身就是 DAG 层那套实现（同一个 {@code file.csv.sink}）⇒ 线性层与 DAG 层
 * 落盘 / 落消息的行为完全一致，不另写一套。
 *
 * @author zifang
 */
public class ConnectorRowWriter implements JobWriter {

    /** 空输出口（线性层没有下游，汇算子只管写）。 */
    private static final Output NOOP_OUTPUT = new Output() {
        @Override
        public void collect(Record record) {
            // 线性层没有下游
        }

        @Override
        public void collect(OutputTag tag, Record record) {
            // 线性层没有下游
        }
    };

    private final CountingSink sink;

    /**
     * @param sink 汇算子（已打开）
     */
    public ConnectorRowWriter(CountingSink sink) {
        this.sink = sink;
    }

    @Override
    public void write(Map<String, Object> row) throws Exception {
        if (row == null) {
            return;
        }
        sink.processElement(new Record(row, Record.NO_EVENT_TIME, System.currentTimeMillis(), null), NOOP_OUTPUT);
    }

    @Override
    public void flush() {
        // 连接器汇自行刷批（文件汇每行即写；HTTP 汇每行即发）
    }

    @Override
    public long written() {
        return sink.written();
    }

    @Override
    public void close() throws Exception {
        sink.close();
    }

    /** 汇算子（取证用）。 */
    public CountingSink sink() {
        return sink;
    }
}
