package com.zifang.z.etl.core.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.CountingSink;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON 文件汇（输出类型）：把流写成 JSON 文件。
 *
 * <p>两种形态：{@code ndjson}（默认，一行一个 JSON 对象，便于追加与流式消费）、
 * {@code json}（整个文件一个 JSON 数组，收尾时一次性落盘）。
 *
 * @author zifang
 */
public class FileJsonSink implements CountingSink {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PipelineNode node;
    private String path;
    private String format = "ndjson";
    private String encoding = "UTF-8";
    private boolean append;
    private List<String> declaredColumns;

    private BufferedWriter writer;
    private List<Map<String, Object>> collected = new ArrayList<Map<String, Object>>();
    private long written;

    public FileJsonSink(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        path = node.string("path");
        if (path == null) {
            throw new IllegalArgumentException("file.json.sink 节点 " + node.getId() + " 缺少 path");
        }
        format = node.string("format", "ndjson").toLowerCase();
        encoding = node.string("encoding", "UTF-8");
        append = node.bool("append", false);
        declaredColumns = node.strings("columns");

        Path target = Paths.get(path);
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        if ("json".equals(format)) {
            return;
        }
        if (append) {
            writer = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(target, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    Charset.forName(encoding)));
        } else {
            writer = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(target, StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE),
                    Charset.forName(encoding)));
        }
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        if (declaredColumns.isEmpty()) {
            body.putAll(record.row());
        } else {
            for (String column : declaredColumns) {
                body.put(column, record.row().get(column));
            }
        }
        if ("json".equals(format)) {
            collected.add(body);
        } else {
            writer.write(MAPPER.writeValueAsString(body));
            writer.newLine();
        }
        written++;
    }

    @Override
    public void processWatermark(long watermark) {
        // 汇不涉水位
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 汇不涉定时器
    }

    @Override
    public void close() throws Exception {
        if ("json".equals(format)) {
            Path target = Paths.get(path);
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(target, StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE),
                    Charset.forName(encoding)));
            try {
                out.write(MAPPER.writeValueAsString(collected));
            } finally {
                out.flush();
                out.close();
            }
            return;
        }
        if (writer != null) {
            writer.flush();
            writer.close();
            writer = null;
        }
    }

    @Override
    public long written() {
        return written;
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
