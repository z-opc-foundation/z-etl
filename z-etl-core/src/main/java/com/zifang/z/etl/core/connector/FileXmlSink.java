package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.CountingSink;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * XML 文件汇（输出类型）：XMLStreamWriter 流式写，一行一条记录元素。
 *
 * <p>口径：{@code rootName}（默认 {@code rows}）包住全部记录，{@code recordName}
 * （默认 {@code row}）是一条记录；标量列 → 子元素文本（转义交给 writer）；
 * null → 空元素；Map / List 列跳过（XML 扁平行不装嵌套结构）。
 * 覆盖写（不追加）；{@code columns} 声明列序。
 *
 * @author zifang
 */
public class FileXmlSink implements CountingSink {

    private final PipelineNode node;
    private String path;
    private String rootName = "rows";
    private String recordName = "row";
    private String encoding = "UTF-8";
    private boolean declaration = true;
    private List<String> declaredColumns = new ArrayList<String>();

    private XMLStreamWriter writer;
    private BufferedWriter out;
    private long written;

    public FileXmlSink(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        path = node.string("path");
        if (path == null) {
            throw new IllegalArgumentException("file.xml.sink 节点 " + node.getId() + " 缺少 path");
        }
        rootName = node.string("rootName", "rows");
        recordName = node.string("recordName", "row");
        encoding = node.string("encoding", "UTF-8");
        declaration = node.bool("declaration", true);
        declaredColumns = node.strings("columns");

        Path target = Paths.get(path);
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        out = new BufferedWriter(new OutputStreamWriter(
                Files.newOutputStream(target, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE),
                Charset.forName(encoding)));
        writer = XMLOutputFactory.newInstance().createXMLStreamWriter(out);
        if (declaration) {
            writer.writeStartDocument(encoding, "1.0");
        }
        writer.writeStartElement(rootName);
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        writer.writeStartElement(recordName);
        if (declaredColumns.isEmpty()) {
            for (Map.Entry<String, Object> entry : record.row().entrySet()) {
                writeColumn(entry.getKey(), entry.getValue());
            }
        } else {
            for (String column : declaredColumns) {
                writeColumn(column, record.row().get(column));
            }
        }
        writer.writeEndElement();
        written++;
    }

    /** 写一列：标量 → 文本元素；null → 空元素；Map / List → 跳过。 */
    private void writeColumn(String name, Object value) throws Exception {
        if (value instanceof Map || value instanceof Iterable) {
            return;
        }
        writer.writeStartElement(name);
        if (value != null) {
            writer.writeCharacters(String.valueOf(value));
        }
        writer.writeEndElement();
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
        if (writer != null) {
            writer.writeEndElement();
            writer.writeEndDocument();
            writer.flush();
            writer.close();
            writer = null;
        }
        if (out != null) {
            out.flush();
            out.close();
            out = null;
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
