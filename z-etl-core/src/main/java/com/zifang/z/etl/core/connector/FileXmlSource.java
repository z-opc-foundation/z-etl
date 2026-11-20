package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * XML 文件源（输入类型）：StAX 流式解析，把重复元素当成一条条记录。
 *
 * <p>口径：
 * <ul>
 *   <li>{@code recordPath} 指定记录元素路径（从根元素起算的点路径，如 {@code data.item}）；
 *       不声明就取根元素下第一个子元素（在流里首次遇到时定位，不回头重读）；</li>
 *   <li>记录的子元素 → 列；同名子元素出现多次 → 合成一个 List；嵌套子元素 → 递归成 Map；</li>
 *   <li>属性 → 列，列名加前缀（默认 {@code @}）；</li>
 *   <li>{@code columns} 做字段投影；{@code columnTypes} 声明列类型（未声明的走推断）；
 *       {@code limit} 最多产多少行。</li>
 * </ul>
 *
 * @author zifang
 */
public class FileXmlSource extends AbstractPullSource {

    private String path;
    private List<String> recordPath = new ArrayList<String>();
    private String encoding = "UTF-8";
    private String attributePrefix = "@";
    private List<String> declaredColumns = new ArrayList<String>();
    private Map<String, Object> columnTypes;
    private long limit;

    private XMLStreamReader parser;
    private InputStream stream;
    /** 当前元素路径（根起算），与 recordPath 逐段对齐判断是否命中记录元素。 */
    private final Deque<String> currentPath = new ArrayDeque<String>();
    private long produced;

    public FileXmlSource(PipelineNode node) {
        super(node);
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        path = node.string("path");
        if (path == null) {
            throw new IllegalArgumentException("file.xml.source 节点 " + node.getId() + " 缺少 path");
        }
        String declared = node.string("recordPath");
        if (declared != null && !declared.trim().isEmpty()) {
            for (String part : declared.trim().split("\\.")) {
                if (!part.isEmpty()) {
                    recordPath.add(part);
                }
            }
        }
        encoding = node.string("encoding", "UTF-8");
        attributePrefix = node.string("attributePrefix", "@");
        declaredColumns = node.strings("columns");
        columnTypes = node.map("columnTypes");
        limit = node.longValue("limit", 0L);

        stream = Files.newInputStream(Paths.get(path));
        parser = XMLInputFactory.newInstance().createXMLStreamReader(stream, Charset.forName(encoding).name());
    }

    @Override
    protected Map<String, Object> doNext() throws Exception {
        if (limit > 0L && produced >= limit) {
            return null;
        }
        while (parser.hasNext()) {
            int event = parser.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                currentPath.addLast(parser.getLocalName());
                if (recordPath.isEmpty() && currentPath.size() == 2) {
                    // 未声明记录路径：根元素下第一个子元素就是记录模板
                    recordPath.addAll(currentPath);
                }
                if (isRecordElement()) {
                    Map<String, Object> row = readRecord(parser);
                    currentPath.removeLast();
                    produced++;
                    return row;
                }
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                currentPath.removeLast();
            }
        }
        return null;
    }

    /** 当前元素路径是否正好落在记录元素上（逐段相等）。 */
    private boolean isRecordElement() {
        if (recordPath.isEmpty() || recordPath.size() != currentPath.size()) {
            return false;
        }
        int index = 0;
        for (String part : currentPath) {
            if (!recordPath.get(index).equals(part)) {
                return false;
            }
            index++;
        }
        return true;
    }

    /** 读一条记录：属性 + 子元素（重复子元素合成 List，嵌套递归成 Map）。 */
    private Map<String, Object> readRecord(XMLStreamReader reader) throws Exception {
        Map<String, Object> raw = new LinkedHashMap<String, Object>();
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            putColumn(raw, attributePrefix + reader.getAttributeLocalName(i),
                    reader.getAttributeValue(i));
        }
        while (true) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                putColumn(raw, reader.getLocalName(), readElement(reader));
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                return project(raw);
            }
        }
    }

    /** 读单个元素：有子元素递归成 Map，否则取文本（空文本 → null）。 */
    private Object readElement(XMLStreamReader reader) throws Exception {
        StringBuilder text = new StringBuilder();
        Map<String, Object> nested = null;
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            if (nested == null) {
                nested = new LinkedHashMap<String, Object>();
            }
            nested.put(attributePrefix + reader.getAttributeLocalName(i), reader.getAttributeValue(i));
        }
        while (true) {
            int event = reader.next();
            if (event == XMLStreamConstants.CHARACTERS || event == XMLStreamConstants.CDATA
                    || event == XMLStreamConstants.SPACE) {
                text.append(reader.getText());
            } else if (event == XMLStreamConstants.START_ELEMENT) {
                if (nested == null) {
                    nested = new LinkedHashMap<String, Object>();
                }
                nested.put(reader.getLocalName(), readElement(reader));
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                if (nested != null) {
                    return nested;
                }
                String value = text.toString().trim();
                return value.isEmpty() ? null : value;
            }
        }
    }

    /** 同名列合成 List（保留出现次序）；投影之外的名字直接丢。 */
    @SuppressWarnings("unchecked")
    private void putColumn(Map<String, Object> row, String name, Object value) {
        if (!declaredColumns.isEmpty() && !declaredColumns.contains(name)) {
            return;
        }
        Object previous = row.get(name);
        if (!row.containsKey(name)) {
            row.put(name, value);
            return;
        }
        if (previous instanceof List) {
            ((List<Object>) previous).add(value);
            return;
        }
        List<Object> list = new ArrayList<Object>();
        list.add(previous);
        list.add(value);
        row.put(name, list);
    }

    /** 类型归一（投影已在入列时做过）。 */
    private Map<String, Object> project(Map<String, Object> raw) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            row.put(entry.getKey(), ColumnTypes.convert(entry.getValue(),
                    ColumnTypes.declared(columnTypes, entry.getKey())));
        }
        return row;
    }

    @Override
    public void close() {
        if (parser != null) {
            try {
                parser.close();
            } catch (Exception ignore) {
                // 关闭异常不影响主流程
            }
            parser = null;
        }
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception ignore) {
                // 关闭异常不影响主流程
            }
            stream = null;
        }
    }
}
