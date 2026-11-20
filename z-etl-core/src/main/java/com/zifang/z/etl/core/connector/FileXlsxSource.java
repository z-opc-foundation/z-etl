package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Excel（xlsx）文件源（输入类型）：**零依赖**读第一张工作表。
 *
 * <p>实现口径：xlsx 就是 zip + XML ⇒ 用 JDK 自带的 {@link ZipFile} + StAX 解析，
 * 不引 POI（镜像体积与版本耦合都不划算）。
 * <ul>
 *   <li>共享字符串（{@code xl/sharedStrings.xml}）按索引还原文本；</li>
 *   <li>内联字符串（{@code t="inlineStr"}）、布尔、公式结果（{@code t="str"}）都还原成文本 / 布尔；</li>
 *   <li>数值单元格按内容归一（整数 → Long，小数 → BigDecimal）；</li>
 *   <li>日期靠样式（{@code styles.xml} 的数字格式）判定 ⇒ **本实现不做日期还原**，
 *       日期列请用 {@code columnTypes} 显式声明并接受文本形态，或用 CSV / JSON 输入。</li>
 * </ul>
 *
 * @author zifang
 */
public class FileXlsxSource extends AbstractPullSource {

    private String path;
    private String sheetName;
    private List<String> declaredColumns = new ArrayList<String>();
    private Map<String, Object> columnTypes;

    private ZipFile zip;
    private List<List<String>> rows = new ArrayList<List<String>>();
    private int rowIndex;
    private List<String> names = new ArrayList<String>();

    public FileXlsxSource(PipelineNode node) {
        super(node);
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        path = node.string("path");
        if (path == null) {
            throw new IllegalArgumentException("file.xlsx.source 节点 " + node.getId() + " 缺少 path");
        }
        sheetName = node.string("sheet");
        declaredColumns = node.strings("columns");
        columnTypes = node.map("columnTypes");

        zip = new ZipFile(Paths.get(path).toFile());
        List<String> sharedStrings = readSharedStrings();
        List<List<String>> sheetRows = readSheet(sharedStrings);
        boolean header = node.bool("header", true);
        if (header && !sheetRows.isEmpty()) {
            names = sheetRows.get(0);
            sheetRows.remove(0);
        } else if (!declaredColumns.isEmpty()) {
            names = new ArrayList<String>(declaredColumns);
        }
        rows = sheetRows;
        rowIndex = 0;
    }

    @Override
    protected Map<String, Object> doNext() throws Exception {
        if (rowIndex >= rows.size()) {
            return null;
        }
        List<String> values = rows.get(rowIndex++);
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        for (int i = 0; i < values.size(); i++) {
            String name = columnName(i);
            row.put(name, ColumnTypes.convert(values.get(i), ColumnTypes.declared(columnTypes, name)));
        }
        return row;
    }

    private String columnName(int index) {
        if (index < names.size()) {
            return names.get(index);
        }
        if (index < declaredColumns.size()) {
            return declaredColumns.get(index);
        }
        return "col_" + (index + 1);
    }

    /** 共享字符串表。 */
    private List<String> readSharedStrings() throws Exception {
        List<String> shared = new ArrayList<String>();
        ZipEntry entry = zip.getEntry("xl/sharedStrings.xml");
        if (entry == null) {
            return shared;
        }
        InputStream stream = zip.getInputStream(entry);
        try {
            XMLStreamReader reader = newReader(stream);
            StringBuilder current = null;
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT) {
                    String name = reader.getLocalName();
                    if ("si".equals(name)) {
                        current = new StringBuilder();
                    } else if ("t".equals(name) && current != null) {
                        current.append(reader.getElementText());
                    }
                } else if (event == XMLStreamConstants.END_ELEMENT) {
                    if ("si".equals(reader.getLocalName()) && current != null) {
                        shared.add(current.toString());
                        current = null;
                    }
                }
            }
            reader.close();
        } finally {
            stream.close();
        }
        return shared;
    }

    /** 工作表 → 行（每行是单元格文本列表）。 */
    private List<List<String>> readSheet(List<String> sharedStrings) throws Exception {
        ZipEntry entry = findSheetEntry();
        if (entry == null) {
            throw new IllegalArgumentException("file.xlsx.source 节点 " + node.getId()
                    + " 在 " + path + " 里找不到工作表");
        }
        List<List<String>> sheetRows = new ArrayList<List<String>>();
        InputStream stream = zip.getInputStream(entry);
        try {
            XMLStreamReader reader = newReader(stream);
            List<String> currentRow = null;
            int currentColumn = -1;
            String cellType = null;
            StringBuilder value = null;
            boolean inlineString = false;
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.START_ELEMENT) {
                    String name = reader.getLocalName();
                    if ("row".equals(name)) {
                        currentRow = new ArrayList<String>();
                        currentColumn = -1;
                    } else if ("c".equals(name)) {
                        currentColumn = cellColumn(reader.getAttributeValue(null, "r"));
                        String type = reader.getAttributeValue(null, "t");
                        inlineString = "inlineStr".equals(type);
                        value = new StringBuilder();
                    } else if ("t".equals(name) && inlineString) {
                        value.append(reader.getElementText());
                    } else if ("v".equals(name)) {
                        String text = reader.getElementText();
                        value = new StringBuilder(text);
                    }
                } else if (event == XMLStreamConstants.END_ELEMENT) {
                    String name = reader.getLocalName();
                    if ("c".equals(name) && currentRow != null) {
                        String text = value == null ? "" : value.toString();
                        String resolved = resolve(cellType, text, sharedStrings);
                        if (currentColumn < 0) {
                            currentRow.add(resolved);
                        } else {
                            while (currentRow.size() <= currentColumn) {
                                currentRow.add(null);
                            }
                            currentRow.set(currentColumn, resolved);
                        }
                        value = null;
                    } else if ("row".equals(name) && currentRow != null) {
                        sheetRows.add(currentRow);
                        currentRow = null;
                    }
                }
            }
            reader.close();
        } finally {
            stream.close();
        }
        return sheetRows;
    }

    /** 单元格值还原。 */
    private String resolve(String type, String text, List<String> sharedStrings) {
        if (text == null) {
            return null;
        }
        if ("s".equals(type)) {
            try {
                int index = Integer.parseInt(text.trim());
                return index >= 0 && index < sharedStrings.size() ? sharedStrings.get(index) : null;
            } catch (NumberFormatException e) {
                return text;
            }
        }
        if ("b".equals(type)) {
            return "1".equals(text.trim()) ? "true" : "false";
        }
        return text;
    }

    /** 单元格引用（{@code B3}）→ 列序号（0 起）。 */
    private int cellColumn(String reference) {
        if (reference == null) {
            return -1;
        }
        int column = 0;
        boolean any = false;
        for (int i = 0; i < reference.length(); i++) {
            char ch = reference.charAt(i);
            if (ch >= 'A' && ch <= 'Z') {
                column = column * 26 + (ch - 'A' + 1);
                any = true;
            } else if (ch >= 'a' && ch <= 'z') {
                column = column * 26 + (ch - 'a' + 1);
                any = true;
            } else {
                break;
            }
        }
        return any ? column - 1 : -1;
    }

    /** 找工作表条目（按 sheetName 精确匹配，否则取第一个 sheetN.xml）。 */
    private ZipEntry findSheetEntry() {
        if (sheetName != null) {
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (entry.getName().endsWith(sheetName + ".xml")) {
                    return entry;
                }
            }
        }
        ZipEntry fallback = null;
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            String name = entry.getName();
            if (name.startsWith("xl/worksheets/") && name.endsWith(".xml")) {
                if (fallback == null || name.compareTo(fallback.getName()) < 0) {
                    fallback = entry;
                }
            }
        }
        return fallback;
    }

    /** 安全 XML 读器（关外部实体，防 XXE）。 */
    private XMLStreamReader newReader(InputStream stream) throws XMLStreamException {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
        Reader reader = new InputStreamReader(stream, Charset.forName("UTF-8"));
        return factory.createXMLStreamReader(reader);
    }

    @Override
    public void close() {
        if (zip != null) {
            try {
                zip.close();
            } catch (Exception ignore) {
                // 关闭异常不影响主流程
            }
            zip = null;
        }
    }

    /** 工作表路径（取证用）。 */
    public Path file() {
        return Paths.get(path);
    }

    /** 文件字节数（取证用）。 */
    public long size() throws java.io.IOException {
        return Files.size(Paths.get(path));
    }
}
