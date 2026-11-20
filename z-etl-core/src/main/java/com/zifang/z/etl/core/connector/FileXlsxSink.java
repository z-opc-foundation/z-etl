package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.CountingSink;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Excel 文件汇（输出类型）：把流写成真正的 xlsx（**零依赖**：JDK 自带 zip + 手写 worksheet XML）。
 *
 * <p>口径：
 * <ul>
 *   <li>{@code columns} 决定列顺序；没声明时按**首行**的键序（后续行缺列补空）；</li>
 *   <li>{@code header=true}（默认）第一行写列名；</li>
 *   <li>单元格类型：数字 → 数值单元格（{@code BigDecimal} 用 toPlainString 防科学计数法）；
 *       布尔 → {@code t="b"}；时间（Timestamp/Date/LocalDateTime）→ **Excel 序列数 + 日期样式**
 *       （styles.xml 的 numFmtId 14，1900 日期系统基日 1899-12-30）⇒ 用 {@link FileXlsxSource}
 *       读回来还是 Timestamp（真往返）；其它 → 内联字符串（XML 转义）；</li>
 *   <li>覆盖写（{@code append} 不支持 —— xlsx 追加要整包重写，不值得）；</li>
 *   <li>行随到随写（流式 zip），{@code close()} 收尾（流水线收尾时刷包）。</li>
 * </ul>
 *
 * @author zifang
 */
public class FileXlsxSink implements CountingSink {

    /** 1900 日期系统的序列数基日（Excel 的著名BUG：把 1900 当闰年，基日取 1899-12-30 修正）。 */
    private static final long EXCEL_EPOCH_MILLIS = Timestamp.valueOf("1899-12-30 00:00:00").getTime();

    private static final String DATE_STYLE_INDEX = "1";

    private final PipelineNode node;
    private String path;
    private String sheetName = "Sheet1";
    private boolean header = true;
    private List<String> declaredColumns;

    private ZipOutputStream zip;
    private List<String> columns;
    private boolean headerWritten;
    private int rowIndex;
    private long written;

    /**
     * @param node 节点声明
     */
    public FileXlsxSink(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        path = node.string("path");
        if (path == null) {
            throw new IllegalArgumentException("file.xlsx.sink 节点 " + node.getId() + " 缺少 path");
        }
        sheetName = node.string("sheetName", "Sheet1");
        header = node.bool("header", true);
        declaredColumns = node.strings("columns");
        columns = new ArrayList<String>(declaredColumns);

        Path target = Paths.get(path);
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        OutputStream out = Files.newOutputStream(target);
        zip = new ZipOutputStream(out, StandardCharsets.UTF_8);
        putEntry("[Content_Types].xml", contentTypes());
        putEntry("_rels/.rels", rootRels());
        putEntry("xl/workbook.xml", workbook());
        putEntry("xl/_rels/workbook.xml.rels", workbookRels());
        putEntry("xl/styles.xml", styles());
        zip.putNextEntry(new ZipEntry("xl/worksheets/sheet1.xml"));
        write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
        write("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>");
    }

    @Override
    public void processElement(Record record, Output output) throws Exception {
        Map<String, Object> row = record.row();
        if (columns.isEmpty()) {
            columns = new ArrayList<String>(new LinkedHashSet<String>(row.keySet()));
        }
        if (header && !headerWritten) {
            writeRow(new ArrayList<Object>(columns));
            headerWritten = true;
        }
        List<Object> values = new ArrayList<Object>(columns.size());
        for (String column : columns) {
            values.add(row.get(column));
        }
        writeRow(values);
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
        if (zip == null) {
            return;
        }
        write("</sheetData></worksheet>");
        zip.closeEntry();
        zip.finish();
        zip.close();
        zip = null;
    }

    /** 已写出的行数（不含表头）。 */
    @Override
    public long written() {
        return written;
    }

    @Override
    public String operatorId() {
        return node.getId();
    }

    /** 写一行（含表头行；值可为 {@code null} ⇒ 跳过该单元格）。 */
    private void writeRow(List<?> values) throws Exception {
        rowIndex++;
        StringBuilder sb = new StringBuilder();
        sb.append("<row r=\"").append(rowIndex).append("\">");
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value == null) {
                continue;
            }
            String reference = colRef(i) + rowIndex;
            sb.append(cell(reference, value));
        }
        sb.append("</row>");
        write(sb.toString());
    }

    /** 单元格编码：数字 → 数值；布尔 → t="b"；时间 → 序列数 + 日期样式；其它 → 内联字符串。 */
    private String cell(String reference, Object value) {
        if (value instanceof Number) {
            String text = value instanceof BigDecimal
                    ? ((BigDecimal) value).toPlainString()
                    : String.valueOf(value);
            return "<c r=\"" + reference + "\"><v>" + text + "</v></c>";
        }
        if (value instanceof Boolean) {
            return "<c r=\"" + reference + "\" t=\"b\"><v>" + (((Boolean) value) ? "1" : "0") + "</v></c>";
        }
        Timestamp timestamp = asTimestamp(value);
        if (timestamp != null) {
            double serial = (timestamp.getTime() - EXCEL_EPOCH_MILLIS) / 86400000.0;
            return "<c r=\"" + reference + "\" s=\"" + DATE_STYLE_INDEX + "\"><v>" + serial(serial) + "</v></c>";
        }
        return "<c r=\"" + reference + "\" t=\"inlineStr\"><is><t>"
                + escape(String.valueOf(value)) + "</t></is></c>";
    }

    /** 时间族 → Timestamp（Timestamp/Date/LocalDateTime；其它返回 {@code null}）。 */
    private Timestamp asTimestamp(Object value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof java.sql.Date) {
            return new Timestamp(((java.sql.Date) value).getTime());
        }
        if (value instanceof Date) {
            return new Timestamp(((Date) value).getTime());
        }
        if (value instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) value);
        }
        return null;
    }

    /** 序列数文本：整天不带小数点，带时间保留到毫秒（6 位小数内）。 */
    private String serial(double serial) {
        if (serial == Math.floor(serial)) {
            return String.valueOf((long) serial);
        }
        return BigDecimal.valueOf(serial).setScale(6, RoundingMode.HALF_UP)
                .stripTrailingZeros().toPlainString();
    }

    /** 列序号 → Excel 列字母（0 → A，25 → Z，26 → AA）。 */
    private static String colRef(int index) {
        StringBuilder sb = new StringBuilder();
        int current = index;
        while (current >= 0) {
            sb.insert(0, (char) ('A' + current % 26));
            current = current / 26 - 1;
        }
        return sb.toString();
    }

    private void write(String text) throws Exception {
        zip.write(text.getBytes(StandardCharsets.UTF_8));
    }

    private void putEntry(String name, String content) throws Exception {
        zip.putNextEntry(new ZipEntry(name));
        zip.write(content.getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
    }

    private static String escape(String text) {
        String escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
        return escaped.replace("\"", "&quot;").replace("'", "&apos;")
                // 控制字符（XML 1.0 不允许）→ 空格
                .replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", " ");
    }

    private String contentTypes() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                + "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
                + "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
                + "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
                + "<Override PartName=\"/xl/workbook.xml\""
                + " ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>"
                + "<Override PartName=\"/xl/worksheets/sheet1.xml\""
                + " ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
                + "<Override PartName=\"/xl/styles.xml\""
                + " ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>"
                + "</Types>";
    }

    private String rootRels() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                + "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
                + "<Relationship Id=\"rId1\""
                + " Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\""
                + " Target=\"xl/workbook.xml\"/>"
                + "</Relationships>";
    }

    private String workbook() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                + "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\""
                + " xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
                + "<sheets><sheet name=\"" + escape(sheetName) + "\" sheetId=\"1\" r:id=\"rId1\"/></sheets>"
                + "</workbook>";
    }

    private String workbookRels() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                + "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
                + "<Relationship Id=\"rId1\""
                + " Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\""
                + " Target=\"worksheets/sheet1.xml\"/>"
                + "<Relationship Id=\"rId2\""
                + " Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\""
                + " Target=\"styles.xml\"/>"
                + "</Relationships>";
    }

    /** 样式：样式 0 = 常规，样式 1 = 内置日期格式（numFmtId 14 ⇒ 读回来还原成 Timestamp）。 */
    private String styles() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                + "<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
                + "<fonts count=\"1\"><font><sz val=\"11\"/><name val=\"Calibri\"/></font></fonts>"
                + "<fills count=\"2\"><fill><patternFill patternType=\"none\"/></fill>"
                + "<fill><patternFill patternType=\"gray125\"/></fill></fills>"
                + "<borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>"
                + "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>"
                + "<cellXfs count=\"2\">"
                + "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/>"
                + "<xf numFmtId=\"14\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>"
                + "</cellXfs>"
                + "</styleSheet>";
    }
}
