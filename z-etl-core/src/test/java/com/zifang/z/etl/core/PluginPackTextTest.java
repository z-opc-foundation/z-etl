package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 插件包（文本侧）验收：regex.extract / split / template / rename / flatten / json.extract。
 *
 * <p>口径全部是无状态逐行变换：原行保留、新列追加或替换；声明缺项跑之前就报错。
 *
 * @author zifang
 */
class PluginPackTextTest {

    @Test
    @DisplayName("regex.extract：捕获组落列；no-match 计数；drop 丢行")
    void regexExtract() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-regex\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {raw: '010-12345678'}\n"
                + "        - {raw: 'not-a-phone'}\n"
                + "  - id: re\n"
                + "    type: regex.extract\n"
                + "    config:\n"
                + "      column: raw\n"
                + "      pattern: \"(\\\\d{3,4})-(\\\\d{7,8})\"\n"
                + "      outputs: {1: area, 2: number}\n"
                + "      onNoMatch: drop\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: re}\n"
                + "  - {from: re, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals(1, rows.size(), "not-a-phone 被 drop");
        assertEquals("010", rows.get(0).get("area"));
        assertEquals("12345678", rows.get(0).get("number"));
        assertEquals(Long.valueOf(1L), metricsOf(result, "re").get("no-match"));
    }

    @Test
    @DisplayName("regex.extract 缺 outputs ⇒ 声明期报错")
    void regexValidation() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(""
                + "name: plugin-regex-broken\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config: {rows: [{raw: a}]}\n"
                + "  - id: re\n"
                + "    type: regex.extract\n"
                + "    config: {column: raw, pattern: '(a)'}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: re}\n"
                + "  - {from: re, to: out}\n"));
        assertTrue(error.getMessage().contains("outputs"));
    }

    @Test
    @DisplayName("split：按分隔符切进声明列名，原列替换，段数不足补 null")
    void split() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-split\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {full_name: '张 三'}\n"
                + "        - {full_name: '欧阳'}\n"
                + "  - id: sp\n"
                + "    type: split\n"
                + "    config:\n"
                + "      column: full_name\n"
                + "      separator: ' '\n"
                + "      names: [first_name, last_name]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: sp}\n"
                + "  - {from: sp, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals("张", rows.get(0).get("first_name"));
        assertEquals("三", rows.get(0).get("last_name"));
        assertNull(rows.get(0).get("full_name"), "缺省 keepOriginal=false，原列替换掉");
        assertEquals("欧阳", rows.get(1).get("first_name"));
        assertNull(rows.get(1).get("last_name"), "段数不足补 null");
    }

    @Test
    @DisplayName("template：${列名} 占位符合成；缺列/null 给空串")
    void template() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-template\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {last_name: 张, first_name: 三, city: 北京}\n"
                + "        - {last_name: 李, first_name: 四, city: null}\n"
                + "  - id: tp\n"
                + "    type: template\n"
                + "    config:\n"
                + "      template: \"${last_name}${first_name}（${city}）\"\n"
                + "      name: display\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: tp}\n"
                + "  - {from: tp, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals("张三（北京）", rows.get(0).get("display"));
        assertEquals("李四（）", rows.get(1).get("display"), "null 占位给空串");
    }

    @Test
    @DisplayName("rename：mapping 改名，其余列不动，顺序保持")
    void rename() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-rename\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {user_id: 1, uname: 张, city: 北京}\n"
                + "  - id: rn\n"
                + "    type: rename\n"
                + "    config:\n"
                + "      mapping: {user_id: id, uname: name}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: rn}\n"
                + "  - {from: rn, to: out}\n");
        List<Map<String, Object>> row = rowsOf(result);
        assertEquals(Arrays.asList("id", "name", "city"),
                new ArrayList<String>(row.get(0).keySet()), "改名保序，未声明列不动");
        assertEquals(Integer.valueOf(1), row.get(0).get("id"));
        assertEquals("张", row.get(0).get("name"));
    }

    @Test
    @DisplayName("flatten：嵌套 Map 拍成点路径列，数组不动")
    void flatten() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-flatten\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 1, user: {name: 张, address: {city: 北京, zip: '100000'}}, tags: [vip]}\n"
                + "  - id: fl\n"
                + "    type: flatten\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: fl}\n"
                + "  - {from: fl, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals("北京", rows.get(0).get("user.address.city"));
        assertEquals("100000", rows.get(0).get("user.address.zip"));
        assertEquals(Arrays.asList("vip"), rows.get(0).get("tags"), "数组不拍");
        assertNull(rows.get(0).get("user"));
    }

    @Test
    @DisplayName("json.extract：点路径取字段落列；onError=null 给 null 并计数")
    void jsonExtract() throws Exception {
        PipelineRunResult result = run(""
                + "name: plugin-json\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {payload: '{\"user\":{\"name\":\"张\"},\"total\":99.5}'}\n"                + "        - {payload: 'not-json'}\n"
                + "  - id: js\n"
                + "    type: json.extract\n"
                + "    config:\n"
                + "      column: payload\n"
                + "      fields: {user.name: user_name, total: total}\n"
                + "      onError: 'null'\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: js}\n"
                + "  - {from: js, to: out}\n");
        List<Map<String, Object>> rows = rowsOf(result);
        assertEquals("张", rows.get(0).get("user_name"));
        assertEquals(99.5, ((Number) rows.get(0).get("total")).doubleValue(), 0.0001);
        assertNull(rows.get(1).get("user_name"), "解析失败列全 null");
        assertEquals(Long.valueOf(1L), metricsOf(result, "js").get("parse-error"));
    }

    @Test
    @DisplayName("json.extract 缺省 onError=fail ⇒ 坏 JSON 炸行")
    void jsonFailFast() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> run(""
                + "name: plugin-json-fail\n"
                + "nodes:\n"
                + "  - id: src\n"
                + "    type: inline.source\n"
                + "    config: {rows: [{payload: 'not-json'}]}\n"
                + "  - id: js\n"
                + "    type: json.extract\n"
                + "    config: {column: payload, fields: {a: a}}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: src, to: js}\n"
                + "  - {from: js, to: out}\n"));
        assertTrue(error.getMessage().contains("不是合法 JSON"), error.getMessage());
    }

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Map<String, Object>> rowsOf(PipelineRunResult result) {
        SinkOperators.CollectSink sink = (SinkOperators.CollectSink) captured.get("out");
        return sink.rows();
    }

    private Map<String, Long> metricsOf(PipelineRunResult result, String nodeId) {
        return result.getNodeMetrics().get(nodeId);
    }
}
