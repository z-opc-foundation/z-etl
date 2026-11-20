package com.zifang.z.etl.core;

import com.zifang.z.etl.core.operator.SinkOperators;
import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.HeapStateBackend;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.PipelineExecutor;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 维表关联验收（{@code dim.join}，Flink broadcast-state 关维表的口径）。
 *
 * <p>口径：右流（维表流）同键**替换**即热更新、只进状态不下传；主流探测拼行；
 * 声明 {@code ttl} 时按「水位 − ttl」过期维表条目；{@code select} 投影维表列。
 *
 * <p>⚠️ 执行序依赖：执行器按源声明序轮询拉取 ⇒ 测试里**维表源声明在主流之前**，
 * 保证维表条目先于探测到达。
 *
 * @author zifang
 */
class DimJoinOperatorTest {

    private static final long MINUTE = 60_000L;

    @Test
    @DisplayName("基本关联 + select 投影：只拼声明的维表列")
    void joinsWithProjection() throws Exception {
        String yaml = ""
                + "name: dim-demo\n"
                + "nodes:\n"
                + "  - id: dim\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + "      rows:\n"
                + "        - {id: 100, city_name: 杭州, province: 浙江, t: 1000}\n"
                + "  - id: main\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + "      rows:\n"
                + "        - {city_id: 100, amount: 5, t: 2000}\n"
                + "  - id: dj\n"
                + "    type: dim.join\n"
                + "    config:\n"
                + "      left: main\n"
                + "      right: dim\n"
                + "      leftKey: [city_id]\n"
                + "      rightKey: [id]\n"
                + "      select: [city_name]\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: dim, to: dj}\n"
                + "  - {from: main, to: dj}\n"
                + "  - {from: dj, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        List<Map<String, Object>> rows = collectRows(captured);
        System.out.println("DEBUG rows=" + rows + " read=" + result.getRecordsRead()
                + " metrics=" + result.getNodeMetrics());
        assertEquals(1, rows.size(), "维表行不下传，主流 1 行拼 1 行: " + rows);
        assertEquals("杭州", rows.get(0).get("city_name"), "维表列应拼上主流");
        assertTrue(!rows.get(0).containsKey("province"), "select 之外的维表列不拼: " + rows.get(0));
        assertEquals(Integer.valueOf(5), rows.get(0).get("amount"));
        Map<String, Long> metrics = result.getNodeMetrics().get("dj");
        assertEquals(Long.valueOf(1L), metrics.get("dim-updated"));
        assertEquals(Long.valueOf(1L), metrics.get("matched"));
    }

    @Test
    @DisplayName("热更新 + TTL：同键维表重发替换旧值，过期条目被清掉")
    void hotUpdateAndTtlExpiry() throws Exception {
        String yaml = ""
                + "name: dim-ttl\n"
                + "nodes:\n"
                + "  - id: dim\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + "      rows:\n"
                + "        - {id: 100, city_name: 旧杭州, t: " + (0) + "}\n"
                + "        - {id: 100, city_name: 新杭州, t: " + (10 * MINUTE) + "}\n"
                + "  - id: main\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + "      rows:\n"
                + "        - {city_id: 100, amount: 5, t: " + (11 * MINUTE) + "}\n"
                + "        - {city_id: 100, amount: 6, t: " + (12 * MINUTE) + "}\n"
                + "  - id: dj\n"
                + "    type: dim.join\n"
                + "    config:\n"
                + "      left: main\n"
                + "      right: dim\n"
                + "      leftKey: [city_id]\n"
                + "      rightKey: [id]\n"
                + "      select: [city_name]\n"
                + "      ttl: 5m\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: dim, to: dj}\n"
                + "  - {from: main, to: dj}\n"
                + "  - {from: dj, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(2, rows.size());
        assertEquals("旧杭州", rows.get(0).get("city_name"), "第一条主流拼当时在架的维表值");
        assertEquals("新杭州", rows.get(1).get("city_name"), "同键重发应替换旧值（热更新）");
        Map<String, Long> metrics = result.getNodeMetrics().get("dj");
        assertTrue(metrics.containsKey("expired") && metrics.get("expired") >= 1L,
                "水位过后的过期维表条目应清掉并计数，实际: " + metrics);
    }

    @Test
    @DisplayName("joinType=left：主流没命中也发（维表投影列补 null）")
    void leftOuterEmitsUnmatched() throws Exception {
        String yaml = ""
                + "name: dim-left\n"
                + "nodes:\n"
                + "  - id: dim\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + "      rows:\n"
                + "        - {id: 100, city_name: 杭州, t: 1000}\n"
                + "  - id: main\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + "      rows:\n"
                + "        - {city_id: 999, amount: 5, t: 2000}\n"
                + "  - id: dj\n"
                + "    type: dim.join\n"
                + "    config:\n"
                + "      left: main\n"
                + "      right: dim\n"
                + "      leftKey: [city_id]\n"
                + "      rightKey: [id]\n"
                + "      select: [city_name]\n"
                + "      joinType: left\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: dim, to: dj}\n"
                + "  - {from: main, to: dj}\n"
                + "  - {from: dj, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        List<Map<String, Object>> rows = collectRows(captured);
        assertEquals(1, rows.size(), "左侧行不该被丢: " + rows);
        assertEquals(Integer.valueOf(999), rows.get(0).get("city_id"));
        assertTrue(!rows.get(0).containsKey("city_name"), "inner 未命中才发 ⇒ left 模式补 null 列而非拼值: " + rows.get(0));
    }

    @Test
    @DisplayName("声明 ttl 但维表行没事件时间 ⇒ 丢弃并计数")
    void dimRowWithoutEventTimeDropped() throws Exception {
        String yaml = ""
                + "name: dim-no-et\n"
                + "nodes:\n"
                + "  - id: dim\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 100, city_name: 杭州}\n"
                + "  - id: main\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      eventTimeColumn: t\n"
                + "      eventTimeType: LONG\n"
                + "      rows:\n"
                + "        - {city_id: 100, amount: 5, t: 2000}\n"
                + "  - id: dj\n"
                + "    type: dim.join\n"
                + "    config:\n"
                + "      left: main\n"
                + "      right: dim\n"
                + "      leftKey: [city_id]\n"
                + "      rightKey: [id]\n"
                + "      ttl: 5m\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: dim, to: dj}\n"
                + "  - {from: main, to: dj}\n"
                + "  - {from: dj, to: sink}\n";
        Map<String, Operator> captured = new LinkedHashMap<String, Operator>();
        PipelineRunResult result = run(yaml, captured);
        assertEquals(0, collectRows(captured).size(), "维表行没事件时间进不了状态 ⇒ 主流没得拼");
        Map<String, Long> metrics = result.getNodeMetrics().get("dj");
        assertEquals(Long.valueOf(1L), metrics.get("no-event-time-dropped"), "应计数，实际: " + metrics);
    }

    @Test
    @DisplayName("缺 leftKey / rightKey ⇒ 跑之前就报错")
    void requiresKeys() {
        String yaml = ""
                + "name: dim-no-key\n"
                + "nodes:\n"
                + "  - id: dim\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {id: 100, city_name: 杭州}\n"
                + "  - id: main\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows:\n"
                + "        - {city_id: 100, amount: 5}\n"
                + "  - id: dj\n"
                + "    type: dim.join\n"
                + "    config:\n"
                + "      left: main\n"
                + "      right: dim\n"
                + "  - id: sink\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: dim, to: dj}\n"
                + "  - {from: main, to: dj}\n"
                + "  - {from: dj, to: sink}\n";
        try {
            run(yaml, new LinkedHashMap<String, Operator>());
            org.junit.jupiter.api.Assertions.fail("缺关联键应该报错");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("leftKey"), "应点名 leftKey，实际: " + expected.getMessage());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private PipelineRunResult run(String yaml, Map<String, Operator> captured) throws Exception {
        PipelineSpec spec = PipelineParser.parse(yaml);
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend()).execute(spec, captured);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectRows(Map<String, Operator> captured) {
        Operator operator = captured.get("sink");
        assertNotNull(operator, "应捕获到汇算子");
        return ((SinkOperators.CollectSink) operator).rows();
    }
}
