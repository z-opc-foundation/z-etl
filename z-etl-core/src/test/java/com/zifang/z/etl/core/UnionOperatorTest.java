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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多路合并验收：显式 {@code union} 节点 + 不写节点的多入边天然合并。
 *
 * <p>口径：union 只做透传 —— 多条入边接成一路，不改行、不去重；
 * 不声明 union 节点、多条边直插同一节点也天然合并（执行器逐边派发）；
 * 源轮询按声明序轮流拉 ⇒ 合并后的到达序按轮次交错（本测按集合断言，不钉顺序）。
 *
 * @author zifang
 */
class UnionOperatorTest {

    private final Map<String, Operator> captured = new LinkedHashMap<String, Operator>();

    private PipelineRunResult run(String yaml) throws Exception {
        captured.clear();
        return new PipelineExecutor(new MapDataSourceProvider(), new HeapStateBackend())
                .execute(PipelineParser.parse(yaml), captured);
    }

    private List<Object> ids(PipelineRunResult result) {
        List<Object> ids = new ArrayList<Object>();
        for (Map<String, Object> row : ((SinkOperators.CollectSink) captured.get("out")).rows()) {
            ids.add(row.get("id"));
        }
        return ids;
    }

    @Test
    @DisplayName("显式 union 节点：三条流合成一路，行原样透传")
    void explicitUnionMerges() throws Exception {
        PipelineRunResult result = run(""
                + "name: union-explicit\n"
                + "nodes:\n"
                + "  - id: s1\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 1, src: a}, {id: 2, src: a}]\n"
                + "  - id: s2\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 3, src: b}]\n"
                + "  - id: s3\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 4, src: c}]\n"
                + "  - id: uni\n"
                + "    type: union\n"
                + "    config: {}\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: s1, to: uni}\n"
                + "  - {from: s2, to: uni}\n"
                + "  - {from: s3, to: uni}\n"
                + "  - {from: uni, to: out}\n");
        List<Object> ids = ids(result);
        assertEquals(4, ids.size(), "三路合成 4 行");
        Set<Object> distinct = new HashSet<Object>(ids);
        assertEquals(4, distinct.size(), "union 不去重（本来也没有重复）");
        assertTrue(ids.contains(Integer.valueOf(3)) && ids.contains(Integer.valueOf(4)),
                "三路的行都在: " + ids);
    }

    @Test
    @DisplayName("不写 union 节点：两条入边直插汇节点，天然合并")
    void naturalMultiEdgeMerge() throws Exception {
        PipelineRunResult result = run(""
                + "name: union-natural\n"
                + "nodes:\n"
                + "  - id: s1\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 1, src: a}, {id: 2, src: a}]\n"
                + "  - id: s2\n"
                + "    type: inline.source\n"
                + "    config:\n"
                + "      rows: [{id: 3, src: b}]\n"
                + "  - id: out\n"
                + "    type: collect.sink\n"
                + "edges:\n"
                + "  - {from: s1, to: out}\n"
                + "  - {from: s2, to: out}\n");
        List<Object> ids = ids(result);
        assertEquals(3, ids.size(), "两条入边天然合并 3 行");
        assertTrue(ids.contains(Integer.valueOf(1)) && ids.contains(Integer.valueOf(3)),
                "两路的行都在: " + ids);
    }
}
