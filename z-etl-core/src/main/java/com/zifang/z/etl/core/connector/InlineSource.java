package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.OperatorContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内联常量源（输入类型）：把声明里的静态行当成一条流。
 *
 * <p>用途：字典表 / 维表 / 测试夹具 —— 不落文件、不连库，声明里写死即可。
 *
 * <pre>
 * - id: dim
 *   type: inline.source
 *   config:
 *     rows:
 *       - {code: A, name: 正常}
 *       - {code: B, name: 冻结}
 * </pre>
 *
 * @author zifang
 */
public class InlineSource extends AbstractPullSource {

    private List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    private int index;

    public InlineSource(PipelineNode node) {
        super(node);
    }

    @Override
    public void open(OperatorContext context) throws Exception {
        super.open(context);
        List<Map<String, Object>> declared = node.mapList("rows");
        Map<String, Object> columnTypes = node.map("columnTypes");
        for (Map<String, Object> row : declared) {
            Map<String, Object> typed = new LinkedHashMap<String, Object>();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                typed.put(entry.getKey(), ColumnTypes.convert(entry.getValue(),
                        ColumnTypes.declared(columnTypes, entry.getKey())));
            }
            rows.add(typed);
        }
        index = 0;
    }

    @Override
    protected Map<String, Object> doNext() {
        if (index >= rows.size()) {
            return null;
        }
        return rows.get(index++);
    }
}
