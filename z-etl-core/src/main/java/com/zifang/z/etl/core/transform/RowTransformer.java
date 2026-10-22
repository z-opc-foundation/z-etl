package com.zifang.z.etl.core.transform;

import java.util.Map;

/**
 * 行级变换算子。
 *
 * <p>门面化是引擎层的活（需求 008 notes.md §9.6.1 E8）：零件面能给表达式求值，但取列/写列/类型/空值策略
 * 都得引擎自己兜住，所以这里定一个统一门面，未来的行级表达式（z-util-expr）与脚本沙箱（z-script）
 * 以新算子形态挂进来即可，链的骨架不变。
 *
 * @author zifang
 */
public interface RowTransformer {

    /**
     * 变换一行。
     *
     * @param row 源行（列名 → 值）
     * @return 变换后的行（目标列名 → 值）
     */
    Map<String, Object> transform(Map<String, Object> row);

    /**
     * 步骤名（失败信息用；默认取类名）。
     *
     * @return 步骤名
     */
    default String stepName() {
        return getClass().getSimpleName();
    }
}
