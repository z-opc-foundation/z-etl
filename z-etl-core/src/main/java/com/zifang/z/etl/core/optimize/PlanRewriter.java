package com.zifang.z.etl.core.optimize;

import com.zifang.z.etl.core.pipeline.PipelineSpec;

import java.util.List;

/**
 * 计划改写器（DataFusion 的 {@code OptimizerRule} 形状）。
 *
 * <p>一条规则只做一件事：拿到声明图，返回改写后的声明图（可以原样返回），
 * 外加把「改了什么」记进备注 —— 优化必须可解释，不能黑箱。
 *
 * @author zifang
 */
public interface PlanRewriter {

    /**
     * 规则名（explain 输出用）。
     *
     * @return 规则名
     */
    String name();

    /**
     * 优化前的上下文注入（默认无操作）。
     *
     * <p>需要表结构才能安全改写的规则（聚合下推）靠它拿到 {@link TableSchemaResolver}；
     * 拿不到就按「不知道」保守处理。
     *
     * @param resolver 表结构解析（可为 {@code null}）
     */
    default void prepare(TableSchemaResolver resolver) {
        // 默认不需要上下文
    }

    /**
     * 改写计划。
     *
     * @param spec 原声明
     * @param notes 改写备注（规则往里追加，执行器汇总展示）
     * @return 改写后的声明（可原地改，也可新建）
     */
    PipelineSpec rewrite(PipelineSpec spec, List<String> notes);
}
