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
     * 改写计划。
     *
     * @param spec 原声明
     * @param notes 改写备注（规则往里追加，执行器汇总展示）
     * @return 改写后的声明（可原地改，也可新建）
     */
    PipelineSpec rewrite(PipelineSpec spec, List<String> notes);
}
