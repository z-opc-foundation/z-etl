package com.zifang.z.etl.core.spi;

import java.util.Map;

/**
 * AI 执行器 SPI：{@code ai.map} 算子的补列引擎，由宿主装配层注入（future-etl 的
 * {@code AiStep} 口径 —— core 只认接口，不认任何模型 SDK / 远端服务）。
 *
 * <p>未注入时 {@code ai.map} 节点打开即报错说明，管道其余算子不受影响。
 *
 * @author zifang
 */
public interface AiExecutor {

    /**
     * 单行补列：渲染好的提示词进，模型文本出。
     *
     * @param agentCode 智能体编码（声明透传；执行器自行决定路由，可空）
     * @param system    系统提示词（可空）
     * @param prompt    渲染后的用户提示词（${列} 已展开）
     * @param row       当前行（只读视图；执行器可取结构化上下文，不得改写）
     * @return 模型输出文本（null 视为空串）
     * @throws Exception 调用失败（按管道失败策略处理）
     */
    String complete(String agentCode, String system, String prompt, Map<String, Object> row) throws Exception;
}
