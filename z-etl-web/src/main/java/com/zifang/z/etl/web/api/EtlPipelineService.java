package com.zifang.z.etl.web.api;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.meta.api.annotation.BizMethod;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 流水线管控服务（web 层），与 {@link EtlJobService} 同形状。
 *
 * @author zifang
 */
@Service
public class EtlPipelineService {

    private final PipelineEngine pipelineEngine;

    public EtlPipelineService(PipelineEngine pipelineEngine) {
        this.pipelineEngine = pipelineEngine;
    }

    /**
     * 登记流水线（声明文本可以是 JSON 或 yml）。
     */
    @BizMethod(name = "登记流水线", module = "z-etl", description = "登记流水线（声明文本可以是 JSON 或 yml）。", operation = BizMethod.Operation.CREATE)
    public String register(String specText) {
        return pipelineEngine.register(specText);
    }

    /**
     * 流水线名列表。
     */
    @BizMethod(name = "流水线名列表", module = "z-etl", description = "流水线名列表。", operation = BizMethod.Operation.QUERY)
    public List<String> list() {
        return pipelineEngine.listNames();
    }

    /**
     * 取流水线定义。
     */
    @BizMethod(name = "取流水线定义", module = "z-etl", description = "取流水线定义。", operation = BizMethod.Operation.QUERY)
    public com.zifang.z.etl.core.pipeline.PipelineSpec get(String name) {
        return pipelineEngine.get(name);
    }

    /**
     * 计划解释（拓扑序文本计划 + 优化备注）。
     */
    @BizMethod(name = "计划解释", module = "z-etl", description = "拓扑序文本计划 + 优化备注。", operation = BizMethod.Operation.QUERY)
    public String explain(String name) {
        return pipelineEngine.explain(name);
    }

    /**
     * 触发流水线。
     */
    @BizMethod(name = "触发流水线", module = "z-etl", description = "触发流水线。", operation = BizMethod.Operation.ACTION)
    public PipelineRunResult run(String name) throws Exception {
        return pipelineEngine.run(name);
    }

    /**
     * 水位记录。
     */
    @BizMethod(name = "流水线水位记录", module = "z-etl", description = "流水线水位记录。", operation = BizMethod.Operation.ACTION)
    public Checkpoint watermark(String name) {
        return pipelineEngine.watermark(name);
    }

    /**
     * 运行履历。
     */
    @BizMethod(name = "流水线运行履历", module = "z-etl", description = "流水线运行履历。", operation = BizMethod.Operation.ACTION)
    public List<PipelineRunResult> history(String name, int limit) {
        return pipelineEngine.history(name, limit);
    }
}
