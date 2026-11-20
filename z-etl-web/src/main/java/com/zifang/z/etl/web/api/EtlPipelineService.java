package com.zifang.z.etl.web.api;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.Savepoint;
import com.zifang.z.etl.core.checkpoint.Savepoints;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.engine.PipelineSchedule;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.meta.api.annotation.BizMethod;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     * 登记 / 更新调度（cron）。
     *
     * @param pipelineName 流水线名
     * @param cron         cron 表达式
     * @param enabled      是否启用
     * @return 调度登记（含下一次触发时间）
     */
    @BizMethod(name = "登记流水线调度（cron）", module = "z-etl", description = "登记流水线调度（cron 表达式）。", operation = BizMethod.Operation.CREATE)
    public PipelineSchedule schedule(String pipelineName, String cron, boolean enabled) {
        return pipelineEngine.schedule(pipelineName, cron, enabled);
    }

    /**
     * 某条流水线的调度登记。
     */
    @BizMethod(name = "流水线调度登记", module = "z-etl", description = "流水线调度登记（含下一次触发时间）。", operation = BizMethod.Operation.QUERY)
    public PipelineSchedule schedule(String pipelineName) {
        return pipelineEngine.schedule(pipelineName);
    }

    /**
     * 全部调度登记。
     */
    @BizMethod(name = "流水线调度登记列表", module = "z-etl", description = "流水线调度登记列表。", operation = BizMethod.Operation.QUERY)
    public List<PipelineSchedule> schedules() {
        return pipelineEngine.schedules();
    }

    /**
     * 取消调度。
     */
    @BizMethod(name = "取消流水线调度", module = "z-etl", description = "取消流水线调度。", operation = BizMethod.Operation.ACTION)
    public boolean unschedule(String pipelineName) {
        return pipelineEngine.unschedule(pipelineName);
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
        return run(name, null);
    }

    /**
     * 触发流水线（带运行参数）。
     *
     * @param name   流水线名
     * @param params 运行参数（占位符取值；可为 {@code null}）
     * @return 运行结果
     */
    public PipelineRunResult run(String name, Map<String, String> params) throws Exception {
        return pipelineEngine.run(name, params);
    }

    /**
     * 试跑流水线（跑 N 行不落地，不碰水位与状态快照）。
     *
     * @param name       流水线名
     * @param params     运行参数（可为 {@code null}）
     * @param sampleRows 最多处理的行数
     * @return 运行结果（recordsWritten 是「会写多少行」）
     */
    public PipelineRunResult dryRun(String name, Map<String, String> params, long sampleRows) throws Exception {
        return pipelineEngine.dryRun(name, params, sampleRows);
    }

    /**
     * 导出保存点（纯读，不动现场）。
     */
    @BizMethod(name = "导出保存点", module = "z-etl", description = "键控状态快照 + 水位游标打包成自描述 JSON 报文；导出纯读。", operation = BizMethod.Operation.QUERY)
    public Savepoint exportSavepoint(String name) {
        return pipelineEngine.exportSavepoint(name);
    }

    /**
     * 灌回保存点（覆盖写；body = 保存点 JSON 或 {savepoint, targetName} 换名迁移）。
     */
    @BizMethod(name = "灌回保存点", module = "z-etl", description = "把保存点灌回流水线（覆盖写状态与水位；可换名迁移）。", operation = BizMethod.Operation.ACTION)
    public Map<String, Object> importSavepoint(String name, Map<String, Object> body) throws Exception {
        String json = Savepoints.toJson(readSavepoint(name, body));
        String target = targetName(name, body);
        if (target.equals(name)) {
            pipelineEngine.importSavepoint(json);
        } else {
            pipelineEngine.importSavepoint(json, target);
        }
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("target", target);
        result.put("restored", true);
        return result;
    }

    /** body 形状：直接是保存点报文，或包一层 {savepoint: {...}, targetName: "新名"}。 */
    private Savepoint readSavepoint(String name, Map<String, Object> body) throws Exception {
        if (body == null || body.isEmpty()) {
            throw new IllegalArgumentException("body 应为保存点报文或 {savepoint, targetName}");
        }
        Object savepoint = body.containsKey("savepoint") ? body.get("savepoint") : body;
        Savepoint parsed = com.zifang.z.etl.core.checkpoint.Savepoints.fromJson(
                new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(savepoint));
        if (!name.equals(parsed.getPipeline()) && !body.containsKey("targetName")) {
            // 报文里的导出名与路径名不一致且没点名 target ⇒ 以路径名为准（防灌错管道）
            parsed.setPipeline(name);
        }
        return parsed;
    }

    private String targetName(String name, Map<String, Object> body) {
        Object target = body == null ? null : body.get("targetName");
        return target == null || String.valueOf(target).trim().isEmpty() ? name : String.valueOf(target).trim();
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
