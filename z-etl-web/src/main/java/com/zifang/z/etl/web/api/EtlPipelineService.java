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
     * 运行总览：窗口计数 + 每条流水线的最近一次运行 + 调度登记。
     *
     * @param recentLimit 最近运行明细条数
     */
    @BizMethod(name = "运行总览", module = "z-etl",
            description = "近 24h/7d 成功失败计数 + 每条流水线最近一次运行 + 调度登记（运行总览页数据面）。", operation = BizMethod.Operation.QUERY)
    public Map<String, Object> overview(int recentLimit) {
        List<PipelineRunResult> runs = pipelineEngine.recentRuns(500);
        long now = System.currentTimeMillis();
        long start24h = now - 24L * 3600L * 1000L;
        long start7d = now - 7L * 24L * 3600L * 1000L;
        Map<String, Object> h24 = windowCounts(runs, start24h);
        Map<String, Object> d7 = windowCounts(runs, start7d);

        Map<String, PipelineRunResult> lastByPipeline = new LinkedHashMap<String, PipelineRunResult>();
        for (PipelineRunResult run : runs) {
            if (!lastByPipeline.containsKey(run.getPipelineName())) {
                lastByPipeline.put(run.getPipelineName(), run);
            }
        }
        List<Map<String, Object>> pipelines = new java.util.ArrayList<Map<String, Object>>();
        for (String name : pipelineEngine.listNames()) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("name", name);
            PipelineRunResult last = lastByPipeline.get(name);
            if (last != null) {
                row.put("lastStatus", last.getStatus());
                row.put("lastStartedAt", Long.valueOf(last.getStartedAt()));
                row.put("lastDurationMs", Long.valueOf(last.durationMs()));
                row.put("lastRecordsRead", Long.valueOf(last.getRecordsRead()));
                row.put("lastRecordsWritten", Long.valueOf(last.getRecordsWritten()));
            } else {
                row.put("lastStatus", null);
            }
            PipelineSchedule schedule = schedule(name);
            if (schedule != null) {
                Map<String, Object> sched = new LinkedHashMap<String, Object>();
                sched.put("cronExpression", schedule.getCronExpression());
                sched.put("enabled", Boolean.valueOf(schedule.isEnabled()));
                sched.put("nextFireTime", schedule.getNextFireTime());
                row.put("schedule", sched);
            } else {
                row.put("schedule", null);
            }
            pipelines.add(row);
        }

        List<Map<String, Object>> recent = new java.util.ArrayList<Map<String, Object>>();
        int limit = recentLimit <= 0 ? 20 : Math.min(recentLimit, 200);
        for (PipelineRunResult run : runs) {
            if (recent.size() >= limit) {
                break;
            }
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("pipeline", run.getPipelineName());
            row.put("status", run.getStatus());
            row.put("mode", run.getMode());
            row.put("startedAt", Long.valueOf(run.getStartedAt()));
            row.put("durationMs", Long.valueOf(run.durationMs()));
            row.put("recordsRead", Long.valueOf(run.getRecordsRead()));
            row.put("recordsWritten", Long.valueOf(run.getRecordsWritten()));
            row.put("notes", run.getNotes());
            recent.add(row);
        }

        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("h24", h24);
        body.put("d7", d7);
        body.put("pipelines", pipelines);
        body.put("recentRuns", recent);
        return body;
    }

    /** 窗口计数（成功 / 失败 / 总数）。 */
    private static Map<String, Object> windowCounts(List<PipelineRunResult> runs, long windowStart) {
        int success = 0;
        int failed = 0;
        for (PipelineRunResult run : runs) {
            if (run.getStartedAt() < windowStart) {
                continue;
            }
            if (PipelineRunResult.STATUS_FAILED.equals(run.getStatus())) {
                failed++;
            } else {
                success++;
            }
        }
        Map<String, Object> counts = new LinkedHashMap<String, Object>();
        counts.put("success", Integer.valueOf(success));
        counts.put("failed", Integer.valueOf(failed));
        counts.put("total", Integer.valueOf(success + failed));
        return counts;
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
