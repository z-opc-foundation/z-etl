package com.zifang.z.etl.web.api;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.PipelineRunResult;
import com.zifang.z.meta.api.annotation.BizApi;
import com.zifang.z.meta.api.annotation.BizField;
import com.zifang.z.meta.api.annotation.BizParam;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 流水线管控 REST 面（与 {@link EtlJobController} 同形状）。
 *
 * <pre>
 * POST /api/etl/pipelines                  登记流水线（body = JSON 或 yml 声明原文）
 * GET  /api/etl/pipelines                  流水线名列表
 * GET  /api/etl/pipelines/{name}           流水线定义
 * GET  /api/etl/pipelines/{name}/plan      计划解释（拓扑序 + 优化备注）
 * POST /api/etl/pipelines/{name}/run       触发流水线
 * GET  /api/etl/pipelines/{name}/watermark 水位记录
 * GET  /api/etl/pipelines/{name}/runs      运行履历
 * </pre>
 *
 * @author zifang
 */
@RestController
@RequestMapping("/api/etl/pipelines")
public class EtlPipelineController {

    private final EtlPipelineService service;

    public EtlPipelineController(EtlPipelineService service) {
        this.service = service;
    }

    /**
     * 登记流水线。
     */
    @PostMapping
    @BizApi(summary = "登记流水线", description = "登记流水线（body = JSON 或 yml 声明原文）。", tags = {"z-etl"})
    public Map<String, Object> register(@RequestBody @BizParam(name = "specText", type = BizField.FieldType.STRING,
            description = "声明原文（JSON 或 yml）", position = BizParam.Position.BODY) String specText) {
        String name = service.register(specText);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("name", name);
        body.put("registered", Boolean.TRUE);
        return body;
    }

    /**
     * 流水线名列表。
     */
    @GetMapping
    @BizApi(summary = "流水线名列表", description = "流水线名列表。", tags = {"z-etl"})
    public Map<String, Object> list() {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("pipelines", service.list());
        return body;
    }

    /**
     * 流水线定义。
     */
    @GetMapping("/{name}")
    @BizApi(summary = "流水线定义", description = "流水线定义。", tags = {"z-etl"})
    public ResponseEntity<PipelineSpec> get(@PathVariable("name") @BizParam(name = "name",
            type = BizField.FieldType.STRING, description = "流水线名", position = BizParam.Position.PATH) String name) {
        PipelineSpec spec = service.get(name);
        return spec == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(spec);
    }

    /**
     * 计划解释。
     */
    @GetMapping(value = "/{name}/plan", produces = MediaType.TEXT_PLAIN_VALUE)
    @BizApi(summary = "计划解释", description = "拓扑序文本计划 + 优化备注。", tags = {"z-etl"})
    public ResponseEntity<String> plan(@PathVariable("name") @BizParam(name = "name",
            type = BizField.FieldType.STRING, description = "流水线名", position = BizParam.Position.PATH) String name) {
        return ResponseEntity.ok(service.explain(name));
    }

    /**
     * 触发流水线。
     */
    @PostMapping("/{name}/run")
    @BizApi(summary = "触发流水线", description = "触发流水线。", tags = {"z-etl"})
    public PipelineRunResult run(@PathVariable("name") @BizParam(name = "name",
            type = BizField.FieldType.STRING, description = "流水线名", position = BizParam.Position.PATH) String name) throws Exception {
        return service.run(name);
    }

    /**
     * 水位记录。
     */
    @GetMapping("/{name}/watermark")
    @BizApi(summary = "流水线水位记录", description = "流水线水位记录。", tags = {"z-etl"})
    public ResponseEntity<Checkpoint> watermark(@PathVariable("name") @BizParam(name = "name",
            type = BizField.FieldType.STRING, description = "流水线名", position = BizParam.Position.PATH) String name) {
        Checkpoint checkpoint = service.watermark(name);
        return checkpoint == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(checkpoint);
    }

    /**
     * 运行履历。
     */
    @GetMapping("/{name}/runs")
    @BizApi(summary = "流水线运行履历", description = "流水线运行履历。", tags = {"z-etl"})
    public Map<String, Object> runs(@PathVariable("name") @BizParam(name = "name",
            type = BizField.FieldType.STRING, description = "流水线名", position = BizParam.Position.PATH) String name,
                                    @RequestParam(value = "limit", defaultValue = "20")
                                    @BizParam(name = "limit", type = BizField.FieldType.INTEGER, description = "条数上限",
                                            position = BizParam.Position.QUERY) int limit) {
        List<PipelineRunResult> history = service.history(name, limit);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("pipeline", name);
        body.put("runs", history);
        return body;
    }
}
