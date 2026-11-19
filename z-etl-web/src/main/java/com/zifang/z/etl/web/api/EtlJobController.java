package com.zifang.z.etl.web.api;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.meta.api.annotation.BizApi;
import com.zifang.z.meta.api.annotation.BizField;
import com.zifang.z.meta.api.annotation.BizParam;
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
 * 作业管控 REST 面。
 *
 * <pre>
 * POST /api/etl/jobs                 登记作业（body = JSON 或 yml 描述原文）
 * GET  /api/etl/jobs                 作业名列表
 * GET  /api/etl/jobs/{name}          作业描述
 * POST /api/etl/jobs/{name}/run      触发作业（首跑全量 → 之后自动增量）
 * GET  /api/etl/jobs/{name}/watermark 水位记录
 * GET  /api/etl/jobs/{name}/runs     运行履历
 * </pre>
 *
 * @author zifang
 */
@RestController
@RequestMapping("/api/etl/jobs")
public class EtlJobController {

    private final EtlJobService service;

    public EtlJobController(EtlJobService service) {
        this.service = service;
    }

    /**
     * 登记作业。
     *
     * @param descriptorText 描述原文（JSON 或 yml）
     * @return 作业名
     */
    @PostMapping
    @BizApi(summary = "登记作业", description = "登记作业。", tags = { "z-etl" })
    public Map<String, Object> register(@RequestBody @BizParam(name = "descriptorText", type = BizField.FieldType.STRING, description = "描述原文（JSON 或 yml）", position = BizParam.Position.BODY) String descriptorText) {
        String name = service.register(descriptorText);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("name", name);
        body.put("registered", Boolean.TRUE);
        return body;
    }

    /**
     * 作业名列表。
     *
     * @return 作业名
     */
    @GetMapping
    @BizApi(summary = "作业名列表", description = "作业名列表。", tags = { "z-etl" })
    public Map<String, Object> list() {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("jobs", service.list());
        return body;
    }

    /**
     * 作业描述。
     *
     * @param name 作业名
     * @return 描述
     */
    @GetMapping("/{name}")
    @BizApi(summary = "作业描述", description = "作业描述。", tags = { "z-etl" })
    public ResponseEntity<JobDescriptor> get(@PathVariable("name") @BizParam(name = "name", type = BizField.FieldType.STRING, description = "作业名", position = BizParam.Position.PATH) String name) {
        JobDescriptor descriptor = service.get(name);
        return descriptor == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(descriptor);
    }

    /**
     * 触发作业。
     *
     * @param name 作业名
     * @return 运行结果
     */
    @PostMapping("/{name}/run")
    @BizApi(summary = "触发作业", description = "触发作业。", tags = { "z-etl" })
    public EtlRunResult run(@PathVariable("name") @BizParam(name = "name", type = BizField.FieldType.STRING, description = "作业名", position = BizParam.Position.PATH) String name) {
        return service.run(name);
    }

    /**
     * 水位记录。
     *
     * @param name 作业名
     * @return 水位
     */
    @GetMapping("/{name}/watermark")
    @BizApi(summary = "水位记录", description = "水位记录。", tags = { "z-etl" })
    public ResponseEntity<Checkpoint> watermark(@PathVariable("name") @BizParam(name = "name", type = BizField.FieldType.STRING, description = "作业名", position = BizParam.Position.PATH) String name) {
        Checkpoint checkpoint = service.watermark(name);
        return checkpoint == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(checkpoint);
    }

    /**
     * 运行履历。
     *
     * @param name  作业名
     * @param limit 条数上限
     * @return 履历
     */
    @GetMapping("/{name}/runs")
    @BizApi(summary = "运行履历", description = "运行履历。", tags = { "z-etl" })
    public Map<String, Object> runs(@PathVariable("name") @BizParam(name = "name", type = BizField.FieldType.STRING, description = "作业名", position = BizParam.Position.PATH) String name,
                                    @RequestParam(value = "limit", defaultValue = "20")
                                    @BizParam(name = "limit", type = BizField.FieldType.INTEGER, description = "条数上限", position = BizParam.Position.QUERY) int limit) {
        List<EtlRunResult> history = service.history(name, limit);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("job", name);
        body.put("runs", history);
        return body;
    }
}



