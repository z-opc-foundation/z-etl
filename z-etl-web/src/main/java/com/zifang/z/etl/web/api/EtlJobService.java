package com.zifang.z.etl.web.api;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.JobRepository;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorParser;
import com.zifang.z.etl.core.job.JobDescriptorValidator;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 作业管控服务（web 层）。
 *
 * @author zifang
 */
@Service
public class EtlJobService {

    private final JobRepository jobRepository;
    private final CheckpointStore checkpointStore;
    private final EtlEngine etlEngine;

    public EtlJobService(JobRepository jobRepository, CheckpointStore checkpointStore, EtlEngine etlEngine) {
        this.jobRepository = jobRepository;
        this.checkpointStore = checkpointStore;
        this.etlEngine = etlEngine;
    }

    /**
     * 登记作业（描述文本可以是 JSON 或 yml）。
     *
     * @param descriptorText 描述原文
     * @return 作业名
     */
    public String register(String descriptorText) {
        JobDescriptor descriptor = JobDescriptorParser.parse(descriptorText);
        JobDescriptorValidator.validate(descriptor);
        jobRepository.save(descriptor);
        return descriptor.getName();
    }

    /**
     * 作业名列表。
     *
     * @return 作业名
     */
    public List<String> list() {
        return jobRepository.listNames();
    }

    /**
     * 取作业描述。
     *
     * @param name 作业名
     * @return 描述（不存在返回 null）
     */
    public JobDescriptor get(String name) {
        return jobRepository.find(name);
    }

    /**
     * 触发作业。
     *
     * @param name 作业名
     * @return 运行结果
     */
    public EtlRunResult run(String name) {
        return etlEngine.run(name);
    }

    /**
     * 取水位记录（「首跑后自动切增量」的水位可见性）。
     *
     * @param name 作业名
     * @return 水位记录（没有返回 null）
     */
    public Checkpoint watermark(String name) {
        return checkpointStore.load(name);
    }

    /**
     * 运行履历。
     *
     * @param name  作业名
     * @param limit 条数上限（≤0 不限）
     * @return 履历
     */
    public List<EtlRunResult> history(String name, int limit) {
        return jobRepository.runHistory(name, limit);
    }
}
