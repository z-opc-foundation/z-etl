package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.JobDescriptorParser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内存版作业仓储（本地/测试）。
 *
 * <p>落库形态见 {@link JdbcJobRepository}（表 {@code etl_job_def} / {@code etl_job_run}）。
 *
 * @author zifang
 */
public class InMemoryJobRepository implements JobRepository {

    private final Map<String, String> descriptors = new LinkedHashMap<String, String>();
    private final Map<String, List<EtlRunResult>> runs = new LinkedHashMap<String, List<EtlRunResult>>();

    @Override
    public void save(JobDescriptor descriptor) {
        descriptors.put(descriptor.getName(), JobDescriptorParser.toJson(descriptor));
    }

    @Override
    public JobDescriptor find(String name) {
        String json = descriptors.get(name);
        return json == null ? null : JobDescriptorParser.parseJson(json);
    }

    @Override
    public List<String> listNames() {
        return new ArrayList<String>(descriptors.keySet());
    }

    @Override
    public void recordRun(EtlRunResult result) {
        List<EtlRunResult> history = runs.get(result.getJobName());
        if (history == null) {
            history = new ArrayList<EtlRunResult>();
            runs.put(result.getJobName(), history);
        }
        history.add(0, result);
    }

    @Override
    public List<EtlRunResult> runHistory(String jobName, int limit) {
        List<EtlRunResult> history = runs.get(jobName);
        if (history == null) {
            return new ArrayList<EtlRunResult>();
        }
        if (limit > 0 && history.size() > limit) {
            return new ArrayList<EtlRunResult>(history.subList(0, limit));
        }
        return new ArrayList<EtlRunResult>(history);
    }
}
