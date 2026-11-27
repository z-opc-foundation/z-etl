package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.pipeline.PipelineParser;
import com.zifang.z.etl.core.pipeline.PipelineSpec;
import com.zifang.z.etl.core.runtime.PipelineRunResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内存版流水线仓储（测试 / 预览用）。
 *
 * @author zifang
 */
public class InMemoryPipelineRepository implements PipelineRepository {

    private final Map<String, PipelineSpec> specs = new LinkedHashMap<String, PipelineSpec>();
    private final Map<String, List<PipelineRunResult>> runs = new LinkedHashMap<String, List<PipelineRunResult>>();

    @Override
    public void save(PipelineSpec spec) {
        specs.put(spec.getName(), spec);
    }

    @Override
    public PipelineSpec find(String name) {
        return name == null ? null : specs.get(name);
    }

    @Override
    public List<String> listNames() {
        List<String> names = new ArrayList<String>(specs.keySet());
        Collections.sort(names);
        return names;
    }

    @Override
    public void saveRun(PipelineRunResult result) {
        List<PipelineRunResult> history = runs.get(result.getPipelineName());
        if (history == null) {
            history = new ArrayList<PipelineRunResult>();
            runs.put(result.getPipelineName(), history);
        }
        history.add(0, result);
    }

    @Override
    public List<PipelineRunResult> runHistory(String name, int limit) {
        List<PipelineRunResult> history = runs.get(name);
        if (history == null) {
            return Collections.emptyList();
        }
        if (limit <= 0 || history.size() <= limit) {
            return new ArrayList<PipelineRunResult>(history);
        }
        return new ArrayList<PipelineRunResult>(history.subList(0, limit));
    }

    @Override
    public List<PipelineRunResult> recentRuns(int limit) {
        List<PipelineRunResult> all = new ArrayList<PipelineRunResult>();
        for (List<PipelineRunResult> history : runs.values()) {
            all.addAll(history);
        }
        all.sort((a, b) -> Long.compare(b.getStartedAt(), a.getStartedAt()));
        if (limit <= 0 || all.size() <= limit) {
            return all;
        }
        return new ArrayList<PipelineRunResult>(all.subList(0, limit));
    }

    /** 定义原文（取回时重新序列化，保证内容同源）。 */
    public String toYaml(String name) {
        PipelineSpec spec = find(name);
        return spec == null ? null : PipelineParser.toYaml(spec);
    }
}
