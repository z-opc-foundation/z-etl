package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.runtime.PipelineRunResult;

import java.util.List;

/**
 * 流水线仓储（定义落库 + 运行履历），与 {@link JobRepository} 同形状。
 *
 * @author zifang
 */
public interface PipelineRepository {

    /** 落库定义（JSON/yml 双文本，内容同源）。 */
    void save(com.zifang.z.etl.core.pipeline.PipelineSpec spec);

    /** 按名取定义；不存在返回 {@code null}。 */
    com.zifang.z.etl.core.pipeline.PipelineSpec find(String name);

    /** 全部流水线名（字典序）。 */
    List<String> listNames();

    /** 记一条运行履历。 */
    void saveRun(PipelineRunResult result);

    /** 运行履历（新→旧）；{@code limit ≤ 0} 不限。 */
    List<PipelineRunResult> runHistory(String name, int limit);

    /** 跨流水线最近运行（新→旧，运行总览用）；{@code limit ≤ 0} 不限。 */
    List<PipelineRunResult> recentRuns(int limit);

    /** 删除流水线定义（连同运行履历）；不存在返回 {@code false}。 */
    boolean delete(String name);
}
