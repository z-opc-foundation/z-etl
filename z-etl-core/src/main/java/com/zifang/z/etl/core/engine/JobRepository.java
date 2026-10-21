package com.zifang.z.etl.core.engine;

import com.zifang.z.etl.core.job.JobDescriptor;

import java.util.List;

/**
 * 作业定义仓储（**落库**口径）。
 *
 * <p>「job 描述 JSON/yml 可落库、可重跑」的落点：描述落库后按作业名取回即可重跑，
 * 不依赖原始描述文件还在不在。
 *
 * @author zifang
 */
public interface JobRepository {

    /**
     * 落库（按作业名 upsert）。
     *
     * @param descriptor 作业描述
     */
    void save(JobDescriptor descriptor);

    /**
     * 按作业名取回描述。
     *
     * @param name 作业名
     * @return 描述；没有则返回 null
     */
    JobDescriptor find(String name);

    /**
     * 列出全部作业名。
     *
     * @return 作业名列表
     */
    List<String> listNames();

    /**
     * 记录一次运行（履历）。
     *
     * @param result 运行结果
     */
    void recordRun(EtlRunResult result);

    /**
     * 取运行履历（最近的在前）。
     *
     * @param jobName 作业名
     * @param limit   最多返回条数（≤0 表示不限）
     * @return 运行履历
     */
    List<EtlRunResult> runHistory(String jobName, int limit);
}
