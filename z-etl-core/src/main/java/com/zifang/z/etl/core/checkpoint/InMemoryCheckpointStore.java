package com.zifang.z.etl.core.checkpoint;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存版水位存储（本地/测试）。
 *
 * @author zifang
 */
public class InMemoryCheckpointStore implements CheckpointStore {

    private final Map<String, Checkpoint> store = new ConcurrentHashMap<String, Checkpoint>();

    @Override
    public Checkpoint load(String jobName) {
        return store.get(jobName);
    }

    @Override
    public void save(Checkpoint checkpoint) {
        if (checkpoint != null && checkpoint.getJobName() != null) {
            store.put(checkpoint.getJobName(), checkpoint);
        }
    }

    @Override
    public void clear(String jobName) {
        store.remove(jobName);
    }
}
