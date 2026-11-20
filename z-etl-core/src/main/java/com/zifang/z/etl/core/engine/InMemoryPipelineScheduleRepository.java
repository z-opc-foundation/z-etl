package com.zifang.z.etl.core.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内存调度登记仓储（测试 / 预览用）。
 *
 * @author zifang
 */
public class InMemoryPipelineScheduleRepository implements PipelineScheduleRepository {

    private final Map<String, PipelineSchedule> schedules = new LinkedHashMap<String, PipelineSchedule>();

    @Override
    public void save(PipelineSchedule schedule) {
        schedules.put(schedule.getPipelineName(), schedule);
    }

    @Override
    public PipelineSchedule find(String pipelineName) {
        return schedules.get(pipelineName);
    }

    @Override
    public List<PipelineSchedule> list() {
        return new ArrayList<PipelineSchedule>(schedules.values());
    }

    @Override
    public void touch(String pipelineName) {
        PipelineSchedule schedule = schedules.get(pipelineName);
        if (schedule != null) {
            schedule.setLastTriggeredAt(Long.valueOf(System.currentTimeMillis()));
            schedule.setUpdatedAt(Long.valueOf(System.currentTimeMillis()));
        }
    }

    @Override
    public boolean remove(String pipelineName) {
        return schedules.remove(pipelineName) != null;
    }
}
