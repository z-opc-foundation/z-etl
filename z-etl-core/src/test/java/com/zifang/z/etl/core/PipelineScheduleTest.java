package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.InMemoryCheckpointStore;
import com.zifang.z.etl.core.engine.InMemoryPipelineRepository;
import com.zifang.z.etl.core.engine.InMemoryPipelineScheduleRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.engine.PipelineSchedule;
import com.zifang.z.etl.core.runtime.InMemoryPipelineStateStore;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 调度登记验收：登记 cron ⇒ 看得见下一次触发时间 ⇒ 触发后记账 ⇒ 可取消。
 *
 * <p>口径：**z-etl 只登记，不起常驻定时线程** —— 定时器由宿主驱动（调触发口）。
 *
 * @author zifang
 */
class PipelineScheduleTest {

    private PipelineEngine engine;
    private InMemoryPipelineScheduleRepository schedules;

    private static final String SPEC = ""
            + "name: scheduled-pipeline\n"
            + "nodes:\n"
            + "  - id: src\n"
            + "    type: inline.source\n"
            + "    config:\n"
            + "      rows:\n"
            + "        - {id: 1}\n"
            + "  - id: sink\n"
            + "    type: collect.sink\n"
            + "edges:\n"
            + "  - {from: src, to: sink}\n";

    @BeforeEach
    void setUp() {
        schedules = new InMemoryPipelineScheduleRepository();
        engine = new PipelineEngine(new MapDataSourceProvider(), new InMemoryPipelineRepository(),
                new InMemoryCheckpointStore(), new InMemoryPipelineStateStore(), null, schedules);
        engine.register(SPEC);
    }

    @Test
    @DisplayName("登记调度 ⇒ 看得见 cron 与下一次触发时间")
    void scheduleIsVisible() {
        PipelineSchedule schedule = engine.schedule("scheduled-pipeline", "0 * * * *", true);
        assertEquals("scheduled-pipeline", schedule.getPipelineName());
        assertEquals("0 * * * *", schedule.getCronExpression());
        assertTrue(schedule.isEnabled());
        assertNotNull(schedule.getNextFireTime(), "应能算出下一次触发时间");
        assertTrue(schedule.getNextFireTime().longValue() > System.currentTimeMillis());

        assertEquals(1, engine.schedules().size());
        assertNotNull(engine.schedule("scheduled-pipeline"));
    }

    @Test
    @DisplayName("触发一次 ⇒ 记下「上次触发时间」")
    void triggerRecordsLastTriggeredAt() throws Exception {
        engine.schedule("scheduled-pipeline", "*/5 * * * *", true);
        assertNull(engine.schedule("scheduled-pipeline").getLastTriggeredAt());
        engine.run("scheduled-pipeline");
        assertNotNull(engine.schedule("scheduled-pipeline").getLastTriggeredAt(),
                "触发后应记录上次触发时间");
    }

    @Test
    @DisplayName("取消调度 ⇒ 登记消失")
    void unscheduleRemoves() {
        engine.schedule("scheduled-pipeline", "0 * * * *", true);
        assertTrue(engine.unschedule("scheduled-pipeline"));
        assertEquals(0, engine.schedules().size());
        assertNull(engine.schedule("scheduled-pipeline"));
    }

    @Test
    @DisplayName("未登记的流水线 / 非法 cron ⇒ 直接报错")
    void rejectsBadScheduleInput() {
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                engine.schedule("no-such-pipeline", "0 * * * *", true);
            }
        });
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                engine.schedule("scheduled-pipeline", "not a cron", true);
            }
        });
    }
}
