package com.zifang.z.etl.core;

import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.job.ExpressionSpec;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.job.ScriptSpec;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 2：沙箱步骤超时 / 越限被拦截，且 job 失败信息可读（**不静默吞**）。
 *
 * <p>三条闸：
 * <ol>
 *   <li><b>超时</b>：z-script 沙箱自带的墙钟超时（Groovy {@code @ThreadInterrupt} + Future 中断）。</li>
 *   <li><b>产出越限</b>：z-script 侧**没有**内存配额（TASK-20261012-010 实测结论），
 *       本仓在引擎侧兜一道「单步产出大小上限」——这是**兜底**，不是 JVM 堆配额（零件缺口）。</li>
 *   <li><b>不安全引擎硬拒绝</b>：LUA 执行器无沙箱无超时（实测 {@code os.execute} 可逃逸）⇒ 作业描述直接校验不过。</li>
 * </ol>
 *
 * @author zifang
 */
class SandboxGuardTest {

    private EtlTestFixtures fixtures;

    @BeforeEach
    void setUp() throws Exception {
        fixtures = new EtlTestFixtures("sandbox");
        fixtures.createTransformSchema();
        fixtures.seedSource(50, 1L, new Timestamp(1767225600000L));
        EtlSchema.ensure(fixtures.meta());
    }

    private EtlEngine engine() {
        MapDataSourceProvider provider = new MapDataSourceProvider()
                .register(EtlTestFixtures.SOURCE_REF, fixtures.source())
                .register(EtlTestFixtures.TARGET_REF, fixtures.target());
        return new EtlEngine(provider, new com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore(fixtures.meta()));
    }

    /** 造一份「只留一个脚本步骤」的作业描述（脚本内容可替换）。 */
    private JobDescriptor descriptorWithScript(String jobName, ScriptSpec script) {
        JobDescriptor descriptor = EtlTestFixtures.loadTransformDescriptor();
        descriptor.setName(jobName);
        List<ScriptSpec> scripts = new ArrayList<ScriptSpec>();
        scripts.add(script);
        descriptor.getTransform().setScripts(scripts);
        descriptor.getTransform().setExpressions(new ArrayList<ExpressionSpec>());
        return descriptor;
    }

    @Test
    @DisplayName("脚本死循环被超时拦截，job 失败且信息可读（含步骤名/原因/时限）")
    void runawayScriptIsStoppedByTimeout() throws Exception {
        ScriptSpec runaway = new ScriptSpec();
        runaway.setName("runaway");
        runaway.setEngine("GROOVY");
        runaway.setTimeoutMs(Long.valueOf(300L));
        runaway.setMaxOutputChars(Integer.valueOf(256));
        runaway.setScript("def i = 0\nwhile (i < 100000000) { i = i + 1 }\ni\n");

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> engine().run(descriptorWithScript("transform-timeout", runaway)));

        String message = error.getMessage();
        System.out.println("[超时拦截] 失败信息原文: " + message);
        assertTrue(message.contains("script:runaway"), "失败信息应含步骤名，实际: " + message);
        assertTrue(message.contains("超时"), "失败信息应含超时原因，实际: " + message);
        assertTrue(message.contains("300ms"), "失败信息应含超时时限，实际: " + message);
        assertTrue(error.getCause() instanceof com.zifang.z.etl.core.transform.TransformStepException,
                "应保留转换步骤异常作为 cause");

        // 不静默吞：失败后目标表不能留下半截数据
        assertEquals(0L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed"),
                "失败作业不应写入任何行（不静默吞、不留半截数据）");
    }

    @Test
    @DisplayName("脚本产出越限被拦截（引擎侧兜的内存闸），job 失败且信息可读")
    void oversizedScriptOutputIsRejected() throws Exception {
        ScriptSpec bomb = new ScriptSpec();
        bomb.setName("memory_bomb");
        bomb.setEngine("GROOVY");
        bomb.setTimeoutMs(Long.valueOf(5000L));
        bomb.setMaxOutputChars(Integer.valueOf(65536));
        bomb.setScript("def sb = new StringBuilder()\n"
                + "for (int i = 0; i < 1000000; i++) { sb.append('x') }\n"
                + "sb.toString()\n");

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> engine().run(descriptorWithScript("transform-oversize", bomb)));

        String message = error.getMessage();
        System.out.println("[产出越限拦截] 失败信息原文: " + message);
        assertTrue(message.contains("script:memory_bomb"), "失败信息应含步骤名，实际: " + message);
        assertTrue(message.contains("超过单步上限"), "失败信息应含越限原因，实际: " + message);
        assertTrue(message.contains("65536"), "失败信息应含上限值，实际: " + message);
        assertEquals(0L, EtlTestFixtures.count(fixtures.target(), "etl_trade_order_transformed"),
                "失败作业不应写入任何行");
    }

    @Test
    @DisplayName("LUA 执行器被硬拒绝（无沙箱无超时、实测可逃逸），作业描述校验不过")
    void luaEngineIsRejected() {
        ScriptSpec lua = new ScriptSpec();
        lua.setName("lua_step");
        lua.setEngine("LUA");
        lua.setScript("os.execute('touch /tmp/pwned')\n");

        JobDescriptor descriptor = descriptorWithScript("transform-lua", lua);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> engine().run(descriptor));

        System.out.println("[LUA 硬拒绝] 失败信息原文: " + error.getMessage());
        assertTrue(error.getMessage().contains("LUA"), "错误信息应点明 LUA 被拒，实际: " + error.getMessage());
        assertTrue(error.getMessage().contains("硬拒绝"), "错误信息应说明是硬拒绝，实际: " + error.getMessage());
    }

    @Test
    @DisplayName("沙箱拦截危险调用（Runtime/ProcessBuilder 等）并给出可读失败信息")
    void dangerousGroovyCallIsBlockedBySandbox() {
        ScriptSpec escape = new ScriptSpec();
        escape.setName("escape");
        escape.setEngine("GROOVY");
        escape.setTimeoutMs(Long.valueOf(2000L));
        escape.setScript("Runtime.getRuntime().exec('id')\n");

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> engine().run(descriptorWithScript("transform-escape", escape)));

        System.out.println("[沙箱拦截] 失败信息原文: " + error.getMessage());
        assertTrue(error.getMessage().contains("script:escape"), "失败信息应含步骤名，实际: " + error.getMessage());
        assertTrue(error.getMessage().contains("沙箱"), "失败信息应说明被沙箱拦截，实际: " + error.getMessage());
    }
}
