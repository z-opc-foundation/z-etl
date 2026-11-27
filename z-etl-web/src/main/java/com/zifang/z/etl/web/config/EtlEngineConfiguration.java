package com.zifang.z.etl.web.config;

import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.engine.JdbcPipelineRepository;
import com.zifang.z.etl.core.engine.JdbcPipelineScheduleRepository;
import com.zifang.z.etl.core.engine.PipelineScheduleRepository;
import com.zifang.z.etl.core.engine.JobRepository;
import com.zifang.z.etl.core.engine.JdbcJobRepository;
import com.zifang.z.etl.core.engine.PipelineEngine;
import com.zifang.z.etl.core.engine.PipelineRepository;
import com.zifang.z.etl.core.runtime.JdbcPipelineStateStore;
import com.zifang.z.etl.core.runtime.PipelineStateStore;
import com.zifang.z.etl.core.spi.DataSourceProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * 引擎装配。
 *
 * <p>{@link DataSourceProvider} 用 {@link ConditionalOnMissingBean} 挂占位实现：
 * z-dms 仓落地后，由宿主提供真正的实现（解析 z-dms 的 {@code DataSourceDef}）即可顶掉占位，
 * 本模块不需要改。
 *
 * @author zifang
 */
@Configuration
public class EtlEngineConfiguration {

    /**
     * 作业定义 / 水位仓储（落在本域库 {@code z.base.db.etl.*}）。
     */
    @Bean
    public JobRepository etlJobRepository(@Qualifier("dataSourceEtl") DataSource dataSourceEtl) throws Exception {
        EtlSchema.ensure(dataSourceEtl);
        return new JdbcJobRepository(dataSourceEtl);
    }

    /**
     * 水位存储（落库口径 ⇒ 断点续传）。
     */
    @Bean
    public CheckpointStore etlCheckpointStore(@Qualifier("dataSourceEtl") DataSource dataSourceEtl) {
        return new JdbcCheckpointStore(dataSourceEtl);
    }

    /**
     * 流水线定义 / 履历仓储（落 {@code etl_pipeline_def} + {@code etl_pipeline_run}）。
     */
    @Bean
    public PipelineRepository etlPipelineRepository(@Qualifier("dataSourceEtl") DataSource dataSourceEtl) {
        return new JdbcPipelineRepository(dataSourceEtl);
    }

    /**
     * 流水线调度登记（落 {@code etl_pipeline_schedule}）。
     *
     * <p>z-etl 只**登记 cron**；真正的定时器由宿主驱动（z-ops 定时任务 / k8s CronJob 调触发口）。
     */
    @Bean
    public PipelineScheduleRepository etlPipelineScheduleRepository(
            @Qualifier("dataSourceEtl") DataSource dataSourceEtl) {
        return new JdbcPipelineScheduleRepository(dataSourceEtl);
    }

    /**
     * 流水线状态快照存储（落 {@code etl_pipeline_state} ⇒ 跨重启恢复键控状态）。
     */
    @Bean
    public PipelineStateStore etlPipelineStateStore(@Qualifier("dataSourceEtl") DataSource dataSourceEtl) {
        return new JdbcPipelineStateStore(dataSourceEtl);
    }

    /**
     * 消息总线（默认实现：落库表当队列，跨进程可读）。
     *
     * <p>宿主若要接 Kafka / RocketMQ，只需自己声明一个 {@link com.zifang.z.etl.core.spi.MessageBus}
     * Bean 顶掉它（{@code @ConditionalOnMissingBean} 口径）。
     */
    @Bean
    @ConditionalOnMissingBean(com.zifang.z.etl.core.spi.MessageBus.class)
    public com.zifang.z.etl.core.spi.MessageBus etlMessageBus(@Qualifier("dataSourceEtl") DataSource dataSourceEtl)
            throws Exception {
        com.zifang.z.etl.core.connector.JdbcMessageBus.ensure(dataSourceEtl);
        return new com.zifang.z.etl.core.connector.JdbcMessageBus(dataSourceEtl);
    }

    /**
     * 流水线管控引擎（登记 / 触发 / 水位 / 履历 / 计划解释）。
     *
     * <p>AI 执行器（{@code ai.map} 算子用）走宿主装配：宿主声明一个
     * {@link com.zifang.z.etl.core.spi.AiExecutor} Bean（接自家模型网关）即自动接上；
     * 没有就传 null —— {@code ai.map} 节点打开时明确报错说明，其余算子不受影响。
     */
    @Bean
    public PipelineEngine etlPipelineEngine(DataSourceProvider dataSourceProvider,
                                            PipelineRepository pipelineRepository,
                                            CheckpointStore checkpointStore,
                                            PipelineStateStore pipelineStateStore,
                                            com.zifang.z.etl.core.spi.MessageBus messageBus,
                                            PipelineScheduleRepository scheduleRepository,
                                            org.springframework.beans.factory.ObjectProvider<com.zifang.z.etl.core.spi.AiExecutor> aiExecutor) {
        return new PipelineEngine(dataSourceProvider, pipelineRepository, checkpointStore, pipelineStateStore,
                messageBus, scheduleRepository, aiExecutor.getIfAvailable());
    }

    /**
     * 数据源登记仓储（落 {@code etl_datasource} ⇒ 数据源管理页 + Provider 解析共用）。
     */
    @Bean
    public com.zifang.z.etl.core.engine.JdbcDataSourceRegistry etlDataSourceRegistry(
            @Qualifier("dataSourceEtl") DataSource dataSourceEtl) throws Exception {
        EtlSchema.ensure(dataSourceEtl);
        return new com.zifang.z.etl.core.engine.JdbcDataSourceRegistry(dataSourceEtl);
    }

    /**
     * 数据源解析 SPI：默认走数据源登记表（{@code etl_datasource}）。
     *
     * <p>宿主接入 z-dms 后由宿主声明自己的 {@link DataSourceProvider} Bean 顶掉，
     * 本模块不需要改。声明顺序在占位实现之前 ⇒ 占位实现只在宿主与本 Bean 都缺位时生效。
     */
    @Bean
    @ConditionalOnMissingBean(DataSourceProvider.class)
    public DataSourceProvider etlRegistryDataSourceProvider(
            com.zifang.z.etl.core.engine.JdbcDataSourceRegistry etlDataSourceRegistry) {
        return new RegistryDataSourceProvider(etlDataSourceRegistry);
    }

    /**
     * 数据源解析 SPI 兜底：登记表与宿主实现都缺位时明确报错，不静默降级。
     */
    @Bean
    @ConditionalOnMissingBean(DataSourceProvider.class)
    public DataSourceProvider etlPlaceholderDataSourceProvider() {
        return new DataSourceProvider() {
            @Override
            public javax.sql.DataSource resolve(String dataSourceRef) {
                throw new IllegalStateException("数据源 " + dataSourceRef
                        + " 无法解析：z-dms（数据源管控仓）尚未接入本宿主。"
                        + "请由宿主提供 DataSourceProvider 实现（解析 z-dms 的 DataSourceDef）。");
            }
        };
    }

    /**
     * ETL 执行器。
     */
    @Bean
    public EtlEngine etlEngine(DataSourceProvider dataSourceProvider, JobRepository jobRepository,
                                CheckpointStore checkpointStore,
                                com.zifang.z.etl.core.spi.MessageBus messageBus) {
        return new EtlEngine(dataSourceProvider, jobRepository, checkpointStore, messageBus);
    }
}
