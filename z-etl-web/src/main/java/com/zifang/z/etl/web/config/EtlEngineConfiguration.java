package com.zifang.z.etl.web.config;

import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.engine.JobRepository;
import com.zifang.z.etl.core.engine.JdbcJobRepository;
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
     * 数据源解析 SPI：z-dms 未接入时的占位实现（明确报错，不静默降级）。
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
                                CheckpointStore checkpointStore) {
        return new EtlEngine(dataSourceProvider, jobRepository, checkpointStore);
    }
}
