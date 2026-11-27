package com.zifang.z.etl.web.config;

import com.zifang.z.etl.core.connector.DriverUrlDataSource;
import com.zifang.z.etl.core.engine.DataSourceDef;
import com.zifang.z.etl.core.engine.JdbcDataSourceRegistry;
import com.zifang.z.etl.core.spi.DataSourceProvider;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 登记表版数据源解析（数据源管理页的引擎侧闭环）。
 *
 * <p>流水线声明里的 {@code dataSourceRef} 按名到 {@code etl_datasource} 解析；
 * 同一条目缓存复用（按 updated_at 失效，登记更新后下次解析取新配置）。
 * 宿主接入 z-dms 后由宿主声明自己的 {@link DataSourceProvider} Bean 顶掉
 * （{@code @ConditionalOnMissingBean} 口径），本类不需要改。
 *
 * @author zifang
 */
public class RegistryDataSourceProvider implements DataSourceProvider {

    private final JdbcDataSourceRegistry registry;
    private final Map<String, CachedEntry> cache = new ConcurrentHashMap<String, CachedEntry>();

    public RegistryDataSourceProvider(JdbcDataSourceRegistry registry) {
        this.registry = registry;
    }

    @Override
    public DataSource resolve(String dataSourceRef) {
        if (dataSourceRef == null || dataSourceRef.isEmpty()) {
            throw new IllegalArgumentException("dataSourceRef 为空（流水线源/汇节点要写数据源引用名）");
        }
        DataSourceDef def = registry.find(dataSourceRef);
        if (def == null) {
            throw new IllegalArgumentException("数据源 " + dataSourceRef
                    + " 未登记：请先在「数据源管理」页登记（或由宿主 z-dms 提供解析实现）");
        }
        String cacheKey = dataSourceRef + "@" + def.getUpdatedAt();
        CachedEntry cached = cache.get(dataSourceRef);
        if (cached != null && cached.key.equals(cacheKey)) {
            return cached.dataSource;
        }
        DriverUrlDataSource created = new DriverUrlDataSource(def.getUrl(), def.getUsername(), def.getPassword());
        cache.put(dataSourceRef, new CachedEntry(cacheKey, created));
        return created;
    }

    private static final class CachedEntry {
        private final String key;
        private final DataSource dataSource;

        private CachedEntry(String key, DataSource dataSource) {
            this.key = key;
            this.dataSource = dataSource;
        }
    }
}
