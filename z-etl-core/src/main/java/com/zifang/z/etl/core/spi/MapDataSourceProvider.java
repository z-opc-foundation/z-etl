package com.zifang.z.etl.core.spi;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 内存版数据源解析器（本地夹具 / 单机起步形态）。
 *
 * <p>纪律口径：M1 单机线程池起步、不做分布式 master-worker（卡面纪律），
 * 生产形态下由 z-dms 提供实现；本实现供本地/测试环境与「双数据源都在本进程」的场景使用。
 *
 * @author zifang
 */
public class MapDataSourceProvider implements DataSourceProvider {

    private final Map<String, DataSource> dataSources = new LinkedHashMap<String, DataSource>();

    public MapDataSourceProvider register(String ref, DataSource dataSource) {
        dataSources.put(ref, dataSource);
        return this;
    }

    @Override
    public DataSource resolve(String dataSourceRef) {
        DataSource dataSource = dataSources.get(dataSourceRef);
        if (dataSource == null) {
            throw new IllegalArgumentException("数据源引用不存在: " + dataSourceRef
                    + "（已登记: " + String.join(", ", dataSources.keySet()) + "）");
        }
        return dataSource;
    }
}
