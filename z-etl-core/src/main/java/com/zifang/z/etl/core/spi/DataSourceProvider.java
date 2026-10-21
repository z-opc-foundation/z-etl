package com.zifang.z.etl.core.spi;

import javax.sql.DataSource;

/**
 * 数据源解析 SPI。
 *
 * <p>z-etl 的源端/目标端数据源由 **z-dms** 统一管控（需求 008 D1/D2：z-dms 是数据源管控底座，
 * z-etl 是它的第一个重度消费者）。z-dms 仓本轮尚未落地（TASK-20261012-011 仍在 pending/），
 * 故本仓先用这个 SPI 承接：z-dms 落地后由它提供实现即可，**job 描述格式不变**。
 *
 * <p>实现方拿到的 {@code dataSourceRef} 是 z-dms 侧的 {@code DataSourceDef} 引用；
 * 凭据一律由 z-dms 侧解析（z-mist {@code credentialRef}），**本仓不碰明文凭据**。
 *
 * @author zifang
 */
public interface DataSourceProvider {

    /**
     * 按引用解析出数据源。
     *
     * @param dataSourceRef 数据源引用
     * @return JDBC 数据源
     * @throws IllegalArgumentException 引用不存在
     */
    DataSource resolve(String dataSourceRef);
}
