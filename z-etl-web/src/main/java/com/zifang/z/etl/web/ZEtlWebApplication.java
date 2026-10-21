package com.zifang.z.etl.web;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

/**
 * z-etl 管控面启动器。
 *
 * <p>排除 {@link DataSourceAutoConfiguration}：本域的数据源由
 * {@code EtlModuleDataSource}（{@code ModuleDataSourceTemplate}，键名 {@code z.base.db.etl.*}）自建，
 * 不走 Spring Boot 默认数据源自动装配。
 *
 * @author zifang
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@ComponentScan(basePackages = "com.zifang.z.etl")
public class ZEtlWebApplication {

    public static void main(String[] args) {
        SpringApplication.run(ZEtlWebApplication.class, args);
    }
}
