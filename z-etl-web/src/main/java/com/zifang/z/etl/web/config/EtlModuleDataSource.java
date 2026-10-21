package com.zifang.z.etl.web.config;

import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.zifang.z.boot.datasource.starter.ModuleDataSourceTemplate;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;

/**
 * 本域数据源装配（对齐 z-trade 各域的写法）。
 *
 * <p>配置键名模式：{@code z.base.db.etl.*}（{@code ModuleDataSourceTemplate} 的权威前缀常量在这），
 * 缺省回落到 {@code z.base.db.default.*}。
 *
 * <p>{@code @MapperScan} 把 {@code com.zifang.z.etl.core.mapper} 下的 Mapper 绑到**本域**的
 * {@code sqlSessionFactoryEtl} 上 —— 不跟别域共用 sqlSessionFactory，避免跨域 Mapper 互相污染。
 *
 * @author zifang
 */
@Configuration
@MapperScan(
        basePackages = "com.zifang.z.etl.core.mapper",
        sqlSessionFactoryRef = "sqlSessionFactoryEtl"
)
public class EtlModuleDataSource extends ModuleDataSourceTemplate {

    @Bean(name = "dataSourceEtl")
    public DataSource dataSourceEtl(Environment env) {
        return buildDataSource(env, "etl");
    }

    @Bean(name = "sqlSessionFactoryEtl")
    public SqlSessionFactory sqlSessionFactoryEtl(
            @Qualifier("dataSourceEtl") DataSource dataSourceEtl) throws Exception {
        MybatisSqlSessionFactoryBean factoryBean = new MybatisSqlSessionFactoryBean();
        factoryBean.setDataSource(dataSourceEtl);
        factoryBean.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources("classpath*:mapper/**/*.xml"));
        return factoryBean.getObject();
    }
}
