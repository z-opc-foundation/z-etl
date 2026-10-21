package com.zifang.z.etl.core.mapper;

import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 作业元数据读面 Mapper（MyBatis 注解式）。
 *
 * <p>由 web 模块的 {@code @MapperScan(sqlSessionFactoryRef = "sqlSessionFactoryEtl")} 绑到本域
 * sqlSessionFactory 上 ⇒ 读的是 {@code z.base.db.etl.*} 那个库。
 *
 * @author zifang
 */
public interface EtlJobMapper {

    /**
     * 列出作业名。
     *
     * @return 作业名列表
     */
    @Select("SELECT job_name FROM etl_job_def ORDER BY job_name")
    List<String> listJobNames();

    /**
     * 取某个作业的水位记录。
     *
     * @param jobName 作业名
     * @return 水位视图；没有则 null
     */
    @Select("SELECT job_name AS jobName, watermark_column AS watermarkColumn, watermark_type AS watermarkType,"
            + " watermark_value AS watermarkValue, last_mode AS lastMode, last_run_at AS lastRunAt"
            + " FROM etl_job_checkpoint WHERE job_name = #{jobName}")
    EtlCheckpointView findCheckpoint(String jobName);

    /**
     * 作业总数。
     *
     * @return 数量
     */
    @Select("SELECT COUNT(*) FROM etl_job_def")
    int countJobs();
}
