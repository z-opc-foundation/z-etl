package com.zifang.z.etl.core.runtime;

/**
 * 拉取式源（pull source）的统一面。
 *
 * <p>为什么要有这一层：源**不只有 JDBC**（文件 / HTTP / 消息队列 / 内联常量 / 脚本），
 * 但执行器对源的口径只有三件事 —— 拉一条、报水位、报游标 ⇒ 抽成接口后，
 * 加一种新输入类型 = 加一个实现类，**执行器零改动**。
 *
 * <p>实现方负责：吐原始行 + 事件时间 + 水位推进 + 增量过滤（{@code passesResume}）。
 *
 * @author zifang
 */
public interface PullSource extends Operator {

    /**
     * 拉下一条记录。
     *
     * @return 记录；流已到头返回 {@code null}
     * @throws Exception 读取失败
     */
    Record next() throws Exception;

    /** 当前水位（毫秒）；还没有事件时间时为 {@link Long#MIN_VALUE}。 */
    long currentWatermark();

    /** 最后一个带事件时间的原始值（落水位用）。 */
    Object lastEventTimeValue();

    /** 最后一个平局列原始值（落水位用）。 */
    Object lastTiebreakValue();

    /** 事件时间列；没有事件时间的源返回 {@code null}。 */
    String eventTimeColumn();

    /** 事件时间类型（LONG / TIMESTAMP）。 */
    String eventTimeType();

    /** 平局列（断点续传的第二键；未声明为 {@code null}）。 */
    String tiebreakColumn();

    /**
     * 设定断点续传的游标值（增量接力用；首跑传 {@code null}）。
     *
     * @param value    水位值
     * @param tiebreak 平局列值
     */
    void setResume(Object value, Object tiebreak);

    /** 落水位用的列名；{@code null} = 本源不落水位。 */
    String checkpointColumn();

    /** 落水位用的类型（LONG / TIMESTAMP）。 */
    String checkpointType();

    /** 落水位用的值；{@code null} = 本轮不落。 */
    Object checkpointValue();
}
