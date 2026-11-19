package com.zifang.z.etl.core.runtime;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 状态后端 SPI（Flink 的 {@code StateBackend}）。
 *
 * <p>为什么要有这层：键控状态（聚合 / 去重 / 窗口）的存法决定了两件事 ——
 * 能不能**断点续传**（状态要能序列化落库）和能不能**存得下**（堆内 vs 落盘）。
 * 把存法抽象成 SPI，堆内（默认，快）与落盘（慢，能存大状态）都能换。
 *
 * @author zifang
 */
public interface StateBackend {

    /**
     * 为某个算子开一份状态（一个算子一份，互不串味）。
     *
     * @param operatorId 算子 id
     * @return 键控状态存储
     */
    KeyedStateStore createStore(String operatorId);

    /**
     * 全量快照（用于检查点 / 断点续传）。
     *
     * @return 快照字节
     * @throws IOException 序列化失败
     */
    byte[] snapshot() throws IOException;

    /**
     * 恢复快照。
     *
     * @param data 快照字节
     * @throws IOException 反序列化失败
     */
    void restore(byte[] data) throws IOException;

    /**
     * 键控状态存储（Flink 的 {@code KeyedStateBackend}）。
     *
     * <p>口径与 Flink 一致：**状态按 key 存**。算子先 {@link #setCurrentKey(String)} 定住当前 key，
     * 再读写状态；没定 key 就读写 ⇒ 抛错（避免「写到哪个 key 去」的糊涂账）。
     */
    interface KeyedStateStore {

        /** 定住当前 key。 */
        void setCurrentKey(String key);

        /** 当前 key。 */
        String currentKey();

        /** 取一个值状态（同名状态重复取到的是同一份）。 */
        ValueState valueState(String name);

        /** 取一个列表状态。 */
        ListState listState(String name);

        /** 取一个映射状态。 */
        MapState mapState(String name);

        /** 清空本算子的全部状态。 */
        void clearAll();
    }

    /** 值状态（每个 key 一个值）。 */
    interface ValueState {

        /** 读当前 key 的值；没有返回 {@code null}。 */
        Object value();

        /** 写当前 key 的值（{@code null} 表示删除）。 */
        void update(Object value);

        /** 删当前 key 的值。 */
        void clear();
    }

    /** 列表状态（每个 key 一个列表）。 */
    interface ListState {

        /** 追加。 */
        void add(Object value);

        /** 读当前 key 的列表（没有返回空列表）。 */
        List<Object> get();

        /** 覆盖当前 key 的列表。 */
        void update(List<Object> values);

        /** 清当前 key。 */
        void clear();
    }

    /** 映射状态（每个 key 一个 map）。 */
    interface MapState {

        /** 写。 */
        void put(String mapKey, Object value);

        /** 读。 */
        Object get(String mapKey);

        /** 读全量（没有返回空 map）。 */
        Map<String, Object> getAll();

        /** 删。 */
        void remove(String mapKey);

        /** 清当前 key。 */
        void clear();
    }
}
