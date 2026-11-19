package com.zifang.z.etl.core.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 堆内状态后端（默认实现）。
 *
 * <p>状态全在堆里 ⇒ 快，但**换进程就没了**；要断点续传就靠 {@link #snapshot()} 序列化后落库
 * （{@link PipelineStateStore}），重启时 {@link #restore(byte[])} 灌回来。
 *
 * <p>快照用 JDK 序列化 ⇒ 状态值必须可序列化。引擎自带的状态（去重指纹、聚合累加器）都实现了
 * {@link Serializable}；行本身不进状态（去重存的是行指纹，聚合存的是累加器）。
 *
 * @author zifang
 */
public class HeapStateBackend implements StateBackend {

    /** 算子 id → 该算子的状态。 */
    private final Map<String, OperatorState> states = new LinkedHashMap<String, OperatorState>();

    @Override
    public KeyedStateStore createStore(String operatorId) {
        OperatorState operatorState = states.get(operatorId);
        if (operatorState == null) {
            operatorState = new OperatorState();
            states.put(operatorId, operatorState);
        }
        return new HeapKeyedStateStore(operatorState);
    }

    @Override
    public byte[] snapshot() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bytes);
        try {
            out.writeObject(new LinkedHashMap<String, OperatorState>(states));
            out.flush();
        } finally {
            out.close();
        }
        return bytes.toByteArray();
    }

    @Override
    public void restore(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return;
        }
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
        try {
            @SuppressWarnings("unchecked")
            Map<String, OperatorState> restored = (Map<String, OperatorState>) in.readObject();
            states.clear();
            if (restored != null) {
                states.putAll(restored);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("状态快照反序列化失败: " + e.getMessage(), e);
        } finally {
            in.close();
        }
    }

    /** 单个算子的全部状态（可序列化）。 */
    static final class OperatorState implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 状态名 → (key → 值)。 */
        final Map<String, Map<String, Object>> values = new LinkedHashMap<String, Map<String, Object>>();

        /** 状态名 → (key → 列表)。 */
        final Map<String, Map<String, List<Object>>> lists = new LinkedHashMap<String, Map<String, List<Object>>>();

        /** 状态名 → (key → 映射)。 */
        final Map<String, Map<String, Map<String, Object>>> maps =
                new LinkedHashMap<String, Map<String, Map<String, Object>>>();

        Map<String, Object> valueBucket(String name) {
            Map<String, Object> bucket = values.get(name);
            if (bucket == null) {
                bucket = new LinkedHashMap<String, Object>();
                values.put(name, bucket);
            }
            return bucket;
        }

        Map<String, List<Object>> listBucket(String name) {
            Map<String, List<Object>> bucket = lists.get(name);
            if (bucket == null) {
                bucket = new LinkedHashMap<String, List<Object>>();
                lists.put(name, bucket);
            }
            return bucket;
        }

        Map<String, Map<String, Object>> mapBucket(String name) {
            Map<String, Map<String, Object>> bucket = maps.get(name);
            if (bucket == null) {
                bucket = new LinkedHashMap<String, Map<String, Object>>();
                maps.put(name, bucket);
            }
            return bucket;
        }
    }

    /** 堆内键控状态存储。 */
    private static final class HeapKeyedStateStore implements KeyedStateStore {

        private final OperatorState operatorState;
        private String currentKey;

        HeapKeyedStateStore(OperatorState operatorState) {
            this.operatorState = operatorState;
        }

        @Override
        public void setCurrentKey(String key) {
            this.currentKey = key;
        }

        @Override
        public String currentKey() {
            return currentKey;
        }

        private String requireKey() {
            if (currentKey == null) {
                throw new IllegalStateException("键控状态读写前必须先定住当前 key（setCurrentKey）");
            }
            return currentKey;
        }

        @Override
        public ValueState valueState(String name) {
            final String stateName = name;
            return new ValueState() {
                @Override
                public Object value() {
                    return operatorState.valueBucket(stateName).get(requireKey());
                }

                @Override
                public void update(Object value) {
                    if (value == null) {
                        operatorState.valueBucket(stateName).remove(requireKey());
                    } else {
                        operatorState.valueBucket(stateName).put(requireKey(), value);
                    }
                }

                @Override
                public void clear() {
                    operatorState.valueBucket(stateName).remove(requireKey());
                }
            };
        }

        @Override
        public ListState listState(String name) {
            final String stateName = name;
            return new ListState() {
                @Override
                public void add(Object value) {
                    List<Object> bucket = operatorState.listBucket(stateName).get(requireKey());
                    if (bucket == null) {
                        bucket = new ArrayList<Object>();
                        operatorState.listBucket(stateName).put(requireKey(), bucket);
                    }
                    bucket.add(value);
                }

                @Override
                public List<Object> get() {
                    List<Object> bucket = operatorState.listBucket(stateName).get(requireKey());
                    return bucket == null ? new ArrayList<Object>() : bucket;
                }

                @Override
                public void update(List<Object> values) {
                    operatorState.listBucket(stateName).put(requireKey(),
                            values == null ? new ArrayList<Object>() : new ArrayList<Object>(values));
                }

                @Override
                public void clear() {
                    operatorState.listBucket(stateName).remove(requireKey());
                }
            };
        }

        @Override
        public MapState mapState(String name) {
            final String stateName = name;
            return new MapState() {
                @Override
                public void put(String mapKey, Object value) {
                    Map<String, Map<String, Object>> bucket = operatorState.mapBucket(stateName);
                    Map<String, Object> inner = bucket.get(requireKey());
                    if (inner == null) {
                        inner = new LinkedHashMap<String, Object>();
                        bucket.put(requireKey(), inner);
                    }
                    inner.put(mapKey, value);
                }

                @Override
                public Object get(String mapKey) {
                    Map<String, Map<String, Object>> bucket = operatorState.mapBucket(stateName);
                    Map<String, Object> inner = bucket.get(requireKey());
                    return inner == null ? null : inner.get(mapKey);
                }

                @Override
                public Map<String, Object> getAll() {
                    Map<String, Map<String, Object>> bucket = operatorState.mapBucket(stateName);
                    Map<String, Object> inner = bucket.get(requireKey());
                    return inner == null ? new LinkedHashMap<String, Object>() : inner;
                }

                @Override
                public void remove(String mapKey) {
                    Map<String, Map<String, Object>> bucket = operatorState.mapBucket(stateName);
                    Map<String, Object> inner = bucket.get(requireKey());
                    if (inner != null) {
                        inner.remove(mapKey);
                    }
                }

                @Override
                public void clear() {
                    operatorState.mapBucket(stateName).remove(requireKey());
                }
            };
        }

        @Override
        public void clearAll() {
            operatorState.values.clear();
            operatorState.lists.clear();
            operatorState.maps.clear();
        }
    }
}
