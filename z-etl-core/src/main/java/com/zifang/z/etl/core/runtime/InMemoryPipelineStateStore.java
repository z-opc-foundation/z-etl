package com.zifang.z.etl.core.runtime;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 内存状态存储（测试 / 预览用）。
 *
 * <p>快照以 base64 文本挂在内存 map 里 ⇒ 与落库实现走同一条序列化路径，
 * 测出来的行为和落库一致。
 *
 * @author zifang
 */
public class InMemoryPipelineStateStore implements PipelineStateStore {

    private final Map<String, byte[]> states = new LinkedHashMap<String, byte[]>();

    @Override
    public byte[] load(String pipelineId) {
        byte[] data = states.get(pipelineId);
        if (data == null) {
            String text = TEXT_STATES.get(pipelineId);
            return text == null ? null : Base64.getDecoder().decode(text);
        }
        return data;
    }

    @Override
    public void save(String pipelineId, byte[] state) {
        states.put(pipelineId, state);
        TEXT_STATES.put(pipelineId, Base64.getEncoder().encodeToString(state));
    }

    @Override
    public void clear(String pipelineId) {
        states.remove(pipelineId);
        TEXT_STATES.remove(pipelineId);
    }

    /** 文本镜像（跨实现核对用）。 */
    private static final Map<String, String> TEXT_STATES = new LinkedHashMap<String, String>();

    /** 是否存过某条流水线的快照。 */
    public boolean contains(String pipelineId) {
        return states.containsKey(pipelineId);
    }
}
