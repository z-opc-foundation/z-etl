package com.zifang.z.etl.core.runtime;

/**
 * 旁路输出标签（Flink 的 {@code OutputTag}）。
 *
 * <p>口径：**标签 id = 目标节点 id**。算子把「被过滤掉的行」「迟到的行」「被拒的行」
 * 用标签扔给旁路，执行器按标签 id 找到对应节点投递过去 ⇒ 不用再给旁路单独配一套连接。
 *
 * @author zifang
 */
public final class OutputTag {

    private final String id;

    public OutputTag(String id) {
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("旁路输出标签不能为空");
        }
        this.id = id.trim();
    }

    /** 标签 id（= 目标节点 id）。 */
    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof OutputTag)) {
            return false;
        }
        return id.equals(((OutputTag) other).id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "OutputTag{" + id + "}";
    }
}
