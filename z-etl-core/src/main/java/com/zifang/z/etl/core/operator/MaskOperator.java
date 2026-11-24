package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 脱敏算子：按规则把声明列打码（出报告 / 给下游共享数据前的最后一道闸）。
 *
 * <p>配置：{@code masks}（列名 → 规则）。规则四种：
 * <ul>
 *   <li>{@code phone}：11 位手机号，保留前 3 后 4（{@code 138****5678}）；</li>
 *   <li>{@code email}：保留首字符与 @ 域名（{@code a***@example.com}）；</li>
 *   <li>{@code half}：保留前 1/4，其余 {@code *}；</li>
 *   <li>{@code full}：整值 {@code ****}。</li>
 * </ul>
 * null 保持 null；非预期形状（如 phone 规则碰上非 11 位）走 {@code half} 兜底。
 *
 * <pre>
 * - id: mask
 *   type: mask
 *   config:
 *     masks: {phone: phone, email: email, remark: full}
 * </pre>
 *
 * @author zifang
 */
public class MaskOperator implements Operator {

    private final PipelineNode node;

    /** 列名 → 规则。 */
    private final Map<String, String> masks = new LinkedHashMap<String, String>();

    public MaskOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext context) {
        Map<String, Object> declared = node.map("masks");
        if (declared.isEmpty()) {
            throw new IllegalArgumentException("mask 节点 " + node.getId() + " 缺少 masks（列名 → 规则）");
        }
        for (Map.Entry<String, Object> entry : declared.entrySet()) {
            String rule = MapOperator.text(entry.getValue());
            if (!"phone".equals(rule) && !"email".equals(rule) && !"half".equals(rule) && !"full".equals(rule)) {
                throw new IllegalArgumentException("mask 节点 " + node.getId()
                        + " 的规则只支持 phone|email|half|full，列 " + entry.getKey() + " 实际: " + rule);
            }
            masks.put(entry.getKey(), rule);
        }
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(record.row());
        for (Map.Entry<String, String> entry : masks.entrySet()) {
            Object value = record.row().get(entry.getKey());
            if (value != null) {
                newRow.put(entry.getKey(), mask(String.valueOf(value), entry.getValue()));
            }
        }
        output.collect(record.withRow(newRow));
    }

    /** 按规则打码；phone 规则碰上非 11 位走 half 兜底。 */
    private static String mask(String value, String rule) {
        if ("full".equals(rule)) {
            return "****";
        }
        if ("phone".equals(rule) && value.length() == 11 && isDigits(value)) {
            return value.substring(0, 3) + "****" + value.substring(7);
        }
        if ("email".equals(rule)) {
            int at = value.indexOf('@');
            if (at > 0) {
                return value.charAt(0) + "***" + value.substring(at);
            }
        }
        int keep = Math.max(1, value.length() / 4);
        return value.length() <= keep ? value
                : value.substring(0, keep) + repeat(value.length() - keep);
    }

    private static boolean isDigits(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static String repeat(int times) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append('*');
        }
        return sb.toString();
    }

    @Override
    public void processWatermark(long watermark) {
        // 无状态
    }

    @Override
    public void onTimer(long timestamp, String timerName) {
        // 无定时器
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public String operatorId() {
        return node.getId();
    }
}
