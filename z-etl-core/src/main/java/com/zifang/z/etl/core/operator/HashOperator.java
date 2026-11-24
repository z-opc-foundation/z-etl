package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 哈希算子：对声明列做 MD5 / SHA-256 摘要（指纹化 / 匿名化）。
 *
 * <p>配置：{@code columns}（列名列表）、{@code algorithm}（默认 SHA-256，可 MD5）、
 * {@code salt}（可选，拼在值前防彩虹表）、{@code suffix}（输出列后缀，默认空 = 原地覆盖）。
 * null 值保持 null。
 *
 * <pre>
 * - id: hash
 *   type: hash
 *   config:
 *     columns: [phone, id_card]
 *     algorithm: SHA-256
 *     salt: "pepper-2026"
 *     suffix: "_fp"
 * </pre>
 *
 * @author zifang
 */
public class HashOperator implements Operator {

    private final PipelineNode node;
    private final List<String> columns;
    private final String algorithm;
    private final String salt;
    private final String suffix;

    public HashOperator(PipelineNode node) {
        this.node = node;
        this.columns = node.strings("columns");
        this.algorithm = node.string("algorithm", "SHA-256").toUpperCase();
        this.salt = node.string("salt", "");
        this.suffix = node.string("suffix", "");
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("hash 节点 " + node.getId() + " 缺少 columns");
        }
        if (!"SHA-256".equals(algorithm) && !"MD5".equals(algorithm) && !"SHA-1".equals(algorithm)) {
            throw new IllegalArgumentException("hash 节点 " + node.getId()
                    + " 的 algorithm 只支持 SHA-256|SHA-1|MD5，实际: " + algorithm);
        }
    }

    @Override
    public void open(OperatorContext context) {
        // 无状态
    }

    @Override
    public void processElement(Record record, Output output) {
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(record.row());
        for (String column : columns) {
            Object value = record.row().get(column);
            if (value == null) {
                continue;
            }
            String digest = digest(salt + value);
            if (suffix.isEmpty()) {
                newRow.put(column, digest);
            } else {
                newRow.put(column + suffix, digest);
            }
        }
        output.collect(record.withRow(newRow));
    }

    private String digest(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            byte[] bytes = md.digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte item : bytes) {
                hex.append(Character.forDigit((item >> 4) & 0xF, 16));
                hex.append(Character.forDigit(item & 0xF, 16));
            }
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("hash 节点 " + node.getId() + " 摘要算法不可用: " + algorithm, e);
        }
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
