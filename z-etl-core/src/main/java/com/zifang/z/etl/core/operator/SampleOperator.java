package com.zifang.z.etl.core.operator;

import com.zifang.z.etl.core.pipeline.PipelineNode;
import com.zifang.z.etl.core.runtime.Keys;
import com.zifang.z.etl.core.runtime.Operator;
import com.zifang.z.etl.core.runtime.OperatorContext;
import com.zifang.z.etl.core.runtime.Output;
import com.zifang.z.etl.core.runtime.Record;

import java.util.List;
import java.util.Random;

/**
 * 采样算子：按比例放行（调试 / 降载用）。
 *
 * <pre>
 * - id: sample
 *   type: sample
 *   config:
 *     rate: 0.1        # 放行比例（必填，0 &lt; rate ≤ 1）
 *     seed: 42         # 可选：随机种子（声明后同流复现）
 *     hashBy: [user]   # 可选：按键哈希采样（同键永远同判定，键上口径一致）
 * </pre>
 *
 * <p>口径：
 * <ul>
 *   <li><b>随机采样</b>：声明 {@code seed} ⇒ 可复现；不声明 ⇒ 每次跑都不一样；</li>
 *   <li><b>按键哈希采样</b>：{@code hashBy} 声明后按键哈希取模判定 —— 同键永远同判定
 *       （同一用户的行要么全留要么全丢，键上口径一致），且天然可复现；</li>
 *   <li>无状态：行原样放行或丢掉（只计数，不改行）；</li>
 *   <li>采样是<b>丢数据</b>的口径 —— 下游要全量就别接。</li>
 * </ul>
 *
 * @author zifang
 */
public class SampleOperator implements Operator {

    private final PipelineNode node;

    private double rate;
    private Random random;
    private List<String> hashBy = new java.util.ArrayList<String>();

    private OperatorContext context;

    public SampleOperator(PipelineNode node) {
        this.node = node;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.context = ctx;
        String declared = node.string("rate");
        if (declared == null || declared.trim().isEmpty()) {
            throw new IllegalArgumentException("sample 节点 " + node.getId() + " 必须声明 config.rate");
        }
        try {
            rate = Double.parseDouble(declared.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("sample 节点 " + node.getId()
                    + " 的 rate 不是数字: " + declared);
        }
        if (rate <= 0d || rate > 1d) {
            throw new IllegalArgumentException("sample 节点 " + node.getId()
                    + " 的 rate 要在 (0, 1] 内: " + rate);
        }
        String seed = node.string("seed");
        random = seed == null || seed.trim().isEmpty()
                ? new Random() : new Random(Long.parseLong(seed.trim()));
        hashBy = node.strings("hashBy");
    }

    @Override
    public void processElement(Record record, Output output) {
        boolean keep;
        if (!hashBy.isEmpty()) {
            String key = Keys.computeValues(record.row(), hashBy);
            if (key == null) {
                throw new IllegalStateException("sample 节点 " + node.getId() + " 的哈希键列为空，算不出键");
            }
            int bucket = (key.hashCode() & 0x7fffffff) % 10000;
            keep = bucket < (int) (rate * 10000d);
        } else {
            keep = random.nextInt(10000) < (int) (rate * 10000d);
        }
        if (keep) {
            output.collect(record);
            context.count("sampled");
        } else {
            context.count("skipped");
        }
    }

    @Override
    public void processWatermark(long watermark) {
        // 无状态，不碰水位
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
