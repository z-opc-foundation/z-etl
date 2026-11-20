package com.zifang.z.etl.core.checkpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zifang.z.etl.core.runtime.PipelineStateStore;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * 保存点的导出 / 灌回。
 *
 * <p>导出（{@link #export}）纯读：状态存储与水位存储里的现场**原样保留**；
 * 灌回（{@link #apply}）覆盖写：目标管道原有的状态与水位被报文内容替换。
 * 两者合起来就是「作业迁移」：A 环境导出 → B 环境灌回（可换名）→ B 直接增量续跑。
 *
 * @author zifang
 */
public final class Savepoints {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 当前报文格式版本。 */
    public static final int CURRENT_VERSION = 1;

    private Savepoints() {
    }

    /**
     * 从现场导出保存点（纯读，不动状态与水位）。
     *
     * @param pipelineName    流水线名
     * @param stateStore      状态快照存储
     * @param checkpointStore 水位存储
     * @return 保存点
     * @throws IllegalArgumentException 状态与水位都没有（空管道没东西可搬）
     */
    public static Savepoint export(String pipelineName, PipelineStateStore stateStore,
                                   CheckpointStore checkpointStore) {
        byte[] state = stateStore == null ? null : stateStore.load(pipelineName);
        Checkpoint checkpoint = checkpointStore == null ? null : checkpointStore.load(pipelineName);
        if ((state == null || state.length == 0) && checkpoint == null) {
            throw new IllegalArgumentException("流水线 " + pipelineName
                    + " 没有可导出的状态与水位（先跑一次再导出）");
        }
        Savepoint savepoint = new Savepoint();
        savepoint.setPipeline(pipelineName);
        savepoint.setSavedAt(System.currentTimeMillis());
        if (state != null && state.length > 0) {
            savepoint.setStateBase64(Base64.getEncoder().encodeToString(state));
        }
        if (checkpoint != null) {
            savepoint.setWatermarkColumn(checkpoint.getWatermarkColumn());
            savepoint.setWatermarkType(checkpoint.getWatermarkType());
            savepoint.setWatermarkValue(checkpoint.getWatermarkValue());
            savepoint.setTiebreakColumn(checkpoint.getTiebreakColumn());
            savepoint.setTiebreakValue(checkpoint.getTiebreakValue());
            savepoint.setLastMode(checkpoint.getLastMode());
            savepoint.setLastRunAt(checkpoint.getLastRunAt());
        }
        return savepoint;
    }

    /** 保存点 → JSON 文本。 */
    public static String toJson(Savepoint savepoint) throws Exception {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(savepoint);
    }

    /** JSON 文本 → 保存点（校验版本与基本形状）。 */
    public static Savepoint fromJson(String json) throws Exception {
        Savepoint savepoint = MAPPER.readValue(json, Savepoint.class);
        if (savepoint.getVersion() != CURRENT_VERSION) {
            throw new IllegalArgumentException("保存点版本不支持: " + savepoint.getVersion()
                    + "（当前支持 v" + CURRENT_VERSION + "）");
        }
        if (savepoint.getPipeline() == null || savepoint.getPipeline().trim().isEmpty()) {
            throw new IllegalArgumentException("保存点缺 pipeline 字段");
        }
        if (!savepoint.hasState() && !savepoint.hasWatermark()) {
            throw new IllegalArgumentException("保存点既没有状态也没有水位（空报文）");
        }
        return savepoint;
    }

    /**
     * 灌回现场（覆盖写）：状态快照与水位游标按目标名落位。
     *
     * @param savepoint       保存点
     * @param targetName      目标流水线名（可与导出名不同 ⇒ 作业迁移 / 复制）
     * @param stateStore      状态快照存储
     * @param checkpointStore 水位存储
     */
    public static void apply(Savepoint savepoint, String targetName, PipelineStateStore stateStore,
                             CheckpointStore checkpointStore) {
        if (savepoint.hasState()) {
            if (stateStore == null) {
                throw new IllegalStateException("目标环境没有配置状态存储，灌不进状态快照");
            }
            stateStore.save(targetName, Base64.getDecoder().decode(savepoint.getStateBase64()));
        }
        if (savepoint.hasWatermark()) {
            if (checkpointStore == null) {
                throw new IllegalStateException("目标环境没有配置水位存储，灌不进水位游标");
            }
            Checkpoint checkpoint = new Checkpoint(targetName, savepoint.getWatermarkColumn(),
                    savepoint.getWatermarkType(), savepoint.getWatermarkValue(),
                    savepoint.getTiebreakColumn(), savepoint.getTiebreakValue(),
                    savepoint.getLastMode(), savepoint.getLastRunAt());
            checkpointStore.save(checkpoint);
        }
    }

    /** 状态快照字节数（报文里展示「搬了多大」用）。 */
    public static int stateBytes(Savepoint savepoint) {
        return savepoint.hasState() ? Base64.getDecoder().decode(savepoint.getStateBase64()).length : 0;
    }

    /** JSON 文本按 UTF-8 字节（导出落文件用）。 */
    public static byte[] toBytes(Savepoint savepoint) throws Exception {
        return toJson(savepoint).getBytes(StandardCharsets.UTF_8);
    }
}
