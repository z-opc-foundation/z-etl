package com.zifang.z.etl.core;

import com.zifang.z.etl.core.checkpoint.Checkpoint;
import com.zifang.z.etl.core.checkpoint.CheckpointStore;
import com.zifang.z.etl.core.checkpoint.JdbcCheckpointStore;
import com.zifang.z.etl.core.engine.EtlEngine;
import com.zifang.z.etl.core.engine.EtlRunResult;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.job.JobDescriptor;
import com.zifang.z.etl.core.spi.MapDataSourceProvider;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.sql.Timestamp;

/**
 * 验收项 2 的**子进程**：搬运途中被 kill -9 的那一方。
 *
 * <p>为什么起子进程：断点续传要证明的是「**进程**被杀之后重启能续上」，
 * 同 JVM 里模拟异常证明不了「进程被杀」这件事（优雅退出时 finally 还会跑）。
 * 这里让子进程真的跑搬运，父进程在它跑到一半时 {@code kill -9}。
 *
 * <p>子进程之间靠 H2 **文件**库接力（内存库随进程一起没了 ⇒ 证明不了续传）。
 *
 * <p>用法：{@code java -cp <test-classpath> com.zifang.z.etl.core.WatermarkKillResumeChild <dir> <mode> [n]}
 * <ul>
 *   <li>{@code seed <n>}：建表 + 灌 n 行</li>
 *   <li>{@code full}：全量搬运（首跑，落水位）</li>
 *   <li>{@code grow <n>}：再灌 n 行（时间戳更晚）</li>
 *   <li>{@code copy}：增量搬运（每落一次水位就往 progress.log 追一行）</li>
 *   <li>{@code report}：打印行数 / 水位（父进程据此对账）</li>
 * </ul>
 *
 * @author zifang
 */
public final class WatermarkKillResumeChild {

    static final String JOB_NAME = "wm-kill";
    private static final Timestamp BASE = new Timestamp(1767225600000L);

    private WatermarkKillResumeChild() {
    }

    /**
     * 子进程入口。
     *
     * @param args 参数（目录 / 模式 / 可选行数）
     * @throws Exception 失败直接抛出（父进程看退出码）
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法: WatermarkKillResumeChild <dir> <mode> [n]");
            System.exit(2);
        }
        String dir = args[0];
        String mode = args[1];
        int rows = args.length > 2 ? Integer.parseInt(args[2]) : 0;

        DataSource source = WatermarkTestFixtures.h2File(dir + File.separator + "src");
        DataSource target = WatermarkTestFixtures.h2File(dir + File.separator + "dst");
        DataSource meta = WatermarkTestFixtures.h2File(dir + File.separator + "meta");

        if ("seed".equals(mode)) {
            WatermarkTestFixtures.createSchema(source, target);
            EtlSchema.ensure(meta);
            WatermarkTestFixtures.insertOrders(source, 1L, rows, BASE, 60000L);
            System.out.println("SEED rows=" + rows);
            return;
        }
        if ("grow".equals(mode)) {
            // 时间戳必须**晚于**已有水位（首跑的水位 = BASE + (seedRows-1)*60000）⇒ 取一个足够大的偏移
            WatermarkTestFixtures.insertOrders(source, 900000L, rows,
                    new Timestamp(BASE.getTime() + 2000000000L), 60000L);
            System.out.println("GROW rows=" + rows);
            return;
        }
        if ("report".equals(mode)) {
            System.out.println("REPORT source=" + WatermarkTestFixtures.count(source,
                    WatermarkTestFixtures.SOURCE_TABLE));
            System.out.println("REPORT target=" + WatermarkTestFixtures.count(target,
                    WatermarkTestFixtures.TARGET_TABLE));
            JdbcCheckpointStore store = new JdbcCheckpointStore(meta);
            Checkpoint checkpoint = store.load(JOB_NAME);
            System.out.println("REPORT watermark=" + (checkpoint == null ? "" : checkpoint.getWatermarkValue()));
            System.out.println("REPORT tiebreak=" + (checkpoint == null ? "" : checkpoint.getTiebreakValue()));
            System.out.println("REPORT lastRunAt=" + (checkpoint == null ? "" : checkpoint.getLastRunAt()));
            return;
        }

        // —— full / copy：真跑引擎 ——
        EtlSchema.ensure(meta);
        JdbcCheckpointStore store = new JdbcCheckpointStore(meta);
        final CheckpointStore delegate = store;
        final String progressFile = dir + File.separator + "progress.log";
        // 落盘保证：进程被 SIGKILL 时，已提交的页必须已经在磁盘上（MVStore 默认延迟刷盘）
        final java.sql.Connection durable = target.getConnection();
        if ("copy".equals(mode)) {
            // 每次 copy 重开进度文件 ⇒ 父进程看到的一定是**这一轮**推进的页
            File progress = new File(progressFile);
            if (progress.exists() && !progress.delete()) {
                System.out.println("WARN progress 文件删不掉: " + progressFile);
            }
        }
        CheckpointStore logging = new CheckpointStore() {
            @Override
            public Checkpoint load(String jobName) {
                return delegate.load(jobName);
            }

            @Override
            public void save(Checkpoint checkpoint) {
                delegate.save(checkpoint);
                checkpoint(durable);
                appendProgress(progressFile, checkpoint);
            }

            @Override
            public void clear(String jobName) {
                delegate.clear(jobName);
            }
        };

        EtlEngine engine = new EtlEngine(new MapDataSourceProvider()
                .register(WatermarkTestFixtures.SOURCE_REF, source)
                .register(WatermarkTestFixtures.TARGET_REF, target), logging);
        JobDescriptor descriptor = WatermarkTestFixtures.job(JOB_NAME, "id", 0L, "FAIL", 200);

        EtlRunResult result = engine.run(descriptor);
        System.out.println("RUN mode=" + result.getMode()
                + " rowsRead=" + result.getRowsRead()
                + " rowsWritten=" + result.getRowsWritten()
                + " watermarkBefore=" + result.getWatermarkBefore()
                + " watermarkAfter=" + result.getWatermarkAfter()
                + " reconciled=" + result.isReconciled()
                + " sourceCount=" + result.getReport().getSourceCount()
                + " targetCount=" + result.getReport().getTargetCount());
        System.out.flush();
    }

    /** 强制 MVStore 立即刷盘（H2 的 CHECKPOINT）—— 让「已提交」在 SIGKILL 后真的还在。 */
    private static void checkpoint(java.sql.Connection connection) {
        try {
            java.sql.Statement statement = connection.createStatement();
            try {
                statement.execute("CHECKPOINT");
            } finally {
                statement.close();
            }
        } catch (Exception ignore) {
            // 取证辅助，刷不出去也不影响搬运
        }
    }

    /** 水位每落一次就追一行（父进程据此判断「跑到一半了」并 kill）。 */
    private static void appendProgress(String file, Checkpoint checkpoint) {
        Writer writer = null;
        try {
            writer = new FileWriter(file, true);
            writer.write("watermark=" + checkpoint.getWatermarkValue()
                    + "#" + checkpoint.getTiebreakValue()
                    + " at=" + checkpoint.getLastRunAt()
                    + System.lineSeparator());
            writer.flush();
        } catch (Exception e) {
            // 取证辅助，写不进去不影响搬运
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception ignore) {
                    // 忽略
                }
            }
        }
    }
}
