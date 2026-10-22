package com.zifang.z.etl.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收项 2：搬运中途 kill 进程，重启后从断点继续，最终对账一致。
 *
 * <p>实测口径（不是模拟异常，是**真的杀进程**）：
 * <ol>
 *   <li>子进程 A：造 20000 行源数据（首次）；</li>
 *   <li>子进程 B：全量搬运（首跑，落水位）⇒ 源/目标各 20000 行；</li>
 *   <li>子进程 C：源表再涨 20000 行；</li>
 *   <li>子进程 D：增量搬运 —— 父进程看到它已经提交了几页（progress.log 有记录）就
 *       {@code kill -9}（SIGKILL，没有 finally、没有优雅退出）；</li>
 *   <li>子进程 E（= 重启）：再次增量搬运 ⇒ **从断点继续**（水位之后才搬），
 *       最终源/目标行数一致、校验和一致（对账零差异）。</li>
 * </ol>
 *
 * <p>跨进程接力靠 H2 **文件**库（内存库随进程消失 ⇒ 证明不了续传）。
 * 全部在本地文件库上跑，不连任何真实库。
 *
 * @author zifang
 */
class WatermarkResumeAfterKillTest {

    private static final int SEED_ROWS = 20000;
    private static final int GROW_ROWS = 20000;

    @Test
    @DisplayName("搬运中途 kill -9 ⇒ 重启从断点继续 ⇒ 最终对账一致")
    void resumeFromCheckpointAfterProcessKilled() throws Exception {
        Path dir = Files.createTempDirectory("wm-kill");
        String classpath = resolveClasspath();
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";

        // —— 子进程 A：造数 ——
        String seedOut = runChild(javaBin, classpath, dir, "seed", SEED_ROWS);
        assertTrue(seedOut.contains("SEED rows=" + SEED_ROWS), "造数子进程输出异常: " + seedOut);

        // —— 子进程 B：首跑全量 ——
        String fullOut = runChild(javaBin, classpath, dir, "full", 0);
        assertTrue(fullOut.contains("mode=init"), "首跑应是全量: " + fullOut);
        Map<String, String> afterFull = report(javaBin, classpath, dir);
        assertEquals(SEED_ROWS, Long.parseLong(afterFull.get("source")), "源表行数");
        assertEquals(SEED_ROWS, Long.parseLong(afterFull.get("target")), "首跑后目标行数");
        String watermarkAfterFull = afterFull.get("watermark");
        assertTrue(watermarkAfterFull != null && !watermarkAfterFull.isEmpty(), "首跑后应有水位");

        // —— 子进程 C：源表继续涨 ——
        runChild(javaBin, classpath, dir, "grow", GROW_ROWS);

        // —— 子进程 D：增量搬运，父进程中途 kill -9 ——
        Path progress = dir.resolve("progress.log");
        Files.deleteIfExists(progress);
        Process copy = new ProcessBuilder(javaBin, "-cp", classpath,
                WatermarkKillResumeChild.class.getName(), dir.toString(), "copy")
                .redirectErrorStream(true)
                .redirectOutput(dir.resolve("copy.log").toFile())
                .start();
        waitForProgress(progress, 2, copy);
        copy.destroyForcibly();
        int exitCode = copy.waitFor();
        assertTrue(exitCode != 0, "子进程应是被 SIGKILL 掉的（退出码 " + exitCode + "）");

        // —— 杀完之后：水位已推进（页级提交落盘），但搬运没搬完 ——
        Map<String, String> afterKill = report(javaBin, classpath, dir);
        long targetAfterKill = Long.parseLong(afterKill.get("target"));
        assertTrue(targetAfterKill > SEED_ROWS,
                "kill 前应已提交了若干页，目标行数 " + targetAfterKill + " 应大于 " + SEED_ROWS);
        assertTrue(targetAfterKill < SEED_ROWS + GROW_ROWS,
                "kill 时搬运应还没完成，目标行数 " + targetAfterKill);
        String watermarkAfterKill = afterKill.get("watermark");
        assertTrue(watermarkAfterKill.compareTo(watermarkAfterFull) > 0,
                "水位应已按页推进: " + watermarkAfterFull + " → " + watermarkAfterKill);

        // —— 子进程 E：重启续传 ——
        String resumeOut = runChild(javaBin, classpath, dir, "copy", 0);
        assertTrue(resumeOut.contains("mode=incremental"), "重启后应走增量（从断点继续）: " + resumeOut);
        assertTrue(resumeOut.contains("reconciled=true"), "续传完成后对账应一致: " + resumeOut);

        Map<String, String> afterResume = report(javaBin, classpath, dir);
        long total = SEED_ROWS + GROW_ROWS;
        assertEquals(total, Long.parseLong(afterResume.get("source")), "源表总行数");
        assertEquals(total, Long.parseLong(afterResume.get("target")), "目标表总行数（行数对账零差异）");
        assertTrue(afterResume.get("watermark").compareTo(watermarkAfterKill) > 0,
                "续传后水位应继续推进");
    }

    /** 等子进程至少推进 N 页（progress.log 有 N 行）——超时就直接放行（后面断言会报出来）。 */
    private static void waitForProgress(Path progress, int lines, Process process) throws Exception {
        long deadline = System.currentTimeMillis() + 60000L;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(progress)) {
                List<String> content = Files.readAllLines(progress);
                if (content.size() >= lines) {
                    return;
                }
            }
            if (!process.isAlive()) {
                return;
            }
            Thread.sleep(20L);
        }
    }

    /** 跑一个子进程并等它结束。 */
    private static String runChild(String javaBin, String classpath, Path dir, String mode, int rows)
            throws Exception {
        Process process = new ProcessBuilder(javaBin, "-cp", classpath,
                WatermarkKillResumeChild.class.getName(), dir.toString(), mode, String.valueOf(rows))
                .redirectErrorStream(true)
                .start();
        String output = drain(process);
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("子进程 " + mode + " 退出码 " + exit + "，输出: " + output);
        }
        return output;
    }

    /** 跑子进程拿行数 / 水位报告。 */
    private static Map<String, String> report(String javaBin, String classpath, Path dir) throws Exception {
        String output = runChild(javaBin, classpath, dir, "report", 0);
        Map<String, String> values = new HashMap<String, String>();
        for (String line : output.split("\\r?\\n")) {
            if (line.startsWith("REPORT ")) {
                String body = line.substring("REPORT ".length()).trim();
                int index = body.indexOf('=');
                if (index > 0) {
                    values.put(body.substring(0, index), body.substring(index + 1));
                }
            }
        }
        return values;
    }

    private static String drain(Process process) throws Exception {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8")));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
        } finally {
            reader.close();
        }
        return sb.toString();
    }

    /**
     * 取子进程的 classpath。
     *
     * <p>Surefire 默认用「manifest-only jar」启动测试 JVM ⇒ {@code java.class.path} 里只有一个
     * 引导 jar，真正的依赖都在它的 manifest {@code Class-Path} 里 ⇒ 这里把 manifest 展开。
     */
    private static String resolveClasspath() throws Exception {
        String raw = System.getProperty("java.class.path");
        StringBuilder sb = new StringBuilder();
        for (String part : raw.split(File.pathSeparator)) {
            if (part == null || part.trim().isEmpty()) {
                continue;
            }
            File file = new File(part);
            if (file.isFile() && part.endsWith(".jar")) {
                try {
                    FileInputStream in = new FileInputStream(file);
                    try {
                        Manifest manifest = new Manifest(in);
                        String entries = manifest.getMainAttributes().getValue("Class-Path");
                        if (entries != null) {
                            for (String entry : entries.split("\\s+")) {
                                if (entry == null || entry.trim().isEmpty()) {
                                    continue;
                                }
                                File resolved = new File(file.getParentFile(), entry);
                                sb.append(resolved.getAbsolutePath()).append(File.pathSeparator);
                            }
                        }
                    } finally {
                        in.close();
                    }
                } catch (Exception notAManifest) {
                    // 不是 manifest jar（普通依赖 jar）⇒ 本身就已在 classpath 里，不用展开
                    sb.append(file.getAbsolutePath()).append(File.pathSeparator);
                }
            } else {
                sb.append(file.getAbsolutePath()).append(File.pathSeparator);
            }
        }
        String classpath = sb.toString();
        if (!classpath.contains("test-classes")) {
            throw new IllegalStateException("解析出来的 classpath 里没有测试类目录，无法起子进程: " + classpath);
        }
        return classpath;
    }
}
