package com.zifang.z.etl.core.connector;

import com.zifang.z.etl.core.pipeline.OperatorType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 线性作业层的连接器类型映射（job 描述里的 {@code source.type} / {@code target.type}）。
 *
 * <p>口径：**线性层与 DAG 层共用同一批连接器实现** —— job 描述里写 {@code type: file.csv}，
 * 跑起来的就是 DAG 层那个 {@code file.csv.source} 算子（不是另写一套）。
 *
 * @author zifang
 */
public final class ConnectorTypes {

    /** JDBC 类型（走 SQL 那条老路）。 */
    public static final String JDBC = "jdbc";

    private ConnectorTypes() {
    }

    /** 是否 JDBC 类型。 */
    public static boolean isJdbc(String type) {
        return JDBC.equals(normalize(type));
    }

    /**
     * 源类型串 → 算子类型。
     *
     * @param type job 描述里的源类型
     * @return 算子类型；未知返回 {@code null}
     */
    public static OperatorType sourceType(String type) {
        String key = normalize(type);
        if (JDBC.equals(key)) {
            return OperatorType.JDBC_SOURCE;
        }
        if ("csv".equals(key) || "file.csv".equals(key)) {
            return OperatorType.FILE_CSV_SOURCE;
        }
        if ("json".equals(key) || "ndjson".equals(key) || "file.json".equals(key)) {
            return OperatorType.FILE_JSON_SOURCE;
        }
        if ("xlsx".equals(key) || "excel".equals(key) || "file.xlsx".equals(key)) {
            return OperatorType.FILE_XLSX_SOURCE;
        }
        if ("http".equals(key) || "rest".equals(key) || "api".equals(key)) {
            return OperatorType.HTTP_SOURCE;
        }
        if ("inline".equals(key) || "static".equals(key) || "const".equals(key)) {
            return OperatorType.INLINE_SOURCE;
        }
        if ("script".equals(key) || "groovy".equals(key)) {
            return OperatorType.SCRIPT_SOURCE;
        }
        if ("message".equals(key) || "mq".equals(key) || "kafka".equals(key) || "queue".equals(key)) {
            return OperatorType.MESSAGE_SOURCE;
        }
        return null;
    }

    /**
     * 汇类型串 → 算子类型。
     *
     * @param type job 描述里的目标类型
     * @return 算子类型；未知返回 {@code null}
     */
    public static OperatorType sinkType(String type) {
        String key = normalize(type);
        if (JDBC.equals(key)) {
            return OperatorType.JDBC_SINK;
        }
        if ("csv".equals(key) || "file.csv".equals(key)) {
            return OperatorType.FILE_CSV_SINK;
        }
        if ("json".equals(key) || "ndjson".equals(key) || "file.json".equals(key)) {
            return OperatorType.FILE_JSON_SINK;
        }
        if ("xlsx".equals(key) || "excel".equals(key) || "file.xlsx".equals(key)) {
            return OperatorType.FILE_XLSX_SINK;
        }
        if ("http".equals(key) || "rest".equals(key) || "webhook".equals(key) || "api".equals(key)) {
            return OperatorType.HTTP_SINK;
        }
        if ("message".equals(key) || "mq".equals(key) || "kafka".equals(key) || "queue".equals(key)) {
            return OperatorType.MESSAGE_SINK;
        }
        if ("console".equals(key)) {
            return OperatorType.CONSOLE_SINK;
        }
        if ("collect".equals(key)) {
            return OperatorType.COLLECT_SINK;
        }
        return null;
    }

    /**
     * 该连接器必填的 option 键（缺了就报错，不静默跑空）。
     *
     * @param type 算子类型
     * @return 必填键列表
     */
    public static List<String> requiredOptions(OperatorType type) {
        if (type == null) {
            return Collections.emptyList();
        }
        List<String> keys = new ArrayList<String>();
        switch (type) {
            case FILE_CSV_SOURCE:
            case FILE_JSON_SOURCE:
            case FILE_XLSX_SOURCE:
                keys.add("path");
                break;
            case HTTP_SOURCE:
                keys.add("url");
                break;
            case INLINE_SOURCE:
                keys.add("rows");
                break;
            case SCRIPT_SOURCE:
                keys.add("script");
                break;
            case FILE_CSV_SINK:
            case FILE_JSON_SINK:
            case FILE_XLSX_SINK:
                keys.add("path");
                break;
            case MESSAGE_SOURCE:
            case MESSAGE_SINK:
                keys.add("topic");
                break;
            default:
                break;
        }
        return keys;
    }

    /** 类型串归一（小写 + 下划线视作点）。 */
    private static String normalize(String type) {
        if (type == null) {
            return JDBC;
        }
        String key = type.trim().toLowerCase().replace('_', '.');
        return key.isEmpty() ? JDBC : key;
    }
}
