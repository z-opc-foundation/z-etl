package com.zifang.z.etl.core.job;

import com.zifang.util.db.support.Identifiers;

import java.util.ArrayList;
import java.util.List;

/**
 * 作业描述校验器。
 *
 * <p>表名/列名只能拼进 SQL 文本（无法参数绑定），所以**必须**过 {@link Identifiers} 白名单
 * （{@code [A-Za-z0-9_]{1,128}}，至多 {@code schema.table} 两段）——
 * 这是需求 008 notes.md §9.1.1 / §9.8 的安全底线。
 *
 * @author zifang
 */
public final class JobDescriptorValidator {

    private JobDescriptorValidator() {
    }

    /**
     * 校验描述；不合法时抛 {@link IllegalArgumentException}（消息里逐条列出问题）。
     */
    public static void validate(JobDescriptor descriptor) {
        List<String> errors = errors(descriptor);
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("作业描述不合法: " + String.join("; ", errors));
        }
    }

    /**
     * 列出全部问题（空列表 = 合法）。
     */
    public static List<String> errors(JobDescriptor descriptor) {
        List<String> errors = new ArrayList<String>();
        if (descriptor == null) {
            errors.add("描述为空");
            return errors;
        }
        if (isBlank(descriptor.getName())) {
            errors.add("name 不能为空");
        }
        if (descriptor.modeEnum() == null) {
            errors.add("mode 非法（可用: auto / init / incremental），实际值: " + descriptor.getMode());
        }
        SourceSpec source = descriptor.getSource();
        if (source == null) {
            errors.add("source 不能为空");
        } else {
            if (isBlank(source.getDataSourceRef())) {
                errors.add("source.dataSourceRef 不能为空");
            }
            checkTable(errors, "source.table", source.getTable());
            if (source.getPageSize() <= 0) {
                errors.add("source.pageSize 必须为正数");
            }
            if (source.getColumns() != null) {
                for (String column : source.getColumns()) {
                    if (!Identifiers.isSafe(column)) {
                        errors.add("source.columns 含非法标识符: " + column);
                    }
                }
            }
            if (!isBlank(source.getOrderBy()) && !Identifiers.isSafe(source.getOrderBy())) {
                errors.add("source.orderBy 含非法标识符: " + source.getOrderBy());
            }
        }
        TargetSpec target = descriptor.getTarget();
        if (target == null) {
            errors.add("target 不能为空");
        } else {
            if (isBlank(target.getDataSourceRef())) {
                errors.add("target.dataSourceRef 不能为空");
            }
            checkTable(errors, "target.table", target.getTable());
            if (target.getBatchSize() <= 0) {
                errors.add("target.batchSize 必须为正数");
            }
            String writeMode = target.getWriteMode() == null ? "upsert" : target.getWriteMode().trim().toLowerCase();
            if (!"upsert".equals(writeMode) && !"insert".equals(writeMode)) {
                errors.add("target.writeMode 非法（可用: upsert / insert），实际值: " + target.getWriteMode());
            }
        }
        if (descriptor.getIdempotencyKey().isEmpty()) {
            errors.add("idempotencyKey 不能为空（重跑幂等靠它）");
        } else {
            for (String key : descriptor.getIdempotencyKey()) {
                if (!Identifiers.isSafe(key)) {
                    errors.add("idempotencyKey 含非法标识符: " + key);
                }
            }
        }
        WatermarkSpec watermark = descriptor.getWatermark();
        if (watermark != null) {
            if (isBlank(watermark.getColumn())) {
                errors.add("watermark.column 不能为空");
            } else if (!Identifiers.isSafe(watermark.getColumn())) {
                errors.add("watermark.column 含非法标识符: " + watermark.getColumn());
            }
            String type = watermark.getType() == null ? "LONG" : watermark.getType().trim().toUpperCase();
            if (!"LONG".equals(type) && !"TIMESTAMP".equals(type)) {
                errors.add("watermark.type 非法（可用: LONG / TIMESTAMP），实际值: " + watermark.getType());
            }
            if (!isBlank(watermark.getTargetColumn()) && !Identifiers.isSafe(watermark.getTargetColumn())) {
                errors.add("watermark.targetColumn 含非法标识符: " + watermark.getTargetColumn());
            }
        }
        TransformSpec transform = descriptor.getTransform();
        if (transform != null && transform.getFieldMappings() != null) {
            for (FieldMapping mapping : transform.getFieldMappings()) {
                if (mapping == null || isBlank(mapping.getSource())) {
                    errors.add("transform.fieldMappings 存在无 source 的映射");
                    continue;
                }
                if (!Identifiers.isSafe(mapping.getSource())) {
                    errors.add("transform.fieldMappings 含非法源标识符: " + mapping.getSource());
                }
                if (!isBlank(mapping.getTarget()) && !Identifiers.isSafe(mapping.getTarget())) {
                    errors.add("transform.fieldMappings 含非法目标标识符: " + mapping.getTarget());
                }
                if (!isBlank(mapping.getType()) && TypeNames.isKnown(mapping.getType()) == false) {
                    errors.add("transform.fieldMappings 含未知类型: " + mapping.getType()
                            + "（可用: " + TypeNames.supported() + "）");
                }
            }
        }
        return errors;
    }

    private static void checkTable(List<String> errors, String field, String table) {
        if (isBlank(table)) {
            errors.add(field + " 不能为空");
            return;
        }
        if (!Identifiers.isSafeQualifiedName(table)) {
            errors.add(field + " 非法标识符: " + table + "（只允许 table 或 schema.table，段内 [A-Za-z0-9_]{1,128}）");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
