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
        if (transform != null) {
            if (transform.getFieldMappings() != null) {
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
            checkTypeNormalizations(errors, transform.getTypeNormalizations());
            checkExpressions(errors, transform.getExpressions());
            checkScripts(errors, transform.getScripts());
        }
        checkPushdown(errors, source == null ? null : source.getPushdown());
        return errors;
    }

    /** 类型归一规则校验。 */
    private static void checkTypeNormalizations(List<String> errors, List<TypeNormalizationSpec> specs) {
        if (specs == null) {
            return;
        }
        for (TypeNormalizationSpec spec : specs) {
            if (spec == null || isBlank(spec.getColumn())) {
                errors.add("transform.typeNormalizations 存在无 column 的规则");
                continue;
            }
            if (!Identifiers.isSafe(spec.getColumn())) {
                errors.add("transform.typeNormalizations 含非法列标识符: " + spec.getColumn());
            }
            if (!TypeNames.isKnown(spec.getType())) {
                errors.add("transform.typeNormalizations 含未知类型: " + spec.getType()
                        + "（可用: " + TypeNames.supported() + "）");
            }
            if (spec.getScale() != null && spec.getScale().intValue() < 0) {
                errors.add("transform.typeNormalizations 的 scale 不能为负: " + spec.getColumn());
            }
        }
    }

    /** 行级表达式校验（含引擎白名单与不安全引擎的显式拒绝）。 */
    private static void checkExpressions(List<String> errors, List<ExpressionSpec> specs) {
        if (specs == null) {
            return;
        }
        for (ExpressionSpec spec : specs) {
            if (spec == null || isBlank(spec.getName())) {
                errors.add("transform.expressions 存在无 name 的规则");
                continue;
            }
            if (!Identifiers.isSafe(spec.getName())) {
                errors.add("transform.expressions 含非法输出列名: " + spec.getName());
            }
            if (isBlank(spec.getExpression())) {
                errors.add("transform.expressions[" + spec.getName() + "] 缺表达式");
            }
            String engine = spec.effectiveEngine();
            if (!ExpressionSpec.ENGINE_EL.equals(engine) && !ExpressionSpec.ENGINE_GROOVY.equals(engine)) {
                errors.add("transform.expressions[" + spec.getName() + "] 引擎不支持: " + spec.getEngine()
                        + "（可用: " + ExpressionSpec.ENGINE_EL + " / " + ExpressionSpec.ENGINE_GROOVY
                        + "；LUA 无沙箱可逃逸、JS 模块无 JS 引擎 ⇒ 硬拒绝）");
            }
            if (!isBlank(spec.getType()) && !TypeNames.isKnown(spec.getType())) {
                errors.add("transform.expressions[" + spec.getName() + "] 含未知类型: " + spec.getType()
                        + "（可用: " + TypeNames.supported() + "）");
            }
        }
    }

    /** 脚本沙箱步骤校验（含 LUA 的显式拒绝与超时/上限的取值范围）。 */
    private static void checkScripts(List<String> errors, List<ScriptSpec> specs) {
        if (specs == null) {
            return;
        }
        for (ScriptSpec spec : specs) {
            if (spec == null || isBlank(spec.getName())) {
                errors.add("transform.scripts 存在无 name 的步骤");
                continue;
            }
            if (!Identifiers.isSafe(spec.getName())) {
                errors.add("transform.scripts 含非法输出列名: " + spec.getName());
            }
            if (isBlank(spec.getScript())) {
                errors.add("transform.scripts[" + spec.getName() + "] 缺脚本文本");
            }
            String engine = spec.effectiveEngine();
            if (!ScriptSpec.ENGINE_GROOVY.equals(engine) && !ScriptSpec.ENGINE_EL.equals(engine)) {
                errors.add("transform.scripts[" + spec.getName() + "] 引擎不支持: " + spec.getEngine()
                        + "（可用: " + ScriptSpec.ENGINE_GROOVY + " / " + ScriptSpec.ENGINE_EL
                        + "；LUA 执行器无沙箱无超时、实测可逃逸 ⇒ 硬拒绝）");
            }
            if (spec.getTimeoutMs() != null && spec.getTimeoutMs().longValue() <= 0L) {
                errors.add("transform.scripts[" + spec.getName() + "] timeoutMs 必须为正数");
            }
            if (spec.getMaxOutputChars() != null && spec.getMaxOutputChars().intValue() <= 0) {
                errors.add("transform.scripts[" + spec.getName() + "] maxOutputChars 必须为正数");
            }
            if (!isBlank(spec.getType()) && !TypeNames.isKnown(spec.getType())) {
                errors.add("transform.scripts[" + spec.getName() + "] 含未知类型: " + spec.getType()
                        + "（可用: " + TypeNames.supported() + "）");
            }
        }
    }

    /** SQL 下推校验（函数白名单 + 标识符白名单）。 */
    private static void checkPushdown(List<String> errors, PushdownSpec pushdown) {
        if (pushdown == null) {
            return;
        }
        if (pushdown.getGroupBy() != null) {
            for (String column : pushdown.getGroupBy()) {
                if (!Identifiers.isSafe(column)) {
                    errors.add("source.pushdown.groupBy 含非法标识符: " + column);
                }
            }
        }
        if (pushdown.getAggregates() != null) {
            for (AggregateSpec aggregate : pushdown.getAggregates()) {
                if (aggregate == null) {
                    errors.add("source.pushdown.aggregates 存在空规则");
                    continue;
                }
                String function = aggregate.effectiveFunction();
                boolean allowed = false;
                for (String safe : AggregateSpec.SAFE_FUNCTIONS) {
                    if (safe.equals(function)) {
                        allowed = true;
                        break;
                    }
                }
                if (!allowed) {
                    errors.add("source.pushdown.aggregates 聚合函数不支持: " + aggregate.getFunction()
                            + "（可用: " + String.join("/", AggregateSpec.SAFE_FUNCTIONS) + "）");
                }
                String column = aggregate.getColumn();
                if (isBlank(column)) {
                    errors.add("source.pushdown.aggregates 缺聚合列");
                } else if (!"*".equals(column.trim()) && !Identifiers.isSafe(column)) {
                    errors.add("source.pushdown.aggregates 含非法列标识符: " + column);
                } else if ("*".equals(column.trim()) && !"COUNT".equals(function)) {
                    errors.add("source.pushdown.aggregates 只有 COUNT 允许 *，实际: " + function);
                }
                if (!Identifiers.isSafe(aggregate.effectiveAlias())) {
                    errors.add("source.pushdown.aggregates 含非法别名: " + aggregate.effectiveAlias());
                }
            }
        }
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
