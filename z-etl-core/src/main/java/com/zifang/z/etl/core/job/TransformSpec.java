package com.zifang.z.etl.core.job;

import java.util.ArrayList;
import java.util.List;

/**
 * transform 链定义。
 *
 * <p>链上四类算子（本仓实现的全部口径）：
 * <ol>
 *   <li><b>字段映射</b>（{@link FieldMapping}）：源列 → 目标列 + 落库类型。</li>
 *   <li><b>行级表达式</b>（{@link ExpressionSpec}）：z-util-expr 行级求值，产出新列。</li>
 *   <li><b>脚本沙箱</b>（{@link ScriptSpec}）：z-script 沙箱执行复杂清洗，产出新列。</li>
 *   <li><b>类型归一</b>（{@link TypeNormalizationSpec}）：链尾收口，统一校类型/小数位/空值策略。</li>
 * </ol>
 *
 * <p>第四类「SQL pushdown」不在链上 —— 它作用在**源库取数**阶段（{@link SourceSpec#getPushdown()}），
 * 把过滤/聚合推到源库做，引擎只拿结果。
 *
 * @author zifang
 */
public class TransformSpec {

    private List<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();

    /** 类型归一规则（链尾收口）。 */
    private List<TypeNormalizationSpec> typeNormalizations = new ArrayList<TypeNormalizationSpec>();

    /** 行级表达式规则。 */
    private List<ExpressionSpec> expressions = new ArrayList<ExpressionSpec>();

    /** 脚本沙箱步骤。 */
    private List<ScriptSpec> scripts = new ArrayList<ScriptSpec>();

    public List<FieldMapping> getFieldMappings() {
        return fieldMappings;
    }

    public void setFieldMappings(List<FieldMapping> fieldMappings) {
        this.fieldMappings = fieldMappings == null ? new ArrayList<FieldMapping>() : fieldMappings;
    }

    public List<TypeNormalizationSpec> getTypeNormalizations() {
        return typeNormalizations;
    }

    public void setTypeNormalizations(List<TypeNormalizationSpec> typeNormalizations) {
        this.typeNormalizations = typeNormalizations == null
                ? new ArrayList<TypeNormalizationSpec>() : typeNormalizations;
    }

    public List<ExpressionSpec> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<ExpressionSpec> expressions) {
        this.expressions = expressions == null ? new ArrayList<ExpressionSpec>() : expressions;
    }

    public List<ScriptSpec> getScripts() {
        return scripts;
    }

    public void setScripts(List<ScriptSpec> scripts) {
        this.scripts = scripts == null ? new ArrayList<ScriptSpec>() : scripts;
    }
}
