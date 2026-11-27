package com.zifang.z.etl.web.api;

import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.meta.api.annotation.BizApi;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 算子目录 REST 面：给控制台画布调色盘与「数据源 / 输出源」目录页供数。
 *
 * <pre>
 * GET /api/etl/operators   全部算子（id / kind / desc），按枚举声明序
 * </pre>
 *
 * @author zifang
 */
@RestController
@RequestMapping("/api/etl/operators")
public class EtlOperatorController {

    private static final Map<String, String> DESC = new LinkedHashMap<String, String>();

    static {
        DESC.put("jdbc.source", "关系库源：分页游标拉数 + SQL 下推 + 事件时间水位 + 断点续传");
        DESC.put("file.csv.source", "CSV 文件源（表头/分隔符/引号可配，列类型按内容推断）");
        DESC.put("file.json.source", "JSON 文件源（ndjson 一行一个 / json 数组 + itemPath 定位）");
        DESC.put("file.xlsx.source", "Excel 第一张工作表（零依赖，日期样式还原成 Timestamp）");
        DESC.put("file.xml.source", "XML 文件源（StAX 流式解析，recordPath 定位记录元素）");
        DESC.put("http.source", "REST / JSON 接口当流拉（pageParam 翻页到空页停）");
        DESC.put("mill.source", "资料磨坊源：全格式文档 / 网页 → 分块行或整件成品行");
        DESC.put("inline.source", "内联常量源（声明里写死的静态行）");
        DESC.put("script.source", "脚本源：z-script 沙箱脚本生成数据");
        DESC.put("message.source", "消息源：Kafka / RocketMQ / 库表队列经 MessageBus SPI");
        DESC.put("pipeline.ref", "子管道引用源：已登记管道当视图复用（汇换收集、不碰水位）");
        DESC.put("filter", "过滤：表达式为真才放行，未命中可走旁路输出");
        DESC.put("route", "路由：n 路条件分流，rules 顺序判定命中即走旁路，fallback 兜底");
        DESC.put("map", "行级映射：表达式产出新列");
        DESC.put("index", "行号：每行编一个序号列，可按键各自编号（ROW_NUMBER 无排序版）");
        DESC.put("script", "自由脚本：z-script 沙箱，可产出一列或整行");
        DESC.put("script.remote", "平台脚本：调 z-script 脚本中心已上线脚本（app + AK）");
        DESC.put("async.map", "异步映射：每行异步外呼 HTTP 补列，容量背压（Flink AsyncFunction 口径）");
        DESC.put("ai.map", "AI 映射：每行渲染提示词调大模型补列（执行器宿主装配，AiExecutor SPI）");
        DESC.put("field.mapping", "字段映射：源列 → 目标列 + 落库类型");
        DESC.put("type.normalize", "类型归一：类型 + 定点小数位 + 空值策略");
        DESC.put("assert", "数据质量断言：行级判定 + 流结束结算，违规可失败/旁路/只计数");
        DESC.put("union", "多路合并：多条入边接成一路（不写也可多入边天然合并）");
        DESC.put("explode", "炸行：一行拆多行（数组列炸开 / 字符串切分，UNNEST 口径）");
        DESC.put("topn", "截断：按键 / 全局 Top-N（批口径，Flink Rank 保守子集）");
        DESC.put("limit", "截取：前 N 条放行（边流边截不整流，可配 offset）");
        DESC.put("sample", "采样：按比例放行（随机可复现 / 按键哈希）");
        DESC.put("distinct", "全行去重：整行 / 列子集判重，内存指纹 + 上限兜底");
        DESC.put("unpivot", "列转行：melt，声明列竖排成 名字列+值列");
        DESC.put("pivot", "行转列：keyBy 分组 + 透视列值变列名 + 聚合（物化口径，columnPrefix 给派生列加前缀）");
        DESC.put("regex.extract", "正则提取：捕获组落列，不匹配可 null 或丢行");
        DESC.put("split", "分列：按分隔符切进声明的输出列名");
        DESC.put("template", "模板合成：${列名} 占位符拼串落声明列");
        DESC.put("rename", "列改名：mapping 旧列名 → 新列名");
        DESC.put("flatten", "拍平：嵌套 Map 拍成点路径列");
        DESC.put("json.extract", "JSON 提取：列内 JSON 文本按点路径取字段落列");
        DESC.put("hash", "哈希：MD5 / SHA 摘要，指纹化 / 匿名化");
        DESC.put("mask", "脱敏：phone / email / half / full 四种打码规则");
        DESC.put("date.format", "时间格式化：epoch / ISO / 自定义格式 → 目标格式");
        DESC.put("sort", "排序：物化整流，只适合有界数据");
        DESC.put("profile", "列画像：行原样透传，流尾按列出 rows/nulls/distinct/min/max/mean 画像行");
        DESC.put("aggregate", "键控聚合：无窗口滚动累计，键变化即发一次");
        DESC.put("window.aggregate", "窗口聚合：事件时间窗口 + 水位触发 + 允许迟到");
        DESC.put("dedup", "键控去重：按键保留首条 / 末条，带存活时间");
        DESC.put("join", "连接：左右两路按键关联");
        DESC.put("diff", "差异：两流按键比对出变化流（INSERT/UPDATE/DELETE，快照 diff 口径）");
        DESC.put("except", "差集：左有右无的行原样发出（SQL EXCEPT 口径，物化）");
        DESC.put("intersect", "交集：两侧都有的行原样发出（SQL INTERSECT 口径，物化）");
        DESC.put("interval.join", "区间连接：关联键 + 事件时间区间（Flink IntervalJoin 口径）");
        DESC.put("dim.join", "维表关联：右流即维表，同键替换热更新 + TTL（broadcast-state 口径）");
        DESC.put("cep.pattern", "模式匹配：步骤序列 + 次数量词 + within 时间窗（Flink CEP 保守子集）");
        DESC.put("jdbc.sink", "JDBC 落地：批量 upsert / insert（幂等键）");
        DESC.put("file.csv.sink", "写 CSV 文件（表头 + 列顺序，可追加）");
        DESC.put("file.json.sink", "写 JSON 文件（ndjson / json 数组两种形态）");
        DESC.put("file.xlsx.sink", "写 Excel 文件（零依赖 zip + worksheet）");
        DESC.put("file.xml.sink", "写 XML 文件（XMLStreamWriter 流式写）");
        DESC.put("http.sink", "HTTP 落地：POST 到外部接口（webhook / 业务 API）");
        DESC.put("message.sink", "消息落地：发到 Kafka / RocketMQ / 库表队列");
        DESC.put("console.sink", "控制台落地：只打日志不落库（调试）");
        DESC.put("collect.sink", "内存落地：把行收进列表（测试 / 预览）");
    }

    /**
     * 全部算子目录。
     */
    @GetMapping
    @BizApi(summary = "算子目录", description = "全部算子（id / kind / desc），给画布调色盘与目录页供数。", tags = {"z-etl"})
    public Map<String, Object> list() {
        List<Map<String, Object>> operators = new ArrayList<Map<String, Object>>();
        for (OperatorType type : OperatorType.values()) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("id", type.id());
            item.put("kind", type.kind().name());
            item.put("desc", DESC.get(type.id()));
            operators.add(item);
        }
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("operators", operators);
        return body;
    }
}
