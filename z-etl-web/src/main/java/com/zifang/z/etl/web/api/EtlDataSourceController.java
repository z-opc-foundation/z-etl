package com.zifang.z.etl.web.api;

import com.zifang.z.etl.core.engine.DataSourceDef;
import com.zifang.z.etl.core.engine.DataSourceIntrospector;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.engine.JdbcDataSourceRegistry;
import com.zifang.z.meta.api.annotation.BizApi;
import com.zifang.z.meta.api.annotation.BizField;
import com.zifang.z.meta.api.annotation.BizParam;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据源登记 REST 面。
 *
 * <pre>
 * GET    /api/etl/datasources          数据源列表（密码掩码，不出网）
 * POST   /api/etl/datasources          登记 / 更新（body = name/type/url/username/password/description；
 *                                      编辑时 password 留空 = 保留旧密码）
 * DELETE /api/etl/datasources/{name}   删除
 * POST   /api/etl/datasources/test     测试连接（body 同上；已登记条目 password 留空 = 用库里存的）
 * GET    /api/etl/datasources/{name}/tables  表结构探查（表清单 + 列）
 * POST   /api/etl/datasources/{name}/preview 样例行预览（body = {table, limit}）
 * </pre>
 *
 * @author zifang
 */
@RestController
@RequestMapping("/api/etl/datasources")
public class EtlDataSourceController {

    private static final String PASSWORD_MASK = "********";

    private final JdbcDataSourceRegistry registry;

    public EtlDataSourceController(@Qualifier("dataSourceEtl") DataSource dataSourceEtl) throws SQLException {
        EtlSchema.ensure(dataSourceEtl);
        this.registry = new JdbcDataSourceRegistry(dataSourceEtl);
    }

    /** 数据源列表（密码不出网）。 */
    @GetMapping
    @BizApi(summary = "数据源列表", description = "数据源列表（密码掩码，不出网）。", tags = {"z-etl"})
    public Map<String, Object> list() {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("datasources", masked(registry.list()));
        return body;
    }

    /** 登记 / 更新。 */
    @PostMapping
    @BizApi(summary = "登记数据源", description = "登记 / 更新数据源（body = name/type/url/username/password/description；"
            + "编辑时 password 留空 = 保留旧密码）。", tags = {"z-etl"})
    public Map<String, Object> save(@RequestBody @BizParam(name = "def", type = BizField.FieldType.JSON,
            description = "数据源定义", position = BizParam.Position.BODY) DataSourceDef def) {
        requireName(def);
        registry.save(def, true);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("name", def.getName());
        body.put("saved", Boolean.TRUE);
        return body;
    }

    /** 删除。 */
    @DeleteMapping("/{name}")
    @BizApi(summary = "删除数据源", description = "删除数据源登记。", tags = {"z-etl"})
    public Map<String, Object> delete(@PathVariable("name") @BizParam(name = "name",
            type = BizField.FieldType.STRING, description = "数据源名", position = BizParam.Position.PATH) String name) {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("name", name);
        body.put("removed", registry.delete(name));
        return body;
    }

    /**
     * 测试连接（实际握手 + SELECT 1）。
     *
     * <p>body 可只带未登记的新表单，也可对已登记条目省略 password（用库里存的真值）。
     */
    @PostMapping("/test")
    @BizApi(summary = "测试数据源连接", description = "实际握手 + SELECT 1；已登记条目 password 留空 = 用库里存的真值。", tags = {"z-etl"})
    public Map<String, Object> test(@RequestBody @BizParam(name = "def", type = BizField.FieldType.JSON,
            description = "数据源定义（url/username/password；name 已登记时 password 可留空）",
            position = BizParam.Position.BODY) DataSourceDef def) {
        requireName(def);
        if (def.getPassword() == null || def.getPassword().isEmpty()) {
            DataSourceDef stored = registry.find(def.getName());
            if (stored != null) {
                def.setPassword(stored.getPassword());
            }
        }
        String error = JdbcDataSourceRegistry.testConnection(def, 10);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("name", def.getName());
        body.put("ok", error == null);
        body.put("message", error == null ? "连接成功" : error);
        return body;
    }

    /**
     * 表结构探查（表清单 + 列）。
     *
     * <p>门户 ETL 台账要「选表 → 看列」，这个能力以前只在未来的参照实现里有；
     * 现在由我们自己的 z-etl 提供，凭据不出 z-etl。</p>
     */
    @GetMapping("/{name}/tables")
    @BizApi(summary = "数据源表结构", description = "列出可用的表 / 视图及其列（凭据留在 z-etl 侧）。", tags = {"z-etl"})
    public Map<String, Object> tables(@PathVariable("name") @BizParam(name = "name",
            type = BizField.FieldType.STRING, description = "数据源名", position = BizParam.Position.PATH) String name) {
        DataSourceDef def = registry.find(name);
        if (def == null) {
            throw new IllegalArgumentException("数据源不存在：" + name);
        }
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("name", name);
        body.put("tables", new DataSourceIntrospector().tables(def));
        return body;
    }

    /**
     * 样例行预览（读前 N 行）。
     */
    @PostMapping("/{name}/preview")
    @BizApi(summary = "数据源样例行", description = "读取表的前 N 行（body = {\"table\":\"表名\",\"limit\":20}）。",
            tags = {"z-etl"})
    public Map<String, Object> preview(@PathVariable("name") @BizParam(name = "name",
            type = BizField.FieldType.STRING, description = "数据源名", position = BizParam.Position.PATH) String name,
            @RequestBody(required = false) @BizParam(name = "query",
                    type = BizField.FieldType.JSON, description = "{\"table\":\"表名\",\"limit\":20}",
                    position = BizParam.Position.BODY) Map<String, Object> query) {
        DataSourceDef def = registry.find(name);
        if (def == null) {
            throw new IllegalArgumentException("数据源不存在：" + name);
        }
        String table = query == null ? null : asString(query.get("table"));
        if (table == null || table.trim().isEmpty()) {
            throw new IllegalArgumentException("table 不能为空");
        }
        int limit = query == null || query.get("limit") == null ? 20 : asInt(query.get("limit"), 20);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("name", name);
        body.put("table", table);
        body.put("rows", new DataSourceIntrospector().preview(def, table, limit));
        return body;
    }

    private static void requireName(DataSourceDef def) {
        if (def == null || def.getName() == null || def.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("数据源名不能为空");
        }
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static int asInt(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static List<Map<String, Object>> masked(List<DataSourceDef> defs) {
        List<Map<String, Object>> rows = new java.util.ArrayList<Map<String, Object>>();
        for (DataSourceDef def : defs) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("name", def.getName());
            row.put("type", def.getType());
            row.put("url", def.getUrl());
            row.put("username", def.getUsername());
            row.put("password", def.getPassword() == null || def.getPassword().isEmpty() ? "" : PASSWORD_MASK);
            row.put("description", def.getDescription());
            row.put("extra", def.getExtra());
            row.put("createdAt", def.getCreatedAt());
            row.put("updatedAt", def.getUpdatedAt());
            rows.add(row);
        }
        return rows;
    }
}
