package com.zifang.z.etl.web;

import com.zifang.z.etl.core.engine.DataSourceDef;
import com.zifang.z.etl.web.api.EtlDataSourceController;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据源登记 REST 面（web 层）验收：登记 / 列表掩码 / 测试连接（留空密码取库存真值）/ 删除。
 *
 * <p>跑在本地 H2 上，不连生产库。
 *
 * @author zifang
 */
class EtlDataSourceControllerTest {

    private EtlDataSourceController controller;

    @BeforeEach
    void setUp() throws Exception {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:datasource-web-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        controller = new EtlDataSourceController((DataSource) ds);
    }

    private static DataSourceDef def(String name, String url, String password) {
        DataSourceDef def = new DataSourceDef();
        def.setName(name);
        def.setType("jdbc");
        def.setUrl(url);
        def.setUsername("sa");
        def.setPassword(password);
        def.setDescription("测试");
        return def;
    }

    @Test
    @DisplayName("登记 → 列表掩码（密码不出网）→ 测试连接 → 删除")
    void saveListTestDelete() {
        String h2Url = "jdbc:h2:mem:datasource-target-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
        Map<String, Object> saved = controller.save(def("web-ds", h2Url, "sa-secret"));
        assertEquals("web-ds", saved.get("name"));

        List<Map<String, Object>> list = (List<Map<String, Object>>) controller.list().get("datasources");
        assertEquals(1, list.size());
        assertEquals("********", list.get(0).get("password"), "密码掩码不出网");

        Map<String, Object> test = controller.test(def("web-ds", h2Url, ""));
        assertEquals(Boolean.TRUE, test.get("ok"), "留空密码用库存真值握手");
        assertNotNull(test.get("message"));

        Map<String, Object> removed = controller.delete("web-ds");
        assertEquals(Boolean.TRUE, removed.get("removed"));
        assertEquals(0, ((List<?>) controller.list().get("datasources")).size());
    }

    @Test
    @DisplayName("测试连接：坏 url 报 ok=false + 错误摘要")
    void testBadConnection() {
        Map<String, Object> test = controller.test(def("bad-ds",
                "jdbc:h2:mem:x;NON_EXISTENT_SETTING=1", ""));
        assertEquals(Boolean.FALSE, test.get("ok"));
        assertTrue(!String.valueOf(test.get("message")).isEmpty());
    }

    @Test
    @DisplayName("缺数据源名 ⇒ 明确报错，不静默")
    void requiresName() {
        try {
            controller.save(def("", "jdbc:h2:mem:x", ""));
            throw new AssertionError("缺名应当报错");
        } catch (IllegalArgumentException expected) {
            assertFalse(String.valueOf(expected.getMessage()).isEmpty());
        }
    }
}
