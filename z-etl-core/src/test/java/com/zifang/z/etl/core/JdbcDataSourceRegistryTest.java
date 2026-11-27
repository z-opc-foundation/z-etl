package com.zifang.z.etl.core;

import com.zifang.z.etl.core.engine.DataSourceDef;
import com.zifang.z.etl.core.engine.EtlSchema;
import com.zifang.z.etl.core.engine.JdbcDataSourceRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据源登记仓储验收：登记 / 更新（密码保留口径）/ 删除 / 列表不带密码 / 测试连接。
 *
 * <p>⚠️ 硬边界：登记库与被测连接全是本地 H2 内存库，不连生产 RDS。
 *
 * @author zifang
 */
class JdbcDataSourceRegistryTest {

    private JdbcDataSourceRegistry registry;
    private DataSource meta;

    @BeforeEach
    void setUp() throws Exception {
        meta = EtlTestFixtures.h2("datasource-registry-" + System.nanoTime());
        EtlSchema.ensure(meta);
        registry = new JdbcDataSourceRegistry(meta);
    }

    private static DataSourceDef def(String name, String url, String password) {
        DataSourceDef def = new DataSourceDef();
        def.setName(name);
        def.setType("jdbc");
        def.setUrl(url);
        def.setUsername("sa");
        def.setPassword(password);
        def.setDescription("测试数据源 " + name);
        return def;
    }

    @Test
    @DisplayName("登记 → 列表（不带密码）→ find（带密码，引擎侧用）→ 删除")
    void saveListFindDelete() {
        String h2Url = "jdbc:h2:mem:registry-target-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
        registry.save(def("ds-a", h2Url, "sa-secret"), false);

        List<DataSourceDef> listed = registry.list();
        assertEquals(1, listed.size());
        assertEquals("ds-a", listed.get(0).getName());
        assertEquals("sa-secret", listed.get(0).getPassword(),
                "仓储层列表带全量行；掩码口径由 web 层负责");

        DataSourceDef found = registry.find("ds-a");
        assertNotNull(found);
        assertEquals("sa-secret", found.getPassword(), "find 带密码（只供引擎解析）");

        assertTrue(registry.delete("ds-a"));
        assertEquals(0, registry.list().size());
    }

    @Test
    @DisplayName("更新：password 留空 = 保留旧密码；显式给值 = 换密码")
    void updateKeepsPasswordOnBlank() {
        String h2Url = "jdbc:h2:mem:registry-target-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
        registry.save(def("ds-b", h2Url, "old-pass"), false);

        DataSourceDef edit = def("ds-b", h2Url, "");
        edit.setUrl(h2Url + ";IFEXISTS=FALSE");
        registry.save(edit, true);
        assertEquals("old-pass", registry.find("ds-b").getPassword(), "留空保留旧密码");

        registry.save(def("ds-b", h2Url, "new-pass"), true);
        assertEquals("new-pass", registry.find("ds-b").getPassword(), "显式给值换密码");
    }

    @Test
    @DisplayName("测试连接：H2 真握手成功；坏 url 报错摘要")
    void testConnection() {
        String h2Url = "jdbc:h2:mem:registry-target-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
        assertNull(JdbcDataSourceRegistry.testConnection(def("ds-ok", h2Url, ""), 5), "H2 握手成功");

        String error = JdbcDataSourceRegistry.testConnection(def("ds-bad", "jdbc:h2:mem:x;NON_EXISTENT_SETTING=1", ""), 5);
        assertNotNull(error, "坏连接给错误摘要");
        assertTrue(!error.isEmpty());
    }
}
