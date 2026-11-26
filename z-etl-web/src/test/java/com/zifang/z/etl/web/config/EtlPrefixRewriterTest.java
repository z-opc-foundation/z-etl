package com.zifang.z.etl.web.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * /etl 前缀改写规则：控制台与 API 的 /etl/** 一律裸化，近似前缀不误伤。
 *
 * @author zifang
 */
class EtlPrefixRewriterTest {

    @Test
    @DisplayName("控制台静态页 /etl/index.html → /index.html")
    void consolePageRewrite() {
        assertEquals("/index.html", EtlPrefixRewriter.rewrittenUri("/etl/index.html"));
    }

    @Test
    @DisplayName("API /etl/api/etl/operators → /api/etl/operators")
    void apiRewrite() {
        assertEquals("/api/etl/operators", EtlPrefixRewriter.rewrittenUri("/etl/api/etl/operators"));
    }

    @Test
    @DisplayName("根别名 /etl 与 /etl/ 不改写（走 302 重定向到 /etl/index.html）")
    void rootAliasRedirectsNotRewritten() {
        assertNull(EtlPrefixRewriter.rewrittenUri("/etl"));
        assertNull(EtlPrefixRewriter.rewrittenUri("/etl/"));
    }

    @Test
    @DisplayName("裸路径原样放行（本地 aio 剥前缀形态）")
    void barePathPassThrough() {
        assertNull(EtlPrefixRewriter.rewrittenUri("/index.html"));
        assertNull(EtlPrefixRewriter.rewrittenUri("/api/etl/operators"));
    }

    @Test
    @DisplayName("近似前缀 /etlx /etl2 不误伤")
    void similarPrefixNotTouched() {
        assertNull(EtlPrefixRewriter.rewrittenUri("/etlx/index.html"));
        assertNull(EtlPrefixRewriter.rewrittenUri("/etl2/api/etl/operators"));
    }
}