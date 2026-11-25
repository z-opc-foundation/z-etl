package com.zifang.z.etl.web;

import com.zifang.z.etl.core.pipeline.OperatorType;
import com.zifang.z.etl.web.api.EtlOperatorController;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 算子目录 REST 面：枚举全覆盖 + 每条带中文说明 + 三类算子族齐备。
 *
 * @author zifang
 */
class EtlOperatorControllerTest {

    @Test
    @DisplayName("算子目录：枚举 51 个全覆盖，每条有 id/kind/desc")
    void catalogCoversAllOperatorTypes() {
        Map<String, Object> body = new EtlOperatorController().list();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) body.get("operators");
        assertEquals(OperatorType.values().length, operators.size());
        for (int i = 0; i < operators.size(); i++) {
            Map<String, Object> item = operators.get(i);
            assertEquals(OperatorType.values()[i].id(), item.get("id"));
            assertEquals(OperatorType.values()[i].kind().name(), item.get("kind"));
            assertNotNull(item.get("desc"), item.get("id") + " 缺说明");
            assertTrue(String.valueOf(item.get("desc")).length() >= 4, item.get("id") + " 说明太短");
        }
    }

    @Test
    @DisplayName("算子族：源 / 变换 / 有状态 / 汇 四类齐备")
    void fourKindsArePresent() {
        Map<String, Object> body = new EtlOperatorController().list();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operators = (List<Map<String, Object>>) body.get("operators");
        assertTrue(countKind(operators, "SOURCE") >= 10, "源算子应不少于 10");
        assertTrue(countKind(operators, "SINK") >= 9, "汇算子应不少于 9");
        assertTrue(countKind(operators, "TRANSFORM") >= 20, "变换算子应不少于 20");
        assertTrue(countKind(operators, "STATEFUL") >= 7, "有状态算子应不少于 7");
    }

    private static int countKind(List<Map<String, Object>> operators, String kind) {
        int n = 0;
        for (Map<String, Object> item : operators) {
            if (kind.equals(item.get("kind"))) {
                n++;
            }
        }
        return n;
    }
}
