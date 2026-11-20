package com.zifang.z.etl.core.spi;

import com.zifang.z.etl.core.pipeline.PipelineSpec;

/**
 * 管道解析 SPI：{@code pipeline.ref} 节点按名引用别的管道时，由它负责「名字 → 定义」。
 *
 * <p>引擎层用落库管道清单实现（{@code repository.find}）；裸执行器没配它 ⇒
 * 引用节点打开时就报错说明，不静默跑空。
 *
 * @author zifang
 */
public interface PipelineRefResolver {

    /**
     * 按名取管道定义。
     *
     * @param name 管道名
     * @return 定义；不存在返回 {@code null}
     */
    PipelineSpec resolve(String name);
}
