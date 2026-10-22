package com.zifang.z.etl.core.engine;

/**
 * 源侧水位统计（越界 / 回拨检测的输入）。
 *
 * <p>三个数：
 * <ul>
 *   <li>{@code total}：源表行数（按作业过滤口径）。</li>
 *   <li>{@code maxValue}：源表当前最大水位值（null = 源表空）。</li>
 *   <li>{@code staleRows}：水位值**早于**已存游标的行数 —— 时钟回拨 / 迟到数据的滞留量。</li>
 * </ul>
 *
 * @author zifang
 */
public class SourceWatermarkStats {

    private long total;
    private String maxValue;
    private long staleRows;

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public String getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }

    public long getStaleRows() {
        return staleRows;
    }

    public void setStaleRows(long staleRows) {
        this.staleRows = staleRows;
    }

    /**
     * 游标是否越界（游标跑到了源数据前面：源表被清 / 被重建 / 水位被手工改大）。
     *
     * @return true = 越界
     */
    public boolean isOutOfRange() {
        return total <= 0L || maxValue == null || maxValue.trim().isEmpty();
    }

    @Override
    public String toString() {
        return "源侧水位统计：行数 " + total + "，最大水位 " + maxValue + "，滞留行 " + staleRows;
    }
}
