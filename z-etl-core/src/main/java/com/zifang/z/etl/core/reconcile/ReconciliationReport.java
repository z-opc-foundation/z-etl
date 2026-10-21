package com.zifang.z.etl.core.reconcile;

/**
 * 对账报告（搬运完自动出源/目标双口径结果）。
 *
 * <p>双口径 = **行数** + **校验和**。行数相等只说明「条数对」，校验和相等才说明「键的多重集对」
 * （能同时抓住漏行与重行）。
 *
 * @author zifang
 */
public class ReconciliationReport {

    private long sourceCount;
    private long targetCount;
    private String sourceChecksum;
    private String targetChecksum;
    private boolean countMatched;
    private boolean checksumMatched;

    public long getSourceCount() {
        return sourceCount;
    }

    public void setSourceCount(long sourceCount) {
        this.sourceCount = sourceCount;
    }

    public long getTargetCount() {
        return targetCount;
    }

    public void setTargetCount(long targetCount) {
        this.targetCount = targetCount;
    }

    public String getSourceChecksum() {
        return sourceChecksum;
    }

    public void setSourceChecksum(String sourceChecksum) {
        this.sourceChecksum = sourceChecksum;
    }

    public String getTargetChecksum() {
        return targetChecksum;
    }

    public void setTargetChecksum(String targetChecksum) {
        this.targetChecksum = targetChecksum;
    }

    public boolean isCountMatched() {
        return countMatched;
    }

    public void setCountMatched(boolean countMatched) {
        this.countMatched = countMatched;
    }

    public boolean isChecksumMatched() {
        return checksumMatched;
    }

    public void setChecksumMatched(boolean checksumMatched) {
        this.checksumMatched = checksumMatched;
    }

    /** 双口径全过 = 对账一致。 */
    public boolean matched() {
        return countMatched && checksumMatched;
    }

    @Override
    public String toString() {
        return "对账: 源 " + sourceCount + " 行 / 目标 " + targetCount + " 行"
                + "，校验和 " + sourceChecksum + " vs " + targetChecksum
                + "，行数" + (countMatched ? "一致" : "不一致")
                + "，校验和" + (checksumMatched ? "一致" : "不一致");
    }
}
