package com.ecommerce.etl.model;

import java.io.Serializable;

/**
 * POJO representing a row in the sales_summary table.
 */
public class SalesSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    private String summaryType;     // by_product | by_category | by_customer | by_month
    private String dimensionKey;    // The group-by key value
    private String dimensionLabel;  // Human-readable label
    private long   totalQuantitySold;
    private double totalRevenue;
    private long   totalOrders;
    private double avgOrderValue;

    // ── Constructors ────────────────────────────────────────────

    public SalesSummary() {}

    public SalesSummary(String summaryType, String dimensionKey, String dimensionLabel,
                        long totalQuantitySold, double totalRevenue, long totalOrders,
                        double avgOrderValue) {
        this.summaryType       = summaryType;
        this.dimensionKey      = dimensionKey;
        this.dimensionLabel    = dimensionLabel;
        this.totalQuantitySold = totalQuantitySold;
        this.totalRevenue      = totalRevenue;
        this.totalOrders       = totalOrders;
        this.avgOrderValue     = avgOrderValue;
    }

    // ── Getters & Setters ───────────────────────────────────────

    public String getSummaryType()            { return summaryType; }
    public void setSummaryType(String v)      { this.summaryType = v; }

    public String getDimensionKey()           { return dimensionKey; }
    public void setDimensionKey(String v)     { this.dimensionKey = v; }

    public String getDimensionLabel()         { return dimensionLabel; }
    public void setDimensionLabel(String v)   { this.dimensionLabel = v; }

    public long getTotalQuantitySold()        { return totalQuantitySold; }
    public void setTotalQuantitySold(long v)  { this.totalQuantitySold = v; }

    public double getTotalRevenue()           { return totalRevenue; }
    public void setTotalRevenue(double v)     { this.totalRevenue = v; }

    public long getTotalOrders()              { return totalOrders; }
    public void setTotalOrders(long v)        { this.totalOrders = v; }

    public double getAvgOrderValue()          { return avgOrderValue; }
    public void setAvgOrderValue(double v)    { this.avgOrderValue = v; }

    @Override
    public String toString() {
        return String.format(
            "SalesSummary[type=%s, key=%s, label=%s, qty=%d, revenue=%.2f, orders=%d, aov=%.2f]",
            summaryType, dimensionKey, dimensionLabel, totalQuantitySold,
            totalRevenue, totalOrders, avgOrderValue
        );
    }
}

