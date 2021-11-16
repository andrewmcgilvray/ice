/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.processor;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.processor.CostAndUsageReport.Column;
import com.netflix.ice.tag.Region;

public class LineItem {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final DateTimeFormatter amazonBillingDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter amazonBillingDateFormat2 = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss").withZone(DateTimeZone.UTC);
    public static final DateTimeFormatter amazonBillingDateFormatISO = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(DateTimeZone.UTC);

    private int lineNumber = 0;
    
    protected int accountIdIndex;
    protected int payerAccountIdIndex;
    protected int productIndex;
    protected int zoneIndex;
    protected int reservedIndex;
    protected int descriptionIndex;
    protected int usageTypeIndex;
    protected int operationIndex;
    protected int usageQuantityIndex;
    protected int startTimeIndex;
    protected int endTimeIndex;
    protected int rateIndex;
    protected int costIndex;
    protected int resourceIndex;
    
    protected String[] items;
    
    private int lineItemIdIndex;
    private int billTypeIndex;
    private final int resourceTagStartIndex;
    private final String[] resourceTagsHeader;
    private int purchaseOptionIndex;
    private int lineItemTypeIndex;
    private int lineItemNormalizationFactorIndex;
    private int lineItemProductCodeIndex;
    private int productNormalizationSizeFactorIndex; // First appeared in 2017-07
    private int productUsageTypeIndex;
    private int publicOnDemandCostIndex;
    private LineItemType lineItemType;
    private int pricingUnitIndex;
    private int reservationArnIndex;
    private int productRegionIndex;
    private int productServicecodeIndex;
    private int lineItemTaxTypeIndex;
    private int lineItemLegalEntityIndex;
    
    // First appeared in 2018-01 (Net versions added in 2019-01)
    private int reservationAmortizedUpfrontCostForUsageIndex;
    private int reservationAmortizedUpfrontFeeForBillingPeriodIndex;
    private int reservationRecurringFeeForUsageIndex;
    private int reservationUnusedAmortizedUpfrontFeeForBillingPeriodIndex;
    private int reservationUnusedQuantityIndex;
    private int reservationUnusedRecurringFeeIndex;
    private int reservationNumberOfReservationsIndex;
    private int reservationStartTimeIndex;
    private int reservationEndTimeIndex;
    
    // First appeared in 2019-11 with Net versions
    private int savingsPlanAmortizedUpfrontCommitmentForBillingPeriodIndex = -1;
    private int savingsPlanRecurringCommitmentForBillingPeriodIndex = -1;
    private int savingsPlanStartTimeIndex = -1;
    private int savingsPlanEndTimeIndex = -1;
    private int savingsPlanArnIndex = -1;
    private int savingsPlanEffectiveCostIndex = -1;
    private int savingsPlanTotalCommitmentToDateIndex = -1;
    private int savingsPlanUsedCommitmentIndex = -1;
    private int savingsPlanPaymentOptionIndex = -1;
    private int savingsPlanPurchaseTermIndex = -1;
    private int savingsPlanOfferingTypeIndex = -1;
    
    private static Map<String, Double> normalizationFactors = Maps.newHashMap();
    
    {
        normalizationFactors.put("nano", 0.25);
        normalizationFactors.put("micro", 0.5);
        normalizationFactors.put("small", 1.0);
        normalizationFactors.put("medium", 2.0);
        normalizationFactors.put("large", 4.0);
        normalizationFactors.put("xlarge", 8.0);
    }
    
    // For testing
    protected LineItem(String[] items) {
        resourceTagStartIndex = -1;
        resourceTagsHeader = null;
        this.items = items;
    }
            
    public LineItem(boolean useBlended, DateTime costAndUsageNetUnblendedStartDate, CostAndUsageReport report) {
        lineItemIdIndex = report.getColumnIndex("identity",  "LineItemId");
        billTypeIndex = report.getColumnIndex("bill", "BillType");
        payerAccountIdIndex = report.getColumnIndex("bill", "PayerAccountId");
        accountIdIndex = report.getColumnIndex("lineItem", "UsageAccountId");
        productIndex = report.getColumnIndex("product", "ProductName");
        zoneIndex = report.getColumnIndex("lineItem", "AvailabilityZone");
        descriptionIndex = report.getColumnIndex("lineItem", "LineItemDescription");
        usageTypeIndex = report.getColumnIndex("lineItem", "UsageType");
        operationIndex = report.getColumnIndex("lineItem", "Operation");
        usageQuantityIndex = report.getColumnIndex("lineItem", "UsageAmount");
        startTimeIndex = report.getColumnIndex("lineItem", "UsageStartDate");
        endTimeIndex = report.getColumnIndex("lineItem", "UsageEndDate");
        if (costAndUsageNetUnblendedStartDate != null && !report.getStartTime().isBefore(costAndUsageNetUnblendedStartDate) && report.getColumnIndex("lineItem", "NetUnblendedRate") > 0) {            
            rateIndex = report.getColumnIndex("lineItem", "NetUnblendedRate");
            costIndex = report.getColumnIndex("lineItem", "NetUnblendedCost");
            reservationAmortizedUpfrontCostForUsageIndex = report.getColumnIndex("reservation", "NetAmortizedUpfrontCostForUsage");
            reservationAmortizedUpfrontFeeForBillingPeriodIndex = report.getColumnIndex("reservation", "NetAmortizedUpfrontFeeForBillingPeriod");
            reservationRecurringFeeForUsageIndex = report.getColumnIndex("reservation", "NetRecurringFeeForUsage");
            reservationUnusedAmortizedUpfrontFeeForBillingPeriodIndex = report.getColumnIndex("reservation", "NetUnusedAmortizedUpfrontFeeForBillingPeriod");
            reservationUnusedRecurringFeeIndex = report.getColumnIndex("reservation", "NetUnusedRecurringFee");
        }
        else {
            rateIndex = report.getColumnIndex("lineItem", useBlended ? "BlendedRate" : "UnblendedRate");
            costIndex = report.getColumnIndex("lineItem", useBlended ? "BlendedCost" : "UnblendedCost");
            reservationAmortizedUpfrontCostForUsageIndex = report.getColumnIndex("reservation", "AmortizedUpfrontCostForUsage");
            reservationAmortizedUpfrontFeeForBillingPeriodIndex = report.getColumnIndex("reservation", "AmortizedUpfrontFeeForBillingPeriod");
            reservationRecurringFeeForUsageIndex = report.getColumnIndex("reservation", "RecurringFeeForUsage");
            reservationUnusedAmortizedUpfrontFeeForBillingPeriodIndex = report.getColumnIndex("reservation", "UnusedAmortizedUpfrontFeeForBillingPeriod");
            reservationUnusedRecurringFeeIndex = report.getColumnIndex("reservation", "UnusedRecurringFee");
        }
        reservationUnusedQuantityIndex = report.getColumnIndex("reservation", "UnusedQuantity");
        reservationNumberOfReservationsIndex = report.getColumnIndex("reservation", "NumberOfReservations");
        reservationStartTimeIndex = report.getColumnIndex("reservation", "StartTime");
        reservationEndTimeIndex = report.getColumnIndex("reservation", "EndTime");
        
        resourceIndex = report.getColumnIndex("lineItem", "ResourceId");
        reservedIndex = report.getColumnIndex("pricing", "term");
        
        resourceTagStartIndex = report.getCategoryStartIndex("resourceTags");
        resourceTagsHeader = report.getCategoryHeader("resourceTags");
        // Call getColumnIndex for each resource column so it's marked as used for the CSV parser
        List<Column> resourceColumns = report.getCategoryColumns("resourceTags");
        for (Column c: resourceColumns)
            report.getColumnIndex(c.category, c.name);
        
        purchaseOptionIndex = report.getColumnIndex("pricing", "PurchaseOption");
        lineItemTypeIndex = report.getColumnIndex("lineItem", "LineItemType");
        lineItemNormalizationFactorIndex = report.getColumnIndex("lineItem", "NormalizationFactor");
        lineItemProductCodeIndex = report.getColumnIndex("lineItem", "ProductCode");
        productNormalizationSizeFactorIndex = report.getColumnIndex("product", "normalizationSizeFactor");
        productUsageTypeIndex = report.getColumnIndex("product",  "usagetype"); 
        
        publicOnDemandCostIndex = report.getColumnIndex("pricing", "publicOnDemandCost");        
        pricingUnitIndex = report.getColumnIndex("pricing", "unit");
        reservationArnIndex = report.getColumnIndex("reservation", "ReservationARN");
        productRegionIndex = report.getColumnIndex("product", "region");
        productServicecodeIndex = report.getColumnIndex("product", "servicecode");
        lineItemTaxTypeIndex = report.getColumnIndex("lineItem", "TaxType");
        lineItemLegalEntityIndex = report.getColumnIndex("lineItem", "LegalEntity");
        
        // SavingsPlan beginning 2019-11
        if (!report.getStartTime().isBefore(new DateTime("2019-11", DateTimeZone.UTC))) {
            savingsPlanArnIndex = report.getColumnIndex("savingsPlan", "SavingsPlanARN");
            savingsPlanStartTimeIndex = report.getColumnIndex("savingsPlan", "StartTime");
            savingsPlanEndTimeIndex = report.getColumnIndex("savingsPlan", "EndTime");
            savingsPlanTotalCommitmentToDateIndex = report.getColumnIndex("savingsPlan", "TotalCommitmentToDate");
            savingsPlanUsedCommitmentIndex = report.getColumnIndex("savingsPlan", "UsedCommitment");
            savingsPlanPaymentOptionIndex = report.getColumnIndex("savingsPlan", "PaymentOption");
            savingsPlanPurchaseTermIndex = report.getColumnIndex("savingsPlan", "PurchaseTerm");
            savingsPlanOfferingTypeIndex = report.getColumnIndex("savingsPlan", "OfferingType");
            
            if (costAndUsageNetUnblendedStartDate != null && !report.getStartTime().isBefore(costAndUsageNetUnblendedStartDate) && report.getColumnIndex("savingsPlan", "NetSavingsPlanEffectiveCost") > 0) {
                savingsPlanAmortizedUpfrontCommitmentForBillingPeriodIndex = report.getColumnIndex("savingsPlan", "NetAmortizedUpfrontCommitmentForBillingPeriod");
                savingsPlanRecurringCommitmentForBillingPeriodIndex = report.getColumnIndex("savingsPlan", "NetRecurringCommitmentForBillingPeriod");
                savingsPlanEffectiveCostIndex = report.getColumnIndex("savingsPlan", "NetSavingsPlanEffectiveCost");
            }
            else {
                savingsPlanAmortizedUpfrontCommitmentForBillingPeriodIndex = report.getColumnIndex("savingsPlan", "AmortizedUpfrontCommitmentForBillingPeriod");
                savingsPlanRecurringCommitmentForBillingPeriodIndex = report.getColumnIndex("savingsPlan", "RecurringCommitmentForBillingPeriod");
                savingsPlanEffectiveCostIndex = report.getColumnIndex("savingsPlan", "SavingsPlanEffectiveCost");
            }
        }
    }
    
    public String toString() {
        String[] values = new String[]{
            billTypeIndex < 0 ? "" : items[billTypeIndex],
            accountIdIndex < 0 ? "" : items[accountIdIndex],
            lineItemTypeIndex < 0 ? "" : items[lineItemTypeIndex],
            productIndex < 0 ? "" : items[productIndex],
            operationIndex < 0 ? "" : items[operationIndex],
            usageTypeIndex < 0 ? "" : items[usageTypeIndex],
            descriptionIndex < 0 ? "" : items[descriptionIndex],
            usageQuantityIndex < 0 ? "" : items[usageQuantityIndex],
            startTimeIndex < 0 ? "" : items[startTimeIndex],
            endTimeIndex < 0 ? "" : items[endTimeIndex],
            rateIndex < 0 ? "" : items[rateIndex],
            costIndex < 0 ? "" : items[costIndex],
            zoneIndex < 0 ? "" : items[zoneIndex],
            resourceIndex < 0 ? "" : items[resourceIndex],
            reservedIndex < 0 ? "" : items[reservedIndex],
            purchaseOptionIndex < 0 ? "" : items[purchaseOptionIndex],
            lineItemTypeIndex < 0 ? "" : items[lineItemTypeIndex],
            lineItemNormalizationFactorIndex < 0 ? "" : items[lineItemNormalizationFactorIndex],
            productUsageTypeIndex < 0 ? "" : items[productUsageTypeIndex],
            publicOnDemandCostIndex < 0 ? "" : items[publicOnDemandCostIndex],
            pricingUnitIndex < 0 ? "" : items[pricingUnitIndex],
            reservationArnIndex < 0 ? "" : items[reservationArnIndex],
        };
        return StringUtils.join(values, ",");
    }
    
    public String[] getItems() {
        return items;
    }
    
    public void setItems(String[] items) {
        this.items = items;
        lineNumber++;
        lineItemType = null;
        try {
            lineItemType = LineItemType.valueOf(items[lineItemTypeIndex]);
        } catch (Exception e) {
            logger.error("Unknown lineItemType: " + items[lineItemTypeIndex] + ", " + toString());
            lineItemType = null;
        }
    }
    
    public int size() {
        return resourceTagStartIndex + resourceTagsHeader.length;
    }
     
    public int getLineNumber() {
        return lineNumber;
    }
    
    public String getAccountId() {
        return items[accountIdIndex];
    }
    
    public String getPayerAccountId() {
        return items[payerAccountIdIndex];
    }
    
    public String getProduct() {
        return items[productIndex];
    }
    
    public String getZone() {
        return items[zoneIndex];
    }
    
    public String getReserved() {
        return items[reservedIndex];
    }
    
    public String getDescription() {
        return items[descriptionIndex];
    }
    
    public String getOperation() {
        return items[operationIndex];
    }
    
    public String getStartTime() {
        return items[startTimeIndex];
    }
    
    public String getEndTime() {
        return items[endTimeIndex];
    }
    
    public String getRate() {
        return items[rateIndex];
    }
    
    public String getResource() {
        return resourceIndex >= 0 ? items[resourceIndex] : null;
    }
    
    public void setResource(String resourceId) {
        items[resourceIndex] = resourceId;
    }
    
    public boolean hasResources() {
        return resourceIndex >= 0 && items.length > resourceIndex;
    }

    public int getBillTypeIndex() {
        return billTypeIndex;
    }

    public int getAccountIdIndex() {
        return accountIdIndex;
    }

    public int getPayerAccountIdIndex() {
        return payerAccountIdIndex;
    }

    public int getProductIndex() {
        return productIndex;
    }

    public int getZoneIndex() {
        return zoneIndex;
    }

    public int getReservedIndex() {
        return reservedIndex;
    }

    public int getDescriptionIndex() {
        return descriptionIndex;
    }

    public int getUsageTypeIndex() {
        return usageTypeIndex;
    }

    public int getOperationIndex() {
        return operationIndex;
    }

    public int getUsageQuantityIndex() {
        return usageQuantityIndex;
    }

    public int getStartTimeIndex() {
        return startTimeIndex;
    }

    public int getEndTimeIndex() {
        return endTimeIndex;
    }

    public int getRateIndex() {
        return rateIndex;
    }

    public int getCostIndex() {
        return costIndex;
    }

    public int getResourceIndex() {
        return resourceIndex;
    }
    
    /**
     * BillType
     */
    public static enum BillType {
        Anniversary,
        Purchase,
        Refund;
    }

    public BillType getBillType() {
        return BillType.valueOf(items[billTypeIndex]);
    }
    
    public String getCost() {
        if (lineItemType == LineItemType.DiscountedUsage) {
            String recurringFee = getRecurringFeeForUsage();
            if (!recurringFee.isEmpty())
                return recurringFee;
        }
        return items[costIndex];
    }

    public long getStartMillis() {
        return amazonBillingDateFormatISO.parseMillis(items[startTimeIndex]);
    }

    public long getEndMillis() {
        return amazonBillingDateFormatISO.parseMillis(items[endTimeIndex]);
    }
    
    public String getUsageType() {
        String purchaseOption = getPurchaseOption();
        String usageType = null;
        if ((lineItemType == LineItemType.RIFee || lineItemType == LineItemType.DiscountedUsage) && (purchaseOption.isEmpty() || !purchaseOption.equals("All Upfront")))
            usageType = items[productUsageTypeIndex];
        if (usageType == null || usageType.isEmpty())
            usageType = items[usageTypeIndex];
        
        return usageType;
    }
    
    public String[] getResourceTagsHeader() {
        String[] header = new String[resourceTagsHeader.length];
        for (int i = 0; i < resourceTagsHeader.length; i++)
            header[i] = resourceTagsHeader[i].substring("resourceTags/".length());
            
        return header;
    }

    public int getResourceTagsSize() {
        if (items.length - resourceTagStartIndex <= 0)
            return 0;
        return items.length - resourceTagStartIndex;
    }

    public String getResourceTag(int index) {
        if (items.length <= resourceTagStartIndex + index)
            return "";
        return items[resourceTagStartIndex + index];
    }

    public Map<String, String>  getResourceTags() {
        Map<String, String> tags = Maps.newHashMap();
        for (int i = 0; i < resourceTagsHeader.length && i+resourceTagStartIndex < items.length; i++) {
            if (items[i+resourceTagStartIndex].isEmpty()) {
                continue;
            }
            String tag = resourceTagsHeader[i].substring("resourceTags/".length());
            if (tag.startsWith("user:"))
                tag = tag.substring("user:".length());
            tags.put(tag, items[i+resourceTagStartIndex]);
        }
        return tags;
    }
    
    public boolean isReserved() {
        if (reservedIndex > items.length) {
            logger.error("Line item record too short. Reserved index = " + reservedIndex + ", record length = " + items.length);
            return false;
        }
        switch (lineItemType) {
        case Tax:
            return false;
            
        case DiscountedUsage:
        case RIFee:
            return true;
            
        default:
            break;
        }
        return items[reservedIndex].toLowerCase().equals("reserved") || 
                items[usageTypeIndex].contains("HeavyUsage") ||
                !getReservationArn().isEmpty();
    }

    public String getPurchaseOption() {
        return purchaseOptionIndex >= 0 ? items[purchaseOptionIndex] : "";
    }

    public LineItemType getLineItemType() {
        return lineItemType;
    }
    
    public static double computeProductNormalizedSizeFactor(String usageType) {
        String ut = usageType;
        if (ut.contains(":"))
            ut = ut.split(":")[1];
        
        if (ut.startsWith("db."))
            ut = ut.substring("db.".length());
        
        String[] usageParts = ut.split("\\.");
        if (usageParts.length < 2)
            return 1.0;
        String size = usageParts[1];
        
        if (size.endsWith("xlarge") && size.length() > "xlarge".length())
            return Double.parseDouble(size.substring(0, size.lastIndexOf("xlarge"))) * 8;
        
        Double factor = normalizationFactors.get(size);
        return factor == null ? 1.0 : factor;
    }
    
    public String getUsageQuantity() {
        String purchaseOption = getPurchaseOption();
        if (purchaseOption.isEmpty() || purchaseOption.equals("All Upfront")) {
            return items[usageQuantityIndex];
        }

        if (lineItemType == LineItemType.DiscountedUsage) {
            double usageAmount = Double.parseDouble(items[usageQuantityIndex]);
            String linf = items[lineItemNormalizationFactorIndex];
            double normFactor = (linf.isEmpty() || linf.equals("NA")) ? computeProductNormalizedSizeFactor(items[usageTypeIndex]) : Double.parseDouble(linf);
            String pnsf = items[productNormalizationSizeFactorIndex];
            double productFactor = (pnsf.isEmpty() || pnsf.equals("NA")) ? computeProductNormalizedSizeFactor(items[productUsageTypeIndex]) : Double.parseDouble(pnsf);
            Double actualUsage = usageAmount * normFactor / productFactor;            
            return actualUsage.toString();
        }
        return items[usageQuantityIndex];
    }
    
    public int getPublicOnDemandCostIndex() {
        return publicOnDemandCostIndex;
    }
    
    public String getPublicOnDemandCost() {
        return publicOnDemandCostIndex >= 0 ? items[publicOnDemandCostIndex] : "";
    }
    
    public int getLineItemTypeIndex() {
        return lineItemTypeIndex;
    }

    public int getResourceTagStartIndex() {
        return resourceTagStartIndex;
    }

    public int getPurchaseOptionIndex() {
        return purchaseOptionIndex;
    }

    public int getLineItemNormalizationFactorIndex() {
        return lineItemNormalizationFactorIndex;
    }

    public String getLineItemNormalizationFactor() {
        return lineItemNormalizationFactorIndex >= 0 ? items[lineItemNormalizationFactorIndex] : "";
    }

    public int getProductNormalizationSizeFactorIndex() {
        return productNormalizationSizeFactorIndex;
    }

    public int getProductUsageTypeIndex() {
        return productUsageTypeIndex;
    }
    
    public int getReservationArnIndex() {
        return reservationArnIndex;
    }
    
    public int getProductRegionIndex() {
        return productRegionIndex;
    }
    
    public Region getProductRegion() {
        String region = productRegionIndex >= 0 ? items[productRegionIndex] : "";
        return region.isEmpty() ? Region.US_EAST_1 : Region.getRegionByName(region);
    }

    public String getPricingUnit() {
        String unit = pricingUnitIndex >= 0 ? items[pricingUnitIndex] : "";
        if (unit.equals("Hrs")) {
            unit = "hours";
        }
        else if (unit.equals("GB-Mo")) {
            unit = "GB";
        }
        else {
            // Don't bother with any other units as AWS is extremely inconsistent
            String usageType = getUsageType();
            if (usageType.startsWith("AW-HW-1")) {
                // AmazonWorkSpaces
            }
            else if (usageType.contains("Lambda-GB-Second") || usageType.contains("Bytes") || usageType.contains("ByteHrs") || getDescription().contains("GB")) {
                unit = "GB";
            }            
            // Won't indicate "hours" for instance usage, so clients must handle that themselves.
        }
        return unit;
    }
    
    public String getLineItemId() {
        return lineItemIdIndex >= 0 ? items[lineItemIdIndex] : "";
    }
    
    public String getReservationArn() {
        return reservationArnIndex >= 0 ? items[reservationArnIndex] : "";
    }

    public int getAmortizedUpfrontCostForUsageIndex() {
        return reservationAmortizedUpfrontCostForUsageIndex;
    }
    
    public String getAmortizedUpfrontCostForUsage() {
        return reservationAmortizedUpfrontCostForUsageIndex >= 0 ? items[reservationAmortizedUpfrontCostForUsageIndex] : "";
    }
    
    public boolean hasAmortizedUpfrontCostForUsage() {
        return reservationAmortizedUpfrontCostForUsageIndex >= 0;
    }
    
    public int getAmortizedUpfrontFeeForBillingPeriodIndex() {
        return reservationAmortizedUpfrontFeeForBillingPeriodIndex;
    };

    public String getAmortizedUpfrontFeeForBillingPeriod() {
        return reservationAmortizedUpfrontFeeForBillingPeriodIndex >= 0 ? items[reservationAmortizedUpfrontFeeForBillingPeriodIndex] : "";
    }

    public boolean hasAmortizedUpfrontFeeForBillingPeriod() {
        return reservationAmortizedUpfrontFeeForBillingPeriodIndex >= 0;
    }
    
    public int getRecurringFeeForUsageIndex() {
        return reservationRecurringFeeForUsageIndex;
    }
    
    public String getRecurringFeeForUsage() {
        return reservationRecurringFeeForUsageIndex >= 0 ? items[reservationRecurringFeeForUsageIndex] : "";
    }
    
    public int getUnusedAmortizedUpfrontFeeForBillingPeriodIndex() {
        return reservationUnusedAmortizedUpfrontFeeForBillingPeriodIndex;
    }
    
    public int getUnusedQuantityIndex() {
        return reservationUnusedQuantityIndex;
    }
    
    public int getUnusedRecurringFeeIndex() {
        return reservationUnusedRecurringFeeIndex;
    }

    public Double getUnusedAmortizedUpfrontRate() {
        // Return the amortization rate for an unused RI
        if (reservationUnusedAmortizedUpfrontFeeForBillingPeriodIndex >= 0 &&
                reservationUnusedQuantityIndex >= 0) {
            Double amort = Double.parseDouble(items[reservationUnusedAmortizedUpfrontFeeForBillingPeriodIndex]);
            Double quantity = Double.parseDouble(items[reservationUnusedQuantityIndex]);
            if (amort != null && quantity != null)
                return quantity == 0.0 ? 0.0 : amort / quantity;
        }
        return null;
    }
    
    public Double getUnusedRecurringRate() {
        // Return the recurring rate for an unused RI
        if (reservationUnusedRecurringFeeIndex >= 0 &&
                reservationUnusedQuantityIndex >= 0) {
            Double fee = Double.parseDouble(items[reservationUnusedRecurringFeeIndex]);
            Double quantity = Double.parseDouble(items[reservationUnusedQuantityIndex]);
            if (fee != null && quantity != null)
                return quantity == 0.0 ? 0.0 : fee / quantity;
        }
        return null;
    }
    
    public int getLineItemProductCodeIndex() {
        return lineItemProductCodeIndex;
    }
    
    public String getProductServiceCode() {
        // Favor the lineItem/ProductCode over product/ServiceCode because Lambda for example has AWSDataTransfer as the serviceCode
        if (lineItemProductCodeIndex >= 0 && !items[lineItemProductCodeIndex].isEmpty())
            return items[lineItemProductCodeIndex];
        
        if (productServicecodeIndex >= 0 && !items[productServicecodeIndex].isEmpty())
            return items[productServicecodeIndex];
        
        return "";
    }
    
    public int getTaxTypeIndex() {
        return lineItemTaxTypeIndex;
    }
    
    public String getTaxType() {
        return lineItemTaxTypeIndex >= 0 ? items[lineItemTaxTypeIndex] : "";
    }

    public int getLegalEntityIndex() {
        return lineItemLegalEntityIndex;
    }
    
    public String getLegalEntity() {
        return lineItemLegalEntityIndex >= 0 ? items[lineItemLegalEntityIndex] : "";        
    }

    public int getReservationStartTimeIndex() {
        return reservationStartTimeIndex;
    }

    public String getReservationStartTime() {
        return reservationStartTimeIndex >= 0 ? items[reservationStartTimeIndex] : null;
    }
    
    public int getReservationEndTimeIndex() {
        return reservationEndTimeIndex;
    }

    public String getReservationEndTime() {
        return reservationEndTimeIndex >= 0 ? items[reservationEndTimeIndex] : null;
    }
    
    public int getReservationNumberOfReservationsIndex() {
        return reservationNumberOfReservationsIndex;
    }

    public String getReservationNumberOfReservations() {
        return reservationNumberOfReservationsIndex >= 0 ? items[reservationNumberOfReservationsIndex] : null;
    }

    /**
     * Savings Plans began 2019-11
     */
    public int getSavingsPlanAmortizedUpfrontCommitmentForBillingPeriodIndex() {
        return savingsPlanAmortizedUpfrontCommitmentForBillingPeriodIndex;
    }
    public int getSavingsPlanRecurringCommitmentForBillingPeriodIndex() {
        return savingsPlanRecurringCommitmentForBillingPeriodIndex;
    }
    public int getSavingsPlanStartTimeIndex() {
        return savingsPlanStartTimeIndex;
    }
    public int getSavingsPlanEndTimeIndex() {
        return savingsPlanEndTimeIndex;
    }
    public int getSavingsPlanArnIndex() {
        return savingsPlanArnIndex;
    }
    public int getSavingsPlanEffectiveCostIndex() {
        return savingsPlanEffectiveCostIndex;
    }
    public int getSavingsPlanTotalCommitmentToDateIndex() {
        return savingsPlanTotalCommitmentToDateIndex;
    }
    public int getSavingsPlanUsedCommitmentIndex() {
        return savingsPlanUsedCommitmentIndex;
    }
    public int getSavingsPlanPaymentOptionIndex() {
        return savingsPlanPaymentOptionIndex;
    }
    
    public String getSavingsPlanAmortizedUpfrontCommitmentForBillingPeriod() {
        return savingsPlanAmortizedUpfrontCommitmentForBillingPeriodIndex >= 0 ? items[savingsPlanAmortizedUpfrontCommitmentForBillingPeriodIndex] : null;
    }
    
    public String getSavingsPlanRecurringCommitmentForBillingPeriod() {
        return savingsPlanRecurringCommitmentForBillingPeriodIndex >= 0 ? items[savingsPlanRecurringCommitmentForBillingPeriodIndex] : null;
    }

    public String getSavingsPlanStartTime() {
        return savingsPlanStartTimeIndex >= 0 ? items[savingsPlanStartTimeIndex] : null;
    }

    public String getSavingsPlanEndTime() {
        return savingsPlanEndTimeIndex >= 0 ? items[savingsPlanEndTimeIndex] : null;
    }

    public String getSavingsPlanArn() {
        return savingsPlanArnIndex >= 0 ? items[savingsPlanArnIndex] : null;
    }

    public String getSavingsPlanEffectiveCost() {
        return savingsPlanEffectiveCostIndex >= 0 ? items[savingsPlanEffectiveCostIndex] : null;
    }

    public Double getSavingsPlanNormalizedUsage() {
        if (savingsPlanTotalCommitmentToDateIndex >= 0 && savingsPlanUsedCommitmentIndex >= 0 &&
                !items[savingsPlanUsedCommitmentIndex].isEmpty() && !items[savingsPlanTotalCommitmentToDateIndex].isEmpty() &&
                !items[savingsPlanTotalCommitmentToDateIndex].equals("0")) {
            return Double.parseDouble(items[savingsPlanUsedCommitmentIndex]) / Double.parseDouble(items[savingsPlanTotalCommitmentToDateIndex]);
        }
        return null;
    }
    
    public String getSavingsPlanPaymentOption() {
        return savingsPlanPaymentOptionIndex >= 0 ? items[savingsPlanPaymentOptionIndex] : null;
    }

    public String getSavingsPlanPurchaseTerm() {
        return savingsPlanPurchaseTermIndex >= 0 ? items[savingsPlanPurchaseTermIndex] : null;
    }
    
    public String getSavingsPlanOfferingType() {
        return savingsPlanOfferingTypeIndex >= 0 ? items[savingsPlanOfferingTypeIndex] : null;
    }
    
}

