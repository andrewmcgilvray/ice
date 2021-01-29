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

import java.util.Map;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.netflix.ice.tag.Region;

public abstract class LineItem {
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
    
	public String[] getItems() {
		return items;
	}
	
    public void setItems(String[] items) {
    	this.items = items;
    	lineNumber++;
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
    
    public String getUsageType() {
    	return items[usageTypeIndex];
    }
    
    public String getOperation() {
    	return items[operationIndex];
    }
    
    public String getUsageQuantity() {
    	return items[usageQuantityIndex];
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
    
    public String getCost() {
    	return items[costIndex];
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

    public long getStartMillis() {
        try {
            return amazonBillingDateFormat.parseMillis(items[startTimeIndex]);
        }
        catch (IllegalArgumentException e) {
            return amazonBillingDateFormat2.parseMillis(items[startTimeIndex]);
        }
    }

    public long getEndMillis() {
        try {
            return amazonBillingDateFormat.parseMillis(items[endTimeIndex]);
        }
        catch (IllegalArgumentException e) {
            return amazonBillingDateFormat2.parseMillis(items[endTimeIndex]);
        }
    }
    
    abstract public String[] getResourceTagsHeader();
    
    abstract public int getResourceTagsSize();
    
    abstract public String getResourceTag(int index);
    
    abstract public Map<String, String> getResourceTags();
    
    abstract public boolean isReserved();

	public String getPurchaseOption() {
		return null;
	}

	public String getPublicOnDemandCost() {
		return "";
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

	/**
	 * LineItemType is one of the columns from AWS Cost and Usage reports
	 */
	public static enum LineItemType {
		Credit,
		DiscountedUsage,
		EdpDiscount,
		Fee,
		Refund,
		RIFee,
		RiVolumeDiscount,
		Tax,
		Usage,
		SavingsPlanUpfrontFee,
		SavingsPlanRecurringFee,
		SavingsPlanCoveredUsage,
		SavingsPlanNegation,
		PrivateRateDiscount;
	}
	
	public LineItemType getLineItemType() {
		return null;
	}
	
	public String getPricingUnit() {
    	String usageType = getUsageType();
		String unit = "";
		if (usageType.startsWith("AW-HW-1")) {
			// AmazonWorkSpaces
		}
		else if (usageType.contains("Lambda-GB-Second") || usageType.contains("Bytes") || usageType.contains("ByteHrs") || getDescription().contains("GB")) {
            unit = "GB";
    	}
        
        // Won't indicate "hours" for instance usage, so clients must handle that themselves.
        return unit;
	}

	abstract public String getLineItemId();

	public String getReservationArn() {
		return "";
	}
	
	/**
	 * reservation/AmortizedUpfrontCostForUsage appeared 2018-01 in CUR files
	 * reservation/NetAmortizedUpfrontCostForUsage appeared 2019-01 in CUR files
	 * @return
	 */
	public String getAmortizedUpfrontCostForUsage() {
		return "";
	}
	
	public boolean hasAmortizedUpfrontCostForUsage() {
		return false;
	}
	
	/**
	 * reservation/AmortizedUpfrontFeeForBillingPeriod appeared 2018-01 in CUR files
	 * reservation/NetAmortizedUpfrontFeeForBillingPeriod appeared 2019-01 in CUR files
	 * @return
	 */
	public String getAmortizedUpfrontFeeForBillingPeriod() {
		return "";
	}
	
	public boolean hasAmortizedUpfrontFeeForBillingPeriod() {
		return false;
	}
	
	/**
	 * reservation/RecurringFeeForUsage appeared 2018-01 in CUR files
	 * reservation/NetRecurringFeeForUsage appeared 2019-01 in CUR files
	 * @return recurring fee
	 */
	public String getRecurringFeeForUsage() {
		return "";
	}
	
	public Double getUnusedAmortizedUpfrontRate() {
		return null;
	}
	
	public Double getUnusedRecurringRate() {
		return null;
	}

	public Region getProductRegion() {
		return null;
	}
	
	public String getProductServiceCode() {
		return null;
	}
	
	public String getTaxType() {
		return null;
	}

	public String getLegalEntity() {
		return null;
	}

	public String getReservationStartTime() {
		return null;
	}
	
	public String getReservationEndTime() {
		return null;
	}
	
	public String getReservationNumberOfReservations() {
		return null;
	}
	
	public String getLineItemNormalizationFactor() {
		return null;
	}
	
	/**
	 * Savings Plans began 2019-11
	 */
	public String getSavingsPlanAmortizedUpfrontCommitmentForBillingPeriod() {
		return null;
	}
	
	public String getSavingsPlanRecurringCommitmentForBillingPeriod() {
		return null;
	}

	public String getSavingsPlanStartTime() {
		return null;
	}

	public String getSavingsPlanEndTime() {
		return null;
	}

	public String getSavingsPlanArn() {
		return null;
	}

	public String getSavingsPlanEffectiveCost() {
		return null;
	}

	public Double getSavingsPlanNormalizedUsage() {
		return null;
	}
	
	public String getSavingsPlanPaymentOption() {
		return null;
	}
	
	public String getSavingsPlanPurchaseTerm() {
		return null;
	}
	
	public String getSavingsPlanOfferingType() {
		return null;
	}
	
	public BillType getBillType() {
		return null;
	}
}
