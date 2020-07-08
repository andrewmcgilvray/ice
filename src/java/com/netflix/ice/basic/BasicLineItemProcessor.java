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
package com.netflix.ice.basic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.*;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.processor.*;
import com.netflix.ice.processor.ReservationService.ReservationInfo;
import com.netflix.ice.tag.*;
import com.netflix.ice.tag.Zone.BadZone;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/*
 * All reservation usage starts out tagged as BonusReservedInstances and is later reassigned proper tags
 * based on it's usage by the ReservationProcessor.
 */
public class BasicLineItemProcessor implements LineItemProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    protected AccountService accountService;
    protected ProductService productService;
    protected ReservationService reservationService;

    protected ResourceService resourceService;
    protected final int numUserTags;
    
    public BasicLineItemProcessor(
    		AccountService accountService, 
    		ProductService productService, 
    		ReservationService reservationService, 
    		ResourceService resourceService) {
    	this.accountService = accountService;
    	this.productService = productService;
    	this.reservationService = reservationService;
    	this.resourceService = resourceService;
    	this.numUserTags = resourceService == null ? 0 : resourceService.getCustomTags().size();
    }
    
    protected Product getProduct(LineItem lineItem) {
    	// DBR version
    	return productService.getProductByServiceName(lineItem.getProduct());
    }
    
    protected boolean ignore(String fileName, DateTime reportStart, DateTime reportModTime, String root, Interval usageInterval, LineItem lineItem) {
        if (StringUtils.isEmpty(lineItem.getAccountId()) ||
            StringUtils.isEmpty(lineItem.getProduct()) ||
            StringUtils.isEmpty(lineItem.getCost()))
            return true;

        Account account = accountService.getAccountById(lineItem.getAccountId(), root);
        if (account == null)
            return true;

        Product product = productService.getProduct(lineItem.getProduct(), lineItem.getProductServiceCode());
        
        LineItemType lineItemType = lineItem.getLineItemType();
    	if (lineItemType != LineItemType.Credit && lineItemType != LineItemType.Tax) {
    		// Not a Credit or Tax line item, so must have some non-empty fields
    		if (StringUtils.isEmpty(lineItem.getUsageType()) ||
	            (StringUtils.isEmpty(lineItem.getOperation()) && lineItemType != LineItemType.SavingsPlanRecurringFee && !product.isSupport()) ||
	            StringUtils.isEmpty(lineItem.getUsageQuantity())) {
    	    	
    			return true;
    		}
    	}

    	if (!product.isRegistrar() && lineItem.getLineItemType() != LineItemType.RIFee) {
    		// Registrar product renewals occur before they expire, so often start in the following month.
    		// We handle the out-of-date-range problem later.
    		// All other cases are ignored here.
    		long nextMonthStartMillis = reportStart.plusMonths(1).getMillis();
	        if (usageInterval.getStartMillis() >= nextMonthStartMillis) {
	        	logger.error(fileName + " line item starts in a later month. Line item type = " + lineItemType + ", product = " + lineItem.getProduct() + ", cost = " + lineItem.getCost());
	        	return true;
	        }
	        if (usageInterval.getEndMillis() > nextMonthStartMillis) {
	        	logger.error(fileName + " line item ends in a later month. Line item type = " + lineItemType + ", product = " + lineItem.getProduct() + ", cost = " + lineItem.getCost());
	        	return true;
	        }
    	}
    	
    	return false;
    }
    
    protected String getUsageTypeStr(String usageTypeStr, Product product) {
    	if (product.isCloudFront()) {
    		// Don't strip the edge location from the usage type
    		return usageTypeStr;
    	}
        int index = usageTypeStr.indexOf("-");
        String regionShortName = index > 0 ? usageTypeStr.substring(0, index) : "";
        Region region = regionShortName.isEmpty() ? null : Region.getRegionByShortName(regionShortName);
        return region == null ? usageTypeStr : usageTypeStr.substring(index+1);
    }
    
    protected Region getRegion(LineItem lineItem) {
    	String usageTypeStr = lineItem.getUsageType();
        int index = usageTypeStr.indexOf("-");
        String regionShortName = index > 0 ? usageTypeStr.substring(0, index) : "";
        Region region = regionShortName.isEmpty() ? null : Region.getRegionByShortName(regionShortName);
        return region == null ? Region.US_EAST_1 : region;
    }
    
    protected Zone getZone(String fileName, Region region, LineItem lineItem) {
    	String zoneStr = lineItem.getZone();
    	if (zoneStr.isEmpty() || region.name.equals(zoneStr))
    		return null;
    	
    	if (!zoneStr.startsWith(region.name)) {
			logger.warn(fileName + " LineItem with mismatched regions: Product=" + lineItem.getProduct() + ", UsageType=" + lineItem.getUsageType() + ", AvailabilityZone=" + lineItem.getZone() + ", Region=" + region + ", Description=" + lineItem.getDescription());
    		return null;
    	}
    	
    	Zone zone;
		try {
			zone = region.getZone(zoneStr);
		} catch (BadZone e) {
			logger.error(fileName + " Error getting zone " + lineItem.getZone() + " in region " + region + ": " + e.getMessage() + ", " + lineItem.toString());
			return null;
		}     
		return zone;
    }
    
    protected void addResourceInstance(LineItem lineItem, Instances instances, TagGroup tg) {
        // Add all resources to the instance catalog
        if (instances != null && lineItem.hasResources() && !tg.product.isDataTransfer() && !tg.product.isCloudWatch())
        	instances.add(lineItem.getResource(), lineItem.getStartMillis(), tg.usageType.toString(), lineItem.getResourceTags(), tg.account, tg.region, tg.zone, tg.product);
    }
    
    protected TagGroup getTagGroup(LineItem lineItem, Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup rg) {
        return TagGroup.getTagGroup(account, region, zone, product, operation, usageType, rg);
    }
    
    private Interval getUsageInterval(String fileName, DateTime reportStart, LineItem lineItem) {
        long millisStart = lineItem.getStartMillis();
        long millisEnd = lineItem.getEndMillis();

        Product origProduct = productService.getProduct(lineItem.getProduct(), lineItem.getProductServiceCode());
        if (origProduct.isRegistrar()) {
        	// Put all out-of-month registrar fees at the start of the month
	        long nextMonthStartMillis = reportStart.plusMonths(1).getMillis();
        	if (millisStart > nextMonthStartMillis) {
        		millisStart = reportStart.getMillis();
        	}
        	// Put the whole fee in the first hour
        	millisEnd = new DateTime(millisStart, DateTimeZone.UTC).plusHours(1).getMillis();
        }
        else if (origProduct.isSupport()) {
        	// Put the whole fee in the first hour
        	millisEnd = new DateTime(millisStart, DateTimeZone.UTC).plusHours(1).getMillis();
        	logger.info(fileName + " Support: " + lineItem);
        }
        
        LineItemType lit = lineItem.getLineItemType();
        if (lit != null) {
	        switch (lit) {
	        case Credit:
	        	// Most credits have end times that are one second into the next hour
	        	// Truncate partial seconds end time.
	        	millisEnd = new DateTime(millisEnd, DateTimeZone.UTC).withSecondOfMinute(0).getMillis();
	        	break;
	        case Tax:
	        	break;
	        case RIFee:
	        	break;
	        default:
	        	break;
	        }
        }
        
        return new Interval(millisStart, millisEnd, DateTimeZone.UTC);
    }
    
    public Result process(
    		String fileName,
    		long reportMilli,
    		boolean processDelayed,
    		String root,
    		LineItem lineItem,
    		CostAndUsageData costAndUsageData,
    		Instances instances,
    		double edpDiscount) {
    	
    	final long startMilli = costAndUsageData.getStartMilli();
    	final DateTime reportStart = new DateTime(startMilli, DateTimeZone.UTC);
    	final DateTime reportModTime = new DateTime(reportMilli, DateTimeZone.UTC).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        final Interval usageInterval = getUsageInterval(fileName, reportStart, lineItem);
        
    	if (ignore(fileName, reportStart, reportModTime, root, usageInterval, lineItem))
    		return Result.ignore;
    	
        final Account account = accountService.getAccountById(lineItem.getAccountId(), root);
        final Region region = getRegion(lineItem);
        final Zone zone = getZone(fileName, region, lineItem);
       
        PurchaseOption defaultReservationPurchaseOption = reservationService.getDefaultPurchaseOption(usageInterval.getStartMillis());
        String purchaseOption = lineItem.getPurchaseOption();
        ReservationArn reservationArn = ReservationArn.get(lineItem.getReservationArn());
        if (StringUtils.isEmpty(purchaseOption) && !reservationArn.name.isEmpty()) {
        	ReservationInfo resInfo = reservationService.getReservation(reservationArn);
        	if (resInfo != null)
        		defaultReservationPurchaseOption = ((Operation.ReservationOperation) resInfo.tagGroup.operation).getPurchaseOption();
        }
        		       
        // Remap assignments for product, operation, and usageType to break out reserved instances and split out a couple EC2 types like ebs and eip
        ReformedMetaData reformedMetaData = reform(lineItem, defaultReservationPurchaseOption);
        
        final Product product = reformedMetaData.product;
        final Operation operation = reformedMetaData.operation;
        final UsageType usageType = reformedMetaData.usageType;
        
        final TagGroup tagGroup = getTagGroup(lineItem, account, region, zone, product, operation, usageType, null);
        
        int startIndex = (int)((usageInterval.getStartMillis() - startMilli)/ AwsUtils.hourMillis);
        int endIndex = (int)((usageInterval.getEndMillis() + 1000 - startMilli)/ AwsUtils.hourMillis);

        // Add all resources to the instance catalog
        addResourceInstance(lineItem, instances, tagGroup);

        double costValue = Double.parseDouble(lineItem.getCost());
        final Result result = getResult(lineItem, reportStart, reportModTime, tagGroup, processDelayed, lineItem.isReserved(), costValue);

        ResourceGroup resourceGroup = null;
        if (resourceService != null) {
            resourceGroup = resourceService.getResourceGroup(account, region, product, lineItem, usageInterval.getStartMillis());
        }
        
        // Do line-item-specific processing
        LineItemType lineItemType = lineItem.getLineItemType();
        if (lineItemType != null) {
        	switch (lineItem.getLineItemType()) {
	        case RIFee:
	        	if (processDelayed) {
	                // Grab the unused rates for the reservation processor.
	            	TagGroupRI tgri = TagGroupRI.get(account, region, zone, product, Operation.getReservedInstances(((Operation.ReservationOperation) operation).getPurchaseOption()), usageType, resourceGroup, reservationArn);
	            	addReservation(fileName, lineItem, costAndUsageData, tgri, startMilli);
	        	}
	        	break;
	        	
	        case SavingsPlanRecurringFee:
	        	// Grab the amortization and recurring fee for the savings plan processor.
	        	String arn = lineItem.getSavingsPlanArn();
	        	PurchaseOption po = PurchaseOption.get(lineItem.getSavingsPlanPaymentOption());
	        	TagGroupSP tgsp = TagGroupSP.get(account, region, zone, product, Operation.getSavings(po), usageType, resourceGroup, SavingsPlanArn.get(arn));
	        	costAndUsageData.addSavingsPlan(tgsp, po, lineItem.getSavingsPlanPurchaseTerm(), lineItem.getSavingsPlanOfferingType(),
	        					new DateTime(lineItem.getSavingsPlanStartTime(), DateTimeZone.UTC).getMillis(),
	        					new DateTime(lineItem.getSavingsPlanEndTime(), DateTimeZone.UTC).getMillis(),
	        					lineItem.getSavingsPlanRecurringCommitmentForBillingPeriod(), 
	        					lineItem.getSavingsPlanAmortizedUpfrontCommitmentForBillingPeriod());
	        	break;
	        	
	        case SavingsPlanCoveredUsage:
	        	costValue = Double.parseDouble(lineItem.getSavingsPlanEffectiveCost());
	        	break;
	        
	        case Tax:
	        	break;
	        	
	        default:
	        	
	        	break;
	        }
        }
        
        if (result == Result.ignore || result == Result.delay)
            return result;

        final String description = lineItem.getDescription();
        boolean monthlyCost = StringUtils.isEmpty(description) ? false : description.toLowerCase().contains("-month");
    	double usageValue = Double.parseDouble(lineItem.getUsageQuantity());

        if (result == Result.daily) {
            long millisStart = usageInterval.getStart().withTimeAtStartOfDay().getMillis();
            startIndex = (int)((millisStart - startMilli)/ AwsUtils.hourMillis);
            endIndex = startIndex + 24;
        }
        else if (result == Result.monthly) {
            startIndex = 0;
            endIndex = costAndUsageData.getUsage(null).getNum();
            int numHoursInMonth = new DateTime(startMilli, DateTimeZone.UTC).dayOfMonth().getMaximumValue() * 24;
            usageValue = usageValue * endIndex / numHoursInMonth;
            costValue = costValue * endIndex / numHoursInMonth;
        }
        else if (result == Result.hourlyTruncate) {
            endIndex = Math.min(endIndex, costAndUsageData.getUsage(null).getNum());
        }

        if (monthlyCost) {
            int numHoursInMonth = new DateTime(startMilli, DateTimeZone.UTC).dayOfMonth().getMaximumValue() * 24;
            usageValue = usageValue * numHoursInMonth;
        }

        int[] indexes;
        if (endIndex - startIndex > 1) {
            usageValue = usageValue / (endIndex - startIndex);
            costValue = costValue / (endIndex - startIndex);
            indexes = new int[endIndex - startIndex];
            for (int i = 0; i < indexes.length; i++)
                indexes[i] = startIndex + i;
        }
        else {
            indexes = new int[]{startIndex};
        }

        TagGroup resourceTagGroup = null;
        if (resourceService != null) {
            resourceTagGroup = getTagGroup(lineItem, account, region, zone, product, operation, usageType, resourceGroup);
        }
        
    	//if (endIndex >= (int)((reportModTime.getMillis() - startMilli)/ AwsUtils.hourMillis))
    	//	logger.info("Line item ends after report date, result " + result + ", end index " + endIndex + ", " + lineItem);
    	
        addData(fileName, lineItem, tagGroup, resourceTagGroup, costAndUsageData, usageValue, costValue, result == Result.monthly, indexes, edpDiscount, startMilli);
        return result;
    }

    protected void addData(String fileName, LineItem lineItem, TagGroup tagGroup, TagGroup resourceTagGroup,
    		CostAndUsageData costAndUsageData, double usageValue, double costValue, boolean monthly, int[] indexes, double edpDiscount, long startMilli) {
        final ReadWriteData usageData = costAndUsageData.getUsage(null);
        final ReadWriteData costData = costAndUsageData.getCost(null);
        ReadWriteData usageDataOfProduct = costAndUsageData.getUsage(tagGroup.product);
        ReadWriteData costDataOfProduct = costAndUsageData.getCost(tagGroup.product);
        final Product product = tagGroup.product;

        if (resourceService != null) {
            if (usageDataOfProduct == null) {            	
                usageDataOfProduct = new ReadWriteData(numUserTags);
                costDataOfProduct = new ReadWriteData(numUserTags);
                costAndUsageData.putUsage(tagGroup.product, usageDataOfProduct);
                costAndUsageData.putCost(tagGroup.product, costDataOfProduct);
            }
        }
        
        for (int i : indexes) {
            //
            // For DBR reports, Redshift and RDS have cost as a monthly charge, but usage appears hourly.
            //		EC2 has cost reported in each usage lineitem.
            // The reservation processor handles determination on what's unused.
            if (!monthly || !(product.isRedshift() || product.isRdsInstance())) {
            	addValue(usageData, i, tagGroup, usageValue);                	
            }

            addValue(costData, i, tagGroup, costValue);
            
            if (resourceService != null) {
                if (!((product.isRedshift() || product.isRds()) && monthly)) {
                	addValue(usageDataOfProduct, i, resourceTagGroup, usageValue);
                }
                
                addValue(costDataOfProduct, i, resourceTagGroup, costValue);
                
                // Collect statistics on tag coverage
            	boolean[] userTagCoverage = resourceService.getUserTagCoverage(lineItem);
            	costAndUsageData.addTagCoverage(null, i, tagGroup, userTagCoverage);
            	costAndUsageData.addTagCoverage(product, i, resourceTagGroup, userTagCoverage);
            }
        }
    }

    protected void addValue(ReadWriteData rwd, int i, TagGroup tagGroup, double value) {
        Double oldV = rwd.get(i, tagGroup);
        if (oldV != null) {
            value += oldV;
        }

        rwd.put(i, tagGroup, value);
    }
    
    protected void addReservation(
    		String fileName,
    		LineItem lineItem,
    		CostAndUsageData costAndUsageData,
    		TagGroupRI tg, long startMilli) {    	
        return;        
    }
    
    protected void addSavingsPlan(LineItem lineItem, CostAndUsageData costAndUsageData) {
    	return;
    }

    protected Result getResult(LineItem lineItem, DateTime reportStart, DateTime reportModTime, TagGroup tg, boolean processDelayed, boolean reservationUsage, double costValue) {
        Result result = Result.hourly;
        if (tg.product.isEc2Instance()) {
            result = processEc2Instance(processDelayed, reservationUsage, tg.operation, tg.zone);
        }
        else if (tg.product.isRedshift()) {
            result = processRedshift(processDelayed, reservationUsage, tg.operation, costValue);
            //logger.info("Process Redshift " + operation + " " + costValue + " returned: " + result);
        }
        else if (tg.product.isDataTransfer()) {
            result = processDataTranfer(processDelayed, tg.usageType, costValue);
        }
        else if (tg.product.isCloudHsm()) {
            result = processCloudhsm(processDelayed, tg.usageType);
        }
        else if (tg.product.isEbs()) {
            result = processEbs(tg.usageType);
        }
        else if (tg.product.isRds() || tg.product.isRdsInstance()) {
            result = processRds(tg.usageType, processDelayed, reservationUsage, tg.operation, costValue);
//            if (startIndex == 0 && reservationUsage) {
//            	logger.info(" ----- RDS usage=" + usageType + ", delayed=" + processDelayed + ", operation=" + operation + ", cost=" + costValue + ", result=" + result);
//            }
        }
        else if (tg.product.isElastiCache()) {
        	result = processElastiCache(processDelayed, reservationUsage, tg.operation, costValue);
        }
        
        if (tg.product.isS3() && (tg.usageType.name.startsWith("TimedStorage-") || tg.usageType.name.startsWith("IATimedStorage-")))
            result = Result.daily;

        return result;
    }
    
    private Result processEc2Instance(boolean processDelayed, boolean reservationUsage, Operation operation, Zone zone) {
        if (!processDelayed && zone == null && operation.isBonus() && reservationUsage)
            return Result.delay; // Delay monthly cost on no upfront and partial upfront reservations
        else if (processDelayed && zone == null && operation.isBonus() && reservationUsage)
            return Result.ignore; // Ignore monthly cost on no upfront and partial upfront reservations - We'll process the unused rates now if CUR
        else
            return Result.hourly;
    }

    private Result processRedshift(boolean processDelayed, boolean reservationUsage, Operation operation, double costValue) {
        if (!processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.delay;
        else if (!processDelayed && costValue == 0 && operation.isBonus() && reservationUsage)
            return Result.hourly;
        else if (processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.monthly;
        else
            return Result.hourly;
    }

    protected Result processDataTranfer(boolean processDelayed, UsageType usageType, double costValue) {
    	// Data Transfer accounts for the vast majority of TagGroup variations when user tags are used.
    	// To minimize the impact, ignore the zero-cost data-in usage types.
    	if (usageType.name.endsWith("-In-Bytes") && costValue == 0.0)
    		return Result.ignore;
    	else if (!processDelayed && usageType.name.contains("PrevMon-DataXfer-"))
            return Result.delay;
        else if (processDelayed && usageType.name.contains("PrevMon-DataXfer-"))
            return Result.monthly;
        else
            return Result.hourly;
    }

    protected Result processCloudhsm(boolean processDelayed, UsageType usageType) {
        if (!processDelayed && usageType.name.contains("CloudHSMUpfront"))
            return Result.delay;
        else if (processDelayed && usageType.name.contains("CloudHSMUpfront"))
            return Result.monthly;
        else
            return Result.hourly;
    }

    protected Result processEbs(UsageType usageType) {
        if (usageType.name.startsWith("EBS:SnapshotUsage"))
            return Result.daily;
        else
            return Result.hourly;
    }

    private Result processRds(UsageType usageType, boolean processDelayed, boolean reservationUsage, Operation operation, double costValue) {
        if (usageType.name.startsWith("RDS:ChargedBackupUsage"))
            return Result.daily;
        else if (!processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.delay; // Must be a monthly charge for all the hourly usage
        else if (!processDelayed && costValue == 0 && operation.isBonus() && reservationUsage)
            return Result.hourly; // Must be the hourly usage
        else if (processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.monthly; // This is the post processing of the monthly charge for all the hourly usage
        else
            return Result.hourly;
    }
    
    private Result processElastiCache(boolean processDelayed, boolean reservationUsage, Operation operation, double costValue) {
    	if (!processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.delay; // Must be a monthly charge for all the hourly usage
        else if (!processDelayed && costValue == 0 && operation.isBonus() && reservationUsage)
            return Result.hourly; // Must be the hourly usage
        else if (processDelayed && costValue != 0 && operation.isBonus() && reservationUsage)
            return Result.monthly; // This is the post processing of the monthly charge for all the hourly usage
        else
            return Result.hourly;
    }


    private Operation getReservationOperation(LineItem lineItem, Product product, PurchaseOption defaultReservationPurchaseOption) {    	
    	String purchaseOption = lineItem.getPurchaseOption();
    	
    	if (lineItem.getLineItemType() == LineItemType.Credit) {
    		return Operation.reservedInstancesCredits;
    	}
    	if (StringUtils.isNotEmpty(purchaseOption)) {
    		return Operation.getBonusReservedInstances(PurchaseOption.get(purchaseOption));
    	}
    	
        double cost = Double.parseDouble(lineItem.getCost());

        if (lineItem.getLineItemType() == LineItemType.RIFee) {
        	if (product.isElastiCache()) {
	    		// ElastiCache still uses the Legacy Heavy/Medium/Light reservation model for older instance families and
	    		// RIFee line items don't have PurchaseOption set.
        		String[] usage = lineItem.getUsageType().split(":");
        		String family = usage.length > 1 ? usage[1].substring("cache.".length()).split("\\.")[0] : "m1";
        		List<String> legacyInstanceFamilies = Lists.newArrayList("m1", "m2", "m3", "m4", "t1", "t2", "r1", "r2", "r3", "r4");
        		if (legacyInstanceFamilies.contains(family)) {
        			if (usage[0].contains("HeavyUsage"))
        				return Operation.getBonusReservedInstances(PurchaseOption.Heavy);
        			if (usage[0].contains("MediumUsage"))
        				return Operation.getBonusReservedInstances(PurchaseOption.Medium);
        			if (usage[0].contains("LightUsage"))
        				return Operation.getBonusReservedInstances(PurchaseOption.Light);
        		}
        	}
        	
        	if (lineItem.hasAmortizedUpfrontFeeForBillingPeriod()) {
        		// RIFee line items have amortization and recurring fee info as of 2018-01-01
    			// determine purchase option from amort and recurring
    			Double amortization = Double.parseDouble(lineItem.getAmortizedUpfrontFeeForBillingPeriod());
    			return amortization > 0.0 ? (cost > 0.0 ? Operation.bonusReservedInstancesPartialUpfront : Operation.bonusReservedInstancesAllUpfront) : Operation.bonusReservedInstancesNoUpfront;
        	}
    	}
        else if (lineItem.getLineItemType() == LineItemType.DiscountedUsage) {
        	if (lineItem.hasAmortizedUpfrontCostForUsage()) {
        		// DiscountedUsage line items have amortization and recurring fee info as of 2018-01-01
        		Double amortization = Double.parseDouble(lineItem.getAmortizedUpfrontCostForUsage());
        		Double recurringCost = Double.parseDouble(lineItem.getRecurringFeeForUsage());
        		return amortization > 0.0 ? (recurringCost > 0.0 ? Operation.bonusReservedInstancesPartialUpfront : Operation.bonusReservedInstancesAllUpfront) : Operation.bonusReservedInstancesNoUpfront;
        	}
        }
    	
		if (cost == 0 && (product.isEc2() || lineItem.getDescription().contains(" 0.0 "))) {
        	return Operation.bonusReservedInstancesAllUpfront;
        }
        return Operation.getBonusReservedInstances(defaultReservationPurchaseOption);
    }
    
    protected ReformedMetaData reform(LineItem lineItem, PurchaseOption defaultReservationPurchaseOption) {

    	Product product = productService.getProduct(lineItem.getProduct(), lineItem.getProductServiceCode());
    	
        Operation operation = null;
        UsageType usageType = null;
        InstanceOs os = null;
        InstanceDb db = null;

        String usageTypeStr = getUsageTypeStr(lineItem.getUsageType(), product);
        final String operationStr = lineItem.getOperation();
        boolean reservationUsage = lineItem.isReserved();
        boolean isCredit = lineItem.getLineItemType() == LineItemType.Credit;

        if (product.isRds() && usageTypeStr.endsWith("xl")) {
            // Many of the "m" and "r" families end with "xl" rather than "xlarge" (e.g. db.m4.10xl", so need to fix it.
        	usageTypeStr = usageTypeStr + "arge";
        }
        
        if (lineItem.getLineItemType() == LineItemType.Tax) {
        	operation = Operation.getTaxOperation(lineItem.getTaxType());
            usageType = UsageType.getUsageType(usageTypeStr.isEmpty() ? "Tax - " + lineItem.getLegalEntity() : usageTypeStr, "");
        }
        else if (usageTypeStr.startsWith("ElasticIP:")) {
            product = productService.getProduct(Product.Code.Eip);
        }
        else if (usageTypeStr.startsWith("EBS:") || operationStr.equals("EBS Snapshot Copy") || usageTypeStr.startsWith("EBSOptimized:")) {
            product = productService.getProduct(Product.Code.Ebs);
        }
        else if (usageTypeStr.startsWith("CW:")) {
            product = productService.getProduct(Product.Code.CloudWatch);
        }
        else if ((product.isEc2() || product.isEmr()) && (usageTypeStr.startsWith("BoxUsage") || usageTypeStr.startsWith("SpotUsage")) && operationStr.startsWith("RunInstances")) {
        	// Line item for hourly "All Upfront", "Spot", or "On-Demand" EC2 instance usage
        	boolean spot = usageTypeStr.startsWith("SpotUsage");
            int index = usageTypeStr.indexOf(":");
            usageTypeStr = index < 0 ? "m1.small" : usageTypeStr.substring(index+1);

            if (reservationUsage) {
            	operation = getReservationOperation(lineItem, product, defaultReservationPurchaseOption);
            }
            else if (spot)
            	operation = isCredit ? Operation.spotInstanceCredits : Operation.spotInstances;
            else
                operation = isCredit ? Operation.ondemandInstanceCredits : Operation.ondemandInstances;
            os = getInstanceOs(operationStr);
        }
        else if (product.isRedshift() && usageTypeStr.startsWith("Node") && operationStr.startsWith("RunComputeNode")) {
        	// Line item for hourly Redshift instance usage both On-Demand and Reserved.
            usageTypeStr = currentRedshiftUsageType(usageTypeStr.split(":")[1]);
            
            if (reservationUsage) {
            	operation = getReservationOperation(lineItem, product, defaultReservationPurchaseOption);
            }
            else {
	            operation = isCredit ? Operation.ondemandInstanceCredits : Operation.ondemandInstances;
            }
            os = getInstanceOs(operationStr);
        }
        else if (product.isRds() && (usageTypeStr.startsWith("InstanceUsage") || usageTypeStr.startsWith("Multi-AZUsage")) && operationStr.startsWith("CreateDBInstance")) {
        	// Line item for hourly RDS instance usage - both On-Demand and Reserved
        	boolean multiAZ = usageTypeStr.startsWith("Multi");
            usageTypeStr = usageTypeStr.split(":")[1];
            
            if (multiAZ) {
            	usageTypeStr += UsageType.multiAZ;
            }
            
            if (reservationUsage) {
            	operation = getReservationOperation(lineItem, product, defaultReservationPurchaseOption);
            }
            else {
            	operation = isCredit ? Operation.ondemandInstanceCredits : Operation.ondemandInstances;
            }
            db = getInstanceDb(operationStr);
        }
        else if (product.isElasticsearch() && usageTypeStr.startsWith("ESInstance") && operationStr.startsWith("ESDomain")) {
        	// Line item for hourly Elasticsearch instance usage both On-Demand and Reserved.
            if (reservationUsage) {
            	operation = getReservationOperation(lineItem, product, defaultReservationPurchaseOption);
            }
            else {
            	operation = isCredit ? Operation.ondemandInstanceCredits : Operation.ondemandInstances;
            }
        }
        else if (product.isElastiCache() && usageTypeStr.startsWith("NodeUsage") && operationStr.startsWith("CreateCacheCluster")) {
        	// Line item for hourly ElastiCache node usage both On-Demand and Reserved.
            if (reservationUsage) {
            	operation = getReservationOperation(lineItem, product, defaultReservationPurchaseOption);
	        }
	        else {
	        	operation = isCredit ? Operation.ondemandInstanceCredits : Operation.ondemandInstances;
	        }
        }
        else if (usageTypeStr.startsWith("HeavyUsage") || usageTypeStr.startsWith("MediumUsage") || usageTypeStr.startsWith("LightUsage")) {
        	// If DBR report: Line item for hourly "No Upfront" or "Partial Upfront" EC2 or monthly "No Upfront" or "Partial Upfront" for Redshift and RDS
        	// If Cost and Usage report: monthly "No Upfront" or "Partial Upfront" for EC2, RDS, Redshift, and ES
            int index = usageTypeStr.indexOf(":");
            if (index < 0) {
                usageTypeStr = "m1.small";
            }
            else {
                usageTypeStr = usageTypeStr.substring(index+1);
                if (product.isRedshift()) {
                	usageTypeStr = currentRedshiftUsageType(usageTypeStr);
                }
            }
            if (product.isRds()) {
                db = getInstanceDb(operationStr);
                if (lineItem.getLineItemType() == LineItemType.RIFee) {
                	String normFactorStr = lineItem.getLineItemNormalizationFactor();
                	if (!normFactorStr.isEmpty()) {
	                	// Determine if we have a multi-AZ reservation by looking at the normalization factor, numberOfReservations, and instance family size
	                	Double normalizationFactor = Double.parseDouble(lineItem.getLineItemNormalizationFactor());
	                	double usageTypeTypicalNormalizationFactor = CostAndUsageReportLineItem.computeProductNormalizedSizeFactor(usageTypeStr);
	                	// rough math -- actually would be a factor of two
	                	if (normalizationFactor / usageTypeTypicalNormalizationFactor > 1.5) {
	                        usageTypeStr += UsageType.multiAZ;
	                	}
                	}
                }
            }
        	operation = getReservationOperation(lineItem, product, defaultReservationPurchaseOption);
            os = getInstanceOs(operationStr);
        }
        
        // Re-map all Data Transfer costs except API Gateway and CouldFront to Data Transfer (same as the AWS Billing Page Breakout)
        if (!product.isCloudFront() && !product.isApiGateway()) {
        	if (usageTypeStr.equals("DataTransfer-Regional-Bytes") || usageTypeStr.endsWith("-In-Bytes") || usageTypeStr.endsWith("-Out-Bytes"))
        		product = productService.getProduct(Product.Code.DataTransfer);
        }

        // Usage type string is empty for Support recurring fees.
        if (usageTypeStr.equals("Unknown") || usageTypeStr.equals("Not Applicable") || usageTypeStr.isEmpty()) {
            usageTypeStr = product.getIceName();
        }

        if (operation == null) {
            operation = isCredit ? Operation.getCreditOperation(operationStr) : Operation.getOperation(operationStr);
        }

        if (operation instanceof Operation.ReservationOperation) {
	        if (product.isEc2()) {
	            product = productService.getProduct(Product.Code.Ec2Instance);
	            usageTypeStr = usageTypeStr + os.usageType;
	        }
	        else if (product.isRds()) {
	            product = productService.getProduct(Product.Code.RdsInstance);
	            usageTypeStr = usageTypeStr + "." + db;
	            operation = operation.isBonus() ? operation : isCredit ? Operation.ondemandInstanceCredits : Operation.ondemandInstances;
	        }
	        else if (product.isElasticsearch()) {
	            usageTypeStr = usageTypeStr.substring(usageTypeStr.indexOf(":") + 1);
	            if (!usageTypeStr.endsWith(".elasticsearch")) // RIFee contains suffix, Usage does not.
	            	usageTypeStr += ".elasticsearch";
	        }
	        else if (product.isElastiCache()) {
	        	usageTypeStr = usageTypeStr.substring(usageTypeStr.indexOf(":") + 1) + "." + getInstanceCache(operationStr);
	        }
        }

        if (usageType == null) {
        	String unit = (operation instanceof Operation.ReservationOperation) ? "hours" : lineItem.getPricingUnit(); 
            usageType = UsageType.getUsageType(usageTypeStr, unit);
//            if (StringUtils.isEmpty(usageType.unit)) {
//            	logger.info("No units for " + usageTypeStr + ", " + operation + ", " + description + ", " + product);
//            }
        }
        
        // Override operation if this is savings plan covered usage
        if (lineItem.getLineItemType() == LineItemType.SavingsPlanCoveredUsage) {
        	operation = Operation.getSavingsPlanBonus(PurchaseOption.get(lineItem.getSavingsPlanPaymentOption()));
        }

        return new ReformedMetaData(product, operation, usageType);
    }

    protected InstanceOs getInstanceOs(String operationStr) {
        int index = operationStr.indexOf(":");
        String osStr = index > 0 ? operationStr.substring(index) : "";
        return InstanceOs.withCode(osStr);
    }

    protected InstanceDb getInstanceDb(String operationStr) {
        int index = operationStr.indexOf(":");
        String osStr = index > 0 ? operationStr.substring(index) : "";
        return InstanceDb.withCode(osStr);
    }
    
    protected InstanceCache getInstanceCache(String operationStr) {
        int index = operationStr.indexOf(":");
        String cacheStr = index > 0 ? operationStr.substring(index) : "";
        return InstanceCache.withCode(cacheStr);
    }

    protected static final Map<String, String> redshiftUsageTypeMap = Maps.newHashMap();
    static {
    	redshiftUsageTypeMap.put("dw.hs1.xlarge", "ds1.xlarge");
    	redshiftUsageTypeMap.put("dw1.xlarge", "ds1.xlarge");
    	redshiftUsageTypeMap.put("dw.hs1.8xlarge", "ds1.8xlarge");
    	redshiftUsageTypeMap.put("dw1.8xlarge", "ds1.8xlarge");
    	redshiftUsageTypeMap.put("dw2.large", "dc1.large");
    	redshiftUsageTypeMap.put("dw2.8xlarge", "dc1.8xlarge");
    }

    protected String currentRedshiftUsageType(String usageType) {
    	if (redshiftUsageTypeMap.containsKey(usageType))
    		return redshiftUsageTypeMap.get(usageType);
    	return usageType;
    }
    
    protected static class ReformedMetaData{
        public final Product product;
        public final Operation operation;
        public final UsageType usageType;
        public ReformedMetaData(Product product, Operation operation, UsageType usageType) {
            this.product = product;
            this.operation = operation;
            this.usageType = usageType;
        }
        
        public String toString() {
            return "\"" + product + "\",\"" + operation + "\",\"" + usageType + "\"";
        }
    }
    
}
