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
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.processor.LineItem.BillType;
import com.netflix.ice.processor.LineItem.LineItemType;
import com.netflix.ice.processor.ReservationService.ReservationInfo;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Operation.SavingsPlanOperation;
import com.netflix.ice.tag.Zone.BadZone;
import com.netflix.ice.tag.InstanceCache;
import com.netflix.ice.tag.InstanceDb;
import com.netflix.ice.tag.InstanceOs;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.SavingsPlanArn;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

/*
* All reservation usage starts out tagged as BonusReservedInstances and is later reassigned proper tags
* based on it's usage by the ReservationProcessor.
*/
public class CostAndUsageReportLineItemProcessor implements LineItemProcessor {
	public final static long jan1_2018 = new DateTime("2018-01", DateTimeZone.UTC).getMillis();
	
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    protected AccountService accountService;
    protected ProductService productService;
    protected ReservationService reservationService;

    protected ResourceService resourceService;
    protected final int numUserTags;
	
	public CostAndUsageReportLineItemProcessor(
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
   
    protected boolean ignore(String fileName, DateTime reportStart, DateTime reportModTime, String root, Interval usageInterval, LineItem lineItem) {    	
    	BillType billType = lineItem.getBillType();
    	if (billType == BillType.Purchase || billType == BillType.Refund) {
            Product product = productService.getProduct(lineItem.getProduct(), lineItem.getProductServiceCode());
            if (!product.isSupport()) {
	        	// Skip purchases and refunds for everything except support
	    		logger.info(fileName + " Skip Purchase/Refund: " + lineItem);
	    		return true;
            }
    	}
    	
    	// Cost and Usage report-specific checks
    	LineItemType lit = lineItem.getLineItemType();
    	if (lit == LineItemType.EdpDiscount ||
    		lit == LineItemType.RiVolumeDiscount ||
    		lit == LineItemType.SavingsPlanNegation ||
    		lit == LineItemType.SavingsPlanUpfrontFee ||
    		lit == LineItemType.PrivateRateDiscount) {
    		return true;
    	}
    	
    	if (lineItem.getLineItemType() == LineItemType.SavingsPlanRecurringFee && usageInterval.getStartMillis() >= reportModTime.getMillis()) {
    		// Don't show unused recurring fees for future hours in the month.
    		return true;
    	}        
    	
    	if (lit == LineItemType.Tax && Double.parseDouble(lineItem.getCost()) == 0)
    		return true;
    	
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
    
	public Region getRegion(LineItem lineItem) {
		// Region can be obtained from the following sources with the following precedence:
		//  1. lineItem/UsageType prefix (us-east-1 has no prefix)
		//  2. lineItem/AvailabilityZone
		//  3. product/region
    	String usageTypeStr = lineItem.getUsageType();
    	
		// If it's a tax line item with no usageType, put it in the global region
    	if (lineItem.getLineItemType() == LineItemType.Tax && usageTypeStr.isEmpty())
    		return Region.GLOBAL;
    	
        int index = usageTypeStr.indexOf("-");
        String regionShortName = index > 0 ? usageTypeStr.substring(0, index) : "";
        
        Region region = null;
        if (!regionShortName.isEmpty()) {
        	// Try to get region from usage type prefix. Value may not be a region code, so can come back null
        	region = Region.getRegionByShortName(regionShortName);
        }
        if (region == null) {
        	String zone = lineItem.getZone();
        	if (zone.isEmpty()) {
        		region = lineItem.getProductRegion();
        	}
        	else {
        		for (Region r: Region.getAllRegions()) {
        			if (zone.startsWith(r.name)) {
        				region = r;
        				break;
        			}
        		}
        	}
        }
        
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
        if (operation.isSavingsPlan()) {
        	SavingsPlanArn savingsPlanArn = SavingsPlanArn.get(lineItem.getSavingsPlanArn());
        	return TagGroupSP.get(account, region, zone, product, operation, usageType, rg, savingsPlanArn);
        }

        ReservationArn reservationArn = ReservationArn.get(lineItem.getReservationArn());
        if (operation instanceof Operation.ReservationOperation && !reservationArn.name.isEmpty() && !operation.isCredit()) {
        	return TagGroupRI.get(account, region, zone, product, operation, usageType, rg, reservationArn);
        }
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
            endIndex = costAndUsageData.get(null).getNum();
            int numHoursInMonth = new DateTime(startMilli, DateTimeZone.UTC).dayOfMonth().getMaximumValue() * 24;
            usageValue = usageValue * endIndex / numHoursInMonth;
            costValue = costValue * endIndex / numHoursInMonth;
        }
        else if (result == Result.hourlyTruncate) {
            endIndex = Math.min(endIndex, costAndUsageData.get(null).getNum());
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

    protected void addReservation(
    		String fileName,
    		LineItem lineItem,
    		CostAndUsageData costAndUsageData,
    		TagGroupRI tg,
    		long startMilli) {
    	
        // If processing an RIFee from a CUR, create a reservation for the reservation processor.
        if (lineItem.getLineItemType() != LineItemType.RIFee || startMilli < jan1_2018)
        	return;
        
        // TODO: Handle reservations for DynamoDB
        if (tg.product.isDynamoDB()) {
        	return;
        }
        
        if (!(tg.operation instanceof ReservationOperation)) {
        	logger.error(fileName + " operation is not a reservation operation, tag: " + tg + "\n" + lineItem);
        	return;
        }
        
        int count = Integer.parseInt(lineItem.getReservationNumberOfReservations());
    	// AWS Reservations are applied within the hour they are purchased. We process full hours, so adjust to start of hour.
        // The reservations stop being applied during the hour in which the reservation expires. We process full hours, so extend to end of hour.
        DateTime start = new DateTime(lineItem.getReservationStartTime(), DateTimeZone.UTC).withMinuteOfHour(0).withSecondOfMinute(0);
        DateTime end = new DateTime(lineItem.getReservationEndTime(), DateTimeZone.UTC).withMinuteOfHour(0).withSecondOfMinute(0).plusHours(1);
        PurchaseOption purchaseOption = ((ReservationOperation) tg.operation).getPurchaseOption();        
        
        Double usageQuantity = Double.parseDouble(lineItem.getUsageQuantity());
        double hourlyFixedPrice = Double.parseDouble(lineItem.getAmortizedUpfrontFeeForBillingPeriod()) / usageQuantity;
        double usagePrice = Double.parseDouble(lineItem.getCost()) / usageQuantity;
        
        double hourlyUnusedFixedPrice = lineItem.getUnusedAmortizedUpfrontRate();
        double unusedUsagePrice = lineItem.getUnusedRecurringRate();
        
        if (hourlyUnusedFixedPrice > 0.0 && Math.abs(hourlyUnusedFixedPrice - hourlyFixedPrice) > 0.0001)
        	logger.info(fileName + " used and unused fixed prices are different, used: " + hourlyFixedPrice + ", unused: " + hourlyUnusedFixedPrice + ", tg: " + tg);
        if (unusedUsagePrice > 0.0 && Math.abs(unusedUsagePrice - usagePrice) > 0.0001)
        	logger.info(fileName + " used and unused usage prices are different, used: " + usagePrice + ", unused: " + unusedUsagePrice + ", tg: " + tg);
		
        Reservation r = new Reservation(tg, count, start.getMillis(), end.getMillis(), purchaseOption, hourlyFixedPrice, usagePrice);
        costAndUsageData.addReservation(r);
        
        if (ReservationArn.debugReservationArn != null && tg.arn == ReservationArn.debugReservationArn) {
        	logger.info(fileName + " RI: count=" + r.count + ", tg=" + tg);
        }
    }
		
	private boolean applyMonthlyUsage(LineItemType lineItemType, boolean monthly, Product product) {
        // For CAU reports, EC2, Redshift, and RDS have cost as a monthly charge, but usage appears hourly.
        // 	so unlike EC2, we have to process the monthly line item to capture the cost,
        // 	but we don't want to add the monthly line items to the usage.
        // The reservation processor handles determination on what's unused.
		return lineItemType != LineItemType.Credit && (!monthly || !(product.isRedshift() || product.isRdsInstance() || product.isEc2Instance() || product.isElasticsearch() || product.isElastiCache()));
	}
	    
	private void addHourData(
			String fileName,
			LineItem lineItem,
			LineItemType lineItemType, boolean monthly, TagGroup tagGroup,
			boolean isReservationUsage, ReservationArn reservationArn,
			double usage, double cost, double edpDiscount,
			CostAndUsageData data, Product product,
			int hour, String amort, String publicOnDemandCost, long startMilli) {
		
		boolean isFirstHour = hour == 0;
		switch (lineItemType) {
		case SavingsPlanRecurringFee:
			return;
			
		case SavingsPlanCoveredUsage:
			addSavingsPlanSavings(fileName, lineItem, lineItemType, tagGroup, data, product, hour, cost, edpDiscount, publicOnDemandCost);
			break;
			
		default:
			break;
		}
		
		boolean debug = isReservationUsage && ReservationArn.debugReservationArn != null && isFirstHour && reservationArn == ReservationArn.debugReservationArn;
		
        if (applyMonthlyUsage(lineItemType, monthly, tagGroup.product)) {
        	data.add(product, hour, tagGroup, 0, usage);
            if (debug) {
            	logger.info(fileName + " " + lineItemType + " usage=" + usage + ", tg=" + tagGroup);
            }
        }
        
        // Additional entries for reservations
        if (isReservationUsage && !tagGroup.product.isDynamoDB()) {
        	if (lineItemType != LineItemType.Credit)
        		addAmortizationAndSavings(fileName, tagGroup, reservationArn, data, hour, product, cost, edpDiscount, amort, publicOnDemandCost, debug, lineItemType, startMilli);
        	// Reservation costs are handled through the ReservationService (see addReservation() above) except
        	// after Jan 1, 2019 when they added net costs to DiscountedUsage records.
        	if (cost != 0 && (lineItemType == LineItemType.Credit || lineItemType == LineItemType.DiscountedUsage)) {
        		data.add(product, hour, tagGroup, cost, 0);
                if (debug) {
                	logger.info(fileName + " " + lineItemType + " cost=" + cost + ", tg=" + tagGroup);
                }
        	}
        }
        else {
    		data.add(product, hour, tagGroup, cost, 0);
        }
	}
	
	private void addSavingsPlanSavings(String fileName, LineItem lineItem, LineItemType lineItemType, TagGroup tagGroup, CostAndUsageData data, Product product, int hour,
			double costValue, double edpDiscount, String publicOnDemandCost) {
    	// Don't include the EDP discount in the savings - we track that separately
		// costValue is the effectiveCost which includes amortization
    	if (publicOnDemandCost.isEmpty()) {
    		logger.warn(fileName + " " + lineItemType + " No public onDemand cost in line item for tg=" + tagGroup);
    		return;
    	}
        PurchaseOption paymentOption = PurchaseOption.get(lineItem.getSavingsPlanPaymentOption());
		SavingsPlanOperation savingsOp = Operation.getSavingsPlanSavings(paymentOption);
		TagGroup tg = TagGroup.getTagGroup(tagGroup.account,  tagGroup.region, tagGroup.zone, tagGroup.product, savingsOp, tagGroup.usageType, tagGroup.resourceGroup);
		double publicCost = Double.parseDouble(publicOnDemandCost);
		double edpCost = publicCost * (1 - edpDiscount);
		double savings = edpCost - costValue;
		data.add(product, hour, tg, savings, 0);
	}
	
	private void addUnusedSavingsPlanData(LineItem lineItem, TagGroup tagGroup, TagGroup resourceTagGroup, Product product, CostAndUsageData costAndUsageData, int hour) {
		double normalizedUsage = lineItem.getSavingsPlanNormalizedUsage();
		if (normalizedUsage >= 1.0)
			return;
		
		double amortization = Double.parseDouble(lineItem.getSavingsPlanAmortizedUpfrontCommitmentForBillingPeriod());
		double recurring = Double.parseDouble(lineItem.getSavingsPlanRecurringCommitmentForBillingPeriod());
		double unusedAmort = amortization * (1.0 - normalizedUsage);
		double unusedRecurring = recurring * (1.0 - normalizedUsage);
        
        PurchaseOption paymentOption = PurchaseOption.get(lineItem.getSavingsPlanPaymentOption());

        if (unusedAmort > 0.0) {
    		SavingsPlanOperation amortOp = Operation.getSavingsPlanUnusedAmortized(paymentOption);
    		TagGroup tgAmort = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, amortOp, tagGroup.usageType, tagGroup.resourceGroup);
    		costAndUsageData.add(null, hour, tgAmort, unusedAmort, 0);
    		if (resourceService != null) {
        		tgAmort = TagGroup.getTagGroup(resourceTagGroup.account, resourceTagGroup.region, resourceTagGroup.zone, resourceTagGroup.product, amortOp, resourceTagGroup.usageType, resourceTagGroup.resourceGroup);
    	        costAndUsageData.add(product, hour, tgAmort, unusedAmort, 0);
    		}
        }
        if (unusedRecurring > 0.0) {
        	SavingsPlanOperation unusedOp = Operation.getSavingsPlanUnused(paymentOption);
    		TagGroup tgRecurring = TagGroup.getTagGroup(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, unusedOp, tagGroup.usageType, tagGroup.resourceGroup);
    		costAndUsageData.add(null, hour, tgRecurring, unusedRecurring, 0);
    		if (resourceService != null) {
    			tgRecurring = TagGroup.getTagGroup(resourceTagGroup.account, resourceTagGroup.region, resourceTagGroup.zone, resourceTagGroup.product, unusedOp, resourceTagGroup.usageType, resourceTagGroup.resourceGroup);
    	        costAndUsageData.add(product, hour, tgRecurring, unusedRecurring, 0);
    		}
        }
	}

    protected void addData(String fileName, LineItem lineItem, TagGroup tagGroup, TagGroup resourceTagGroup, CostAndUsageData costAndUsageData, 
    		double usageValue, double costValue, boolean monthly, int[] indexes, double edpDiscount, long startMilli) {
		
        final Product product = tagGroup.product;
        final LineItemType lineItemType = lineItem.getLineItemType();
                        
        if (lineItemType == LineItemType.SavingsPlanRecurringFee) {
        	if (indexes.length > 1) {
        		logger.error(fileName + " SavingsPlanRecurringFee with more than one hour of data");
        	}
        	addUnusedSavingsPlanData(lineItem, tagGroup, resourceTagGroup, product, costAndUsageData, indexes[0]);
        	return;
        }
        else if (lineItemType == LineItemType.SavingsPlanCoveredUsage) {
        	costAndUsageData.addSavingsPlanProduct(tagGroup.product);
        }
        
        boolean reservationUsage = lineItem.isReserved();
        ReservationArn reservationArn = ReservationArn.get(lineItem.getReservationArn());
    	String amort = lineItem.getAmortizedUpfrontCostForUsage();
    	String publicOnDemandCost = lineItem.getPublicOnDemandCost();

        if (lineItemType == LineItemType.Credit && ReservationArn.debugReservationArn != null && reservationArn == ReservationArn.debugReservationArn)
        	logger.info(fileName + " Credit: " + lineItem);
        
        for (int i : indexes) {
            addHourData(fileName, lineItem, lineItemType, monthly, tagGroup, reservationUsage, reservationArn, usageValue, costValue, edpDiscount, costAndUsageData, null, i, amort, publicOnDemandCost, startMilli);

            if (resourceService != null) {
                addHourData(fileName, lineItem, lineItemType, monthly, resourceTagGroup, reservationUsage, reservationArn, usageValue, costValue, edpDiscount, costAndUsageData, product, i, amort, publicOnDemandCost, startMilli);
                
                // Collect statistics on tag coverage
            	boolean[] userTagCoverage = resourceService.getUserTagCoverage(lineItem);
            	costAndUsageData.addTagCoverage(null, i, tagGroup, userTagCoverage);
            	costAndUsageData.addTagCoverage(product, i, resourceTagGroup, userTagCoverage);
            }
        }
    }
	
	private void addAmortizationAndSavings(String fileName, TagGroup tagGroup, ReservationArn reservationArn, CostAndUsageData data, int hour, Product product,
			double costValue, double edpDiscount, String amort, String publicOnDemandCost, boolean debug, LineItemType lineItemType, long startMilli) {
        // If we have an amortization cost from a DiscountedUsage line item, save it as amortization
    	double amortCost = 0.0;
    	if (amort.isEmpty()) {
    		if (startMilli >= jan1_2018)
    			logger.warn(fileName + " " + lineItemType + " No amortization in line item for tg=" + tagGroup);
    		return;
    	}
		amortCost = Double.parseDouble(amort);
		if (amortCost > 0.0) {
    		ReservationOperation amortOp = ReservationOperation.getAmortized(((ReservationOperation) tagGroup.operation).getPurchaseOption());
    		TagGroupRI tg = TagGroupRI.get(tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, amortOp, tagGroup.usageType, tagGroup.resourceGroup, reservationArn);
    		data.add(product, hour, tg, amortCost, 0);
            if (debug) {
            	logger.info(fileName + " " + lineItemType + " amort=" + amortCost + ", tg=" + tg);
            }
		}

    	// Compute and store savings if Public OnDemand Cost and Amortization is available
    	// Don't include the EDP discount in the savings - we track that separately
    	if (publicOnDemandCost.isEmpty()) {
    		if (startMilli >= jan1_2018)
    			logger.warn(fileName + " " + lineItemType + " No public onDemand cost in line item for tg=" + tagGroup);
    		return;
    	}
		ReservationOperation savingsOp = ReservationOperation.getSavings(((ReservationOperation) tagGroup.operation).getPurchaseOption());
		TagGroupRI tg = TagGroupRI.get(tagGroup.account,  tagGroup.region, tagGroup.zone, tagGroup.product, savingsOp, tagGroup.usageType, tagGroup.resourceGroup, reservationArn);
		double publicCost = Double.parseDouble(publicOnDemandCost);
		double edpCost = publicCost * (1 - edpDiscount);
		double savings = edpCost - costValue - amortCost;
		data.add(product, hour, tg, savings, 0);
        if (debug) {
        	logger.info(fileName + " " + lineItemType + " savings=" + savings + ", tg=" + tg);
        }
	}		
	
    protected Result getResult(LineItem lineItem, DateTime reportStart, DateTime reportModTime, TagGroup tg, boolean processDelayed, boolean reservationUsage, double costValue) {        
        switch (lineItem.getLineItemType()) {
        case RIFee:
            // Monthly recurring fees for EC2, RDS, and Redshift reserved instances
        	// Prior to Jan 1, 2018 we have to get cost from the RIFee record, so process as Monthly cost.
        	// As of Jan 1, 2018, we use the recurring fee and amortization values from DiscountedUsage line items.
        	if (reportStart.getMillis() >= jan1_2018) {
	            // We use the RIFee line items to extract the reservation info
		        return processDelayed ? Result.ignore : Result.delay;
        	}
        	return processDelayed ? Result.monthly : Result.delay;
        	
        case DiscountedUsage:
        case SavingsPlanCoveredUsage:
        	return Result.hourly;
        	
        case SavingsPlanRecurringFee:
        	// If within a day of the report mod date, delay and truncate, else let it through.
        	if (!processDelayed && lineItem.getStartMillis() < reportModTime.minusDays(1).getMillis())
        			return Result.hourly;

        	return processDelayed ? Result.hourlyTruncate : Result.delay;
        	
        case Credit:
        case Tax:
        	// Taxes and Credits often end in the future. Delay and truncate
        	return processDelayed ? Result.hourlyTruncate : Result.delay;
        	
        default:
        	break;
        		
        }
        
        Result result = Result.hourly;
        if (tg.product.isDataTransfer()) {
            result = processDataTranfer(processDelayed, tg.usageType, costValue);
        }
        else if (tg.product.isCloudHsm()) {
            result = processCloudhsm(processDelayed, tg.usageType);
        }
        else if (tg.product.isEbs()) {
            result = processEbs(tg.usageType);
        }
        else if (tg.product.isRds()) {
            if (tg.usageType.name.startsWith("RDS:ChargedBackupUsage"))
                result = Result.daily;
        }
        
        if (tg.product.isS3() && (tg.usageType.name.startsWith("TimedStorage-") || tg.usageType.name.startsWith("IATimedStorage-")))
            result = Result.daily;

        return result;
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
        boolean dedicated = false;

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
        else if ((product.isEc2() || product.isEmr()) &&
        		(usageTypeStr.startsWith("BoxUsage") || usageTypeStr.startsWith("SpotUsage") || usageTypeStr.startsWith("DedicatedUsage")) && 
        		operationStr.startsWith("RunInstances")) {
        	
        	// Line item for hourly "All Upfront", "Spot", or "On-Demand" EC2 instance usage
        	boolean spot = usageTypeStr.startsWith("SpotUsage");
        	dedicated = usageTypeStr.startsWith("DedicatedUsage");
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
	            usageTypeStr = usageTypeStr + os.usageType + (dedicated ? ".dedicated" : "");
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
