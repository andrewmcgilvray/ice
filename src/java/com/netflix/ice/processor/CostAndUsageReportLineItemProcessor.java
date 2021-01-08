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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import com.netflix.ice.basic.BasicLineItemProcessor;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.LineItem.BillType;
import com.netflix.ice.common.LineItem.LineItemType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Operation.SavingsPlanOperation;
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
public class CostAndUsageReportLineItemProcessor extends BasicLineItemProcessor {
	public final static long jan1_2018 = new DateTime("2018-01", DateTimeZone.UTC).getMillis();
	
	public CostAndUsageReportLineItemProcessor(
			AccountService accountService,
			ProductService productService,
			ReservationService reservationService,
			ResourceService resourceService) {
		super(accountService, productService, reservationService,
				resourceService);
	}
   
	@Override
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
    	
    	return super.ignore(fileName, reportStart, reportModTime, root, usageInterval, lineItem);
    }

	@Override
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

	@Override
    protected TagGroup getTagGroup(LineItem lineItem, Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup rg) {
        if (operation.isSavingsPlan()) {
        	SavingsPlanArn savingsPlanArn = SavingsPlanArn.get(lineItem.getSavingsPlanArn());
        	return TagGroupSP.get(account, region, zone, product, operation, usageType, rg, savingsPlanArn);
        }

        ReservationArn reservationArn = ReservationArn.get(lineItem.getReservationArn());
        if (operation instanceof Operation.ReservationOperation && !reservationArn.name.isEmpty() && !operation.isCredit()) {
        	return TagGroupRI.get(account, region, zone, product, operation, usageType, rg, reservationArn);
        }
        return super.getTagGroup(lineItem, account, region, zone, product, operation, usageType, rg);
    }


	@Override
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

	@Override
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
	
	@Override
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

}
