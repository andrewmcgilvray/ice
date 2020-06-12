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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;

public class CostAndUsageReservationProcessor extends ReservationProcessor {
    // Unused rates and amortization for RIs were added to CUR on 2018-01
	final static long jan1_2018 = new DateTime("2018-01", DateTimeZone.UTC).getMillis();
	
	public CostAndUsageReservationProcessor(
			Set<Account> reservationOwners, ProductService productService,
			PriceListService priceListService) throws IOException {
		super(reservationOwners, productService,
				priceListService);
	}
	
	private void add(ReadWriteData data, int hour, TagGroup tg, double value) {
		Double amount = data.get(hour, tg);
		if (amount == null)
			amount = 0.0;
		amount += value;
		data.put(hour, tg, amount);
	}
	
	@Override
	protected void processReservations(
			ReservationService reservationService,
			CostAndUsageData data,
			Long startMilli) {
		
//		DateTime start = DateTime.now();
		
		ReadWriteData usageData = data.getUsage(product);
		ReadWriteData costData = data.getCost(product);

		// Scan the first hour and look for reservation usage with no ARN and log errors
	    for (TagGroup tagGroup: usageData.getTagGroups(0)) {
	    	if (tagGroup.operation instanceof ReservationOperation) {
	    		ReservationOperation ro = (ReservationOperation) tagGroup.operation;
	    		if (ro.getPurchaseOption() != null) {
	    			if (!(tagGroup instanceof TagGroupRI))
	    				logger.error("   --- Reserved Instance usage without reservation ID: " + tagGroup + ", " + usageData.get(0, tagGroup));
//	    			else if (tagGroup.product == productService.getProductByName(Product.rdsInstance))
//	    				logger.error("   --- RDS instance tagGroup: " + tagGroup);
	    		}
	    	}
	    }
	    
	    Map<Product, Integer> numHoursByProduct = product == null ? getNumHoursByProduct(reservationService, data) : null;
	    
		for (int i = 0; i < usageData.getNum(); i++) {
			// For each hour of usage...
			processHour(i, reservationService, usageData, costData, startMilli, numHoursByProduct);
		}
				
//		logger.info("process time in seconds: " + Seconds.secondsBetween(start, DateTime.now()).getSeconds());
	}
	
	private Map<Product, Integer> getNumHoursByProduct(ReservationService reservationService, CostAndUsageData data) {
	    Map<Product, Integer> numHoursByProduct = Maps.newHashMap();
    	for (ServiceCode sc: ServiceCode.values()) {
    		// EC2 and RDS Instances are broken out into separate products, so need to grab those
    		Product prod = null;
    		switch (sc) {
    		case AmazonEC2:
        		prod = productService.getProduct(Product.Code.Ec2Instance);
        		break;
    		case AmazonRDS:
    			prod = productService.getProduct(Product.Code.RdsInstance);
    			break;
    		default:
    			prod = productService.getProductByServiceCode(sc.name());
    			break;
    		}
    		if (reservationService.hasReservations(prod)) {
    			ReadWriteData rwd = data.getUsage(prod);
    		    if (rwd != null) {
    		    	numHoursByProduct.put(prod, rwd.getNum());
    		    }
    		}
    	}
    	return numHoursByProduct;
	}
	
	private void processHour(
			int hour,
			ReservationService reservationService,
			ReadWriteData usageData,
			ReadWriteData costData,
			long startMilli,
			Map<Product, Integer> numHoursByProduct) {
		// Process reservations for the hour using the ReservationsService loaded from the ReservationCapacityPoller (Before Jan 1, 2018)

		Set<ReservationArn> reservationArns = reservationService.getReservations(startMilli + hour * AwsUtils.hourMillis, product);
		    
	    List<TagGroupRI> riTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: usageData.getTagGroups(hour)) {
	    	if (tagGroup instanceof TagGroupRI) {
	    		riTagGroups.add((TagGroupRI) tagGroup);
	    	}
	    }
	    
	    for (ReservationArn reservationArn: reservationArns) {		    	
		    // Get the reservation info for the utilization and tagGroup in the current hour
		    ReservationService.ReservationInfo reservation = reservationService.getReservation(reservationArn);
		    
		    if (product == null && numHoursByProduct != null) {
		    	Integer numHours = numHoursByProduct.get(reservation.tagGroup.product);
		    	if (numHours != null && numHours <= hour) {
			    	// Only process the number of hours that we have in the
			    	// resource data to minimize the amount of unused data we include at the ragged end of data within
			    	// the month. This also keeps the numbers matching between non-resource and resource data sets.
			    	continue;
		    	}
		    }		    
		    
		    double reservedUnused = reservation.capacity;
		    TagGroup rtg = reservation.tagGroup;
						    
		    PurchaseOption purchaseOption = ((ReservationOperation) rtg.operation).getPurchaseOption();
		    
		    double savingsRate = 0.0;
		    if (startMilli < jan1_2018) {
		        InstancePrices instancePrices = prices.get(rtg.product);
			    double onDemandRate = instancePrices.getOnDemandRate(rtg.region, rtg.usageType);			    
		        savingsRate = onDemandRate - reservation.reservationHourlyCost - reservation.upfrontAmortized;
		    }
		    
            if (ReservationArn.debugReservationArn != null && hour == 0 && reservationArn == ReservationArn.debugReservationArn) {
            	logger.info("RI hour 0: capacity/initial reservedUnused=" + reservedUnused + ", reservationArn=" + reservationArn);
            }
            
		    for (TagGroupRI tg: riTagGroups) {
		    	if (tg.arn != reservationArn)
		    		continue;
		    	
			    // grab the RI tag group value
			    Double used = usageData.remove(hour, tg);
			    Double cost = null;
			    Double amort = null;
			    Double savings = null;
			    if (startMilli >= jan1_2018) {
				    /*
				     *  Cost, Amortization, and Savings will be in the map as of Jan. 1, 2018
				     */
				    cost = costData.remove(hour, tg);
				    if ((cost == null || cost == 0) && purchaseOption != PurchaseOption.AllUpfront)
				    	logger.warn("No cost in map for tagGroup: " + tg);			    
				    
				    amort = null;
				    if (purchaseOption != PurchaseOption.NoUpfront) {
					    // See if we have amortization in the map already
					    TagGroupRI atg = TagGroupRI.get(tg.account, tg.region, tg.zone, tg.product, Operation.getAmortized(purchaseOption), tg.usageType, tg.resourceGroup, tg.arn);
					    amort = costData.remove(hour, atg);
					    if (hour == 0 && amort == null)
					    	logger.warn("No amortization in map for tagGroup: " + atg);
				    }
				    
				    TagGroupRI stg = TagGroupRI.get(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(purchaseOption), tg.usageType, tg.resourceGroup, tg.arn);
				    savings = costData.remove(hour, stg);
				    if (hour == 0 && savings == null)
				    	logger.warn("No savings in map for tagGroup: " + stg);
			    }
			    
			    if (used != null && used > 0.0) {
			    	double adjustedUsed = convertFamilyUnits(used, tg.usageType, rtg.usageType);
				    // If CUR has recurring cost (starting 2018-01), then it's already in the map. Otherwise we have to compute it from the reservation
			    	double adjustedCost = (cost != null && cost > 0) ? cost : adjustedUsed * reservation.reservationHourlyCost;
			    	double adjustedAmortization = (amort != null && amort > 0) ? amort : adjustedUsed * reservation.upfrontAmortized;
			    	double adjustedSavings = (savings != null && savings > 0) ? savings : adjustedUsed * savingsRate;
				    reservedUnused -= adjustedUsed;
	                if (ReservationArn.debugReservationArn != null && hour == 0 && reservationArn == ReservationArn.debugReservationArn) {
	                	logger.info("RI hour 0: cost=" + adjustedCost + ", used=" + used + ", adjustedUsage=" + adjustedUsed + ", reservedUnused=" + reservedUnused + ", tg=" + tg);
	                }
				    if (rtg.account == tg.account) {
					    // Used by owner account, mark as used
					    TagGroup usedTagGroup = null;
					    usedTagGroup = tg.withOperation(Operation.getReservedInstances(purchaseOption));
					    add(usageData, hour, usedTagGroup, used);						    
					    add(costData, hour, usedTagGroup, adjustedCost);						    
					    // assign amortization
					    if (adjustedAmortization > 0.0) {
					        TagGroup amortTagGroup = tg.withOperation(Operation.getAmortized(purchaseOption));
						    add(costData, hour, amortTagGroup, adjustedAmortization);
					    }
				    }
				    else {
				    	// Borrowed by other account, mark as borrowed/lent
					    TagGroup borrowedTagGroup = tg.withOperation(Operation.getBorrowedInstances(purchaseOption));
					    TagGroup lentTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(purchaseOption), rtg.usageType, tg.resourceGroup);
					    add(usageData, hour, borrowedTagGroup, used);
					    add(costData, hour, borrowedTagGroup, adjustedCost);
					    add(usageData, hour, lentTagGroup, adjustedUsed);
					    add(costData, hour, lentTagGroup, adjustedCost);
					    // assign amortization
					    if (adjustedAmortization > 0.0) {
					        TagGroup borrowedAmortTagGroup = tg.withOperation(Operation.getBorrowedAmortized(purchaseOption));
					        TagGroup lentAmortTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getLentAmortized(purchaseOption), rtg.usageType, tg.resourceGroup);
						    add(costData, hour, borrowedAmortTagGroup, adjustedAmortization);
						    add(costData, hour, lentAmortTagGroup, adjustedAmortization);
					    }
				    }
				    // assign savings
			        TagGroup savingsTagGroup = tg.withOperation(Operation.getSavings(purchaseOption));
				    add(costData, hour, savingsTagGroup, adjustedSavings);
			    }
		    }

            if (ReservationArn.debugReservationArn != null && hour == 0 && reservationArn == ReservationArn.debugReservationArn) {
            	logger.info("RI hour 0: total unused=" + reservedUnused + ", reservationArn=" + reservationArn);
            }
		    // Unused
		    boolean haveUnused = Math.abs(reservedUnused) > 0.0001;
		    if (haveUnused) {			    	
			    ResourceGroup riResourceGroup = product == null ? null : rtg.resourceGroup;
			    TagGroup unusedTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUnusedInstances(purchaseOption), rtg.usageType, riResourceGroup);
			    add(usageData, hour, unusedTagGroup, reservedUnused);
			    double unusedHourlyCost = reservedUnused * reservation.reservationHourlyCost;
			    add(costData, hour, unusedTagGroup, unusedHourlyCost);

			    if (reservedUnused < 0.0) {
			    	logger.error("Too much usage assigned to RI: " + hour + ", unused=" + reservedUnused + ", tag: " + unusedTagGroup);
			    }
			    double unusedFixedCost = reservedUnused * reservation.upfrontAmortized;
			    if (reservation.upfrontAmortized > 0.0) {
				    // assign unused amortization to owner
			        TagGroup upfrontTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUnusedAmortized(purchaseOption), rtg.usageType, riResourceGroup);
				    add(costData, hour, upfrontTagGroup, unusedFixedCost);
			    }
				    
			    // subtract amortization and hourly rate from savings for owner
		        TagGroup savingsTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getSavings(purchaseOption), rtg.usageType, riResourceGroup);
			    add(costData, hour, savingsTagGroup, -unusedFixedCost - unusedHourlyCost);
		    }
	    }
	    
	    // Scan the usage and cost maps to clean up any leftover entries with TagGroupRI
	    cleanup(hour, usageData, "usage", startMilli, reservationService);
	    cleanup(hour, costData, "cost", startMilli, reservationService);
	}
	    
	private void cleanup(int hour, ReadWriteData data, String which, long startMilli, ReservationService reservationService) {
	    List<TagGroupRI> riTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: data.getTagGroups(hour)) {
	    	if (tagGroup instanceof TagGroupRI) {
	    		riTagGroups.add((TagGroupRI) tagGroup);
	    	}
	    }
	    
	    Map<Tag, Integer> leftovers = Maps.newHashMap();
	    for (TagGroupRI tg: riTagGroups) {
	    	Integer i = leftovers.get(tg.operation);
	    	i = 1 + ((i == null) ? 0 : i);
	    	leftovers.put(tg.operation, i);
	    	
//	    	if (tg.operation.isBonus()) {
//	    		logger.info("Bonus reservation at hour " + hour + ": " + reservationService.getReservation(tg.arn));
//	    	}

	    	Double v = data.remove(hour, tg);
	    	TagGroup newTg = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, tg.resourceGroup);
	    	add(data, hour, newTg, v);
	    }
	    for (Tag t: leftovers.keySet()) {
	    	DateTime time = new DateTime(startMilli + hour * AwsUtils.hourMillis, DateTimeZone.UTC);
	    	logger.info("Found " + leftovers.get(t) + " unconverted " + which + " RI TagGroups on hour " + hour + " (" + time + ") for operation " + t);
	    }
	}
}
