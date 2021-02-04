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
	
	@Override
	protected void processReservations(
			ReservationService reservationService,
			CostAndUsageData data,
			Long startMilli) {
		
//		DateTime start = DateTime.now();
		
		DataSerializer ds = data.get(product);
		
		// Scan the first hour and look for reservation usage with no ARN and log errors
	    for (TagGroup tagGroup: ds.getTagGroups(0)) {
	    	if (tagGroup.operation instanceof ReservationOperation) {
	    		ReservationOperation ro = (ReservationOperation) tagGroup.operation;
	    		if (ro.getPurchaseOption() != null) {
	    			if (!(tagGroup instanceof TagGroupRI))
	    				logger.error("   --- Reserved Instance usage without reservation ID: " + tagGroup + ", " + ds.get(0, tagGroup));
//	    			else if (tagGroup.product == productService.getProductByName(Product.rdsInstance))
//	    				logger.error("   --- RDS instance tagGroup: " + tagGroup);
	    		}
	    	}
	    }
	    
	    Map<Product, Integer> numHoursByProduct = product == null ? getNumHoursByProduct(reservationService, data) : null;
	    
		for (int i = 0; i < ds.getNum(); i++) {
			// For each hour of usage...
			processHour(i, reservationService, ds, startMilli, numHoursByProduct);
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
    		    if (data.get(prod) != null) {
    		    	numHoursByProduct.put(prod, data.getNum(prod));
    		    }
    		}
    	}
    	return numHoursByProduct;
	}
	
	private void processHour(
			int hour,
			ReservationService reservationService,
			DataSerializer ds,
			long startMilli,
			Map<Product, Integer> numHoursByProduct) {
		// Process reservations for the hour using the ReservationsService loaded from the ReservationCapacityPoller (Before Jan 1, 2018)

		Set<ReservationArn> reservationArns = reservationService.getReservations(startMilli + hour * AwsUtils.hourMillis, product);
		    
	    List<TagGroupRI> riTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: ds.getTagGroups(hour)) {
	    	if (tagGroup instanceof TagGroupRI && tagGroup.operation.isBonus()) {
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
		    	DataSerializer.CostAndUsage used = ds.remove(hour, tg);
		    	DataSerializer.CostAndUsage amort = null;
		    	DataSerializer.CostAndUsage savings = null;
			    if (startMilli >= jan1_2018) {
				    /*
				     *  Cost, Amortization, and Savings will be in the map as of Jan. 1, 2018
				     */
				    if ((used == null || used.cost == 0) && purchaseOption != PurchaseOption.AllUpfront)
				    	logger.warn("No cost in map for tagGroup: " + tg);			    
				    
				    if (purchaseOption != PurchaseOption.NoUpfront) {
					    // See if we have amortization in the map already
					    TagGroupRI atg = TagGroupRI.get(tg.account, tg.region, tg.zone, tg.product, Operation.getAmortized(purchaseOption), tg.usageType, tg.resourceGroup, tg.arn);
					    amort = ds.remove(hour, atg);
					    if (hour == 0 && amort == null)
					    	logger.warn("No amortization in map for tagGroup: " + atg);
				    }
				    
				    TagGroupRI stg = TagGroupRI.get(tg.account, tg.region, tg.zone, tg.product, Operation.getSavings(purchaseOption), tg.usageType, tg.resourceGroup, tg.arn);
				    savings = ds.remove(hour, stg);
				    if (hour == 0 && savings == null)
				    	logger.warn("No savings in map for tagGroup: " + stg);
			    }
			    
			    if (used != null && used.usage > 0.0) {
			    	double adjustedUsed = convertFamilyUnits(used.usage, tg.usageType, rtg.usageType);
				    // If CUR has recurring cost (starting 2018-01), then it's already in the map. Otherwise we have to compute it from the reservation
			    	double adjustedCost = (used.cost > 0) ? used.cost : adjustedUsed * reservation.reservationHourlyCost;
			    	double adjustedAmortization = (amort != null && amort.cost > 0) ? amort.cost : adjustedUsed * reservation.upfrontAmortized;
			    	double adjustedSavings = (savings != null && savings.cost > 0) ? savings.cost : adjustedUsed * savingsRate;
				    reservedUnused -= adjustedUsed;
	                if (ReservationArn.debugReservationArn != null && hour == 0 && reservationArn == ReservationArn.debugReservationArn) {
	                	logger.info("RI hour 0: cost=" + adjustedCost + ", used=" + used.usage + ", adjustedUsage=" + adjustedUsed + ", reservedUnused=" + reservedUnused + ", tg=" + tg);
	                }
				    if (rtg.account == tg.account) {
					    // Used by owner account, mark as used
					    TagGroup usedTagGroup = null;
					    usedTagGroup = tg.withOperation(Operation.getReservedInstances(purchaseOption));
					    ds.add(hour, usedTagGroup, adjustedCost, used.usage);
					    // assign amortization
					    if (adjustedAmortization > 0.0) {
					        TagGroup amortTagGroup = tg.withOperation(Operation.getAmortized(purchaseOption));
						    ds.add(hour, amortTagGroup, adjustedAmortization, 0);
					    }
				    }
				    else {
				    	// Borrowed by other account, mark as borrowed/lent
					    TagGroup borrowedTagGroup = tg.withOperation(Operation.getBorrowedInstances(purchaseOption));
					    TagGroup lentTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getLentInstances(purchaseOption), rtg.usageType, tg.resourceGroup);
					    ds.add(hour, borrowedTagGroup, adjustedCost, used.usage);
					    ds.add(hour, lentTagGroup, adjustedCost, adjustedUsed);
					    // assign amortization
					    if (adjustedAmortization > 0.0) {
					        TagGroup borrowedAmortTagGroup = tg.withOperation(Operation.getBorrowedAmortized(purchaseOption));
					        TagGroup lentAmortTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getLentAmortized(purchaseOption), rtg.usageType, tg.resourceGroup);
					        ds.add(hour, borrowedAmortTagGroup, adjustedAmortization, 0);
					        ds.add(hour, lentAmortTagGroup, adjustedAmortization, 0);
					    }
				    }
				    // assign savings
			        TagGroup savingsTagGroup = tg.withOperation(Operation.getSavings(purchaseOption));
			        ds.add(hour, savingsTagGroup, adjustedSavings, 0);
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
			    double unusedHourlyCost = reservedUnused * reservation.reservationHourlyCost;
			    ds.add(hour, unusedTagGroup, unusedHourlyCost, reservedUnused);

			    if (reservedUnused < 0.0) {
			    	logger.error("Too much usage assigned to RI: " + hour + ", unused=" + reservedUnused + ", tag: " + unusedTagGroup);
			    }
			    double unusedFixedCost = reservedUnused * reservation.upfrontAmortized;
			    if (reservation.upfrontAmortized > 0.0) {
				    // assign unused amortization to owner
			        TagGroup upfrontTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getUnusedAmortized(purchaseOption), rtg.usageType, riResourceGroup);
				    ds.add(hour, upfrontTagGroup, unusedFixedCost, 0);
			    }
				    
			    // subtract amortization and hourly rate from savings for owner
		        TagGroup savingsTagGroup = TagGroup.getTagGroup(rtg.account, rtg.region, rtg.zone, rtg.product, Operation.getSavings(purchaseOption), rtg.usageType, riResourceGroup);
			    ds.add(hour, savingsTagGroup, -unusedFixedCost - unusedHourlyCost, 0);
		    }
	    }
	    
	    // Scan the usage and cost maps to clean up any leftover entries with TagGroupRI
	    cleanup(hour, ds, startMilli, reservationService);
	}
	    
	private void cleanup(int hour, DataSerializer ds, long startMilli, ReservationService reservationService) {
	    List<TagGroupRI> riTagGroups = Lists.newArrayList();
	    for (TagGroup tagGroup: ds.getTagGroups(hour)) {
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

	    	DataSerializer.CostAndUsage v = ds.remove(hour, tg);
	    	TagGroup newTg = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, tg.resourceGroup);
	    	ds.add(hour, newTg, v);
	    }
	    for (Tag t: leftovers.keySet()) {
	    	DateTime time = new DateTime(startMilli + hour * AwsUtils.hourMillis, DateTimeZone.UTC);
	    	logger.info("Found " + leftovers.get(t) + " unconverted RI TagGroups on hour " + hour + " (" + time + ") for operation " + t);
	    }
	}
}
