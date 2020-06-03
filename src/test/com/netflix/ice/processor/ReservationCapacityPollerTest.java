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

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.ec2.model.OfferingClassType;
import com.amazonaws.services.ec2.model.ReservedInstances;
import com.amazonaws.services.ec2.model.ReservedInstancesId;
import com.amazonaws.services.ec2.model.ReservedInstancesModification;
import com.amazonaws.services.ec2.model.ReservedInstancesModificationResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.processor.CanonicalReservedInstances;
import com.netflix.ice.processor.ReservationCapacityPoller;
import com.netflix.ice.processor.ReservationCapacityPoller.Ec2Mods;
import com.netflix.ice.processor.ReservationService.ReservationKey;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.tag.Region;

public class ReservationCapacityPollerTest {
	static Ec2Mods ec2mods;
	private static final String resourceDir = "src/test/resources/";

	@BeforeClass
	public static void initMods() {
		List<ReservedInstancesModification> mods = Lists.newArrayList();
		ReservedInstancesModificationResult[] modificationResults = new ReservedInstancesModificationResult[]{
				new ReservedInstancesModificationResult().withReservedInstancesId("0bd43db3-dd52-4d5f-8770-642d2198ceb9"),
				new ReservedInstancesModificationResult().withReservedInstancesId("c4bcda44-e778-4e4e-aaa9-637fe88d5957"),
				new ReservedInstancesModificationResult().withReservedInstancesId("c5479449-f359-4ed1-8671-32cdc235ea24"),
			
		};
		ReservedInstancesModification mod = new ReservedInstancesModification()
			.withStatus("fulfilled")
			.withReservedInstancesIds(new ReservedInstancesId().withReservedInstancesId("1fcd999c-c669-46bf-9911-8b873f6e09b9"))
			.withModificationResults(modificationResults);
		
		mods.add(mod);
		ec2mods = new ReservationCapacityPoller(null).new Ec2Mods(mods);
	}
	
	@Test
	public void testEc2Mods() {
		String parentId = ec2mods.getModResId("0bd43db3-dd52-4d5f-8770-642d2198ceb9");
		assertTrue("Didn't find parent reservation", parentId.equals("1fcd999c-c669-46bf-9911-8b873f6e09b9"));		
	}
	
	@Test
	public void testGetFixedPrice() throws Exception {
		ReservedInstances ri = new ReservedInstances()
		.withReservedInstancesId("1fcd999c-c669-46bf-9911-8b873f6e09b9")
		.withInstanceType("i3.large")
		.withInstanceCount(1)
		.withFixedPrice((float) 0.0)
		.withUsagePrice((float) 0.0)
		.withOfferingType("All Upfront")
		.withStart((new DateTime("2018-10-01")).toDate())
		.withOfferingClass(OfferingClassType.Convertible)
		.withDuration(94608000L);

		CanonicalReservedInstances cri = new CanonicalReservedInstances("1", "ap-northeast-2", ri, "");		
		PriceListService pls = new PriceListService(resourceDir, null, null);
		ReservationCapacityPoller rcp = new ReservationCapacityPoller(null);

		double fixedPrice = rcp.getFixedPrice(cri, Region.AP_NORTHEAST_2, pls);
		assertEquals("Wrong fixed Price", 2429.0, fixedPrice, 0.01);
	}
	
	@Test
	public void testHandleEC2Modifications() throws Exception {
		ReservedInstances parentRI = new ReservedInstances()
		.withReservedInstancesId("1fcd999c-c669-46bf-9911-8b873f6e09b9")
		.withInstanceType("c4.2xlarge")
		.withInstanceCount(1)
		.withFixedPrice((float) 100.0)
		.withUsagePrice((float) 0.0)
		.withOfferingType("Partial Upfront");
	
		ReservedInstances childRI = new ReservedInstances()
		.withReservedInstancesId("0bd43db3-dd52-4d5f-8770-642d2198ceb9")
		.withInstanceType("c4.xlarge")
		.withInstanceCount(2)
		.withFixedPrice((float) 0.0)
		.withUsagePrice((float) 0.0)
		.withOfferingType("Partial Upfront");

	
		Map<ReservationKey, CanonicalReservedInstances> reservations = Maps.newHashMap();
		
		ReservationKey parentKey = new ReservationKey("1", "us-east-1", "1fcd999c-c669-46bf-9911-8b873f6e09b9");
		ReservationKey childKey = new ReservationKey("1", "us-east-1", "0bd43db3-dd52-4d5f-8770-642d2198ceb9");
		reservations.put(parentKey, new CanonicalReservedInstances("1", "us-east-1", parentRI, ""));
		reservations.put(childKey, new CanonicalReservedInstances("1", "us-east-1", childRI, ""));
		ReservationCapacityPoller rcp = new ReservationCapacityPoller(null);
		
		PriceListService pls = new PriceListService(resourceDir, null, null);
		
		rcp.handleEC2Modifications(reservations, ec2mods, Region.US_EAST_1, pls);
		
		CanonicalReservedInstances child = reservations.get(childKey);
		assertEquals("Wrong fixed price", 50.0, child.getFixedPrice(), 0.001);		
	}
	
	@Test
	public void testHandleEC2ModificationsConvertibleExchange() throws Exception {
		/*
		 * Test exchange of a convertible instance for an RI in a new instance family
		 * There won't be a modification record with parent association, so method
		 * needs to look up the fixed price in the price list.
		 */
		ReservedInstances parentRI = new ReservedInstances()
		.withReservedInstancesId("1fcd999c-c669-46bf-9911-8b873f6e09b9")
		.withInstanceType("c4.2xlarge")
		.withInstanceCount(1)
		.withFixedPrice((float) 2478.0)
		.withUsagePrice((float) 0.0)
		.withOfferingType("Partial Upfront")
		.withStart((new DateTime("2018-09-01")).toDate())
		.withOfferingClass(OfferingClassType.Convertible)
		.withDuration(31536000L);
	
		ReservedInstances childRI = new ReservedInstances()
		.withReservedInstancesId("5ac43db3-dd52-4d5f-8770-642d2198ceb9")
		.withInstanceType("c3.xlarge")
		.withInstanceCount(2)
		.withFixedPrice((float) 0.0)
		.withUsagePrice((float) 0.0)
		.withOfferingType("Partial Upfront")
		.withStart((new DateTime("2018-10-01")).toDate())
		.withOfferingClass(OfferingClassType.Convertible)
		.withDuration(31536000L);


	
		Map<ReservationKey, CanonicalReservedInstances> reservations = Maps.newHashMap();
		
		ReservationKey parentKey = new ReservationKey("1", "us-east-1", "1fcd999c-c669-46bf-9911-8b873f6e09b9");
		ReservationKey childKey = new ReservationKey("1", "us-east-1", "0bd43db3-dd52-4d5f-8770-642d2198ceb9");
		reservations.put(parentKey, new CanonicalReservedInstances("1", "us-east-1", parentRI, ""));
		reservations.put(childKey, new CanonicalReservedInstances("1", "us-east-1", childRI, ""));
		ReservationCapacityPoller rcp = new ReservationCapacityPoller(null);
		PriceListService pls = new PriceListService(resourceDir, null, null);
		
		rcp.handleEC2Modifications(reservations, ec2mods, Region.US_WEST_2, pls);
		
		CanonicalReservedInstances child = reservations.get(childKey);
		assertEquals("Wrong fixed price", 1760.0, child.getFixedPrice(), 0.001);
		
		// test a standard reservation
		childRI.setOfferingClass(OfferingClassType.Standard);
		reservations = Maps.newHashMap();
		reservations.put(parentKey, new CanonicalReservedInstances("1", "us-east-1", parentRI, ""));
		reservations.put(childKey, new CanonicalReservedInstances("1", "us-east-1", childRI, ""));
		rcp.handleEC2Modifications(reservations, ec2mods, Region.US_WEST_2, pls);
		child = reservations.get(childKey);
		assertEquals("Wrong fixed price", 1016.0, child.getFixedPrice(), 0.001);
	}
	
	@Test
	public void testMultiplier() {
		ReservationCapacityPoller rcp = new ReservationCapacityPoller(null);
		assertEquals("Wrong multipliers converting micro to xlarge", rcp.multiplier("xlarge") / rcp.multiplier("micro"), 16.0, 0.001);
		assertEquals("Wrong multipliers converting small to 4xlarge", rcp.multiplier("4xlarge") / rcp.multiplier("small"), 32.0, 0.001);
	}
	
	@Test
	public void testUpdateReservations() throws Exception {
		BasicReservationService reservationService = new BasicReservationService(ReservationPeriod.threeyear, PurchaseOption.PartialUpfront);
		ReservationCapacityPoller poller = new ReservationCapacityPoller(null);
		ProductService ps = new BasicProductService();
		AccountService as = new BasicAccountService();
		
		// Create the account records
		as.getAccountById("123456789012", "");
		
		Map<ReservationKey, CanonicalReservedInstances> reservationsFromApi = Maps.newHashMap();
		String res = "123456789012,AmazonEC2,us-east-1,4aaaaaaa-2222-4567-89ab-01234567890,,r3.xlarge,Availability Zone,us-east-1a,false,2017-03-14 05:57:49,2018-03-13 04:00:00,31536000,0.0,1028.0,2,Linux/UNIX,retired,USD,Partial Upfront,Hourly:0.063,,standard,"; 
		String res1 = "123456789012,AmazonRDS,ap-southeast-2,ri-2017-06-03-07-09-23-658,573bbbbb-7c6b-1234-5678-123456789012,db.t2.micro,,,false,2017-06-03 06:08:27,2018-06-03 06:08:27,31536000,0.0,79.0,2,postgresql,retired,USD,Partial Upfront,Hourly:0.012,null,null,";
		CanonicalReservedInstances ri = new CanonicalReservedInstances(res);
		CanonicalReservedInstances ri1 = new CanonicalReservedInstances(res1);
		
		reservationsFromApi.put(new ReservationKey(ri.getAccountId(), ri.getRegion(), ri.getReservationId()), ri);
		reservationsFromApi.put(new ReservationKey(ri1.getAccountId(), ri1.getRegion(), ri1.getReservationId()), ri1);
		poller.updateReservations(reservationsFromApi, as,
				DateTime.parse("2018-01-01T00:00:00Z").getMillis(), ps,
				new BasicResourceService(ps, new String[]{}, false), reservationService);
		assertEquals("Wrong number of reservations", reservationsFromApi.size(), reservationService.getReservations().size());
	}
}
