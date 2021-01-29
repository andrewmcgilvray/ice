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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ivy.util.StringUtils;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.processor.ReservationService.ReservationPeriod;
import com.netflix.ice.processor.ReservationService.ReservationKey;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

public class ReservationProcessorTest {
    protected static Logger logger = LoggerFactory.getLogger(ReservationProcessorTest.class);
	private static final String resourceDir = "src/test/resources/";

	private final Product ec2Instance = productService.getProduct(Product.Code.Ec2Instance);
	private final Product rdsInstance = productService.getProduct(Product.Code.RdsInstance);
	private final Product elastiCache = productService.getProduct(Product.Code.ElastiCache);

    // reservationAccounts is a cross-linked list of accounts where each account
	// can borrow reservations from any other.
	private static Map<Account, Set<String>> reservationOwners = Maps.newHashMap();
	
	private static final int numAccounts = 3;
	public static List<Account> accounts = Lists.newArrayList();
	public static Map<String, AccountConfig> accountConfigs = Maps.newHashMap();
	static {
		// Auto-populate the accounts list based on numAccounts
		
		// Every account is a reservation owner for these tests
		List<String> products = Lists.newArrayList("ec2", "rds", "redshift");
		for (Integer i = 1; i <= numAccounts; i++) {
			// Create accounts of the form Account("111111111111", "Account1")
			String id = StringUtils.repeat(i.toString(), 12);
			String name = "Account" + i.toString();
			accountConfigs.put(id, new AccountConfig(id, name, null, null, null, products, null, null));			
		}
		AccountService as = new BasicAccountService(accountConfigs);
		for (Integer i = 1; i <= numAccounts; i++) {
			// Load the account list for the tests to use
			accounts.add(as.getAccountByName("Account" + i.toString()));
		}
		accountService = as;
		
		// Initialize the zones we use
		try {
			eu_west_1b = Region.EU_WEST_1.getZone("eu-west-1b");
			eu_west_1c = Region.EU_WEST_1.getZone("eu-west-1c");
			us_east_1a = Region.US_EAST_1.getZone("us-east-1a");
			us_east_1b = Region.US_EAST_1.getZone("us-east-1b");
			us_east_1c = Region.US_EAST_1.getZone("us-east-1c");
			us_west_2a = Region.US_WEST_2.getZone("us-west-2a");
			us_west_2b = Region.US_WEST_2.getZone("us-west-2b");
			us_west_2c = Region.US_WEST_2.getZone("us-west-2c");
			ap_southeast_2a = Region.AP_SOUTHEAST_2.getZone("ap-southeast-2a");
		} catch (BadZone e) {
		}
	}
	
	private static ProductService productService;
	private static ResourceService resourceService;
	public static AccountService accountService;
	private static PriceListService priceListService;
	private static Zone eu_west_1b;
	private static Zone eu_west_1c;
	private static Zone us_east_1a;
	private static Zone us_east_1b;
	private static Zone us_east_1c;
	private static Zone us_west_2a;
	private static Zone us_west_2b;
	private static Zone us_west_2c;
	private static Zone ap_southeast_2a;

	@BeforeClass
	public static void init() throws Exception {
		priceListService = new PriceListService(resourceDir, null, null);
		priceListService.init();

		productService = new BasicProductService();

		resourceService = new BasicResourceService(productService, new String[]{"TagKeyA"}, false);
	}
	
	private Map<TagGroup, CostAndUsage> makeDataMap(Datum[] data) {
		Map<TagGroup, CostAndUsage> m = Maps.newHashMap();
		for (Datum d: data) {
			m.put(d.tagGroup, d.cau);
		}
		return m;
	}
	
	private void runOneHourTestCostAndUsage(long startMillis, String[] reservationsCSV, Datum[] data, Datum[] expected, String debugFamily) throws Exception {
		runOneHourTestCostAndUsageWithOwners(startMillis, reservationsCSV, data, expected, debugFamily, reservationOwners.keySet(), null);
	}
	private void runOneHourTestCostAndUsageWithOwners(
			long startMillis, 
			String[] reservationsCSV, 
			Datum[] data, 
			Datum[] expected, 
			String debugFamily,
			Set<Account> rsvOwners,
			Product product) throws Exception {
		ReservationProcessor rp = new CostAndUsageReservationProcessor(rsvOwners, new BasicProductService(), priceListService);
		runOneHourTestWithOwnersAndProcessor(startMillis, reservationsCSV, data, expected, debugFamily, rp, product);
	}
	
	private void runOneHourTestWithOwnersAndProcessor(
			long startMillis, 
			String[] reservationsCSV, 
			Datum[] data, 
			Datum[] expected, 
			String debugFamily,
			ReservationProcessor reservationProcessor,
			Product product) throws Exception {
		
		CostAndUsageData caud = new CostAndUsageData(startMillis, null, null, TagCoverage.none, null, null);
		caud.enableTagGroupCache(true);
		if (product != null) {
			caud.put(product, new DataSerializer(1));
		}
		
		Map<TagGroup, CostAndUsage> hourData = makeDataMap(data);

		List<Map<TagGroup, CostAndUsage>> rawCau = new ArrayList<Map<TagGroup, CostAndUsage>>();
		rawCau.add(hourData);
		caud.get(product).setData(rawCau, 0);
		
		Region debugRegion = null;
		if (data.length > 0)
			debugRegion = data[0].tagGroup.region;
		else if (expected.length > 0)
			debugRegion = expected[0].tagGroup.region;

		runTest(startMillis, reservationsCSV, caud, product, debugFamily, debugRegion, reservationProcessor);

		assertEquals("cost and usage size wrong", expected.length, hourData.size());
		for (Datum datum: expected) {
			assertNotNull("should have tag group " + datum.tagGroup, hourData.get(datum.tagGroup));	
			assertEquals("wrong cost value for tag " + datum.tagGroup, datum.cau.cost, hourData.get(datum.tagGroup).cost, 0.001);
			assertEquals("wrong usage value for tag " + datum.tagGroup, datum.cau.usage, hourData.get(datum.tagGroup).usage, 0.001);
		}
	}
	
	private static String convertStartAndEnd(String res) {
		// If start and end times are in milliseconds, convert to AWS billing format
		String[] fields = res.split(",");
		if (!fields[9].contains("-")) {
			Long start = Long.parseLong(fields[9]);
			fields[9] = LineItem.amazonBillingDateFormat.print(new DateTime(start));
		}
		if (!fields[10].contains("-")) {
			Long end = Long.parseLong(fields[10]);
			fields[10] = LineItem.amazonBillingDateFormat.print(new DateTime(end));
		}
		return StringUtils.join(fields, ",");
	}
	
	public static void runTest(
			long startMillis, 
			String[] reservationsCSV, 
			CostAndUsageData data, 
			Product product, 
			String debugFamily, 
			Region debugRegion, 
			ReservationProcessor rp) throws Exception {

		logger.info("Test:");
		Map<ReservationKey, CanonicalReservedInstances> reservations = Maps.newHashMap();
		for (String res: reservationsCSV) {
			String[] fields = res.split(",");
			res = convertStartAndEnd(res);
			reservations.put(new ReservationKey(fields[0], fields[2], fields[3]), new CanonicalReservedInstances(res));
		}
				
		BasicReservationService reservationService = new BasicReservationService(ReservationPeriod.oneyear, PurchaseOption.AllUpfront);
		new ReservationCapacityPoller(null).updateReservations(reservations, accountService, startMillis, productService, resourceService, reservationService);
		
		if (startMillis >= CostAndUsageReservationProcessor.jan1_2018) {
			// Copy the reservations into the CostAndUsageData since we won't have processed RIFee records
			for (Reservation r: reservationService.getReservations().values())
				data.addReservation(r);
		}
		
		rp.setDebugHour(0);
		rp.setDebugFamily(debugFamily);
		Region[] debugRegions = new Region[]{ debugRegion };
		rp.setDebugRegions(debugRegions);
		DateTime start = new DateTime(startMillis);
		
		// Initialize the price lists
    	Map<Product, InstancePrices> prices = Maps.newHashMap();
    	Product p = productService.getProduct(Product.Code.Ec2Instance);
    	prices.put(p, priceListService.getPrices(start, ServiceCode.AmazonEC2));
    	p = productService.getProduct(Product.Code.RdsInstance);
    	if (reservationService.hasReservations(p))
    		prices.put(p, priceListService.getPrices(start, ServiceCode.AmazonRDS));
    	p = productService.getProduct(Product.Code.Redshift);
    	if (reservationService.hasReservations(p))
    		prices.put(p, priceListService.getPrices(start, ServiceCode.AmazonRedshift));
    	p = productService.getProduct(Product.Code.Elasticsearch);
    	if (reservationService.hasReservations(p))
    		prices.put(p, priceListService.getPrices(start, ServiceCode.AmazonES));
    	p = productService.getProduct(Product.Code.ElastiCache);
    	if (reservationService.hasReservations(p))
    		prices.put(p, priceListService.getPrices(start, ServiceCode.AmazonElastiCache));

		rp.process(reservationService, data, product, start, prices);
	}
	
	/*
	 * Test one AZ scoped full-upfront reservation that's used by the owner.
	 */
	@Test
	public void testUsedAllAZ() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.large", 0, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", 0.095, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", 0.175 - 0.095, 0),
		};
		
		Datum[] data = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn, 0, 1),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn, 0.175 - 0.095, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test one AZ scoped full-upfront reservation that isn't used.
	 */
	@Test
	public void testUnusedAllAZ() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,14,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		
		Datum[] data = new Datum[]{
		};
				
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.unusedInstancesAllUpfront, "m1.large", 0, 14),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.unusedAmortizedAllUpfront, "m1.large", 1.3345, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", -1.3345, 0),
		};

		/* Cost and Usage version */
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test two AZ scoped reservations - one NO and one ALL that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedNoAllAZ() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,0.0,1,Linux/UNIX (Amazon VPC),active,USD,No Upfront,Hourly:0.112",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesNoUpfront, "m1.large", 0.112, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.large", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", 0.175 - 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsNoUpfront, "m1.large", 0.175 - 0.112, 0),
		};

		
		Datum[] data = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesNoUpfront, "m1.large", null, arn2, 0, 1),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,0.0,1,Linux/UNIX (Amazon VPC),active,USD,No Upfront,Hourly:0.112",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn1, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesNoUpfront, "m1.large", null, arn2, 0.112, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn1, 0.175 - 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsNoUpfront, "m1.large", null, arn2, 0.175 - 0.112, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}


	/*
	 * Test two equivalent AZ scoped full-upfront reservation that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedSameAllAZ() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
						
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.large", 0, 2),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", 0.190, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", 0.175 * 2.0 - 0.190, 0),
		};

		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");

		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{				
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn1, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn2, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn1, 0.175 - 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn2, 0.175 - 0.095, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test one AZ scoped full-upfront reservations where one instance is used by the owner account and one borrowed by a second account. Three instances are unused.
	 */
	@Test
	public void testAllAZ() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Availability Zone,us-east-1a,false,2016-05-31 13:06:38,2017-05-31 13:06:37,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
						
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.small", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.lentInstancesAllUpfront, "m1.small", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.unusedInstancesAllUpfront, "m1.small", 0, 3),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.unusedAmortizedAllUpfront, "m1.small", 0.07054, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.borrowedAmortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.lentAmortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", 0.044 * 1.0 - 0.09406, 0), // penalty for unused all goes to owner
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", 0.044 * 1.0 - 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.borrowedInstancesAllUpfront, "m1.small", 0, 1),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");

		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Availability Zone,us-east-1a,false,2017-05-31 13:06:38,2018-05-31 13:06:37,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{				
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.small", null, arn1, 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.small", null, arn1, 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", null, arn1, 0.044 * 1.0 - 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", null, arn1, 0.044 * 1.0 - 0.02352, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test one Region scoped full-upfront reservation where one instance is used by the owner account in each of several AZs.
	 */
	@Test
	public void testAllRegionalMultiAZ() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2016-05-31 13:06:38,2017-05-31 13:06:37,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.small", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.small", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.small", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesAllUpfront, "m1.small", 0, 2),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.amortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.amortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedAmortizedAllUpfront, "m1.small", 2.0 * 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", 0.044 - 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.savingsAllUpfront, "m1.small", 0.044 - 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.savingsAllUpfront, "m1.small", 0.044 - 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.savingsAllUpfront, "m1.small", 2.0 * -0.02352, 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2017-05-31 13:06:38,2018-05-31 13:06:37,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.small", null, arn1, 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.amortizedAllUpfront, "m1.small", null, arn1, 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.amortizedAllUpfront, "m1.small", null, arn1, 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", null, arn1, 0.044 - 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.savingsAllUpfront, "m1.small", null, arn1, 0.044 - 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1c, ec2Instance, Operation.savingsAllUpfront, "m1.small", null, arn1, 0.044 - 0.02352, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}
	
	/*
	 * Test two full-upfront reservations - one AZ, one Region that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedOneAZOneRegion() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.large", 0, 2),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", 2.0 * 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", 2.0 * (0.175 - 0.095), 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn1, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn2, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn1, 0.175 - 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn2, 0.175 - 0.095, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test two full-upfront reservations - both AZ that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedAZ() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.large", 0, 2),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", 2.0 * 0.095, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", 2.0 * 0.175 - 2.0 * 0.095, 0),
		};

		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn1, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn2, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn1, 0.175 - 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn2, 0.175 - 0.095, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test two All Upfront reservations - both Region that are both used by the owner account.
	 */
	@Test
	public void testTwoUsedRegion() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.large", 0, 2),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", 2.0 * 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", 2.0 * (0.175 - 0.095), 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn1, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn2, 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn1, 0.175 - 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn2, 0.175 - 0.095, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test two full-upfront reservations - one AZ, one Region that are both used by a borrowing account.
	 */
	@Test
	public void testTwoUsedOneAZOneRegionBorrowed() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,2016-05-31 13:43:29,2017-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.lentInstancesAllUpfront, "m1.large", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.lentAmortizedAllUpfront, "m1.large", 0.095, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.lentInstancesAllUpfront, "m1.large", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.lentAmortizedAllUpfront, "m1.large", 0.095, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.borrowedAmortizedAllUpfront, "m1.large", 0.095, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", 0.175 - 0.095, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.borrowedAmortizedAllUpfront, "m1.large", 0.095, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.savingsAllUpfront, "m1.large", 0.175 - 0.095, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.borrowedInstancesAllUpfront, "m1.large", 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.borrowedInstancesAllUpfront, "m1.large", 0, 1),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Availability Zone,us-east-1a,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.large,Region,,false,2017-05-31 13:43:29,2018-05-31 13:43:28,31536000,0.0,835.0,1,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn2, 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn1, 0.095, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn2, 0.095, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn1, 0.175 - 0.095, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1b, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn2, 0.175 - 0.095, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test one Region scoped full-upfront reservation where four small instance reservations are used by one large instance in the owner account.
	 */
	@Test
	public void testAllRegionalFamily() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2016-05-31 13:06:38,2017-05-31 13:06:37,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.large", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", 0.094, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", 0.044 * 4.0 - 0.094, 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2017-05-31 13:06:38,2018-05-31 13:06:37,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.large", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.large", null, arn1, 0.094, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.large", null, arn1, 0.044 * 4.0 - 0.094, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test one Region scoped partial-upfront reservation where four small instance reservations are used by one large instance in the owner account.
	 */
	@Test
	public void testPartialRegionalFamily() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2016-05-31 13:06:38,2017-05-31 13:06:37,31536000,0.0,123.0,4,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.01",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "m1.large", null, arn1, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesPartialUpfront, "m1.large", 4.0 * 0.01, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedPartialUpfront, "m1.large", 4.0 * 0.014, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartialUpfront, "m1.large", 4.0 * (0.044 - 0.014 - 0.01), 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2017-05-31 13:06:38,2018-05-31 13:06:37,31536000,0.0,123.0,4,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.01",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "m1.large", null, arn1, 4.0 * 0.01, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedPartialUpfront, "m1.large", null, arn1, 4.0 * 0.014, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartialUpfront, "m1.large", null, arn1, 4.0 * (0.044 - 0.014 - 0.01), 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test one Region scoped full-upfront reservation where one instance is used by the owner account and one borrowed by a second account - 3 instances unused.
	 */
	@Test
	public void testAllRegional() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2016-05-31 13:06:38,2017-05-31 13:06:37,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesAllUpfront, "m1.small", 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.lentAmortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.borrowedAmortizedAllUpfront, "m1.small", 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedAmortizedAllUpfront, "m1.small", 3.0 * 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", 0.044 - 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", 0.044 - 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.savingsAllUpfront, "m1.small", 3.0 * -0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesAllUpfront, "m1.small", 0, 3),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.lentInstancesAllUpfront, "m1.small", 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.borrowedInstancesAllUpfront, "m1.small", 0, 1),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2017-05-31 13:06:38,2018-05-31 13:06:37,31536000,0.0,206.0,5,Linux/UNIX (Amazon VPC),active,USD,All Upfront",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.small", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.small", null, arn1, 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.small", null, arn1, 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", null, arn1, 0.044 - 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.small", null, arn1, 0.044 - 0.02352, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test two Region scoped full-upfront reservations where one instance from each is family borrowed by a third account.
	 */
	@Test
	public void testAllTwoRegionalFamilyBorrowed() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2016-05-31 13:06:38,2017-05-31 13:06:37,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
				"222222222222,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2016-05-31 13:06:38,2017-05-31 13:06:37,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		ReservationArn arn2 = ReservationArn.get(accounts.get(1), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
				
		Datum[] data = new Datum[]{
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.xlarge", null, arn1, 0, 0.5),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.xlarge", null, arn2, 0, 0.5),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.xlarge", 8.0 * (0.044 - 0.02352), 0),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.borrowedAmortizedAllUpfront, "m1.xlarge", 8.0 * 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.lentAmortizedAllUpfront, "m1.small", 4.0 * 0.02352, 0),
				new Datum(accounts.get(1), Region.US_EAST_1, null, ec2Instance, Operation.lentAmortizedAllUpfront, "m1.small", 4.0 * 0.02352, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.lentInstancesAllUpfront, "m1.small", 0, 4),
				new Datum(accounts.get(1), Region.US_EAST_1, null, ec2Instance, Operation.lentInstancesAllUpfront, "m1.small", 0, 4),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.borrowedInstancesAllUpfront, "m1.xlarge", 0, 1),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
					"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2017-05-31 13:06:38,2018-05-31 13:06:37,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
					"222222222222,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2017-05-31 13:06:38,2018-05-31 13:06:37,31536000,0.0,206.0,4,Linux/UNIX (Amazon VPC),active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.xlarge", null, arn1, 0, 0.5),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesAllUpfront, "m1.xlarge", null, arn2, 0, 0.5),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.xlarge", null, arn1, 4.0 * 0.02352, 0),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedAllUpfront, "m1.xlarge", null, arn2, 4.0 * 0.02352, 0),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.xlarge", null, arn1, 4.0 * (0.044 - 0.02352), 0),
				new Datum(accounts.get(2), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsAllUpfront, "m1.xlarge", null, arn2, 4.0 * (0.044 - 0.02352), 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}
	
	/*
	 * Test one Region scoped full-upfront RDS reservation where the instance is used.
	 */
	@Test
	public void testAllRDS() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonRDS,us-east-1,ri-2016-05-20-16-50-03-197,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,db.t2.small,,,false,2016-05-20 16:50:23,2017-05-20 16:50:23,31536000,0.0,195.0,1,mysql,active,USD,All Upfront,",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, rdsInstance, "ri-2016-05-20-16-50-03-197");
		
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.reservedInstancesAllUpfront, "db.t2.small.mysql", 0, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.amortizedAllUpfront, "db.t2.small.mysql", 0.0223, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.savingsAllUpfront, "db.t2.small.mysql", 0.034 - 0.0223, 0),
		};

		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.bonusReservedInstancesAllUpfront, "db.t2.small.mysql", null, arn1, 0, 1),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "db");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonRDS,us-east-1,ri-2016-05-20-16-50-03-197,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,db.t2.small,,,false,2017-05-20 16:50:23,2018-05-20 16:50:23,31536000,0.0,195.0,1,mysql,active,USD,All Upfront,",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.bonusReservedInstancesAllUpfront, "db.t2.small.mysql", null, arn1, 0, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.amortizedAllUpfront, "db.t2.small.mysql", null, arn1, 0.0223, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, rdsInstance, Operation.savingsAllUpfront, "db.t2.small.mysql", null, arn1, 0.034 - 0.0223, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}
	
	/*
	 * Test one Region scoped full-upfront RDS reservation where the instance is used.
	 */
	@Test
	public void testPartialRDS() throws Exception {
		long startMillis = DateTime.parse("2017-04-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonRDS,ap-southeast-2,ri-2017-02-01-06-08-23-918,573d345b-7d5d-42eb-a340-5c19bf82b338,db.t2.micro,,,false,2017-02-01 06:08:27,2018-02-01 06:08:27,31536000,0.0,79.0,2,postgresql,active,USD,Partial Upfront,Hourly:0.012",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.AP_SOUTHEAST_2, rdsInstance, "ri-2017-02-01-06-08-23-918");
		
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.reservedInstancesPartialUpfront, "db.t2.micro.postgres", 0.024, 2),
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.amortizedPartialUpfront, "db.t2.micro.postgres", 0.018, 0),
			new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.savingsPartialUpfront, "db.t2.micro.postgres", 2.0 * 0.028 - 0.018 - 2.0 * 0.012, 0),
		};

		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.bonusReservedInstancesPartialUpfront, "db.t2.micro.postgres", null, arn1, 0, 2),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "db");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-04-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonRDS,ap-southeast-2,ri-2017-02-01-06-08-23-918,573d345b-7d5d-42eb-a340-5c19bf82b338,db.t2.micro,,,false,2018-02-01 06:08:27,2019-02-01 06:08:27,31536000,0.0,79.0,2,postgresql,active,USD,Partial Upfront,Hourly:0.012",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.bonusReservedInstancesPartialUpfront, "db.t2.micro.postgres", null, arn1, 0.024, 2),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.amortizedPartialUpfront, "db.t2.micro.postgres", null, arn1, 0.018, 0),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, null, rdsInstance, Operation.savingsPartialUpfront, "db.t2.micro.postgres", null, arn1, 2.0 * 0.028 - 0.018 - 2.0 * 0.012, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}
	
	/*
	 * Test one Region scoped partial-upfront reservation that's used by the owner.
	 */
	@Test
	public void testUsedPartialRegion() throws Exception {
		long startMillis = DateTime.parse("2017-05-05T17:20:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Region,,false,2017-04-27 09:01:29,2018-04-27 09:01:28,31536000,0.0,1060.0,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.121",
		};
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.2xlarge", null, arn2, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesPartialUpfront, "c4.2xlarge", 0.121, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedPartialUpfront, "c4.2xlarge", 0.121, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartialUpfront, "c4.2xlarge", 0.398 - 0.121 - 0.121, 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-05-05T17:20:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Region,,false,2018-04-27 09:01:29,2019-04-27 09:01:28,31536000,0.0,1060.0,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.121",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.2xlarge", null, arn2, 0.121, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedPartialUpfront, "c4.2xlarge", null, arn2, 0.121, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartialUpfront, "c4.2xlarge", null, arn2, 0.398 - 0.121 - 0.121, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	/*
	 * Test one Region scoped partial-upfront reservation for Windows that's used by the owner.
	 */
	@Test
	public void testUsedPartialRegionWindows() throws Exception {
		long startMillis = DateTime.parse("2017-05-05T17:20:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,ap-southeast-2,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,t2.medium,Region,,false,2017-02-01 06:00:35,2018-02-01 06:00:34,31536000,0.0,289.0,1,Windows,active,USD,Partial Upfront,Hourly:0.033,",
		};
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.AP_SOUTHEAST_2, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "t2.medium.windows", null, arn2, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.reservedInstancesPartialUpfront, "t2.medium.windows", 0.033, 1),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.amortizedPartialUpfront, "t2.medium.windows", 0.033, 0),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.savingsPartialUpfront, "t2.medium.windows", 0.082 - 0.033 - 0.033, 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-05-05T17:20:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,ap-southeast-2,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,t2.medium,Region,,false,2018-02-01 06:00:35,2019-02-01 06:00:34,31536000,0.0,289.0,1,Windows,active,USD,Partial Upfront,Hourly:0.033,",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "t2.medium.windows", null, arn2, 0.033, 1),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.amortizedPartialUpfront, "t2.medium.windows", null, arn2, 0.033, 0),
				new Datum(accounts.get(0), Region.AP_SOUTHEAST_2, ap_southeast_2a, ec2Instance, Operation.savingsPartialUpfront, "t2.medium.windows", null, arn2, 0.082 - 0.033 - 0.033, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}

	@Test
	public void testUsedUnusedDifferentRegionAndBorrowedFamilyPartialRegion() throws Exception {
		long startMillis = DateTime.parse("2017-08-01").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-west-2,aaaaaaaa-382f-40b9-b2d3-8641b05313f9,,c4.large,Region,,false,2017-04-12 21:29:39,2018-04-12 21:29:38,31536000,0.0,249.85000610351562,20,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.0285",
			"222222222222,AmazonEC2,eu-west-1,bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd,,c4.xlarge,Region,,false,2017-03-08 09:00:00,2017-08-18 06:07:40,31536000,0.0,340.0,2,Linux/UNIX,retired,USD,Partial Upfront,Hourly:0.039",
		};
		ReservationArn arnB = ReservationArn.get(accounts.get(1), Region.EU_WEST_1, ec2Instance, "bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.2xlarge", null, arnB, 0, 0.25),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnB, 0, 1.5),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.borrowedInstancesPartialUpfront, "c4.2xlarge", 0.039 * 0.50, 0.25),
				new Datum(accounts.get(0), Region.US_WEST_2, null, ec2Instance, Operation.unusedInstancesPartialUpfront, "c4.large", 0.0285 * 20.0, 20),
				new Datum(accounts.get(0), Region.US_WEST_2, null, ec2Instance, Operation.unusedAmortizedPartialUpfront, "c4.large", 0.0285 * 20.0, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, null, ec2Instance, Operation.savingsPartialUpfront, "c4.large", -(0.0285 + 0.0285) * 20.0, 0),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.reservedInstancesPartialUpfront, "c4.xlarge", 0.039 * 1.5, 1.5),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", 0.039 * 1.5, 0),
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.borrowedAmortizedPartialUpfront, "c4.2xlarge", 0.039 * 0.5, 0),
				new Datum(accounts.get(1), Region.EU_WEST_1, null, ec2Instance, Operation.lentAmortizedPartialUpfront, "c4.xlarge", 0.039 * 0.5, 0),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", (0.226 - 0.039 - 0.039) * 1.5, 0),
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.savingsPartialUpfront, "c4.2xlarge", (0.226 - 0.039 - 0.039) * 0.5, 0),
				new Datum(accounts.get(1), Region.EU_WEST_1, null, ec2Instance, Operation.lentInstancesPartialUpfront, "c4.xlarge", 0.5 * 0.039, 0.5),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-08-01T00:00:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-west-2,aaaaaaaa-382f-40b9-b2d3-8641b05313f9,,c4.large,Region,,false,2018-04-12 21:29:39,2019-04-12 21:29:38,31536000,0.0,249.85000610351562,20,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.0285",
				"222222222222,AmazonEC2,eu-west-1,bbbbbbbb-0ce3-4ab0-8d0e-36deac008bdd,,c4.xlarge,Region,,false,2018-03-08 09:00:00,2018-08-18 06:07:40,31536000,0.0,340.0,2,Linux/UNIX,retired,USD,Partial Upfront,Hourly:0.039",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.2xlarge", null, arnB, 0.5 * 0.039, 0.25),
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.amortizedPartialUpfront, "c4.2xlarge", null, arnB, 0.5 * 0.039, 0),
				new Datum(accounts.get(0), Region.EU_WEST_1, eu_west_1b, ec2Instance, Operation.savingsPartialUpfront, "c4.2xlarge", null, arnB, 0.5 * (0.226 - 0.039 - 0.039), 0),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnB, 1.5 * 0.039, 1.5),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", null, arnB, 1.5 * 0.039, 0),
				new Datum(accounts.get(1), Region.EU_WEST_1, eu_west_1c, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", null, arnB, 1.5 * (0.226 - 0.039 - 0.039), 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}	

	@Test
	public void testUsedAndBorrowedPartialRegion() throws Exception {
		long startMillis = DateTime.parse("2017-05-05T17:20:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-west-2,aaaaaaaa-588b-46a2-8c05-cbcf87aed53d,,c3.4xlarge,Region,,false,2017-04-12 23:53:41,2018-04-12 23:53:40,31536000,0.0,2477.60009765625,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.19855",
			"222222222222,AmazonEC2,us-west-2,bbbbbbbb-1942-4e5e-892b-cec03ddb7816,,c3.4xlarge,Region,,false,2016-10-03 15:48:28,2017-10-03 15:48:27,31536000,0.0,2608.0,1,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.209"
		};
		ReservationArn arnA = ReservationArn.get(accounts.get(0), Region.US_WEST_2, ec2Instance, "aaaaaaaa-588b-46a2-8c05-cbcf87aed53d");
		ReservationArn arnB = ReservationArn.get(accounts.get(1), Region.US_WEST_2, ec2Instance, "bbbbbbbb-1942-4e5e-892b-cec03ddb7816");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c3.4xlarge", null, arnA, 0, 1),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c3.4xlarge", null, arnB, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.reservedInstancesPartialUpfront, "c3.4xlarge", 0.199, 1),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.borrowedInstancesPartialUpfront, "c3.4xlarge", 0.209, 1),
				new Datum(accounts.get(1), Region.US_WEST_2, null, ec2Instance, Operation.lentInstancesPartialUpfront, "c3.4xlarge", 0.209, 1),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.amortizedPartialUpfront, "c3.4xlarge", 0.283, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.borrowedAmortizedPartialUpfront, "c3.4xlarge", 0.298, 0),
				new Datum(accounts.get(1), Region.US_WEST_2, null, ec2Instance, Operation.lentAmortizedPartialUpfront, "c3.4xlarge", 0.298, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.savingsPartialUpfront, "c3.4xlarge", 0.84 - 0.283 - 0.199 + 0.84 - 0.298 - 0.209, 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-05-05T17:20:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-west-2,aaaaaaaa-588b-46a2-8c05-cbcf87aed53d,,c3.4xlarge,Region,,false,2018-04-12 23:53:41,2019-04-12 23:53:40,31536000,0.0,2477.60009765625,1,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.19855",
				"222222222222,AmazonEC2,us-west-2,bbbbbbbb-1942-4e5e-892b-cec03ddb7816,,c3.4xlarge,Region,,false,2017-10-03 15:48:28,2018-10-03 15:48:27,31536000,0.0,2608.0,1,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.209"
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c3.4xlarge", null, arnA, 0.199, 1),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.amortizedPartialUpfront, "c3.4xlarge", null, arnA, 0.283, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.savingsPartialUpfront, "c3.4xlarge", null, arnA, 0.84 - 0.283 - 0.199, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c3.4xlarge", null, arnB, 0.209, 1),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.amortizedPartialUpfront, "c3.4xlarge", null, arnB, 0.298, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.savingsPartialUpfront, "c3.4xlarge", null, arnB, 0.84 - 0.298 - 0.209, 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}
	
	@Test
	public void testUsedAndBorrowedPartialRegionAndAZ() throws Exception {
		long startMillis = DateTime.parse("2017-05-05T17:20:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-west-2,aaaaaaaa-08c5-4d02-99f3-d23e51968565,,c4.xlarge,Region,,false,2017-04-12 21:44:42,2018-04-12 21:44:41,31536000,0.0,503.5,15,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.057",
			"222222222222,AmazonEC2,us-west-2,bbbbbbbb-3452-4486-804a-a3d184474ab6,,c4.xlarge,Availability Zone,us-west-2b,false,2016-09-22 23:44:27,2017-09-22 23:44:26,31536000,0.0,590.0,2,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.067",
			"222222222222,AmazonEC2,us-west-2,cccccccc-31f5-463a-bc72-b6e53956184f,,c4.xlarge,Availability Zone,us-west-2a,false,2016-09-22 23:44:27,2017-09-22 23:44:26,31536000,0.0,590.0,1,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.067",
		};
		ReservationArn arnA = ReservationArn.get(accounts.get(0), Region.US_WEST_2, ec2Instance, "aaaaaaaa-08c5-4d02-99f3-d23e51968565");
		ReservationArn arnB = ReservationArn.get(accounts.get(1), Region.US_WEST_2, ec2Instance, "bbbbbbbb-3452-4486-804a-a3d184474ab6");
		ReservationArn arnC = ReservationArn.get(accounts.get(1), Region.US_WEST_2, ec2Instance, "cccccccc-31f5-463a-bc72-b6e53956184f");
		
		Datum[] data = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnC, 0, 1),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnA, 0, 8),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnB, 0, 2),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnA, 0, 3),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnA, 0, 4),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.reservedInstancesPartialUpfront, "c4.xlarge", 0.228, 4),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.reservedInstancesPartialUpfront, "c4.xlarge", 0.171, 3),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.reservedInstancesPartialUpfront, "c4.xlarge", 0.456, 8),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.borrowedInstancesPartialUpfront, "c4.xlarge", 0.134, 2),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.borrowedInstancesPartialUpfront, "c4.xlarge", 0.067, 1),
				new Datum(accounts.get(1), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.lentInstancesPartialUpfront, "c4.xlarge", 0.134, 2),
				new Datum(accounts.get(1), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.lentInstancesPartialUpfront, "c4.xlarge", 0.067, 1),
				
				new Datum(accounts.get(1), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.lentAmortizedPartialUpfront, "c4.xlarge", 2.0 * 0.0674, 0),
				new Datum(accounts.get(1), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.lentAmortizedPartialUpfront, "c4.xlarge", 0.0674, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.borrowedAmortizedPartialUpfront, "c4.xlarge", 2.0 * 0.0674, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.borrowedAmortizedPartialUpfront, "c4.xlarge", 0.0674, 0),
				
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", 4.0 * 0.0575, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", 3.0 * 0.0575, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", 8.0 * 0.0575, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", 4.0 * (0.199 - 0.0575 - 0.057), 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", 2.0 * (0.199 - 0.0674 - 0.067) + 3.0 * (0.199 - 0.0575 - 0.057), 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", 1.0 * (0.199 - 0.0674 - 0.067) + 8.0 * (0.199 - 0.0575 - 0.057), 0),
			};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "c4");
		
		/* Cost and Usage with recurring and amortization data in the DiscountedUsage lineitem */
		startMillis = DateTime.parse("2018-05-05T17:20:00Z").getMillis();
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonEC2,us-west-2,aaaaaaaa-08c5-4d02-99f3-d23e51968565,,c4.xlarge,Region,,false,2018-04-12 21:44:42,2019-04-12 21:44:41,31536000,0.0,503.5,15,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.057",
				"222222222222,AmazonEC2,us-west-2,bbbbbbbb-3452-4486-804a-a3d184474ab6,,c4.xlarge,Availability Zone,us-west-2b,false,2017-09-22 23:44:27,2018-09-22 23:44:26,31536000,0.0,590.0,2,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.067",
				"222222222222,AmazonEC2,us-west-2,cccccccc-31f5-463a-bc72-b6e53956184f,,c4.xlarge,Availability Zone,us-west-2a,false,2017-09-22 23:44:27,2018-09-22 23:44:26,31536000,0.0,590.0,1,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.067",
			};
		arnA = ReservationArn.get(accounts.get(0), Region.US_WEST_2, ec2Instance, "aaaaaaaa-08c5-4d02-99f3-d23e51968565");
		arnB = ReservationArn.get(accounts.get(1), Region.US_WEST_2, ec2Instance, "bbbbbbbb-3452-4486-804a-a3d184474ab6");
		arnC = ReservationArn.get(accounts.get(1), Region.US_WEST_2, ec2Instance, "cccccccc-31f5-463a-bc72-b6e53956184f");
		
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnC, 0.067, 1),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", null, arnC, 0.0674, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", null, arnC, (0.199 - 0.0674 - 0.067), 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnA, 8.0 * 0.057, 8),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", null, arnA, 8.0 * 0.0575, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2a, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", null, arnA, 8.0 * (0.199 - 0.0575 - 0.057), 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnB, 2.0 * 0.0674, 2),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", null, arnB, 2.0 * 0.067, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", null, arnB, 2.0 * (0.199 - 0.0674 - 0.067), 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnA, 3.0 * 0.057, 3),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", null, arnA, 3.0 * 0.0575, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2b, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", null, arnA, 3.0 * (0.199 - 0.0575 - 0.057), 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.xlarge", null, arnA, 4.0 * 0.057, 4),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.amortizedPartialUpfront, "c4.xlarge", null, arnA, 4.0 * 0.0575, 0),
				new Datum(accounts.get(0), Region.US_WEST_2, us_west_2c, ec2Instance, Operation.savingsPartialUpfront, "c4.xlarge", null, arnA, 4.0 * (0.199 - 0.0575 - 0.057), 0),
		};
		runOneHourTestCostAndUsage(startMillis, resCSV, data, expected, "m1");
	}
	
	/*
	 * Test one Region scoped partial-upfront reservation that's used by the owner in a resource Group.
	 */
	@Test
	public void testUsedPartialRegionResourceGroup() throws Exception {
		long startMillis = DateTime.parse("2017-05-05T17:20:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,2aaaaaaa-bbbb-cccc-ddddddddddddddddd,,c4.2xlarge,Region,,false,2017-04-27 09:01:29,2018-04-27 09:01:28,31536000,0.0,1060.0,2,Linux/UNIX,active,USD,Partial Upfront,Hourly:0.121,,,Foo:Bar",
		};
		ReservationArn arn2 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "2aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		String rg = "TagA";
		
		Datum[] data = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "c4.2xlarge", rg, arn2, 0, 1),
		};
		Datum[] expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesPartialUpfront, "c4.2xlarge", rg, 0.121, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedPartialUpfront, "c4.2xlarge", rg, 0.121, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartialUpfront, "c4.2xlarge", rg, 0.398 - 0.121 - 0.121, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesPartialUpfront, "c4.2xlarge", "", 0.121, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedAmortizedPartialUpfront, "c4.2xlarge", "", 0.121, 0),
				new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.savingsPartialUpfront, "c4.2xlarge", "", -0.121 - 0.121, 0),
			};
		runOneHourTestCostAndUsageWithOwners(startMillis, resCSV, data, expected, "c4", reservationOwners.keySet(), ec2Instance);		
	}
	
	/*
	 * Test Partial Upfront Amortized and Unused costs from CUR line items - first released 2018-01
	 */
	@Test
	public void testPartialUpfrontNetCosts() throws Exception {
		long startMillis = DateTime.parse("2019-01-01").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2018-07-01 00:00:01,2019-07-01 00:00:00,31536000,0.0,123.0,2,Linux/UNIX (Amazon VPC),active,USD,Partial Upfront,Hourly:0.01",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		/* One instance used, one unused */
		Datum[] data = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesPartialUpfront, "m1.small", null, arn1, 0.024, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedPartialUpfront, "m1.small", null, arn1, 0.014, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartialUpfront, "m1.small", null, arn1, 0.020, 0),
		};
		
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesPartialUpfront, "m1.small", 0.024, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.amortizedPartialUpfront, "m1.small", 0.014, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedAmortizedPartialUpfront, "m1.small", 0.014, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesPartialUpfront, "m1.small", 0.010, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsPartialUpfront, "m1.small", 0.020, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.savingsPartialUpfront, "m1.small", -(0.010 + 0.014), 0),
		};
		
		runOneHourTestCostAndUsageWithOwners(startMillis, resCSV, data, expected, "m1", reservationOwners.keySet(), null);		
	}
	
	/*
	 * Test No Upfront Unused costs from CUR line items - first released 2018-01
	 */
	@Test
	public void testNoUpfrontNetCosts() throws Exception {
		long startMillis = DateTime.parse("2019-01-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
			// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
			"111111111111,AmazonEC2,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,m1.small,Region,,false,2018-07-01 00:00:01,2019-07-01 00:00:00,31536000,0.0,0.0,2,Linux/UNIX (Amazon VPC),active,USD,No Upfront,Hourly:0.028",
		};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, ec2Instance, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		
		/* One instance used, one unused */
		Datum[] data = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.bonusReservedInstancesNoUpfront, "m1.small", null, arn1, 0.028, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsNoUpfront, "m1.small", null, arn1, 0.016, 0),
		};
		
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.reservedInstancesNoUpfront, "m1.small", 0.028, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.unusedInstancesNoUpfront, "m1.small", 0.028, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, us_east_1a, ec2Instance, Operation.savingsNoUpfront, "m1.small", 0.016, 0),
			new Datum(accounts.get(0), Region.US_EAST_1, null, ec2Instance, Operation.savingsNoUpfront, "m1.small", -0.028, 0),
		};
		
		runOneHourTestCostAndUsageWithOwners(startMillis, resCSV, data, expected, "m1", reservationOwners.keySet(), null);		
	}
	
	@Test
	public void testElastiCache() throws Exception {
		long startMillis = DateTime.parse("2019-01-01T00:00:00Z").getMillis();
		String[] resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonElastiCache,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,cache.m3.medium,Region,,false,2018-07-01 00:00:01,2019-07-01 00:00:00,31536000,0.0,0.0,2,Running Redis,active,USD,No Upfront,Hourly:0.10",
			};
		ReservationArn arn1 = ReservationArn.get(accounts.get(0), Region.US_EAST_1, elastiCache, "1aaaaaaa-bbbb-cccc-ddddddddddddddddd");
		/* One instance used, one unused */
		Datum[] data = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.bonusReservedInstancesNoUpfront, "cache.m3.medium.redis", null, arn1, 0.10, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.savingsNoUpfront, "cache.m3.medium.redis", null, arn1, 0.05, 0),
		};
		
		Datum[] expected = new Datum[]{
			new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.reservedInstancesNoUpfront, "cache.m3.medium.redis", 0.1, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.unusedInstancesNoUpfront, "cache.m3.medium.redis", 0.1, 1),
			new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.savingsNoUpfront, "cache.m3.medium.redis", -0.05, 0),
		};
		
		runOneHourTestCostAndUsageWithOwners(startMillis, resCSV, data, expected, "cache", reservationOwners.keySet(), null);		
		
		// Test with Legacy Heavy Utilization Instance
		resCSV = new String[]{
				// account, product, region, reservationID, reservationOfferingId, instanceType, scope, availabilityZone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount, productDescription, state, currencyCode, offeringType, recurringCharge
				"111111111111,AmazonElastiCache,us-east-1,1aaaaaaa-bbbb-cccc-ddddddddddddddddd,,cache.m3.medium,Region,,false,2018-07-01 00:00:01,2019-07-01 00:00:00,31536000,0.0,0.0,1,Running Redis,active,USD,Heavy Utilization,Hourly:0.10",
			};
		data = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.bonusReservedInstancesHeavy, "cache.m3.medium.redis", null, arn1, 0.10, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.savingsHeavy, "cache.m3.medium.redis", null, arn1, 0.05, 0),
			};
		expected = new Datum[]{
				new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.reservedInstancesHeavy, "cache.m3.medium.redis", 0.1, 1),
				new Datum(accounts.get(0), Region.US_EAST_1, null, elastiCache, Operation.savingsHeavy, "cache.m3.medium.redis", 0.05, 0),
			};
			
		runOneHourTestCostAndUsageWithOwners(startMillis, resCSV, data, expected, "cache", reservationOwners.keySet(), null);		
	}
}
