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

import org.apache.ivy.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.SavingsPlanArn;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class SavingsPlanProcessorTest {
	private static ProductService productService;
	public static AccountService accountService;
	private final Product ec2Instance = productService.getProduct(Product.Code.Ec2Instance);
	private final Product lambda = productService.getProduct(Product.Code.Lambda);
	private static Account a1;
	private static Account a2;
	

	private static final int numAccounts = 2;
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
		a1 = as.getAccountByName("Account1");
		a2 = as.getAccountByName("Account2");
		accountService = as;		
	}

	@BeforeClass
	public static void init() throws Exception {
		productService = new BasicProductService();
	}
	
	public class Datum {
		public TagGroup tagGroup;
		public double value;
		
		public Datum(TagGroup tagGroup, double value)
		{
			this.tagGroup = tagGroup;
			this.value = value;
		}
		
		public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, ResourceGroup resource, double value)
		{
			this.tagGroup = TagGroup.getTagGroup(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), resource);
			this.value = value;
		}

		public Datum(Account account, Region region, Zone zone, Product product, Operation operation, String usageType, ResourceGroup resource, String spArn, double value)
		{
			this.tagGroup = TagGroupSP.get(account, region, zone, product, operation, UsageType.getUsageType(usageType, "hours"), resource, SavingsPlanArn.get(spArn));
			this.value = value;
		}
	}
	
	private Map<TagGroup, Double> makeDataMap(Datum[] data) {
		Map<TagGroup, Double> m = Maps.newHashMap();
		for (Datum d: data) {
			m.put(d.tagGroup, d.value);
		}
		return m;
	}
	
	private void runTest(SavingsPlan sp, Datum[] usageData, Datum[] costData, Datum[] expectedUsage, Datum[] expectedCost, Product product) {
		CostAndUsageData caud = new CostAndUsageData(new DateTime("2019-12", DateTimeZone.UTC).getMillis(), null, null, null, null, null);
		if (product != null) {
			caud.putUsage(product, new ReadWriteData(0));
			caud.putCost(product, new ReadWriteData(0));
		}
		caud.getSavingsPlans().put(sp.arn.name, sp);

		Map<TagGroup, Double> hourUsageData = makeDataMap(usageData);
		Map<TagGroup, Double> hourCostData = makeDataMap(costData);

		List<Map<TagGroup, Double>> ud = new ArrayList<Map<TagGroup, Double>>();
		ud.add(hourUsageData);
		caud.getUsage(null).setData(ud, 0);
		List<Map<TagGroup, Double>> cd = new ArrayList<Map<TagGroup, Double>>();
		cd.add(hourCostData);
		caud.getCost(null).setData(cd, 0);
		
		SavingsPlanProcessor spp = new SavingsPlanProcessor(caud, accountService);
		spp.process(null);

		assertEquals("usage size wrong", expectedUsage.length, hourUsageData.size());
		for (Datum datum: expectedUsage) {
			assertNotNull("should have usage tag group " + datum.tagGroup, hourUsageData.get(datum.tagGroup));	
			assertEquals("wrong usage value for tag " + datum.tagGroup, datum.value, hourUsageData.get(datum.tagGroup), 0.001);
		}
		assertEquals("cost size wrong", expectedCost.length, hourCostData.size());
		for (Datum datum: expectedCost) {
			assertNotNull("should have cost tag group " + datum.tagGroup, hourCostData.get(datum.tagGroup));	
			assertEquals("wrong cost value for tag " + datum.tagGroup, datum.value, hourCostData.get(datum.tagGroup), 0.001);
		}
	}

	@Test
	public void testCoveredUsageNoUpfront() {
		String arn = "arn:aws:savingsplans::" + a1.name + ":savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555";
		SavingsPlan sp = new SavingsPlan(arn, PurchaseOption.NoUpfront, 0.10, 0);
		Datum[] usageData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusNoUpfront, "t3.micro", null, arn, 1.0),
			};
		Datum[] costData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusNoUpfront, "t3.micro", null, arn, 0.012),
			};
		Datum[] expectedUsageData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedNoUpfront, "t3.micro", null, 1.0),
			};
		Datum[] expectedCostData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedNoUpfront, "t3.micro", null, 0.012),
			};
		runTest(sp, usageData, costData, expectedUsageData, expectedCostData, null);
	}
	
	@Test
	public void testCoveredUsageNoUpfrontLambda() throws ResourceException {
		String arn = "arn:aws:savingsplans::" + a1.name + ":savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555";
		SavingsPlan sp = new SavingsPlan(arn, PurchaseOption.NoUpfront, 0.10, 0);
		Datum[] usageData = new Datum[]{
				new Datum(a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanBonusNoUpfront, "Lambda-GB-Second", null, arn, 2.4),
			};
		Datum[] costData = new Datum[]{
				new Datum(a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanBonusNoUpfront, "Lambda-GB-Second", null, arn, 0.000036),
			};
		Datum[] expectedUsageData = new Datum[]{
				new Datum(a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanUsedNoUpfront, "Lambda-GB-Second", null, 2.4),
			};
		Datum[] expectedCostData = new Datum[]{
				new Datum(a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanUsedNoUpfront, "Lambda-GB-Second", null, 0.000036),
			};
		runTest(sp, usageData, costData, expectedUsageData, expectedCostData, null);
		
		// Test with resources
		ResourceGroup rg = ResourceGroup.getResourceGroup(new String[]{"TagA"});
		usageData = new Datum[]{
				new Datum(a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanBonusNoUpfront, "Lambda-GB-Second", rg, arn, 2.4),
			};
		costData = new Datum[]{
				new Datum(a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanBonusNoUpfront, "Lambda-GB-Second", rg, arn, 0.000036),
			};
		expectedUsageData = new Datum[]{
				new Datum(a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanUsedNoUpfront, "Lambda-GB-Second", rg, 2.4),
			};
		expectedCostData = new Datum[]{
				new Datum(a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanUsedNoUpfront, "Lambda-GB-Second", rg, 0.000036),
			};
		runTest(sp, usageData, costData, expectedUsageData, expectedCostData, lambda);
	}
	
	@Test
	public void testCoveredUsagePartialUpfront() {
		String arn = "arn:aws:savingsplans::" + a1.name + ":savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555";
		SavingsPlan sp = new SavingsPlan(arn, PurchaseOption.PartialUpfront, 0.055, 0.045);
		Datum[] usageData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusPartialUpfront, "t3.micro", null, arn, 1.0),
			};
		Datum[] costData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusPartialUpfront, "t3.micro", null, arn, 0.01),
			};
		Datum[] expectedUsageData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedPartialUpfront, "t3.micro", null, 1.0),
			};
		Datum[] expectedCostData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedPartialUpfront, "t3.micro", null, 0.0055),
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanAmortizedPartialUpfront, "t3.micro", null, 0.0045),
			};
		runTest(sp, usageData, costData, expectedUsageData, expectedCostData, null);
	}

	@Test
	public void testCoveredUsageAllUpfront() {
		String arn = "arn:aws:savingsplans::" + a1.name + ":savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555";
		SavingsPlan sp = new SavingsPlan(arn, PurchaseOption.AllUpfront, 0.0, 0.10);
		Datum[] usageData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusAllUpfront, "t3.micro", null, arn, 1.0),
			};
		Datum[] costData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusAllUpfront, "t3.micro", null, arn, 0.012),
			};
		Datum[] expectedUsageData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedAllUpfront, "t3.micro", null, 1.0),
			};
		Datum[] expectedCostData = new Datum[]{
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedAllUpfront, "t3.micro", null, 0.0),
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanAmortizedAllUpfront, "t3.micro", null, 0.012),
			};
		runTest(sp, usageData, costData, expectedUsageData, expectedCostData, null);
	}
	
	@Test
	public void testCoveredUsagePartialUpfrontBorrowed() {
		String arn = "arn:aws:savingsplans::" + a1.name + ":savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555";
		SavingsPlan sp = new SavingsPlan(arn, PurchaseOption.PartialUpfront, 0.055, 0.045);
		Datum[] usageData = new Datum[]{
				new Datum(a2, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusPartialUpfront, "t3.micro", null, arn, 1.0),
			};
		Datum[] costData = new Datum[]{
				new Datum(a2, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusPartialUpfront, "t3.micro", null, arn, 0.01),
			};
		Datum[] expectedUsageData = new Datum[]{
				new Datum(a2, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBorrowedPartialUpfront, "t3.micro", null, 1.0),
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanLentPartialUpfront, "t3.micro", null, 1.0),
			};
		Datum[] expectedCostData = new Datum[]{
				new Datum(a2, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBorrowedPartialUpfront, "t3.micro", null, 0.0055),
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanLentPartialUpfront, "t3.micro", null, 0.0055),
				new Datum(a2, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBorrowedAmortizedPartialUpfront, "t3.micro", null, 0.0045),
				new Datum(a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanLentAmortizedPartialUpfront, "t3.micro", null, 0.0045),
			};
		runTest(sp, usageData, costData, expectedUsageData, expectedCostData, null);
	}
}
