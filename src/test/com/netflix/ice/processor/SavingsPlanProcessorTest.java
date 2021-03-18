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
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.processor.Datum;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.SavingsPlanArn;
import com.netflix.ice.tag.UsageType;

public class SavingsPlanProcessorTest {
	private static ProductService productService;
	public static AccountService accountService;
	private final Product ec2Instance = productService.getProduct(Product.Code.Ec2Instance);
	private final Product lambda = productService.getProduct(Product.Code.Lambda);
	private static Account a1;
	private static Account a2;
	private static SavingsPlanArn arn;
	

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
		arn = SavingsPlanArn.get("arn:aws:savingsplans::" + a1.name + ":savingsplan/abcdef70-abcd-5abc-4k4k-01236ab65555");
	}

	@BeforeClass
	public static void init() throws Exception {
		productService = new BasicProductService();
	}

	private Map<TagGroup, CostAndUsage> makeDataMap(Datum[] data) {
		Map<TagGroup, CostAndUsage> m = Maps.newHashMap();
		for (Datum d: data) {
			m.put(d.tagGroup, d.cau);
		}
		return m;
	}
	
	private void runTest(SavingsPlan sp, Datum[] data, Datum[] expected, Product product) {
		CostAndUsageData caud = new CostAndUsageData(new DateTime("2019-12", DateTimeZone.UTC).getMillis(), null, null, null, null, null);
		if (product != null) {
			caud.put(product, new DataSerializer(1));
		}
		caud.getSavingsPlans().put(sp.tagGroup.arn, sp);

		Map<TagGroup, CostAndUsage> hourData = makeDataMap(data);

		List<Map<TagGroup, CostAndUsage>> rawCau = new ArrayList<Map<TagGroup, CostAndUsage>>();
		rawCau.add(hourData);
		caud.get(product).setData(rawCau, 0);
				
		SavingsPlanProcessor spp = new SavingsPlanProcessor(caud, accountService);
		spp.process(product);

		assertEquals("data size wrong", expected.length, hourData.size());
		for (Datum datum: expected) {
			assertNotNull("should have tag group " + datum.tagGroup, hourData.get(datum.tagGroup));	
			assertEquals("wrong usage value for tag " + datum.tagGroup, datum.cau.usage, hourData.get(datum.tagGroup).usage, 0.001);
			assertEquals("wrong cost value for tag " + datum.tagGroup, datum.cau.cost, hourData.get(datum.tagGroup).cost, 0.001);
		}
	}
	
	private SavingsPlan newSavingsPlan(String usageType, PurchaseOption po, double recurring, double amort) {
		TagGroupSP tg = TagGroupSP.get(CostType.subscription, a1, Region.GLOBAL, null, productService.getProduct("Savings Plans for AWS Compute usage", "ComputeSavingsPlans"), Operation.getOperation("None"), UsageType.getUsageType(usageType, "hours"), null, arn);
		String term = "1yr";
		String offeringType = "ComputeSavingsPlans";
		DateTime start = new DateTime("2020-02", DateTimeZone.UTC);
		DateTime end = new DateTime("2021-02", DateTimeZone.UTC);
		return new SavingsPlan(tg, po, term, offeringType, start.getMillis(), end.getMillis(), recurring, amort);
	}

	@Test
	public void testCoveredUsageNoUpfront() throws ResourceException {
		SavingsPlan sp = newSavingsPlan("ComputeSP:1yrNoUpfront", PurchaseOption.NoUpfront, 0.10, 0);
		Datum[] data = new Datum[]{
				new Datum(CostType.recurring, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusNoUpfront, "t3.micro", null, arn, 0.012, 1),
			};
		Datum[] expected = new Datum[]{
				new Datum(CostType.recurring, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedNoUpfront, "t3.micro", null, 0.012, 1),
			};
		runTest(sp, data, expected, null);
	}
	
	@Test
	public void testCoveredUsageNoUpfrontLambda() throws ResourceException {
		SavingsPlan sp = newSavingsPlan("ComputeSP:1yrNoUpfront", PurchaseOption.NoUpfront, 0.10, 0);
		Datum[] data = new Datum[]{
				new Datum(CostType.recurring, a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanBonusNoUpfront, "Lambda-GB-Second", null, arn, 0.000036, 2.4),
			};
		Datum[] expected = new Datum[]{
				new Datum(CostType.recurring, a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanUsedNoUpfront, "Lambda-GB-Second", null, 0.000036, 2.4),
			};
		runTest(sp, data, expected, null);
		
		// Test with resources
		String rg = "TagA";
		data = new Datum[]{
				new Datum(CostType.recurring, a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanBonusNoUpfront, "Lambda-GB-Second", rg, arn, 0.000036, 2.4),
			};
		expected = new Datum[]{
				new Datum(CostType.recurring, a1, Region.AP_NORTHEAST_1, null, lambda, Operation.savingsPlanUsedNoUpfront, "Lambda-GB-Second", rg, 0.000036, 2.4),
			};
		runTest(sp, data, expected, lambda);
	}
	
	@Test
	public void testCoveredUsagePartialUpfront() throws ResourceException {
		SavingsPlan sp = newSavingsPlan("ComputeSP:1yrPartialUpfront", PurchaseOption.PartialUpfront, 0.055, 0.045);
		Datum[] data = new Datum[]{
				new Datum(CostType.recurring, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusPartialUpfront, "t3.micro", null, arn, 0.01, 1),
			};
		Datum[] expected = new Datum[]{
				new Datum(CostType.recurring, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedPartialUpfront, "t3.micro", null, 0.0055, 1),
				new Datum(CostType.amortization, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanAmortizedPartialUpfront, "t3.micro", null, 0.0045, 0),
			};
		runTest(sp, data, expected, null);
	}

	@Test
	public void testCoveredUsageAllUpfront() throws ResourceException {
		SavingsPlan sp = newSavingsPlan("ComputeSP:1yrAllUpfront", PurchaseOption.AllUpfront, 0.0, 0.10);
		Datum[] data = new Datum[]{
				new Datum(CostType.recurring, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusAllUpfront, "t3.micro", null, arn, 0.012, 1),
			};
		Datum[] expected = new Datum[]{
				new Datum(CostType.recurring, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanUsedAllUpfront, "t3.micro", null, 0, 1),
				new Datum(CostType.amortization, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanAmortizedAllUpfront, "t3.micro", null, 0.012, 0),
			};
		runTest(sp, data, expected, null);
	}
	
	@Test
	public void testCoveredUsagePartialUpfrontBorrowed() throws ResourceException {
		SavingsPlan sp = newSavingsPlan("ComputeSP:1yrPartialUpfront", PurchaseOption.PartialUpfront, 0.055, 0.045);
		Datum[] data = new Datum[]{
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBonusPartialUpfront, "t3.micro", null, arn, 0.01, 1),
			};
		Datum[] expected = new Datum[]{
				new Datum(CostType.recurring, a2, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBorrowedPartialUpfront, "t3.micro", null, 0.0055, 1),
				new Datum(CostType.recurring, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanLentPartialUpfront, "t3.micro", null, 0.0055, 1),
				new Datum(CostType.amortization, a2, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanBorrowedAmortizedPartialUpfront, "t3.micro", null, 0.0045, 0),
				new Datum(CostType.amortization, a1, Region.US_EAST_1, null, ec2Instance, Operation.savingsPlanLentAmortizedPartialUpfront, "t3.micro", null, 0.0045, 0),
			};
		runTest(sp, data, expected, null);
	}
}
