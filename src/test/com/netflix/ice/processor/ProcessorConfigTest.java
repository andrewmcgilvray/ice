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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.IceOptions;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.WorkBucketDataConfig;
import com.netflix.ice.processor.config.AccountConfig;
import com.netflix.ice.processor.config.BillingBucket;
import com.netflix.ice.processor.config.BillingDataConfig;
import com.netflix.ice.processor.pricelist.PriceListService;

public class ProcessorConfigTest {
	private AWSCredentialsProvider credentialsProvider;        
	private ProductService productService;
	private ReservationService reservationService;
	private PriceListService priceListService;
	private Properties props;
	
	class TestConfig extends ProcessorConfig {
		BillingDataConfig billingDataConfig = null;
		WorkBucketDataConfig workBucketDataConfig = null;
		
		public TestConfig(Properties properties,
				AWSCredentialsProvider credentialsProvider,
				ProductService productService,
				ReservationService reservationService,
				PriceListService priceListService)
				throws Exception {
			super(properties, credentialsProvider, productService,
					reservationService,
					priceListService);
		}
		
		@Override
	    protected void initZones() {			
		}
		
		@Override
	    protected Map<String, AccountConfig> getAccountsFromOrganizations() {
			Map<String, AccountConfig> accounts = Maps.newHashMap();
			return accounts;
		}
				
		@Override
		protected BillingDataConfig readBillingDataConfig(BillingBucket bb, String basename) {
			return billingDataConfig;
		}
		
		public void setBillingDataConfig(BillingDataConfig bdc) {
			billingDataConfig = bdc;
		}
		
		@Override
		protected WorkBucketDataConfig downloadWorkBucketDataConfig(boolean force) {
			return workBucketDataConfig;
		}
		
		public void setWorkBucketDataConfig(WorkBucketDataConfig wbdc) {
			workBucketDataConfig = wbdc;
		}
	}
		
	@Before
	public void init() {
		credentialsProvider = new DefaultAWSCredentialsProviderChain();        
        productService = new BasicProductService();
        reservationService = new BasicReservationService(null, null);
        priceListService = null;
        
		props = new Properties();
        props.setProperty(IceOptions.START_MONTH, "2019-01");
        props.setProperty(IceOptions.WORK_S3_BUCKET_NAME, "foo");
        props.setProperty(IceOptions.WORK_S3_BUCKET_REGION, "us-east-1");
        props.setProperty(IceOptions.BILLING_S3_BUCKET_NAME, "bar");
        props.setProperty(IceOptions.BILLING_S3_BUCKET_REGION, "us-east-1");        
        props.setProperty(IceOptions.EDP_DISCOUNTS, "2019-01:5,2019-02:8");
        props.setProperty(IceOptions.INCLUDE_ZERO_COST_USAGE_FOR_PRODUCTS, "");
	}

	@Test
	public void testEdpDiscounts() throws Exception {        
		ProcessorConfig config = new TestConfig(
				props,
	            credentialsProvider,
	            productService,
	            reservationService,
	            priceListService);
		
		// discount prior to start
		assertEquals("Wrong discount", 0.00, config.getDiscount(new DateTime("2018-12-31T00:00:00Z", DateTimeZone.UTC)), 0.0001);
		// discount equal to start
		assertEquals("Wrong discount", 0.05, config.getDiscount(new DateTime("2019-01-01T00:00:00Z", DateTimeZone.UTC)), 0.0001);
		// discount during first period
		assertEquals("Wrong discount", 0.05, config.getDiscount(new DateTime("2019-01-02T13:00:00Z", DateTimeZone.UTC)), 0.0001);
		// discount at start of second period
		assertEquals("Wrong discount", 0.08, config.getDiscount(new DateTime("2019-02-01T00:00:00Z", DateTimeZone.UTC)), 0.0001);
		// discount during second period
		assertEquals("Wrong discount", 0.08, config.getDiscount(new DateTime("2019-02-02T04:00:00Z", DateTimeZone.UTC)), 0.0001);

		assertEquals("Wrong discounted cost", 0.95, config.getDiscountedCost(new DateTime("2019-01-01T00:00:00Z", DateTimeZone.UTC), 1.0), 0.0001);
	}
	
	@Test
	public void testBillingDataConfig() throws Exception {        
		TestConfig config = new TestConfig(
				props,
	            credentialsProvider,
	            productService,
	            reservationService,
	            priceListService);
		
		Map<String, AccountConfig> accounts = Maps.newHashMap();
		String id = "123456789012";
		List<String> parents = Lists.newArrayList("org1");
				
		String configFileBody = 
				"accounts:\n" +
				"  - id: 123456789012\n" +
				"    name: name\n" +
				"    awsName: 'aws Name'\n" +
				"    parents: [org1]\n" +
				"    status: ACTIVE\n";
		config.setBillingDataConfig(new BillingDataConfig(configFileBody));

		config.processBillingDataConfig(accounts);
		
		assertEquals("Missing account", 1, accounts.size());
		AccountConfig ac = accounts.get(id);
		assertEquals("Wrong account ID", id, ac.id);
		assertEquals("Wrong account name", "name", ac.name);
		assertEquals("Wrong account awsName", "aws Name", ac.awsName);
		assertEquals("Wrong account parents", parents, ac.parents);
		assertEquals("Wrong account status", "ACTIVE", ac.status);
	}

	@Test
	public void testWorkBucketDataConfig() throws Exception {
		TestConfig config = new TestConfig(
				props,
	            credentialsProvider,
	            productService,
	            reservationService,
	            priceListService);
		
		Map<String, AccountConfig> accounts = Maps.newHashMap();
		String id1 = "123456789012";
		String id2 = "234567890123";
		
		// Initialize the accounts map with id1
		AccountConfig ac1 = new AccountConfig();
		ac1.setId(id1);
		ac1.setName("name1");
		ac1.setAwsName("awsName1");
		accounts.put(id1, ac1);
		
		// Initialize the work bucket data with id1 and id2
		String json = "{ \"accounts\": [ { \"name\": " + id1 + ", \"iceName\": \"workName1\", \"awsName\": \"workAwsName1\" }, { \"name\": " + id2 + ", \"iceName\": \"workName2\", \"awsName\": \"workAwsName2\"} ] }";
		config.setWorkBucketDataConfig(new WorkBucketDataConfig(json));
		
		// Make sure we only add id2 and don't overwrite id1
		config.processWorkBucketConfig(accounts);
		assertEquals("Missing account", 2, accounts.size());
		AccountConfig ac = accounts.get(id1);
		assertEquals("Wrong account ID", id1, ac.id);
		assertEquals("Wrong account name", "name1", ac.name);
		assertEquals("Wrong account awsName", "awsName1", ac.awsName);

		ac = accounts.get(id2);
		assertEquals("Wrong account ID", id2, ac.id);
		assertEquals("Wrong account name", "workName2", ac.name);
		assertEquals("Wrong account awsName", "workAwsName2", ac.awsName);
	}
	
	@Test
	public void testGetOrganizationAccounts() throws UnsupportedEncodingException, IOException {
	    final String resourcesDir = "src/test/resources/private/";
	    File bbConfig = new File(resourcesDir, "billing-bucket.yml");
	    File tags = new File(resourcesDir, "customTags.csv");
	    
	    if (!(bbConfig.exists() && tags.exists()))
	    	return;
	    	    
		String body = new String(Files.readAllBytes(bbConfig.toPath()), "UTF-8");
		List<String> customTags = Lists.newArrayList(new String(Files.readAllBytes(tags.toPath()), "UTF-8").split(","));
		BillingBucket bb = new BillingBucket(body);
		AwsUtils.workS3BucketRegion = "us-east-1";
		@SuppressWarnings("unused")
		Map<String, AccountConfig> accounts = ProcessorConfig.getOrganizationAccounts(bb, customTags);
	}
}
