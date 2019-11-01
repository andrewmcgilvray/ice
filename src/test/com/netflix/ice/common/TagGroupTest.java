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
package com.netflix.ice.common;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class TagGroupTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static ProductService ps;
	
	@BeforeClass
	public static void init() {
		ps = new BasicProductService();
	}

	@Test
	public void testRdsTags() {		
		AccountService as = new BasicAccountService();
		
		TagGroup tg1 = TagGroup.getTagGroup(as.getAccountById("111111111234", ""), Region.US_EAST_1, null, ps.getProduct("AWS Relational Database Service", "AmazonRDS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountById("111111111234", ""), Region.US_EAST_1, null, ps.getProduct("AWS Relational Database Service", "AmazonRDS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("TagGroups should be equivalent", tg1, tg2);

		tg2 = TagGroup.getTagGroup(as.getAccountById("111111111234", ""), Region.US_EAST_1, null, ps.getProductByName("RDS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("TagGroups with alias should be equivalent", tg1, tg2);
		
		Product p = ps.getProductByName(Product.rdsFull);
		tg2 = TagGroup.getTagGroup(as.getAccountById("111111111234", ""), Region.US_EAST_1, null, p, Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("Product tags should be equivalent", System.identityHashCode(tg1.product), System.identityHashCode(p));
		assertEquals("Product tags should be equivalent", tg1.product, p);
		assertEquals("TagGroups with alias made with new product object should be equivalent", tg1, tg2);		
	}
	
	@Test
	public void testEquals() {
		AccountService as = new BasicAccountService();
		TagGroup tg1 = TagGroup.getTagGroup(as.getAccountById("111111111345", ""), Region.US_EAST_1, null, ps.getProduct("Amazon Relational Food Service", "AmazonRFS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountById("111111111345", ""), Region.US_EAST_1, null, ps.getProduct("Amazon Relational Food Service", "AmazonRFS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertTrue("Should be equal", tg1 == tg2);
		assertEquals("Should be equal", tg1, tg1);
		assertEquals("Should be equal", tg1, tg2);
		
		TagGroup tga = TagGroup.getTagGroup(as.getAccountById("111111111345", ""), Region.US_EAST_1, null, ps.getProductByName("Data Transfer"), Operation.getOperation("PublicIP-Out"), UsageType.getUsageType("USW2-AWS-Out-Bytes", "GB"), null);
		TagGroup tgb = TagGroup.getTagGroup(as.getAccountById("111111111345", ""), Region.US_EAST_1, null, ps.getProductByName("Data Transfer"), Operation.getOperation("PublicIP-Out"), UsageType.getUsageType("USW1-AWS-Out-Bytes", "GB"), null);
		assertFalse("Should not be equal", tga.equals(tgb));
	}
	
}
