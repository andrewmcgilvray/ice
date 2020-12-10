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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Collection;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Zone.BadZone;
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

		tg2 = TagGroup.getTagGroup(as.getAccountById("111111111234", ""), Region.US_EAST_1, null, ps.getProduct(Product.Code.Rds), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		assertEquals("TagGroups with alias should be equivalent", tg1, tg2);
		
		Product p = ps.getProduct(Product.Code.RdsFull);
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
		
		TagGroup tga = TagGroup.getTagGroup(as.getAccountById("111111111345", ""), Region.US_EAST_1, null, ps.getProduct(Product.Code.DataTransfer), Operation.getOperation("PublicIP-Out"), UsageType.getUsageType("USW2-AWS-Out-Bytes", "GB"), null);
		TagGroup tgb = TagGroup.getTagGroup(as.getAccountById("111111111345", ""), Region.US_EAST_1, null, ps.getProduct(Product.Code.DataTransfer), Operation.getOperation("PublicIP-Out"), UsageType.getUsageType("USW1-AWS-Out-Bytes", "GB"), null);
		assertFalse("Should not be equal", tga.equals(tgb));
	}
	
	@Test
	public void testSerializeCsv() throws IOException, ResourceException {
		// No resource group
		AccountService as = new BasicAccountService();
		TagGroup tg1 = TagGroup.getTagGroup(as.getAccountById("111111111345", ""), Region.US_EAST_1, null, ps.getProduct("Amazon Relational Food Service", "AmazonRFS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		
        StringWriter out = new StringWriter();

		TagGroup.Serializer.serializeCsv(out, tg1);
		String expect = "111111111345,us-east-1,,AmazonRFS,CreateDBInstance,RDS:GP2-Storage,GB";
		String got = out.toString();
		assertEquals("no resource tag group csv incorrect", expect, got);
		
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountById("111111111345", ""), Region.US_EAST_1, null, ps.getProduct("Amazon Relational Food Service", "AmazonRFS"), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), ResourceGroup.getResourceGroup(new String[]{"Tag1"}));
		out = new StringWriter();
		TagGroup.Serializer.serializeCsv(out, tg2);
		expect = "111111111345,us-east-1,,AmazonRFS,CreateDBInstance,RDS:GP2-Storage,GB,Tag1";
		got = out.toString();
		assertEquals("resource tag group csv incorrect", expect, got);
	}
	
	@Test
	public void testDeserializeFile() throws IOException, BadZone {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		String dataFile = "src/test/resources/private/tagdb_test.gz";
		int numUserTags = 23;
		File file = new File(dataFile);
		
		if (!file.exists())
			return;
		
        logger.info("trying to read from " + file);
        InputStream is = null;
        DataInputStream in = null;
        is = new FileInputStream(file);
        is = new GZIPInputStream(is);
        in = new DataInputStream(is);
        
        //TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups = TagGroup.Serializer.deserializeTagGroups(as, ps, numUserTags, in);
        TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups = TagGroup.Serializer.deserializeTagGroups(as, ps, numUserTags, in);
        logger.info("Found " + tagGroupsWithResourceGroups.size() + " months of tag groups");
        logger.info("done reading " + file);
	}
	
}
