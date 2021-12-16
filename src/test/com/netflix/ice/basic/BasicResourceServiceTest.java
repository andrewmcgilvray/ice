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
package com.netflix.ice.basic;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagConfig;
import com.netflix.ice.processor.CostAndUsageReport;
import com.netflix.ice.processor.LineItem;
import com.netflix.ice.processor.config.BillingDataConfig;
import com.netflix.ice.tag.ResourceGroup.ResourceException;

public class BasicResourceServiceTest {
    private static final String resourcesDir = "src/test/resources";

    private Account makeAccountWithDefaultTag(String id, String key, String value) {
    	Map<String, String> tags = null;
    	if (key != null) {
    		tags = Maps.newHashMap();
    		tags.put(key, value);
    	}
    	return new Account(id, "account", "account", "email", Lists.newArrayList("Org"), "ACTIVE", null, null, null, tags);
    }
    
	@Test
	public void testGetResourceGroup() throws ResourceException {
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
		CostAndUsageReport caur = new CostAndUsageReport(s3ObjectSummary, new File(resourcesDir, "ResourceTest-Manifest.json"), null, "");
		LineItem li = new LineItem(false, null, caur);		
		String[] item = {
				"123456789012", // PayerAccountId
				"DiscountedUsage", // LineItemType
				"2020-01-01T00:00:00Z", // Usage start date
				"", // aws:createdBy
				"foobar@example.com", // resourceTags/user:TagKey1
				"Prod", // resourceTags/user:TagKey2
				"", // resourceTags/user:TagKey3
				"serviceAPI", // resourceTags/user:TagKey4
		};

		li.setItems(item);
		ProductService ps = new BasicProductService();
		// include a tag not in the line item
		String[] customTags = new String[]{
				"TagKey2", "TagKey4", "VirtualTagKey"
			};
		ResourceService rs = new BasicResourceService(ps, customTags, false);
		rs.initHeader(li.getResourceTagsHeader(), "123456789012");
		Account a = makeAccountWithDefaultTag("123456789012", "VirtualTagKey", "1234");

		TagGroup tg = TagGroup.getTagGroup(CostType.recurring, a, Region.US_EAST_1, null, ps.getProduct(Product.Code.Ec2Instance), null, null, null);
		ResourceGroup resource = rs.getResourceGroup(tg, li, 0);
		UserTag[] tags = resource.getUserTags();
		UserTag[] expected = {UserTag.get("Prod"), UserTag.get("serviceAPI"), UserTag.get("1234")};
		for (int i = 0; i < tags.length; i++)
			assertEquals("ResourceGroup doesn't match at index " + i, expected[i], tags[i]);
	}
	
	@Test
	public void testGetUserTagKeys() {
		ProductService ps = new BasicProductService();
		String[] customTags = new String[]{
				"Environment", "Product"
			};
		ResourceService rs = new BasicResourceService(ps, customTags, false);
		List<UserTagKey> userTagKeys = rs.getUserTagKeys();
		assertEquals("userTags list length is incorrect", 2, userTagKeys.size());
	}
	
	@Test
	public void testSpecialUserTagKeyCharactersAndValues() throws IOException {
		Properties p = new Properties();
		p.load(new StringReader("prop.foo+bar=test\n"));
		for (String name: p.stringPropertyNames()) {
			assertEquals("Incorrect property name", "prop.foo+bar", name);
		}
	}
	
	@Test
	public void testTagConfig() {
		ProductService ps = new BasicProductService();
		String[] customTags = new String[]{
				"Environment"
			};
		List<TagConfig.KeyAlias> aliases = Lists.newArrayList(new TagConfig.KeyAlias("Alias", null, null));
		List<String> displayAliases = Lists.newArrayList("DisplayAlias");
		
		Map<String, List<String>> tagValues = Maps.newHashMap();
		tagValues.put("Prod", Lists.newArrayList("production", "prd"));
		tagValues.put("QA", Lists.newArrayList("test", "quality assurance"));
		TagConfig tc = new TagConfig("Environment", aliases, displayAliases, tagValues);
		
		ResourceService rs = new BasicResourceService(ps, customTags, false);
		List<TagConfig> configs = Lists.newArrayList();
		configs.add(tc);
		rs.setTagConfigs("234567890123", configs);
		
		List<UserTagKey> tagKeys = rs.getUserTagKeys();
		assertEquals("wrong user tag key name", "Environment", tagKeys.get(0).name);
		assertEquals("wrong user tag key display alias", "DisplayAlias", tagKeys.get(0).aliases.get(0));
	}
	
	@Test
	public void testGetUserTagValue() {
		ProductService ps = new BasicProductService();
		String[] customTags = new String[]{
				"Environment"
			};
		
		
		Map<String, List<String>> tagValues = Maps.newHashMap();
		tagValues.put("Prod", Lists.newArrayList("production", "prd"));
		tagValues.put("QA", Lists.newArrayList("test", "quality assurance"));
		TagConfig tc = new TagConfig("Environment", null, null, tagValues);

		BasicResourceService rs = new BasicResourceService(ps, customTags, false);
		List<TagConfig> configs = Lists.newArrayList();
		configs.add(tc);
		rs.setTagConfigs("234567890123", configs);

		String[] item = {
				"somelineitemid",	// LineItemId
				"Anniversary",		// BillType
				"234567890123",		// PayerAccountId
				"234567890123",		// UsageAccountId
				"DiscountedUsage",	// LineItemType
				"2017-09-01T00:00:00Z",	// UsageStartDate
				"2017-09-01T01:00:00Z", // UsageEndDate
				"APS2-InstanceUsage:db.t2.micro", // UsageType
				"CreateDBInstance:0014", // Operation
				"ap-southeast-2", // AvailabilityZone
				"arn:aws:rds:ap-southeast-2:123456789012:db:ss1v3i6xr3d1hss", // ResourceId
				"1.0000000000", // UsageAmount
				"", // NormalizationFactor
				"0.0000000000", // UnblendedRate
				"0.0000000000", // UnblendedCost
				"PostgreSQL, db.t2.micro reserved instance applied", // LineItemDescription
				"Amazon Relational Database Service", // ProductName
				"0.5", // normalizationSizeFactor
				"APS2-InstanceUsage:db.t2.micro", // usagetype
				"Partial Upfront", // PurchaseOption
				"0.0280000000", // publicOnDemandCost
				"Reserved", // term
				"Hrs", // unit
				"production", // resourceTags/user:Environment
		};
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
		CostAndUsageReport caur = new CostAndUsageReport(s3ObjectSummary, new File(resourcesDir, "LineItemTest-Manifest.json"), null, "");
		LineItem li = new LineItem(false, null, caur);		
		li.setItems(item);
		
		// Check for value in alias list
		rs.initHeader(li.getResourceTagsHeader(), "234567890123");		
		String tagValue = rs.getUserTagValue(li, rs.getUserTagKeys().get(0).name);
		assertEquals("Incorrect tag value alias", "Prod", tagValue);
		
		// Check for non-matching-case version of value
		item[item.length - 1] = "prod";
		tagValue = rs.getUserTagValue(li, rs.getUserTagKeys().get(0).name);
		assertEquals("Incorrect tag value alias", "Prod", tagValue);
		
		// Check for tag with leading/trailing white space
		item[item.length - 1] = " pr od ";
		tagValue = rs.getUserTagValue(li, rs.getUserTagKeys().get(0).name);
		assertEquals("Incorrect tag value alias when leading/trailing white space", "Prod", tagValue);
		
		// Check for tag with embedded white space in alias config
		item[item.length - 1] = "qualityassurance";
		tagValue = rs.getUserTagValue(li, rs.getUserTagKeys().get(0).name);
		assertEquals("Incorrect tag value alias when config had a space in the value", "QA", tagValue);
	}
	
	@Test
	public void testDefaultAccountTags() throws ResourceException {
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
		CostAndUsageReport caur = new CostAndUsageReport(s3ObjectSummary, new File(resourcesDir, "ResourceTest-Manifest.json"), null, "");
		LineItem li = new LineItem(false, null, caur);		
		String[] item = {
				"123456789012", // PayerAccountId
				"DiscountedUsage", // LineItemType
				"2020-01-01T00:00:00Z", // Usage start date
				"", // aws:createdBy
				"foobar@example.com", // resourceTags/user:Email
				"", // resourceTags/user:Environment
				"", // resourceTags/user:environment
				"serviceAPI", // resourceTags/user:Product
		};
		li.setItems(item);
		ProductService ps = new BasicProductService();
		String[] customTags = new String[]{
				"Environment", "Product"
			};
		Account a = makeAccountWithDefaultTag("123456789012", "Environment", "Prod");
		
		ResourceService rs = new BasicResourceService(ps, customTags, false);
		rs.initHeader(li.getResourceTagsHeader(), "123456789012");

		TagGroup tg = TagGroup.getTagGroup(CostType.recurring, a, null, null, ps.getProduct(Product.Code.Ec2Instance), null, null, null);

		ResourceGroup resourceGroup = rs.getResourceGroup(tg, li, new DateTime(item[2], DateTimeZone.UTC).getMillis());
		UserTag[] userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set", "Prod", userTags[0].name);
		
		//
		// Test multiple default tags with effective dates
		//
		// First set the effective date for a new value
		a = makeAccountWithDefaultTag("123456789012", "Environment", "Prod/2020-02=Dev");
		tg = TagGroup.getTagGroup(CostType.recurring, a, null, null, ps.getProduct(Product.Code.Ec2Instance), null, null, null);
		resourceGroup = rs.getResourceGroup(tg, li, new DateTime(item[2], DateTimeZone.UTC).getMillis());
		userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set correctly", "Prod", userTags[0].name);
		
		// Now check after the effective date of the second value
		item[2] = "2020-02-01T00:00:00Z";
		li.setItems(item);
		resourceGroup = rs.getResourceGroup(tg, li, new DateTime(item[2], DateTimeZone.UTC).getMillis());
		userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set correctly", "Dev", userTags[0].name);
		
		// Make sure out-of-time-ordered effective dates don't break things
		a = makeAccountWithDefaultTag("123456789012", "Environment", "Prod/2020-02=Dev/2018=QA/2018-02=Test");
		tg = TagGroup.getTagGroup(CostType.recurring, a, null, null, ps.getProduct(Product.Code.Ec2Instance), null, null, null);

		item[2] = "2018-01-01T00:00:00Z";
		li.setItems(item);
		resourceGroup = rs.getResourceGroup(tg, li, new DateTime(item[2], DateTimeZone.UTC).getMillis());
		userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set correctly", "QA", userTags[0].name);
		
		item[2] = "2019-01-01T00:00:00Z";
		li.setItems(item);
		resourceGroup = rs.getResourceGroup(tg, li, new DateTime(item[2], DateTimeZone.UTC).getMillis());
		userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set correctly", "Test", userTags[0].name);
		
		// Check that we can stop setting values with a trailing '='
		a = makeAccountWithDefaultTag("123456789012", "Environment", "Prod/2020-02=/2018=QA/2018-02=Test");
		tg = TagGroup.getTagGroup(CostType.recurring, a, null, null, ps.getProduct(Product.Code.Ec2Instance), null, null, null);
		item[2] = "2020-02-21T00:00:00Z";
		li.setItems(item);
		resourceGroup = rs.getResourceGroup(tg, li, new DateTime(item[2], DateTimeZone.UTC).getMillis());
		userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set correctly", "", userTags[0].name);
		
		// Check that we ignore a trailing '/'
		a = makeAccountWithDefaultTag("123456789012", "Environment", "Prod/2020-02=/2018=QA/2018-02=Test/");
		tg = TagGroup.getTagGroup(CostType.recurring, a, null, null, ps.getProduct(Product.Code.Ec2Instance), null, null, null);
		item[2] = "2020-02-21T00:00:00Z";
		li.setItems(item);
		resourceGroup = rs.getResourceGroup(tg, li, new DateTime(item[2], DateTimeZone.UTC).getMillis());
		userTags = resourceGroup.getUserTags();
		assertEquals("default resource group not set correctly", "", userTags[0].name);		
	}
	
	@Test
	public void testGetUserTagCoverage() {
		ProductService ps = new BasicProductService();
		String[] customTags = new String[]{
				"Environment", "Department", "Email"
			};
		
		class TestLineItem extends LineItem {			
			TestLineItem(String[] items) {
				super(items);
			}
			
		    @Override
		    public int getResourceTagsSize() {
		    	if (items.length <= 0)
		    		return 0;
		    	return items.length;
		    }

		    @Override
		    public String getResourceTag(int index) {
		    	if (items.length <= index)
		    		return "";
		    	return items[index];
		    }

			@Override
			public String[] getResourceTagsHeader() {
				return null;
			}

			@Override
			public Map<String, String> getResourceTags() {
				return null;
			}

			@Override
			public boolean isReserved() {
				return false;
			}

			@Override
			public String getPricingUnit() {
				return null;
			}

			@Override
			public String getLineItemId() {
				return null;
			}
			
			@Override
			public String getPayerAccountId() {
				return "123456789012";
			}
		}
		ResourceService rs = new BasicResourceService(ps, customTags, false);
		rs.initHeader(new String[]{ "user:Email", "user:Department", "user:Environment" }, "123456789012");
		LineItem lineItem = new TestLineItem(new String[]{ "joe@company.com", "1234", "" });
		boolean[] coverage = rs.getUserTagCoverage(lineItem);
		
		assertEquals("Environment isn't first user tag", "Environment", rs.getUserTagKeys().get(0).name);
		assertFalse("Environment is set", coverage[0]);
		assertTrue("Department not set", coverage[1]);
		assertTrue("Email not set", coverage[2]);
	}
	
	private ResourceGroup getResourceGroup(String yaml, String start, String[] tags, String[] customTags, Account payerAccount, Account account) throws Exception {
		List<TagConfig> tagConfigs = Lists.newArrayList();
		if (yaml.startsWith("tags:")) {
			tagConfigs = new BillingDataConfig(yaml).tags;
		}
		else {
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			TagConfig tc = new TagConfig();
			tc = mapper.readValue(yaml, tc.getClass());
			tagConfigs.add(tc);
		}
		
		ProductService ps = new BasicProductService();
		ResourceService rs = new BasicResourceService(ps, customTags, false);
		rs.setTagConfigs(payerAccount.getId(), tagConfigs);
		
		S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
		s3ObjectSummary.setLastModified(new Date());
		CostAndUsageReport caur = new CostAndUsageReport(s3ObjectSummary, new File(resourcesDir, "ResourceTest-Manifest.json"), null, "");
		LineItem li = new LineItem(false, null, caur);		
		
		rs.initHeader(li.getResourceTagsHeader(), payerAccount.getId());
		
		String[] item = {
				"123456789012", // PayerAccountId
				"DiscountedUsage", // LineItemType
				start, // Usage start date
				"", // aws:createdBy
				"", "", "", "",
		};
		for (int i = 3; i < item.length; i++)
			item[i] = tags[i-3];
		
		// Test with mapped value
		li.setItems(item);
		TagGroup tg = TagGroup.getTagGroup(CostType.recurring, account, Region.US_EAST_1, null, ps.getProduct(Product.Code.Ec2Instance), null, null, null);
		return rs.getResourceGroup(tg, li, new DateTime(item[2], DateTimeZone.UTC).getMillis());
	}
	
	@Test
	public void testTagKeyAlias() throws Exception {
		String yaml = "" +
		"name: TagKey1\n" +
		"aliases:\n" +
		"  - name: 'aws:createdBy'\n" +
		"  - name: TagKey2\n";
		
		String[] customTags = new String[]{"TagKey1", "TagKey3"};
		Account payerAccount = makeAccountWithDefaultTag("123456789012", null, null);
		String start = "2020-01-01T00:00:00Z";
		
		// Test without alias
		String[] tags = { "", "SrcValue1", "", "SrcValue3", "SrcValue4" };		
		ResourceGroup resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"SrcValue1", "SrcValue3"}), resource);
		
		// Test with aws:createdBy value as alias
		tags = new String[]{ "awsCreatedByValue", "", "", "SrcValue3", "SrcValue4" };		
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"awsCreatedByValue", "SrcValue3"}), resource);
	}
	
	@Test
	public void testGetTagGroupMapped() throws Exception {
		// Mapping rules for new virtual tag
		String yaml = "" +
		"name: DestKey\n" +
		"aliases:\n" +
		"  - name: TagKey2\n" +
		"values:\n" +
		"  DestValueDefault: [DestValueDefaultAlias]\n" +
		"mapped:\n" +
		"  - maps:\n" +
		"      DestValue1:\n" +
		"        key: TagKey4\n" +
		"        operator: isOneOf\n" +
		"        values: [SrcValue4a]\n" +
		"      DestValue2:\n" +
		"        key: TagKey4\n" +
		"        operator: isOneOf\n" +
		"        values: [SrcValue4b]\n" +
		"";

		String[] customTags = new String[]{"DestKey", "TagKey4"};
		Account payerAccount = makeAccountWithDefaultTag("123456789012", "DestKey", "DestValueDefault");
		String start = "2020-01-01T00:00:00Z";
		
		String[] tags = { "", "SrcValue1", "", "", "SrcValue4a" };
		
		ResourceGroup resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"DestValue1", "SrcValue4a"}), resource);
		
		// Test with value on resource
		tags = new String[]{ "", "TagValue1", "test", "", "SrcValue4a" };
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"test", "SrcValue4a"}), resource);
		
		// Test without mapped value or resource tag - should use account default
		tags = new String[]{ "", "TagValue1", "", "", "" };
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"DestValueDefault", null}), resource);
		
		// Test include filtering
		String yamlWithFilters = yaml +
				"    include: [123456789012]\n";
		tags = new String[]{ "", "TagValue1", "", "", "SrcValue4a" };
		resource = getResourceGroup(yamlWithFilters, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"DestValue1", "SrcValue4a"}), resource);
		
		Account account = makeAccountWithDefaultTag("234567890123", null, null);
		resource = getResourceGroup(yamlWithFilters, start, tags, customTags, payerAccount, account);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{null, "SrcValue4a"}), resource);
		
		// Test exclude filtering
		yamlWithFilters = yaml +
				"    exclude: [123456789012]\n";
		
		resource = getResourceGroup(yamlWithFilters, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"DestValueDefault", "SrcValue4a"}), resource);
		
		resource = getResourceGroup(yamlWithFilters, start, tags, customTags, payerAccount, account);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"DestValue1", "SrcValue4a"}), resource);
		
		// Test start date
		String yamlWithStart = yaml + 
				"  - start: 2020-02\n" +
				"    maps:\n" +
				"      DestValue1a:\n" +
				"        key: TagKey4\n" +
				"        operator: isOneOf\n" +
				"        values: [SrcValue4a]\n" +
				"      DestValue2a:\n" +
				"        key: TagKey4\n" +
				"        operator: isOneOf\n" +
				"        values: [SrcValue4b]\n" +
				"";
		resource = getResourceGroup(yamlWithStart, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"DestValue1", "SrcValue4a"}), resource);
		
		start = "2020-02-01T00:00:00Z";
		resource = getResourceGroup(yamlWithStart, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"DestValue1a", "SrcValue4a"}), resource);		
	}
	
	@Test
	public void testGetTagGroupMappedMultipleSrcKeys() throws Exception {
		Account payerAccount = makeAccountWithDefaultTag("123456789012", "DestKey", "DestValueDefault");
		
		// Mapping rules for new virtual tag
		String yaml = "" +
		"name: DestKey\n" +
		"values:\n" +
		"  DestValueDefault: [DestValueDefaultVariant]\n" +
		"mapped:\n" +
		"  - include: [" + payerAccount.getId() + "]\n" +
		"    maps:\n" +
		"      DestValue1:\n" +
		"        operator: or\n" +
		"        terms:\n" +
		"        - key: TagKey1\n" +
		"          operator: isOneOf\n" +
		"          values: [SrcValue1]\n" +
		"        - key: TagKey2\n" +
		"          operator: isOneOf\n" +
		"          values: [SrcValue2]\n" +
		"";
		
		String start = "2020-01-01T00:00:00Z";
		String[] customTags = new String[]{"DestKey", "TagKey4", "TagKey1", "TagKey2"};
		
		// Test default - should be DestValueDefault
		String[] tags = { "", "", "", "", ""};
		ResourceGroup expect = ResourceGroup.getResourceGroup(new String[]{ "DestValueDefault", "", "", ""});
		ResourceGroup resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		// Test TagKey1 - should give SrcValue1
		tags = new String[]{ "", "SrcValue1", "", "", "SrcValue4"};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue1", "SrcValue4", "SrcValue1", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);

		// Test TagKey2 - should give SrcValue1
		tags = new String[]{ "", "", "SrcValue2", "", "SrcValue4"};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue1", "SrcValue4", "", "SrcValue2"});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
	}
	
	@Test
	public void testGetTagGroupMappedMultiple() throws Exception {
		Account payerAccount = makeAccountWithDefaultTag("123456789012", "DestKey", "DestValueDefault");
		Account account = makeAccountWithDefaultTag("234567890123", null, null);
		
		// Mapping rules for new virtual tag
		String yaml = "" +
		"name: DestKey\n" +
		"values:\n" +
		"  DestValueDefault: [DestValueDefaultVariant]\n" +
		"mapped:\n" +
		"  - include: [" + payerAccount.getId() + "]\n" +
		"    maps:\n" +
		"      DestValue1:\n" +
		"        key: TagKey1\n" +
		"        operator: isOneOf\n" +
		"        values: [SrcValue1]\n" +
		"  - include: [" + account.getId() + "]\n" +
		"    maps:\n" +
		"      DestValue2:\n" +
		"        key: TagKey2\n" +
		"        operator: isOneOf\n" +
		"        values: [SrcValue2]\n" +
		"";
		
		String start = "2020-01-01T00:00:00Z";
		String[] customTags = new String[]{"DestKey", "TagKey4", "TagKey1", "TagKey2"};
		
		// Test default on payerAccount - should be DestValueDefault
		String[] tags = { "", "", "", "", ""};
		ResourceGroup expect = ResourceGroup.getResourceGroup(new String[]{ "DestValueDefault", "", "", ""});
		ResourceGroup resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);

		// Test default on account - should be empty
		expect = ResourceGroup.getResourceGroup(new String[]{ "", "", "", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, account);		
		assertEquals("Resource name doesn't match", expect, resource);

		// Test DestValue1 on payerAccount - should give DestValue1
		tags = new String[]{ "", "SrcValue1", "", "", "SrcValue4"};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue1", "SrcValue4", "SrcValue1", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		// Test DestValue1 on account - should be empty
		expect = ResourceGroup.getResourceGroup(new String[]{ "", "SrcValue4", "SrcValue1", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, account);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		// Test DestValue2 on payerAccount - should give DestValueDefault
		tags = new String[]{ "", "", "SrcValue2", "", "SrcValue4"};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValueDefault", "SrcValue4", "", "SrcValue2"});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		// Test DestValue2 on account - should give DestValue2
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue2", "SrcValue4", "", "SrcValue2"});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, account);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		/* Possible future feature
		// Test DestValue2 on account when mapping from tag not in customTags
		customTags = new String[]{"DestKey", "TagKey4", "TagKey1"};
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue2", "SrcValue4", ""});
		assertEquals("Resource name doesn't match", expect, resource.name);
		*/
	}
	
	@Test
	public void testGetTagGroupMappedMultipleStarts() throws Exception {
		Account payerAccount = makeAccountWithDefaultTag("123456789012", null, null);
		
		// Mapping rules for new virtual tag
		String yaml = "" +
		"name: DestKey\n" +
		"mapped:\n" +
		"  - include: [" + payerAccount.getId() + "]\n" +
		"    maps:\n" +
		"      DestValue1:\n" +
		"        key: TagKey1\n" +
		"        operator: isOneOf\n" + 
		"        values: [SrcValue1a]\n" +
		"      DestValue2:\n" +
		"        key: TagKey1\n" +
		"        operator: isOneOf\n" + 
		"        values: [SrcValue1b]\n" +
		"      DestValue3:\n" +
		"        key: TagKey1\n" +
		"        operator: isOneOf\n" + 
		"        values: [SrcValue1c]\n" +
		"  - include: [" + payerAccount.getId() + "]\n" +
		"    start: 2020-02\n" +
		"    maps:\n" +
		"      '':\n" +
		"        key: TagKey1\n" +
		"        operator: isOneOf\n" + 
		"        values: [SrcValue1a]\n" +
		"      DestValue2:\n" +
		"        key: TagKey1\n" + // add new SrcValue1d
		"        operator: isOneOf\n" + 
		"        values: [SrcValue1d]\n" +
		"      DestValue3:\n" +
		"        key: TagKey1\n" +
		"        operator: isOneOf\n" + 
		"        values: [SrcValue1b]\n" +
		""; // remap SrcValue1b to DestValue3
		
		String start = "2020-01-01T00:00:00Z";
		String[] customTags = new String[]{"DestKey", "TagKey4", "TagKey1", "TagKey2"};
		
		//
		// Check that items processed before 2020-02 are correct
		//
		// Test DestValue1
		String[] tags = new String[]{ "", "SrcValue1a", "", "", ""};		
		ResourceGroup expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue1", "", "SrcValue1a", ""});
		ResourceGroup resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		// Test DestValue2
		tags = new String[]{ "", "SrcValue1b", "", "", ""};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue2", "", "SrcValue1b", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		// Test DestValue3
		tags = new String[]{ "", "SrcValue1c", "", "", ""};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue3", "", "SrcValue1c", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		//
		// Check that items processed after 2020-02 are correct
		start = "2020-02-01T00:00:00Z";
		// Test DestValue1 - should not give DestValue1
		tags = new String[]{ "", "SrcValue1a", "", "", ""};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "", "", "SrcValue1a", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
		
		// Test DestValue2 - should give DestValue2
		tags = new String[]{ "", "SrcValue1d", "", "", ""};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue2", "", "SrcValue1d", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
				
		// Test DestValue3 with remap of 1b
		tags = new String[]{ "", "SrcValue1b", "", "", ""};		
		expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue3", "", "SrcValue1b", ""});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);		
		assertEquals("Resource name doesn't match", expect, resource);
	}

	@Test
	public void testGetTagGroupMappedFromAlias() throws Exception {
		Account payerAccount = makeAccountWithDefaultTag("123456789012", null, null);

		// Mapping rules for new virtual tag
		String yaml = "" +
				"tags:\n" +
				"  - name: DestKey\n" +
				"    aliases:\n" +
				"      - name: TagKey2\n" +
				"    mapped:\n" +
				"      - include: [" + payerAccount.getId() + "]\n" +
				"        maps:\n" +
				"          DestValue1:\n" +
				"            key: TagKey3\n" +
				"            operator: isOneOf\n" +
				"            values: [SrcValue3]\n" +
				"  - name: TagKey4\n" +
				"    aliases:\n" +
				"      - name: TagKey3\n";

		String start = "2020-01-01T00:00:00Z";
		String[] customTags = new String[]{"DestKey", "TagKey4"};

		// Test that mapping is being ignored due to using an alias
		String[] tags = new String[]{ "", "SrcValue1", "", "SrcValue3", "SrcValue4"};
		ResourceGroup expect = ResourceGroup.getResourceGroup(new String[]{ "", "SrcValue4"});
		ResourceGroup resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);
		assertEquals("Resource name doesn't match", expect, resource);
	}

	@Test
	public void testGetTagGroupMappedOverwrite() throws Exception {
		Account payerAccount = makeAccountWithDefaultTag("123456789012", null, null);

		// Mapping rules for new virtual tag
		String yaml = "" +
				"tags:\n" +
				"  - name: TagKey1\n" +
				"    mapped:\n" +
				"      - include: [" + payerAccount.getId() + "]\n" +
				"        force: true\n" +
				"        maps:\n" +
				"          DestValue:\n" +
				"            key: TagKey1\n" +
				"            operator: isOneOf\n" +
				"            values: [SrcValue1]\n" +
				"          srcvalue1a:\n" +
				"            key: TagKey1\n" +
				"            operator: isOneOf\n" +
				"            values: [Srcvalue1a]\n" +
				"";

		String start = "2020-01-01T00:00:00Z";
		String[] customTags = new String[]{"TagKey1"};

		// Test that mapping is being ignored due to using an alias
		String[] tags = new String[]{ "", "SrcValue1", "SrcValue2", "SrcValue3", "SrcValue4"};
		ResourceGroup expect = ResourceGroup.getResourceGroup(new String[]{ "DestValue"});
		ResourceGroup resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);
		assertEquals("Resource name doesn't match", expect, resource);

		// Test overwrite where only case differs in both the source and dest
		tags = new String[]{ "", "SrcValue1a", "SrcValue2", "SrcValue3", "SrcValue4"};
		expect = ResourceGroup.getResourceGroup(new String[]{ "srcvalue1a"});
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);
		assertEquals("Resource name doesn't match", expect, resource);
    }

	@Test
	public void testTagValueFilter() throws Exception {
    	// Test tag value filter that extracts email address from the aws:createdBy tag
		String[] customTags = new String[]{ "TagKey1", "TagKey2", "TagKey4" };
		Account payerAccount = makeAccountWithDefaultTag("123456789012", null, null);
		String start = "2020-01-01T00:00:00Z";

		// Tag config for tag that folds in aws:createdBy and only accepts constrained email addresses
		String yaml = "" +
				"tags:\n" +
				"  - name: TagKey1\n" +
				"    aliases:\n" +
				"      - name: 'aws:createdBy'\n" +
				"        filter: '[a-zA-Z0-9\\.]+@company.com'\n" +
				"    convert: toLower\n";

		// Test with empty TagKey1 value and matching value in alias
		String[] tags = { "AssumedRole:ABCD12EFG23H5IJ6KLM7N:foo.Bar@Company.com", "", "", "SrcValue3", "SrcValue4" };
		ResourceGroup resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"foo.bar@company.com", "", "SrcValue4"}), resource);

		// Test with empty TagKey1 value and non-matching value in alias
		tags[0] = "FooBar";
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"Other", "", "SrcValue4"}), resource);

		// Test with TagKey1 value
		tags[1] = "FooBar@company.com";
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"foobar@company.com", "", "SrcValue4"}), resource);

		// Test value TagKey1 value and value with match in alias
		tags[0] = "AssumedRole:ABCD12EFG23H5IJ6KLM7N:foo.Bar@Company.com";
		tags[1] = "foobar";
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"foobar", "", "SrcValue4"}), resource);

		// Test with capture group in the filter
		yaml = "" +
				"tags:\n" +
				"  - name: TagKey1\n" +
				"    aliases:\n" +
				"      - name: 'aws:createdBy'\n" +
				"        filter: '.*:([a-zA-Z0-9\\.]+)'\n" +
				"    convert: toLower\n";
		tags = new String[]{ "AssumedRole:ABCD12EFG23H5IJ6KLM7N:foo.Bar@Company.com", "", "", "SrcValue3", "SrcValue4" };
		resource = getResourceGroup(yaml, start, tags, customTags, payerAccount, payerAccount);
		assertEquals("Resource name doesn't match", ResourceGroup.getResourceGroup(new String[]{"foo.bar", "", "SrcValue4"}), resource);
	}
}
