package com.netflix.ice.processor.postproc;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;

public class QueryTest {
    static private ProductService ps;
	static private AccountService as;
	static private List<String> userTagKeys;

	@BeforeClass
	public static void init() {
		List<Account> accts = Lists.newArrayList();
		accts.add(new Account("123456789012", "Account1", null));
		accts.add(new Account("234567890123", "Account2", null));
		as = new BasicAccountService(accts);
		ps = new BasicProductService();
		userTagKeys = Lists.newArrayList(new String[]{"Key1","Key2"});
	}
	

	@Test
	public void testGetSingleTagGroup() throws Exception {
		TagGroupFilterConfig tgfc = new TagGroupFilterConfig();
		tgfc.setAccount(Lists.newArrayList("123456789012"));
		tgfc.setRegion(Lists.newArrayList("us-east-1"));
		tgfc.setProduct(Lists.newArrayList(Product.Code.Ec2.toString()));
		tgfc.setOperation(Lists.newArrayList("OP1"));
		tgfc.setUsageType(Lists.newArrayList("UT1"));
		
		Map<String, List<String>> userTags = Maps.newHashMap();
		userTags.put("Key1", Lists.newArrayList("tag1"));
		tgfc.setUserTags(userTags);

		QueryConfig qc = new QueryConfig();
		qc.setFilter(tgfc);
		Query q = new Query(qc, Lists.newArrayList(new String[]{"Key1", "Key2"}), false);

		TagGroup tg = q.getSingleTagGroup(as, ps, false);
		ResourceGroup expect = ResourceGroup.getResourceGroup(new String[]{"tag1", ""});
		assertEquals("incorrect resourceGroup string", expect, tg.resourceGroup);
	}

	@Test
	public void testEmptyOperand() throws Exception {
		QueryConfig oc = new QueryConfig();
		Query q = new Query(oc, userTagKeys, false);
		
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", null, as, ps);
		assertNotNull("tg should match", q.aggregateTagGroup(tg, as, ps));
	}

	@Test
	public void testNonEmptyOperand() throws Exception {
		QueryConfig qc = new QueryConfig();
		TagGroupFilterConfig tgfc = new TagGroupFilterConfig();
		qc.setFilter(tgfc);
		tgfc.setAccount(Lists.newArrayList("123456789012"));
		Query q = new Query(qc, userTagKeys, false);
		
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", null, as, ps);
		assertNotNull("tg should match", q.aggregateTagGroup(tg, as, ps));
		
		tgfc.setAccount(Lists.newArrayList("234567890123"));
		q = new Query(qc, userTagKeys, false);
		assertNull("should not have an aggregationTagGroup", q.aggregateTagGroup(tg, as, ps));
		
		
		List<String> accounts = Lists.newArrayList("234567890123");
		List<Rule.TagKey> exclude = Lists.newArrayList(Rule.TagKey.account);
		tgfc.setAccount(accounts);
		tgfc.setExclude(exclude);
		q = new Query(qc, userTagKeys, false);
		assertNotNull("tg should match", q.aggregateTagGroup(tg, as, ps));		
		TagGroup tg1 = TagGroup.getTagGroup("234567890123", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", null, as, ps);
		assertNull("tg should not match", q.aggregateTagGroup(tg1, as, ps));
	}
	
	@Test
	public void testAggregatesFlag() throws Exception {
		TagGroupFilterConfig tgfc = new TagGroupFilterConfig();
		QueryConfig oc = new QueryConfig();
		oc.setFilter(tgfc);
		oc.setType(RuleConfig.DataType.cost);
		tgfc.setAccount(Lists.newArrayList(new String[]{"123456789012", "234567890123"}));
		Map<String, List<String>> userTags = Maps.newHashMap();
		userTags.put("Key1", Lists.newArrayList("tag1"));
		tgfc.setUserTags(userTags);
		
		Query q = new Query(oc, userTagKeys, false);
		assertFalse("incorrectly says it aggregates", q.hasAggregation());
	}
	
	@Test
	public void testAggregateTagGroupWithUserTags() throws Exception {
		TagGroupFilterConfig tgfc = new TagGroupFilterConfig();
		QueryConfig oc = new QueryConfig();
		oc.setFilter(tgfc);
		Map<String, List<String>> userTags = Maps.newHashMap();
		userTags.put("Key1", Lists.newArrayList("tag1"));
		tgfc.setUserTags(userTags);
		Query q = new Query(oc, userTagKeys, false);
		
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", new String[]{"tag1", ""}, as, ps);
		AggregationTagGroup atg = q.aggregateTagGroup(tg, as, ps);
		assertNotNull("atg should not be null", atg);

		tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", new String[]{"tag2", ""}, as, ps);
		atg = q.aggregateTagGroup(tg, as, ps);
		assertNull("atg should be null", atg);
	}
			
	@Test
	public void testOperandMatchesWithOmmittedValue() throws Exception {
		// Set up the in operand
		QueryConfig inOperand = new QueryConfig();
		TagGroupFilterConfig tgfc = new TagGroupFilterConfig();
		inOperand.setFilter(tgfc);
		inOperand.setType(RuleConfig.DataType.usage);
		tgfc.setProduct(Lists.newArrayList("(?!IOTestProduct$)^.*$"));
		tgfc.setOperation(Lists.newArrayList("(?!OP$)^.*$"));
		tgfc.setUsageType(Lists.newArrayList("(?!UT$)^.*$"));
		List<Rule.TagKey> groupBy = Lists.newArrayList(new Rule.TagKey[]{Rule.TagKey.account,Rule.TagKey.region,Rule.TagKey.zone});
		inOperand.setGroupBy(groupBy);
		Query in = new Query(inOperand, userTagKeys, false);		

		// Test case where omitting a product based on regex with no dependency on 'in' aggregation tag group
		inOperand = new QueryConfig();
		tgfc = new TagGroupFilterConfig();
		inOperand.setFilter(tgfc);
		inOperand.setType(RuleConfig.DataType.cost);
		tgfc.setProduct(Lists.newArrayList("(?!IOTestProduct$)^.*$"));
		tgfc.setOperation(Lists.newArrayList("(?!OP$)^.*$"));
		tgfc.setUsageType(Lists.newArrayList("(?!UT$)^.*$"));
		in = new Query(inOperand, userTagKeys, false);
		TagGroup tg1 = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct", "OP1", "UT1", "", null, as, ps);		
		assertNull("TagGroup should not match product aggregation operand with ommitted product", in.aggregateTagGroup(tg1, as, ps));
		
		tg1 = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP", "UT1", "", null, as, ps);		
		assertNull("TagGroup should not match product aggregation operand with ommitted operation", in.aggregateTagGroup(tg1, as, ps));
		
		tg1 = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP1", "UT", "", null, as, ps);		
		assertNull("TagGroup should not match product aggregation operand with ommitted usageType", in.aggregateTagGroup(tg1, as, ps));		
	}
	
	@Test
	public void testOperandSingleValueNoAggregation() throws Exception {
		QueryConfig oc = new QueryConfig();
		TagGroupFilterConfig tgfc = new TagGroupFilterConfig();
		oc.setType(RuleConfig.DataType.cost);
		tgfc.setAccount(Lists.newArrayList("123456789012"));
		tgfc.setRegion(Lists.newArrayList("us-east-1"));
		tgfc.setProduct(Lists.newArrayList("IOTestProduct1"));
		tgfc.setOperation(Lists.newArrayList("OP1"));
		tgfc.setUsageType(Lists.newArrayList("UT1"));
		oc.setFilter(tgfc);
		
		Query q = new Query(oc, userTagKeys, false);
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP1", "UT1", "", null, as, ps);		
		assertEquals("tag groups should match", tg, q.getSingleTagGroup(as, ps, true));
	}

	@Test
	public void testOperandSingleValueWithAggregation() throws Exception {
		QueryConfig oc = new QueryConfig();
		TagGroupFilterConfig tgfc = new TagGroupFilterConfig();
		oc.setType(RuleConfig.DataType.cost);
		tgfc.setAccount(Lists.newArrayList("123456789012"));
		tgfc.setRegion(Lists.newArrayList("us-east-1"));
		tgfc.setProduct(Lists.newArrayList("IOTestProduct1"));
		oc.setFilter(tgfc);
		
		Query q = new Query(oc, userTagKeys, false);
		TagGroup tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP1", "UT1", "", null, as, ps);		
		assertNotNull("tag group should match", q.aggregateTagGroup(tg, as, ps));

		// Test user tags
		Map<String, List<String>> userTags = Maps.newHashMap();
		userTags.put("Key1", Lists.newArrayList("tag1"));
		tgfc.setUserTags(userTags);
		
		q = new Query(oc, userTagKeys, false);
		tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP1", "UT1", "", new String[]{"tag2", ""}, as, ps);		
		assertNull("tag group should not match with wrong user tag", q.aggregateTagGroup(tg, as, ps));
		
		tg = TagGroup.getTagGroup("123456789012", "us-east-1", null, "IOTestProduct1", "OP1", "UT1", "", new String[]{"tag1", ""}, as, ps);		
		assertNotNull("tag groups should match with correct user tag", q.aggregateTagGroup(tg, as, ps));
	}

	@Test(expected = Exception.class)
	public void testInvalidUserTagKey() throws Exception {
		QueryConfig oc = new QueryConfig();
		TagGroupFilterConfig tgfc = new TagGroupFilterConfig();
		oc.setFilter(tgfc);
		Map<String, List<String>> userTags = Maps.newHashMap();
		userTags.put("Key3", Lists.newArrayList("tag1"));
		tgfc.setUserTags(userTags);
		new Query(oc, userTagKeys, false);
	}
}

