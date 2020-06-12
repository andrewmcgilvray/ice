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
package com.netflix.ice.processor.postproc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;

public class PostProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    static private ProductService ps;
	static private AccountService as;
	static private BasicResourceService rs;
	static private String a1 = "1111111111111";
	static private String a2 = "2222222222222";
	static private String a3 = "3333333333333";
	static final String productCode = Product.Code.CloudFront.serviceCode;
	
	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
		rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		ps.getProduct(Product.Code.CloudFront);
		ps.getProduct(Product.Code.CloudWatch);
	}
	
	public enum DataType {
		cost,
		usage;
	}
    class TagGroupSpec {
    	DataType dataType;
    	String account;
    	String region;
    	String productServiceCode;
    	String operation;
    	String usageType;
    	String[] resourceGroup;
    	Double value;
    	
    	public TagGroupSpec(DataType dataType, String account, String region, String product, String operation, String usageType, String[] resourceGroup, Double value) {
    		this.dataType = dataType;
    		this.account = account;
    		this.region = region;
    		this.productServiceCode = product;
    		this.operation = operation;
    		this.usageType = usageType;
    		this.value = value;
    		this.resourceGroup = resourceGroup;
    	}

    	public TagGroupSpec(DataType dataType, String account, String region, String product, String operation, String usageType, Double value) {
    		this.dataType = dataType;
    		this.account = account;
    		this.region = region;
    		this.productServiceCode = product;
    		this.operation = operation;
    		this.usageType = usageType;
    		this.value = value;
    		this.resourceGroup = null;
    	}

    	public TagGroup getTagGroup() throws Exception {
    		return TagGroup.getTagGroup(account, region, null, productServiceCode, operation, usageType, "", resourceGroup, as, ps);
    	}
    	
    	public TagGroup getTagGroup(String account) throws Exception {
    		return TagGroup.getTagGroup(account, region, null, productServiceCode, operation, usageType, "", resourceGroup, as, ps);
    	}
    }
    
    private void loadData(TagGroupSpec[] dataSpecs, ReadWriteData usageData, ReadWriteData costData, int hour) throws Exception {
        for (TagGroupSpec spec: dataSpecs) {
        	if (spec.dataType == DataType.cost)
        		costData.put(hour, spec.getTagGroup(), spec.value);
        	else
        		usageData.put(hour, spec.getTagGroup(), spec.value);
        }
    }
    
	private void loadComputedCostData(ReadWriteData usageData, ReadWriteData costData) throws Exception {
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP1", "US-Requests-1", 1000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP1", "US-Requests-2", 2000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP1", "US-DataTransfer-Out-Bytes", 4000.0),
        		
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP2", "US-Requests-1", 8000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP2", "US-Requests-2", 16000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP2", "US-DataTransfer-Out-Bytes", 32000.0),
        		
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", productCode, "OP1", "EU-Requests-1", 10000.0),
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", productCode, "OP1", "EU-Requests-2", 20000.0),
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", productCode, "OP1", "EU-DataTransfer-Out-Bytes", 40000.0),
        };
        loadData(dataSpecs, usageData, costData, 0);
	}
	
	private String inputOperandTestYaml = "" +
			"name: inputOperandTest\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  accountAgg:\n" + 
			"    type: usage\n" +
			"    groupBy: [Region,Zone,Product,Operation,UsageType]\n" +
			"    usageType: ${group}-Usage\n" + 
			"  accountAggByList:\n" + 
			"    type: usage\n" +
			"    accounts: [1,2,3]\n" +
			"    usageType: ${group}-Usage\n" + 
			"  accountExAgg:\n" + 
			"    type: usage\n" +
			"    accounts: [1,2,3]\n" +
			"    usageType: ${group}-Usage\n" + 
			"in:\n" + 
			"  type: usage\n" + 
			"  product: Test\n" + 
			"  usageType: (..)-Usage\n" +
			"results:\n" +
			"  - result:\n" + 
			"      type: cost\n" + 
			"      product: Foo\n" + 
			"      usageType: Bar\n" + 
			"    value: '0'\n";
	
	@Test
	public void testOperandHasAggregation() throws Exception {
		Rule rule = new Rule(getConfig(inputOperandTestYaml), as, ps, rs);
		assertFalse("in operand incorrectly indicates that it has aggregation", rule.getIn().hasAggregation());
		assertTrue("accountAgg operand incorrectly indicates it has no aggregation", rule.getOperand("accountAgg").hasAggregation());
		assertTrue("accountAggByList operand incorrectly indicates it has no aggregation", rule.getOperand("accountAggByList").hasAggregation());
		assertTrue("accountExAgg operand incorrectly indicates it has no aggregation", rule.getOperand("accountExAgg").hasAggregation());
	}
	
	private String computedCostYaml = "" +
			"name: ComputedCost\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  data:\n" + 
			"    type: usage\n" + 
			"    usageType: ${group}-DataTransfer-Out-Bytes\n" + 
			"in:\n" + 
			"  type: usage\n" + 
			"  product: " + Product.Code.CloudFront.serviceCode + "\n" + 
			"  usageType: (..)-Requests-[12].*\n" + 
			"results:\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      product: ComputedCost\n" + 
			"      usageType: ${group}-Requests\n" + 
			"    value: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'\n" + 
			"  - result:\n" + 
			"      type: usage\n" + 
			"      product: ComputedCost\n" + 
			"      usageType: ${group}-Requests\n" + 
			"    value: '${in} - (${data} * 4 * 8 / 2)'\n";

	
	private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		RuleConfig rc = new RuleConfig();
		return mapper.readValue(yaml, rc.getClass());
	}
	
	@Test
	public void testGetInValue() throws Exception {
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadComputedCostData(usageData, costData);
		data.putUsage(null, usageData);
		data.putCost(null, costData);
				
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		Rule rule = new Rule(getConfig(computedCostYaml), as, ps, rs);
		
		Map<AggregationTagGroup, Double[]> inMap = pp.getInData(rule, data, true, data.getMaxNum());
		
		assertEquals("Wrong number of matched tags", 3, inMap.size());
		// Scan map and make sure we have 2 US and 1 EU
		int us = 0;
		int eu = 0;
		for (AggregationTagGroup atg: inMap.keySet()) {
			if (atg.getUsageType().name.equals("US"))
				us++;
			else if (atg.getUsageType().name.equals("EU"))
				eu++;
		}
		assertEquals("Wrong number of US tagGroups", 2, us);
		assertEquals("Wrong number of EU tagGroups", 1, eu);
		
		String productCode = Product.Code.CloudFront.serviceCode;
		TagGroupSpec[] specs = new TagGroupSpec[]{
				new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP1", "US", 3000.0),
				new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP2", "US", 24000.0),
				new TagGroupSpec(DataType.usage, a1, "eu-west-1", productCode, "OP1", "EU", 30000.0),
		};
		
		for (TagGroupSpec spec: specs) {
			TagGroup tg = spec.getTagGroup(a1);
			AggregationTagGroup atg = rule.getIn().aggregation.getAggregationTagGroup(tg);
			assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value, inMap.get(atg)[0], 0.001);
		}
		
		// Check that the data operand is flagged as not having aggregations
		assertFalse("Data operand incorrectly indicates it has aggregations", rule.getOperand("data").hasAggregation());
	}
	
	@Test
	public void testProcessReadWriteData() throws Exception {
		
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadComputedCostData(usageData, costData);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(null, usageData);
        data.putCost(null, costData);

		
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		Rule rule = new Rule(getConfig(computedCostYaml), as, ps, rs);
		Map<String, Double[]> operandSingleValueCache = Maps.newHashMap();
		pp.processReadWriteData(rule, data, true, operandSingleValueCache);

		assertEquals("Wrong number of entries in the single value cache", 0, operandSingleValueCache.size());

		// cost: 'in - (data * 4 * 8 / 2) * 0.01 / 1000'
		// 'in' is the sum of the two request values
		//
		// US: ((1000 + 2000) - (4000 * 4 * 8 / 2)) * 0.01 / 1000 == (3000 - 64000) * 0.00001 == 2999.36
		TagGroup usReqs = new TagGroupSpec(DataType.cost, a1, "us-east-1", "ComputedCost", "Credit - OP1", "US-Requests", null).getTagGroup();
		Double value = costData.get(0, usReqs);
		assertNotNull("No cost value for US-Requests", value);
		assertEquals("Wrong cost value for US-Requests", -0.61, value, .0001);
		
		value = usageData.get(0, usReqs);
		assertNotNull("No usage value for US-Requests", value);
		assertEquals("Wrong usage value for US-Requests", -61000.0, value, .0001);
		
		// EU:  ((10000 + 20000) - (40000 * 4 * 8 / 2)) * 0.01 / 1000 == (30000 - 640000) * 0.00001 == 29993.6
		TagGroup euReqs = new TagGroupSpec(DataType.cost, a1, "eu-west-1", "ComputedCost", "Credit - OP1", "EU-Requests", null).getTagGroup();
		Double euValue = costData.get(0, euReqs);
		assertNotNull("No cost value for EU-Requests", euValue);
		assertEquals("Wrong cost value for EU-Requests", -6.1, euValue, .0001);
		
		euValue = usageData.get(0, euReqs);
		assertNotNull("No usage value for EU-Requests", euValue);
		assertEquals("Wrong usage value for EU-Requests", -610000.0, euValue, .0001);
	}
	
	private void loadComputedCostDataWithResources(ReadWriteData usageData, ReadWriteData costData) throws Exception {
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP1", "US-Requests-1", new String[]{"tagA", ""}, 1000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP1", "US-Requests-2", new String[]{"tagA", ""}, 2000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP1", "US-DataTransfer-Out-Bytes", new String[]{"tagA", ""}, 4000.0),
        		
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP2", "US-Requests-1", new String[]{"tagB", ""}, 8000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP2", "US-Requests-2", new String[]{"tagB", ""}, 16000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "OP2", "US-DataTransfer-Out-Bytes", new String[]{"tagB", ""}, 32000.0),
        		
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", productCode, "OP1", "EU-Requests-1", new String[]{"tagC", ""}, 10000.0),
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", productCode, "OP1", "EU-Requests-2", new String[]{"tagC", ""}, 20000.0),
        		new TagGroupSpec(DataType.usage, a1, "eu-west-1", productCode, "OP1", "EU-DataTransfer-Out-Bytes", new String[]{"tagC", ""}, 40000.0),
        };
        loadData(dataSpecs, usageData, costData, 0);
	}
	
	@Test
	public void testProcessReadWriteDataWithResources() throws Exception {
		
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadComputedCostDataWithResources(usageData, costData);
		Product product = ps.getProduct(Product.Code.CloudFront);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(product, usageData);
        data.putCost(product, costData);

		
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		pp.debug = true;
		Rule rule = new Rule(getConfig(computedCostYaml), as, ps, rs);
		Map<String, Double[]> operandSingleValueCache = Maps.newHashMap();
		pp.processReadWriteData(rule, data, false, operandSingleValueCache);
		
		Product outProduct = ps.getProductByServiceCode("ComputedCost");
		ReadWriteData outCostData = data.getCost(outProduct);
		
		assertEquals("Wrong number of entries in the single value cache", 0, operandSingleValueCache.size());

		// out: 'in - (data * 4 * 8 / 2) * 0.01 / 1000'
		// 'in' is the sum of the two request values
		//
		// US: (1000 + 2000) - (4000 * 4 * 8 / 2) * 0.01 / 1000 == 3000 - 64000 * 0.00001 == 2999.36
		TagGroup usReqs = new TagGroupSpec(DataType.cost, a1, "us-east-1", "ComputedCost", "Credit - OP1", "US-Requests", new String[]{"tagA", ""}, 0.0).getTagGroup();
		Double value = outCostData.get(0, usReqs);
		assertNotNull("No value for US-Requests", value);
		assertEquals("Wrong value for US-Requests", -0.61, value, .0001);
		
		// EU:  (10000 + 20000) - (40000 * 4 * 8 / 2) * 0.01 / 1000 == 30000 - 640000 * 0.00001 == 29993.6
		TagGroup euReqs = new TagGroupSpec(DataType.cost, a1, "eu-west-1", "ComputedCost", "Credit - OP1", "EU-Requests", new String[]{"tagC", ""}, 0.0).getTagGroup();
		Double euValue = outCostData.get(0, euReqs);
		assertNotNull("No value for EU-Requests", euValue);
		assertEquals("Wrong value for EU-Requests", -6.1, euValue, .0001);
	}

	// Config to add a surcharge of 3% to all costs split out by account, region, and zone
	// usage is the aggregated cost and cost is the 3% charge
	private String surchargeConfigYaml = "" +
		"name: ComputedCost\n" + 
		"start: 2019-11\n" + 
		"end: 2022-11\n" + 
		"in:\n" + 
		"  type: cost\n" +
		"  groupBy: [Account,Region,Zone]\n" + 
		"results:\n" + 
		"  - result:\n" + 
		"      type: cost\n" + 
		"      product: ComputedCost\n" + 
		"      operation: \n" + 
		"      usageType: Dollar\n" + 
		"    value: '${in} * 0.03'\n" + 
		"  - result:\n" + 
		"      type: usage\n" + 
		"      product: ComputedCost\n" + 
		"      operation: \n" + 
		"      usageType: Dollar\n" + 
		"    value: '${in}'\n";
	
	private void loadSurchargeData(ReadWriteData usageData, ReadWriteData costData) throws Exception {
		String productCode = Product.Code.CloudFront.serviceCode;
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", productCode, "OP1", "US-Requests-1", 1000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", productCode, "OP2", "US-Requests-2", 2000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", productCode, "OP3", "US-DataTransfer-Out-Bytes", 4000.0),
        		
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", productCode, "OP4", "US-Requests-1", 8000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", productCode, "OP5", "US-Requests-2", 16000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", productCode, "OP6", "US-DataTransfer-Out-Bytes", 32000.0),
        		
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", productCode, "OP7", "EU-Requests-1", 10000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", productCode, "OP8", "EU-Requests-2", 20000.0),
        		new TagGroupSpec(DataType.cost, a1, "eu-west-1", productCode, "OP9", "EU-DataTransfer-Out-Bytes", 40000.0),
        		
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "OP1", "US-Requests-1", 1000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "OP2", "US-Requests-2", 2000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "OP3", "US-DataTransfer-Out-Bytes", 4000.0),
        		
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "OP4", "US-Requests-1", 8000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "OP5", "US-Requests-2", 16000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "OP6", "US-DataTransfer-Out-Bytes", 32000.0),
        		
        		new TagGroupSpec(DataType.cost, a2, "eu-west-1", productCode, "OP7", "EU-Requests-1", 10000.0),
        		new TagGroupSpec(DataType.cost, a2, "eu-west-1", productCode, "OP8", "EU-Requests-2", 20000.0),
        		new TagGroupSpec(DataType.cost, a2, "eu-west-1", productCode, "OP9", "EU-DataTransfer-Out-Bytes", 40000.0),
        };
        
        loadData(dataSpecs, usageData, costData, 0);
	}
	
	@Test
	public void testSurchargeGetInValues() throws Exception {
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadSurchargeData(usageData, costData);
        data.putUsage(null, usageData);
        data.putCost(null, costData);
				
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		Rule rule = new Rule(getConfig(surchargeConfigYaml), as, ps, rs);
				
		Map<AggregationTagGroup, Double[]> inMap = pp.getInData(rule, data, true, data.getMaxNum());
		
		assertEquals("Wrong number of matched tags", 4, inMap.size());
		// Scan map and make sure we have 2 us-east-1 and 2 eu-west-1
		int us = 0;
		int eu = 0;
		for (AggregationTagGroup atg: inMap.keySet()) {
			Region r = atg.getRegion();
			if (r == Region.US_EAST_1)
				us++;
			else if (r == Region.EU_WEST_1)
				eu++;
		}
		assertEquals("Wrong number of US tagGroups", 2, us);
		assertEquals("Wrong number of EU tagGroups", 2, eu);
		
		TagGroupSpec[] specs = new TagGroupSpec[]{
				new TagGroupSpec(DataType.usage, a1, "us-east-1", productCode, "", "", 63000.0),
				new TagGroupSpec(DataType.usage, a1, "eu-west-1", productCode, "", "", 70000.0),
		};
		
		for (TagGroupSpec spec: specs) {
			TagGroup tg = spec.getTagGroup(a1);
			AggregationTagGroup atg = rule.getIn().aggregation.getAggregationTagGroup(tg);
			assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value, inMap.get(atg)[0], 0.001);
			tg = spec.getTagGroup(a2);
			assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value, inMap.get(atg)[0], 0.001);
		}
	}

	private String splitCostYaml = "" +
			"name: SplitCost\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  total:\n" + 
			"    type: cost\n" +
			"    product: '(?!GlobalFee$)^.*$'\n" + 
	        "    operation: '(?!.*Savings - |.*Lent )^.*$' # ignore lent and savings\n" +
			"    groupBy: []\n" +
			"    groupByTags: []\n" +
			"    single: true\n" +
			"  lump-cost:\n" +
			"    type: cost\n" + 
			"    accounts: [" + a1 + "]\n" +
			"    regions: [global]\n" +
			"    product: GlobalFee\n" + 
			"    operation: None\n" +
			"    usageType: Dollar\n" + 
			"    groupByTags: []\n" +
			"    single: true\n" +
			"in:\n" + 
			"  type: cost\n" + 
			"  product: '(?!GlobalFee$)^.*$'\n" +
	        "  operation: '(?!.*Savings - |.*Lent )^.*$' # ignore lent and savings\n" +
			"  groupBy: [Account,Region]\n" +
	        "  groupByTags: [Key1]\n" +
			"results:\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      account: '${group}'\n" + 
			"      region: '${group}'\n" +
			"      product: GlobalFee\n" +
			"      operation: Split\n" +
			"      usageType: Dollar\n" + 
			"    value: '${lump-cost} * ${in} / ${total}'\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      account: " + a1 + "\n" +
			"      region: global\n" +
			"      product: GlobalFee\n" + 
			"      operation: None\n" +
			"      usageType: Dollar\n" + 
			"      single: true\n" + 
			"    value: 0.0\n";

	@Test
	public void testGlobalSplit() throws Exception {
		// Split $300 (3% of $10,000) of spend across three accounts based on individual account spend
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "global", "GlobalFee", "None", "Dollar", 300.0),
        		new TagGroupSpec(DataType.usage, a1, "global", "GlobalFee", "None", "Dollar", 10000.0),
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", productCode, "None", "Dollar", 5000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "None", "Dollar", 3000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-east-1", productCode, "None", "Dollar", 1500.0),
        		new TagGroupSpec(DataType.cost, a3, "us-west-2", productCode, "None", "Dollar", 500.0),
        };
        
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadData(dataSpecs, usageData, costData, 0);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(null, usageData);
        data.putCost(null, costData);
        
		Map<TagGroup, Double> in = costData.getData(0);
		for (TagGroup tg: in.keySet())
			logger.info("in cost: " + in.get(tg) + ", " + tg);
		in = usageData.getData(0);
		for (TagGroup tg: in.keySet())
			logger.info("in usage: " + in.get(tg) + ", " + tg);

		
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		pp.debug = true;
		Rule rule = new Rule(getConfig(splitCostYaml), as, ps, rs);
		// Check that the operands are flagged as having aggregations
		assertTrue("in operand incorrectly indicates it has no aggregation", rule.getIn().hasAggregation());
		assertTrue("total operand incorrectly indicates it has no aggregation", rule.getOperand("total").hasAggregation());
		assertFalse("lump-cost operand incorrectly indicates it has no aggregation", rule.getOperand("lump-cost").hasAggregation());

		Map<String, Double[]> operandSingleValueCache = Maps.newHashMap();
		pp.processReadWriteData(rule, data, true, operandSingleValueCache);
		
		ReadWriteData outCostData = data.getCost(null);
		Map<TagGroup, Double> m = outCostData.getData(0);
		for (TagGroup tg: m.keySet())
			logger.info("out: " + m.get(tg) + ", " + tg);
				
		assertEquals("Wrong number of entries in the single value cache", 2, operandSingleValueCache.size());

		// Should have zero-ed out the GlobalFee cost
		TagGroup globalFee = new TagGroupSpec(DataType.cost, a1, "global", "GlobalFee", "None", "Dollar", null).getTagGroup();
		Double value = outCostData.get(0, globalFee);
		assertNotNull("No value for global fee", value);
		assertEquals("Wrong value for global fee", 0.0, value, .001);
		
		// Should have 50/30/15/5% split of $300
		TagGroup a1split = new TagGroupSpec(DataType.cost, a1, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.get(0, a1split);
		assertNotNull("No value for global fee on account 1", value);
		assertEquals("wrong value for account 1", 300.0 * 0.5, value, .001);
		
		TagGroup a2split = new TagGroupSpec(DataType.cost, a2, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.get(0, a2split);
		assertNotNull("No value for global fee on account 2", value);
		assertEquals("wrong value for account 2", 300.0 * 0.3, value, .001);
		
		TagGroup a3split = new TagGroupSpec(DataType.cost, a3, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.get(0, a3split);
		assertNotNull("No value for global fee on account 3", value);
		assertEquals("wrong value for account 3", 300.0 * 0.15, value, .001);
		a3split = new TagGroupSpec(DataType.cost, a3, "us-west-2", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.get(0, a3split);
		assertNotNull("No value for global fee on account 4", value);
		assertEquals("wrong value for account 3", 300.0 * 0.05, value, .001);
	}
	
	@Test
	public void testGlobalSplitWithUserTags() throws Exception {
		// Split $300 (3% of $10,000) of spend across three accounts based on individual account spend
        TagGroupSpec[] globalFeeSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "global", "GlobalFee", "None", "Dollar", new String[]{"", ""}, 300.0),
        		new TagGroupSpec(DataType.usage, a1, "global", "GlobalFee", "None", "Dollar", new String[]{"", ""}, 10000.0),
        };
        TagGroupSpec[] productSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "us-east-1", productCode, "None", "Dollar", new String[]{"Tag1", ""}, 5000.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "None", "Dollar", new String[]{"Tag2", ""}, 3000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-east-1", productCode, "None", "Dollar", new String[]{"Tag3", ""}, 1500.0),
        		new TagGroupSpec(DataType.cost, a3, "us-west-2", productCode, "None", "Dollar", new String[]{"Tag4", ""}, 500.0),
        };
        
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadData(globalFeeSpecs, usageData, costData, 0);
		Product globalFee = ps.getProduct("GlobalFee", "GlobalFee");
        data.putUsage(globalFee, usageData);
        data.putCost(globalFee, costData);
		usageData = new ReadWriteData();
		costData = new ReadWriteData();
		loadData(productSpecs, usageData, costData, 0);
		Product product = ps.getProduct(productCode, productCode);
        data.putUsage(product, usageData);
        data.putCost(product, costData);
        
		Map<TagGroup, Double> in = costData.getData(0);
		for (TagGroup tg: in.keySet())
			logger.info("in cost: " + in.get(tg) + ", " + tg);
		in = usageData.getData(0);
		for (TagGroup tg: in.keySet())
			logger.info("in usage: " + in.get(tg) + ", " + tg);

		
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		pp.debug = true;
		Rule rule = new Rule(getConfig(splitCostYaml), as, ps, rs);
		// Check that the operands are flagged as having aggregations
		assertTrue("in operand incorrectly indicates it has no aggregation", rule.getIn().hasAggregation());
		assertTrue("total operand incorrectly indicates it has no aggregation", rule.getOperand("total").hasAggregation());
		assertFalse("lump-cost operand incorrectly indicates it has no aggregation", rule.getOperand("lump-cost").hasAggregation());

		Map<String, Double[]> operandSingleValueCache = Maps.newHashMap();
		pp.processReadWriteData(rule, data, false, operandSingleValueCache);

		ReadWriteData outCostData = data.getCost(globalFee);
		Map<TagGroup, Double> m = outCostData.getData(0);
		for (TagGroup tg: m.keySet())
			logger.info("globalFee out: " + m.get(tg) + ", " + tg);
				
		assertEquals("Wrong number of entries in the single value cache", 2, operandSingleValueCache.size());

		// Should have zeroed out the GlobalFee cost
		TagGroup globalFeeTag = new TagGroupSpec(DataType.cost, a1, "global", "GlobalFee", "None", "Dollar", new String[]{"", ""}, null).getTagGroup();
		Double value = outCostData.get(0, globalFeeTag);
		assertNotNull("No value for global fee", value);
		assertEquals("Wrong value for global fee", 0.0, value, .001);
		
		// Should have 50/30/15/5% split of $300
		TagGroup a1split = new TagGroupSpec(DataType.cost, a1, "us-east-1", "GlobalFee", "Split", "Dollar", new String[]{"Tag1", ""}, null).getTagGroup();
		value = outCostData.get(0, a1split);
		assertNotNull("No value for global fee on account 1", value);
		assertEquals("wrong value for account 1", 300.0 * 0.5, value, .001);
		
		TagGroup a2split = new TagGroupSpec(DataType.cost, a2, "us-east-1", "GlobalFee", "Split", "Dollar", new String[]{"Tag2", ""}, null).getTagGroup();
		value = outCostData.get(0, a2split);
		assertNotNull("No value for global fee on account 2", value);
		assertEquals("wrong value for account 2", 300.0 * 0.3, value, .001);
		
		TagGroup a3split = new TagGroupSpec(DataType.cost, a3, "us-east-1", "GlobalFee", "Split", "Dollar", new String[]{"Tag3", ""}, null).getTagGroup();
		value = outCostData.get(0, a3split);
		assertNotNull("No value for global fee on account 3", value);
		assertEquals("wrong value for account 3", 300.0 * 0.15, value, .001);
		a3split = new TagGroupSpec(DataType.cost, a3, "us-west-2", "GlobalFee", "Split", "Dollar", new String[]{"Tag4", ""}, null).getTagGroup();
		value = outCostData.get(0, a3split);
		assertNotNull("No value for global fee on account 4", value);
		assertEquals("wrong value for account 3", 300.0 * 0.05, value, .001);
	}
	
	private CostAndUsageData loadMultiProductComputedCostData() throws Exception {
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);

		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
        TagGroupSpec[] cfSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", Product.Code.CloudFront.serviceCode, "OP1", "US-Requests-1", new String[]{"Tag1", ""}, 1000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", Product.Code.CloudFront.serviceCode, "OP1", "US-Requests-2", new String[]{"Tag1", ""}, 2000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", Product.Code.CloudFront.serviceCode, "OP1", "US-Requests-1", new String[]{"Tag2", ""}, 1000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", Product.Code.CloudFront.serviceCode, "OP1", "US-Requests-2", new String[]{"Tag2", ""}, 2000.0),
        };
        loadData(cfSpecs, usageData, costData, 0);
        loadData(cfSpecs, usageData, costData, 1);
		Product cf = ps.getProduct(Product.Code.CloudFront);
        data.putUsage(cf, usageData);
        data.putCost(cf, costData);
                
		usageData = new ReadWriteData();
		costData = new ReadWriteData();
        TagGroupSpec[] cwSpecs = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", Product.Code.CloudWatch.serviceCode, "OP1", "US-DataTransfer-Out-Bytes", new String[]{"Tag1", ""}, 4000.0),
        		new TagGroupSpec(DataType.usage, a1, "us-east-1", Product.Code.CloudWatch.serviceCode, "OP1", "US-DataTransfer-Out-Bytes", new String[]{"Tag2", ""}, 4000.0),
        };
        loadData(cwSpecs, usageData, costData, 0);
        loadData(cwSpecs, usageData, costData, 1);
		Product cw = ps.getProduct(Product.Code.CloudWatch);
        data.putUsage(cw, usageData);
        data.putCost(cw, costData);
        
        return data;
	}
	
	private String computedMultiProductCostYaml = "" +
			"name: ComputedCost\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  data:\n" + 
			"    type: usage\n" + 
			"    product: " + Product.Code.CloudWatch.serviceCode + "\n" + 
			"    usageType: ${group}-DataTransfer-Out-Bytes\n" + 
			"in:\n" + 
			"  type: usage\n" + 
			"  product: " + Product.Code.CloudFront.serviceCode + "\n" + 
			"  usageType: (..)-Requests-[12].*\n" + 
			"results:\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      product: ComputedCost\n" + 
			"      usageType: ${group}-Requests\n" + 
			"    value: '(${in} - (${data} * 4 * 8 / 2)) * 0.01 / 1000'\n" + 
			"  - result:\n" + 
			"      type: usage\n" + 
			"      product: ComputedCost\n" + 
			"      usageType: ${group}-Requests\n" + 
			"    value: '${in} - (${data} * 4 * 8 / 2)'\n";

	@Test
	public void testProcessMultiProductReadWriteDataWithResourcesTwoHours() throws Exception {
		// Test two hours of data with two different resource tags
		CostAndUsageData data = loadMultiProductComputedCostData();
		
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		pp.debug = true;
		Rule rule = new Rule(getConfig(computedMultiProductCostYaml), as, ps, rs);
		Map<String, Double[]> operandSingleValueCache = Maps.newHashMap();
		pp.processReadWriteData(rule, data, false, operandSingleValueCache);
		
		assertEquals("Wrong number of entries in the single value cache", 0, operandSingleValueCache.size());

		Product outProduct = ps.getProductByServiceCode("ComputedCost");
		ReadWriteData outCostData = data.getCost(outProduct);
		
		// out: 'in - (data * 4 * 8 / 2) * 0.01 / 1000'
		// 'in' is the sum of the two request values
		//
		// US: (1000 + 2000) - (4000 * 4 * 8 / 2) * 0.01 / 1000 == 3000 - 64000 * 0.00001 == 2999.36
		for (String[] rg: new String[][]{new String[]{"Tag1", ""}, new String[]{"Tag2", ""}}) {
			TagGroup usReqs = new TagGroupSpec(DataType.cost, a1, "us-east-1", "ComputedCost", "Credit - OP1", "US-Requests", rg, null).getTagGroup();
			for (int hour = 0; hour < 2; hour++) {
				Double value = outCostData.get(hour, usReqs);
				assertNotNull("No value for US-Requests hour " + hour + ", tag " + rg, value);
				assertEquals("Wrong value for US-Requests hour " + hour + ", tag " + rg, -0.61, value, .0001);
			}
		}
		
		// Check that the data operand is flagged as not having aggregations
		assertFalse("Data operand incorrectly indicates it has aggregations", rule.getOperand("data").hasAggregation());
	}

	private String splitMonthlyCostByHourYaml = "" +
			"name: SplitCost\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  total:\n" + 
			"    type: cost\n" +
			"    monthly: true\n" +
			"    product: '(?!GlobalFee$)^.*$'\n" + 
			"    groupBy: []\n" +
			"  lump-cost:\n" +
			"    type: cost\n" + 
			"    monthly: true\n" +
			"    accounts: [" + a1 + "]\n" +
			"    regions: [global]\n" +
			"    product: GlobalFee\n" + 
			"    operation: None\n" +
			"    usageType: Dollar\n" + 
			"in:\n" + 
			"  type: cost\n" + 
			"  product: '(?!GlobalFee$)^.*$'\n" +
			"  groupBy: [Account,Region]\n" +
			"results:\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      account: '${group}'\n" + 
			"      region: '${group}'\n" +
			"      product: GlobalFee\n" +
			"      operation: Split\n" +
			"      usageType: Dollar\n" + 
			"    value: '${lump-cost} * ${in} / ${total}'\n";

	@Test
	public void testMonthlySplitByHour() throws Exception {
		// Split $300 (3% of $10,000) of spend across three accounts and two hours based on individual account spend
        TagGroupSpec[] dataSpecs0 = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "global", "GlobalFee", "None", "Dollar", 300.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "None", "Dollar", 3000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-east-1", productCode, "None", "Dollar", 2000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-west-2", productCode, "None", "Dollar", 1500.0),
        };        
        TagGroupSpec[] dataSpecs1 = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "None", "Dollar", 2000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-east-1", productCode, "None", "Dollar", 1000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-west-2", productCode, "None", "Dollar", 500.0),
        };
        
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadData(dataSpecs0, usageData, costData, 0);
		loadData(dataSpecs1, usageData, costData, 1);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(null, usageData);
        data.putCost(null, costData);
        
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		pp.debug = true;
		Rule rule = new Rule(getConfig(splitMonthlyCostByHourYaml), as, ps, rs);

		Map<String, Double[]> operandSingleValueCache = Maps.newHashMap();
		pp.processReadWriteData(rule, data, true, operandSingleValueCache);
		ReadWriteData outCostData = data.getCost(null);

		Map<TagGroup, Double> m = outCostData.getData(0);
		for (TagGroup tg: m.keySet())
			logger.info("out: " + m.get(tg) + ", " + tg);
		
		assertEquals("Wrong number of entries in the single value cache", 0, operandSingleValueCache.size());

		// Should have 50/30/20% split of $300
		TagGroup a2split = new TagGroupSpec(DataType.cost, a2, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		Double value = outCostData.get(0, a2split);
		value += outCostData.get(1, a2split);
		assertEquals("wrong value for account 2", 300.0 * 0.5, value, .001);
		
		TagGroup a3split = new TagGroupSpec(DataType.cost, a3, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.get(0, a3split);
		value += outCostData.get(1, a3split);
		assertEquals("wrong value for account 3", 300.0 * 0.3, value, .001);
		
		a3split = new TagGroupSpec(DataType.cost, a3, "us-west-2", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.get(0, a3split);
		value += outCostData.get(1, a3split);
		assertEquals("wrong value for account 3", 300.0 * 0.2, value, .001);
	}

	private String splitMonthlyCostByMonthYaml = "" +
			"name: SplitCost\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  total:\n" + 
			"    type: cost\n" +
			"    monthly: true\n" +
			"    product: '(?!GlobalFee$)^.*$'\n" + 
			"    groupBy: []\n" +
			"  lump-cost:\n" +
			"    type: cost\n" + 
			"    monthly: true\n" +
			"    accounts: [" + a1 + "]\n" +
			"    regions: [global]\n" +
			"    product: GlobalFee\n" + 
			"    operation: None\n" +
			"    usageType: Dollar\n" + 
			"in:\n" + 
			"  type: cost\n" + 
			"  monthly: true\n" +
			"  product: '(?!GlobalFee$)^.*$'\n" +
			"  groupBy: [Account,Region]\n" +
			"results:\n" + 
			"  - result:\n" + 
			"      type: cost\n" + 
			"      account: '${group}'\n" + 
			"      region: '${group}'\n" +
			"      product: GlobalFee\n" +
			"      operation: Split\n" +
			"      usageType: Dollar\n" + 
			"    value: '${lump-cost} * ${in} / ${total}'\n";

	@Test
	public void testMonthlySplitByMonth() throws Exception {
		// Split $300 (3% of $10,000) of spend across three accounts and two hours based on individual account spend
        TagGroupSpec[] dataSpecs0 = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a1, "global", "GlobalFee", "None", "Dollar", 300.0),
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "None", "Dollar", 3000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-east-1", productCode, "None", "Dollar", 2000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-west-2", productCode, "None", "Dollar", 1500.0),
        };        
        TagGroupSpec[] dataSpecs1 = new TagGroupSpec[]{
        		new TagGroupSpec(DataType.cost, a2, "us-east-1", productCode, "None", "Dollar", 2000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-east-1", productCode, "None", "Dollar", 1000.0),
        		new TagGroupSpec(DataType.cost, a3, "us-west-2", productCode, "None", "Dollar", 500.0),
        };
        
		ReadWriteData usageData = new ReadWriteData();
		ReadWriteData costData = new ReadWriteData();
		loadData(dataSpecs0, usageData, costData, 0);
		loadData(dataSpecs1, usageData, costData, 1);
		CostAndUsageData data = new CostAndUsageData(0, null, null, null, as, ps);
        data.putUsage(null, usageData);
        data.putCost(null, costData);
        
		PostProcessor pp = new PostProcessor(null, as, ps, rs);
		pp.debug = true;
		Rule rule = new Rule(getConfig(splitMonthlyCostByMonthYaml), as, ps, rs);

		Map<String, Double[]> operandSingleValueCache = Maps.newHashMap();
		pp.processReadWriteData(rule, data, true, operandSingleValueCache);
		ReadWriteData outCostData = data.getCost(null);
				
		Map<TagGroup, Double> m = outCostData.getData(0);
		for (TagGroup tg: m.keySet())
			logger.info("out: " + m.get(tg) + ", " + tg);

		assertEquals("Wrong number of entries in the single value cache", 0, operandSingleValueCache.size());

		// Should have 50/30/20% split of $300
		TagGroup a2split = new TagGroupSpec(DataType.cost, a2, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		Double value = outCostData.get(0, a2split);
		assertEquals("wrong value for account 2", 300.0 * 0.5, value, .001);
		
		TagGroup a3split = new TagGroupSpec(DataType.cost, a3, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.get(0, a3split);
		assertEquals("wrong value for account 3", 300.0 * 0.3, value, .001);
		
		a3split = new TagGroupSpec(DataType.cost, a3, "us-west-2", "GlobalFee", "Split", "Dollar", null).getTagGroup();
		value = outCostData.get(0, a3split);
		assertEquals("wrong value for account 3", 300.0 * 0.2, value, .001);
	}

}
