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
import com.netflix.ice.processor.DataSerializer;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;

public class FixedRuleProcessorTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
        
    static private ProductService ps;
    static private AccountService as;
    static private BasicResourceService rs;
    static private String a1 = "1111111111111";
    static private String a2 = "2222222222222";
    static private String a3 = "3333333333333";
    static final String productCode = Product.Code.CloudFront.serviceCode;
    static final String ec2Instance = Product.Code.Ec2Instance.serviceCode;
    static final String ebs = Product.Code.Ebs.serviceCode;
    static final String cloudWatch = Product.Code.CloudWatch.serviceCode;

    @BeforeClass
    static public void init() {
        ps = new BasicProductService();
        as = new BasicAccountService();
        rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
        ps.getProduct(Product.Code.CloudFront);
        ps.getProduct(Product.Code.CloudWatch);
    }
    
    private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {        
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        RuleConfig rc = new RuleConfig();
        return mapper.readValue(yaml, rc.getClass());
    }

    private void loadComputedCostData(DataSerializer data) throws Exception {
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-Requests-1", 0, 1000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-Requests-2", 0, 2000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-DataTransfer-Out-Bytes", 0, 4000.0),
                
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-Requests-1", 0, 8000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-Requests-2", 0, 16000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-DataTransfer-Out-Bytes", 0, 32000.0),
                
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP1", "EU-Requests-1", 0, 10000.0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP1", "EU-Requests-2", 0, 20000.0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP1", "EU-DataTransfer-Out-Bytes", 0, 40000.0),
        };
        TagGroupSpec.loadData(dataSpecs, data, 0, as, ps);
    }
    
    private String computedCostYaml = "" +
            "name: ComputedCost\n" + 
            "start: 2019-11\n" + 
            "end: 2022-11\n" + 
            "in:\n" + 
            "  filter:\n" + 
            "    costType: [Recurring]\n" + 
            "    product: [" + Product.Code.CloudFront.serviceCode + "]\n" + 
            "    usageType: ['..-Requests-[12].*']\n" + 
            "patterns:\n" +
            "  region: '(..)-.*'\n" +
            "results:\n" + 
            "- out:\n" + 
            "    product: ComputedCost\n" + 
            "    usageType: ${region}-Requests\n" + 
            "  cost: '${in.usage} * 0.01 / 1000'\n" + 
            "  usage: '${in.usage}'\n";

    @Test
    public void testRunQuery() throws Exception {
        CostAndUsageData cauData = new CostAndUsageData(null, 0, null, null, null, as, ps);
        cauData.enableTagGroupCache(true);
        DataSerializer data = cauData.get(null);
        loadComputedCostData(data);
        
        Rule rule = new Rule(getConfig(computedCostYaml), as, ps, rs.getCustomTags());
        FixedRuleProcessor frp = new FixedRuleProcessor(rule, as, ps);
        
        Map<AggregationTagGroup, CostAndUsage[]> inMap = frp.runQuery(rule.getIn(), cauData, true, cauData.getMaxNum(), rule.config.getName());
        
        assertEquals("Wrong number of matched tags", 6, inMap.size());
        // Scan map and make sure we have 4 US and 2 EU
        int us = 0;
        int eu = 0;
        for (AggregationTagGroup atg: inMap.keySet()) {
            String ut = atg.getUsageType().name;
            if (ut.equals("US-Requests-1") || ut.equals("US-Requests-2"))
                us++;
            else if (ut.equals("EU-Requests-1") || ut.equals("EU-Requests-2"))
                eu++;
        }
        assertEquals("Wrong number of US tagGroups", 4, us);
        assertEquals("Wrong number of EU tagGroups", 2, eu);
        
        String productCode = Product.Code.CloudFront.serviceCode;
        TagGroupSpec[] specs = new TagGroupSpec[]{
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-Requests-1", 0, 1000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-Requests-2", 0, 2000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-Requests-1", 0, 8000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-Requests-2", 0, 16000.0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP1", "EU-Requests-1", 0, 10000.0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP1", "EU-Requests-2", 0, 20000.0),
        };
        
        for (TagGroupSpec spec: specs) {
            TagGroup tg = spec.getTagGroup(a1, as, ps);
            AggregationTagGroup atg = rule.getIn().aggregation.getAggregationTagGroup(tg);
            assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value.usage, inMap.get(atg)[0].usage, 0.001);
        }
    }
    
    @Test
    public void testProcessData() throws Exception {
        
        CostAndUsageData cauData = new CostAndUsageData(null, 0, null, null, null, as, ps);
        cauData.enableTagGroupCache(true);
        DataSerializer data = cauData.get(null);
        loadComputedCostData(data);
        
        Rule rule = new Rule(getConfig(computedCostYaml), as, ps, rs.getCustomTags());
        Map<Query, CostAndUsage[]> operandSingleValueCache = Maps.newHashMap();
        
        FixedRuleProcessor frp = new FixedRuleProcessor(rule, as, ps);
        frp.processData(cauData, true, operandSingleValueCache);

        assertEquals("Wrong number of entries in the single value cache", 0, operandSingleValueCache.size());

        // cost: 'in - (data * 4 * 8 / 2) * 0.01 / 1000'
        // 'in' is the sum of the two request values
        //
        // US: ((1000 + 2000) - (4000 * 4 * 8 / 2)) * 0.01 / 1000 == (3000 - 64000) * 0.00001 == 2999.36
        TagGroup usReqs = new TagGroupSpec("Recurring", a1, "us-east-1", "ComputedCost", "OP1", "US-Requests", null).getTagGroup(as, ps);
        CostAndUsage value = data.get(0, usReqs);
        assertNotNull("No value for US-Requests", value);
        assertEquals("Wrong cost value for OP1 US-Requests", 0.03, value.cost, .0001);
        assertEquals("Wrong usage value for OP2 US-Requests", 3000.0, value.usage, .0001);
        
        // EU:  ((10000 + 20000) - (40000 * 4 * 8 / 2)) * 0.01 / 1000 == (30000 - 640000) * 0.00001 == 29993.6
        TagGroup euReqs = new TagGroupSpec("Recurring", a1, "eu-west-1", "ComputedCost", "OP1", "EU-Requests", null).getTagGroup(as, ps);
        CostAndUsage euValue = data.get(0, euReqs);
        assertNotNull("No value for EU-Requests", euValue);
        assertEquals("Wrong cost value for EU-Requests", 0.3, euValue.cost, .0001);
        assertEquals("Wrong usage value for EU-Requests", 30000.0, euValue.usage, .0001);
    }

    private void loadComputedCostDataWithResources(DataSerializer data) throws Exception {
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-Requests-1", new String[]{"tagA", ""}, 0, 1000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-Requests-2", new String[]{"tagA", ""}, 0, 2000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-DataTransfer-Out-Bytes", new String[]{"tagA", ""}, 0, 4000.0),
                
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-Requests-1", new String[]{"tagB", ""}, 0, 8000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-Requests-2", new String[]{"tagB", ""}, 0, 16000.0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-DataTransfer-Out-Bytes", new String[]{"tagB", ""}, 0, 32000.0),
                
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP1", "EU-Requests-1", new String[]{"tagC", ""}, 0, 10000.0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP1", "EU-Requests-2", new String[]{"tagC", ""}, 0, 20000.0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP1", "EU-DataTransfer-Out-Bytes", new String[]{"tagC", ""}, 0, 40000.0),
        };
        TagGroupSpec.loadData(dataSpecs, data, 0, as, ps);
    }
    
    @Test
    public void testProcessDataWithResources() throws Exception {
        
        DataSerializer data = new DataSerializer(2);
        loadComputedCostDataWithResources(data);
        Product product = ps.getProduct(Product.Code.CloudFront);
        CostAndUsageData cauData = new CostAndUsageData(null, 0, null, null, null, as, ps);
        cauData.put(product, data);
        cauData.enableTagGroupCache(true);

        
        Rule rule = new Rule(getConfig(computedCostYaml), as, ps, rs.getCustomTags());
        Map<Query, CostAndUsage[]> operandSingleValueCache = Maps.newHashMap();
        FixedRuleProcessor frp = new FixedRuleProcessor(rule, as, ps);
        frp.debug = true;
        frp.processData(cauData, false, operandSingleValueCache);
        
        Product outProduct = ps.getProductByServiceCode("ComputedCost");
        DataSerializer outData = cauData.get(outProduct);
        
        assertEquals("Wrong number of entries in the single value cache", 0, operandSingleValueCache.size());

        // out: 'in * 0.01 / 1000'
        // 'in' is the sum of the two request values
        //
        // US: (1000 + 2000) * 0.01 / 1000 == 3000 * 0.00001 == 0.03
        TagGroup usReqs = new TagGroupSpec("Recurring", a1, "us-east-1", "ComputedCost", "OP1", "US-Requests", new String[]{"tagA", ""}).getTagGroup(as, ps);
        CostAndUsage value = outData.get(0, usReqs);
        assertNotNull("No value for US-Requests", value);
        assertEquals("Wrong value for US-Requests", .03, value.cost, .0001);
        
        // EU:  (10000 + 20000) * 0.01 / 1000 == 30000 * 0.00001 == 0.3
        TagGroup euReqs = new TagGroupSpec("Recurring", a1, "eu-west-1", "ComputedCost", "OP1", "EU-Requests", new String[]{"tagC", ""}).getTagGroup(as, ps);
        CostAndUsage euValue = outData.get(0, euReqs);
        assertNotNull("No value for EU-Requests", euValue);
        assertEquals("Wrong value for EU-Requests", 0.3, euValue.cost, .0001);
    }
    
    // Config to add a surcharge of 3% to all costs split out by account, region, and zone
    // usage is the aggregated cost and cost is the 3% charge
    private String surchargeConfigYaml = "" +
        "name: ComputedCost\n" + 
        "start: 2019-11\n" + 
        "end: 2022-11\n" + 
        "in:\n" + 
        "  groupBy: [account,region,zone]\n" + 
        "results:\n" + 
        "- out:\n" + 
        "    costType: Recurring\n" + 
        "    product: ComputedCost\n" + 
        "    operation: \n" + 
        "    usageType: Dollar\n" + 
        "  cost: '${in.cost} * 0.03'\n" + 
        "  usage: '${in.cost}'\n";
    
    private void loadSurchargeData(DataSerializer data) throws Exception {
        String productCode = Product.Code.CloudFront.serviceCode;
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP1", "US-Requests-1", 1000, 0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP2", "US-Requests-2", 2000, 0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP3", "US-DataTransfer-Out-Bytes", 4000, 0),
                
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP4", "US-Requests-1", 8000, 0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP5", "US-Requests-2", 16000, 0),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "OP6", "US-DataTransfer-Out-Bytes", 32000, 0),
                
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP7", "EU-Requests-1", 10000, 0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP8", "EU-Requests-2", 20000, 0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "OP9", "EU-DataTransfer-Out-Bytes", 40000, 0),
                
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "OP1", "US-Requests-1", 1000, 0),
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "OP2", "US-Requests-2", 2000, 0),
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "OP3", "US-DataTransfer-Out-Bytes", 4000, 0),
                
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "OP4", "US-Requests-1", 8000, 0),
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "OP5", "US-Requests-2", 16000, 0),
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "OP6", "US-DataTransfer-Out-Bytes", 32000, 0),
                
                new TagGroupSpec("Recurring", a2, "eu-west-1", productCode, "OP7", "EU-Requests-1", 10000, 0),
                new TagGroupSpec("Recurring", a2, "eu-west-1", productCode, "OP8", "EU-Requests-2", 20000, 0),
                new TagGroupSpec("Recurring", a2, "eu-west-1", productCode, "OP9", "EU-DataTransfer-Out-Bytes", 40000, 0),
        };
        
        TagGroupSpec.loadData(dataSpecs, data, 0, as, ps);
    }
    
    @Test
    public void testSurchargeGetInValues() throws Exception {
        CostAndUsageData cauData = new CostAndUsageData(null, 0, null, null, null, as, ps);
        cauData.enableTagGroupCache(true);
        DataSerializer data = cauData.get(null);
        loadSurchargeData(data);
                
        Rule rule = new Rule(getConfig(surchargeConfigYaml), as, ps, rs.getCustomTags());
        FixedRuleProcessor frp = new FixedRuleProcessor(rule, as, ps);
                
        Map<AggregationTagGroup, CostAndUsage[]> inMap = frp.runQuery(rule.getIn(), cauData, true, cauData.getMaxNum(), rule.config.getName());
        
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
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "", "", 63000, 0),
                new TagGroupSpec("Recurring", a1, "eu-west-1", productCode, "", "", 70000, 0),
        };
        
        for (TagGroupSpec spec: specs) {
            TagGroup tg = spec.getTagGroup(a1, as, ps);
            AggregationTagGroup atg = rule.getIn().aggregation.getAggregationTagGroup(tg);
            double got = inMap.get(atg)[0].cost;
            assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value.cost, got, 0.001);
            tg = spec.getTagGroup(a2, as, ps);
            atg = rule.getIn().aggregation.getAggregationTagGroup(tg);
            got = inMap.get(atg)[0].cost;
            assertEquals("Wrong aggregation for " + tg.operation + " " + tg.usageType, spec.value.cost, got, 0.001);
        }
        
        // Now process 
        Map<Query, CostAndUsage[]> operandSingleValueCache = Maps.newHashMap();
        frp.processData(cauData, true, operandSingleValueCache);
        
        DataSerializer outData = cauData.get(null);
        
        assertEquals("Wrong number of entries in the single value cache", 0, operandSingleValueCache.size());

        specs = new TagGroupSpec[]{
                new TagGroupSpec("Recurring", a1, "us-east-1", "ComputedCost", "None", "Dollar", 1890, 63000),
                new TagGroupSpec("Recurring", a1, "eu-west-1", "ComputedCost", "None", "Dollar", 2100, 70000),
                new TagGroupSpec("Recurring", a2, "us-east-1", "ComputedCost", "None", "Dollar", 1890, 63000),
                new TagGroupSpec("Recurring", a2, "eu-west-1", "ComputedCost", "None", "Dollar", 2100, 70000),
        };

        // Should have 4 new items from the aggregated input
        for (TagGroupSpec spec: specs) {
            TagGroup computedCost = spec.getTagGroup(as, ps);
            CostAndUsage value = outData.get(0, computedCost);
            assertNotNull("No value for computed cost", value);
            assertEquals("Wrong cost for computed cost", spec.value.cost, value.cost, .001);
            assertEquals("Wrong usage for computed cost", spec.value.usage, value.usage, .001);
        }
    }

    private String splitCostYaml = "" +
            "name: SplitCost\n" + 
            "start: 2019-11\n" + 
            "end: 2022-11\n" + 
            "operands:\n" + 
            "  total:\n" + 
            "    monthly: true\n" +
            "    filter:\n" + 
            "      product: ['(?!GlobalFee$)^.*$']\n" + 
            "      operation: ['(?!.*Savings - |.*Lent )^.*$'] # ignore lent and savings\n" +
            "    groupBy: []\n" +
            "    groupByTags: []\n" +
            "  lump-cost:\n" +
            "    monthly: true\n" +
            "    filter:\n" + 
            "      costType: [Subscription]\n" +
            "      account: [" + a1 + "]\n" +
            "      region: [global]\n" +
            "      product: [GlobalFee]\n" + 
            "      operation: [None]\n" +
            "      usageType: [Dollar]\n" + 
            "      userTags:\n" + 
            "        Key1: [TagA]\n" + 
            "      singleTagGroup: true\n" + 
            "    groupBy: []\n" +
            "    groupByTags: []\n" +
            "in:\n" + 
            "  filter:\n" + 
            "    product: ['(?!GlobalFee$)^.*$']\n" +
            "    operation: ['(?!.*Savings - |.*Lent )^.*$'] # ignore lent and savings\n" +
            "  groupBy: [account,region]\n" +
            "  groupByTags: [Key1]\n" +
            "results:\n" + 
            "- out:\n" + 
            "    costType: Subscription\n" +
            "    product: GlobalFee\n" +
            "    operation: Split\n" +
            "    usageType: Dollar\n" + 
            "  cost: '${lump-cost.cost} * ${in.cost} / ${total.cost}'\n" + 
            "- out:\n" + 
            "    costType: Subscription\n" +
            "    account: " + a1 + "\n" +
            "    region: global\n" +
            "    product: GlobalFee\n" + 
            "    operation: None\n" +
            "    usageType: Dollar\n" + 
            "    userTags:\n" + 
            "      Key1: TagA\n" + 
            "  single: true\n" + 
            "  cost: 0\n";

    @Test
    public void testGlobalSplit() throws Exception {
        // Split $300 (3% of $10,000) of spend across three accounts based on individual account spend
        TagGroupSpec[] dataSpecs = new TagGroupSpec[]{
                new TagGroupSpec("Subscription", a1, "global", "GlobalFee", "None", "Dollar", 300, 10000),
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "None", "Dollar", 5000, 0),
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "None", "Dollar", 3000, 0),
                new TagGroupSpec("Recurring", a3, "us-east-1", productCode, "None", "Dollar", 1500, 0),
                new TagGroupSpec("Recurring", a3, "us-west-2", productCode, "None", "Dollar", 500, 0),
        };
        
        CostAndUsageData cauData = new CostAndUsageData(null, 0, null, null, null, as, ps);
        cauData.enableTagGroupCache(true);
        DataSerializer data = cauData.get(null);
        TagGroupSpec.loadData(dataSpecs, data, 0, as, ps);
        
        Map<TagGroup, CostAndUsage> in = data.getData(0);
        for (TagGroup tg: in.keySet())
            logger.info("in: " + in.get(tg) + ", " + tg);
        
        Rule rule = new Rule(getConfig(splitCostYaml), as, ps, rs.getCustomTags());
        FixedRuleProcessor frp = new FixedRuleProcessor(rule, as, ps);
        frp.debug = true;
        
        // Check that the operands are flagged as having aggregations
        assertTrue("in operand incorrectly indicates it has no aggregation", rule.getIn().hasAggregation());
        assertTrue("total operand incorrectly indicates it has no aggregation", rule.getOperand("total").hasAggregation());
        assertTrue("lump-cost operand incorrectly indicates it has no aggregation", rule.getOperand("lump-cost").hasAggregation());

        Map<Query, CostAndUsage[]> operandSingleValueCache = Maps.newHashMap();
        frp.processData(cauData, true, operandSingleValueCache);
        
        DataSerializer outData = cauData.get(null);
        Map<TagGroup, CostAndUsage> m = outData.getData(0);
        for (TagGroup tg: m.keySet())
            logger.info("out: " + m.get(tg) + ", " + tg);
                
        assertEquals("Wrong number of entries in the single value cache", 2, operandSingleValueCache.size());

        // Should have zero-ed out the GlobalFee cost
        TagGroup globalFee = new TagGroupSpec("Subscription", a1, "global", "GlobalFee", "None", "Dollar", null).getTagGroup(as, ps);
        CostAndUsage value = outData.get(0, globalFee);
        assertNotNull("No value for global fee", value);
        assertEquals("Wrong value for global fee", 0.0, value.cost, .001);
        
        // Should have 50/30/15/5% split of $300
        TagGroup a1split = new TagGroupSpec("Subscription", a1, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        value = outData.get(0, a1split);
        assertNotNull("No value for global fee on account 1", value);
        assertEquals("wrong value for account 1", 300.0 * 0.5, value.cost, .001);
        
        TagGroup a2split = new TagGroupSpec("Subscription", a2, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        value = outData.get(0, a2split);
        assertNotNull("No value for global fee on account 2", value);
        assertEquals("wrong value for account 2", 300.0 * 0.3, value.cost, .001);
        
        TagGroup a3split = new TagGroupSpec("Subscription", a3, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        value = outData.get(0, a3split);
        assertNotNull("No value for global fee on account 3", value);
        assertEquals("wrong value for account 3", 300.0 * 0.15, value.cost, .001);
        a3split = new TagGroupSpec("Subscription", a3, "us-west-2", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        value = outData.get(0, a3split);
        assertNotNull("No value for global fee on account 4", value);
        assertEquals("wrong value for account 3", 300.0 * 0.05, value.cost, .001);
    }
    
    @Test
    public void testGlobalSplitWithUserTags() throws Exception {
        // Split $300 (3% of $10,000) of spend across three accounts based on individual account spend
        TagGroupSpec[] globalFeeSpecs = new TagGroupSpec[]{
                new TagGroupSpec("Subscription", a1, "global", "GlobalFee", "None", "Dollar", new String[]{"TagA", ""}, 300.0, 10000.0),
        };
        TagGroupSpec[] productSpecs = new TagGroupSpec[]{
                new TagGroupSpec("Recurring", a1, "us-east-1", productCode, "None", "Dollar", new String[]{"Tag1", ""}, 5000.0, 0),
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "None", "Dollar", new String[]{"Tag2", ""}, 3000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-east-1", productCode, "None", "Dollar", new String[]{"Tag3", ""}, 1500.0, 0),
                new TagGroupSpec("Recurring", a3, "us-west-2", productCode, "None", "Dollar", new String[]{"Tag4", ""}, 500.0, 0),
        };
        
        CostAndUsageData cauData = new CostAndUsageData(null, 0, null, null, null, as, ps);
        DataSerializer data = new DataSerializer(2);
        TagGroupSpec.loadData(globalFeeSpecs, data, 0, as, ps);
        Product globalFee = ps.getProduct("GlobalFee", "GlobalFee");
        cauData.put(globalFee, data);
        data = new DataSerializer(2);
        TagGroupSpec.loadData(productSpecs, data, 0, as, ps);
        Product product = ps.getProduct(productCode, productCode);
        cauData.put(product, data);
        cauData.enableTagGroupCache(true);
        
        Map<TagGroup, CostAndUsage> in = data.getData(0);
        for (TagGroup tg: in.keySet())
            logger.info("in: " + in.get(tg) + ", " + tg);
        
        Rule rule = new Rule(getConfig(splitCostYaml), as, ps, rs.getCustomTags());
        FixedRuleProcessor frp = new FixedRuleProcessor(rule, as, ps);
        frp.debug = true;
        
        // Check that the operands are flagged as having aggregations
        assertTrue("in operand incorrectly indicates it has no aggregation", rule.getIn().hasAggregation());
        assertTrue("total operand incorrectly indicates it has no aggregation", rule.getOperand("total").hasAggregation());
        assertTrue("lump-cost operand incorrectly indicates it has no aggregation", rule.getOperand("lump-cost").hasAggregation());

        Map<Query, CostAndUsage[]> operandSingleValueCache = Maps.newHashMap();
        frp.processData(cauData, false, operandSingleValueCache);

        DataSerializer outData = cauData.get(globalFee);
        Map<TagGroup, CostAndUsage> m = outData.getData(0);
        for (TagGroup tg: m.keySet())
            logger.info("globalFee out: " + m.get(tg) + ", " + tg);
                
        assertEquals("Wrong number of entries in the single value cache", 2, operandSingleValueCache.size());

        // Should have zeroed out the GlobalFee cost
        TagGroup globalFeeTag = new TagGroupSpec("Subscription", a1, "global", "GlobalFee", "None", "Dollar", new String[]{"TagA", ""}).getTagGroup(as, ps);
        CostAndUsage value = outData.get(0, globalFeeTag);
        assertNotNull("No value for global fee", value);
        assertEquals("Wrong value for global fee", 0.0, value.cost, .001);
        
        // Should have 50/30/15/5% split of $300
        TagGroup a1split = new TagGroupSpec("Subscription", a1, "us-east-1", "GlobalFee", "Split", "Dollar", new String[]{"Tag1", ""}).getTagGroup(as, ps);
        value = outData.get(0, a1split);
        assertNotNull("No value for global fee on account 1", value);
        assertEquals("wrong value for account 1", 300.0 * 0.5, value.cost, .001);
        
        TagGroup a2split = new TagGroupSpec("Subscription", a2, "us-east-1", "GlobalFee", "Split", "Dollar", new String[]{"Tag2", ""}).getTagGroup(as, ps);
        value = outData.get(0, a2split);
        assertNotNull("No value for global fee on account 2", value);
        assertEquals("wrong value for account 2", 300.0 * 0.3, value.cost, .001);
        
        TagGroup a3split = new TagGroupSpec("Subscription", a3, "us-east-1", "GlobalFee", "Split", "Dollar", new String[]{"Tag3", ""}).getTagGroup(as, ps);
        value = outData.get(0, a3split);
        assertNotNull("No value for global fee on account 3", value);
        assertEquals("wrong value for account 3", 300.0 * 0.15, value.cost, .001);
        a3split = new TagGroupSpec("Subscription", a3, "us-west-2", "GlobalFee", "Split", "Dollar", new String[]{"Tag4", ""}).getTagGroup(as, ps);
        value = outData.get(0, a3split);
        assertNotNull("No value for global fee on account 4", value);
        assertEquals("wrong value for account 3", 300.0 * 0.05, value.cost, .001);
    }
    
    private String splitMonthlyCostByHourYaml = "" +
            "name: SplitCost\n" + 
            "start: 2019-11\n" + 
            "end: 2022-11\n" + 
            "operands:\n" + 
            "  total:\n" + 
            "    monthly: true\n" +
            "    filter:\n" + 
            "      product: ['(?!GlobalFee$)^.*$']\n" + 
            "    groupBy: []\n" +
            "    groupByTags: []\n" +
            "  lump-cost:\n" +
            "    monthly: true\n" +
            "    filter:\n" + 
            "      costType: [Subscription]\n" +
            "      account: [" + a1 + "]\n" +
            "      region: [global]\n" +
            "      product: [GlobalFee]\n" + 
            "      operation: [None]\n" +
            "      usageType: [Dollar]\n" + 
            "      singleTagGroup: true\n" + 
            "in:\n" + 
            "  filter:\n" + 
            "    product: ['(?!GlobalFee$)^.*$']\n" +
            "  groupBy: [account,region]\n" +
            "results:\n" + 
            "- out:\n" + 
            "    costType: Subscription\n" +
            "    product: GlobalFee\n" +
            "    operation: Split\n" +
            "    usageType: Dollar\n" + 
            "  cost: '${lump-cost.cost} * ${in.cost} / ${total.cost}'\n";

    @Test
    public void testMonthlySplitByHour() throws Exception {
        // Split $300 (3% of $10,000) of spend across three accounts and two hours based on individual account spend
        TagGroupSpec[] dataSpecs0 = new TagGroupSpec[]{
                new TagGroupSpec("Subscription", a1, "global", "GlobalFee", "None", "Dollar", 300.0, 0),
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "None", "Dollar", 3000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-east-1", productCode, "None", "Dollar", 2000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-west-2", productCode, "None", "Dollar", 1500.0, 0),
        };        
        TagGroupSpec[] dataSpecs1 = new TagGroupSpec[]{
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "None", "Dollar", 2000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-east-1", productCode, "None", "Dollar", 1000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-west-2", productCode, "None", "Dollar", 500.0, 0),
        };
        
        CostAndUsageData cauData = new CostAndUsageData(null, 0, null, null, null, as, ps);
        cauData.enableTagGroupCache(true);
        DataSerializer data = cauData.get(null);
        TagGroupSpec.loadData(dataSpecs0, data, 0, as, ps);
        TagGroupSpec.loadData(dataSpecs1, data, 1, as, ps);
        
        Rule rule = new Rule(getConfig(splitMonthlyCostByHourYaml), as, ps, rs.getCustomTags());
        FixedRuleProcessor frp = new FixedRuleProcessor(rule, as, ps);
        frp.debug = true;

        Map<Query, CostAndUsage[]> operandSingleValueCache = Maps.newHashMap();
        frp.processData(cauData, true, operandSingleValueCache);
        DataSerializer outData = cauData.get(null);

        Map<TagGroup, CostAndUsage> m = outData.getData(0);
        for (TagGroup tg: m.keySet())
            logger.info("out: " + m.get(tg) + ", " + tg);
        
        assertEquals("Wrong number of entries in the single value cache", 2, operandSingleValueCache.size());

        // Should have 50/30/20% split of $300
        TagGroup a2split = new TagGroupSpec("Subscription", a2, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        CostAndUsage value = outData.get(0, a2split);
        value = value.add(outData.get(1, a2split));
        assertEquals("wrong value for account 2", 300.0 * 0.5, value.cost, .001);
        
        TagGroup a3split = new TagGroupSpec("Subscription", a3, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        value = outData.get(0, a3split);
        value = value.add(outData.get(1, a3split));
        assertEquals("wrong value for account 3", 300.0 * 0.3, value.cost, .001);
        
        a3split = new TagGroupSpec("Subscription", a3, "us-west-2", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        value = outData.get(0, a3split);
        value = value.add(outData.get(1, a3split));
        assertEquals("wrong value for account 3", 300.0 * 0.2, value.cost, .001);
    }

    private String splitMonthlyCostByMonthYaml = "" +
            "name: SplitCost\n" + 
            "start: 2019-11\n" + 
            "end: 2022-11\n" + 
            "operands:\n" + 
            "  total:\n" + 
            "    monthly: true\n" +
            "    filter:\n" + 
            "      product: ['(?!GlobalFee$)^.*$']\n" + 
            "    groupBy: []\n" +
            "    groupByTags: []\n" +
            "  lump-cost:\n" +
            "    monthly: true\n" +
            "    filter:\n" + 
            "      costType: [Subscription]\n" +
            "      account: [" + a1 + "]\n" +
            "      region: [global]\n" +
            "      product: [GlobalFee]\n" + 
            "      operation: [None]\n" +
            "      usageType: [Dollar]\n" + 
            "      singleTagGroup: true\n" + 
            "in:\n" + 
            "  monthly: true\n" +
            "  filter:\n" + 
            "    product: ['(?!GlobalFee$)^.*$']\n" +
            "  groupBy: [account,region]\n" +
            "results:\n" + 
            "- out:\n" + 
            "    costType: Subscription\n" +
            "    product: GlobalFee\n" +
            "    operation: Split\n" +
            "    usageType: Dollar\n" + 
            "  cost: '${lump-cost.cost} * ${in.cost} / ${total.cost}'\n";

    @Test
    public void testMonthlySplitByMonth() throws Exception {
        // Split $300 (3% of $10,000) of spend across three accounts and two hours based on individual account spend
        TagGroupSpec[] dataSpecs0 = new TagGroupSpec[]{
                new TagGroupSpec("Subscription", a1, "global", "GlobalFee", "None", "Dollar", 300.0, 0),
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "None", "Dollar", 3000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-east-1", productCode, "None", "Dollar", 2000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-west-2", productCode, "None", "Dollar", 1500.0, 0),
        };        
        TagGroupSpec[] dataSpecs1 = new TagGroupSpec[]{
                new TagGroupSpec("Recurring", a2, "us-east-1", productCode, "None", "Dollar", 2000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-east-1", productCode, "None", "Dollar", 1000.0, 0),
                new TagGroupSpec("Recurring", a3, "us-west-2", productCode, "None", "Dollar", 500.0, 0),
        };
        
        CostAndUsageData cauData = new CostAndUsageData(null, 0, null, null, null, as, ps);
        cauData.enableTagGroupCache(true);
        DataSerializer data = cauData.get(null);
        TagGroupSpec.loadData(dataSpecs0, data, 0, as, ps);
        TagGroupSpec.loadData(dataSpecs1, data, 1, as, ps);
        
        Rule rule = new Rule(getConfig(splitMonthlyCostByMonthYaml), as, ps, rs.getCustomTags());
        FixedRuleProcessor frp = new FixedRuleProcessor(rule, as, ps);
        frp.debug = true;

        Map<Query, CostAndUsage[]> operandSingleValueCache = Maps.newHashMap();
        frp.processData(cauData, true, operandSingleValueCache);
        DataSerializer outData = cauData.get(null);
                
        Map<TagGroup, CostAndUsage> m = outData.getData(0);
        for (TagGroup tg: m.keySet())
            logger.info("out: " + m.get(tg) + ", " + tg);

        assertEquals("Wrong number of entries in the single value cache", 2, operandSingleValueCache.size());

        // Should have 50/30/20% split of $300
        TagGroup a2split = new TagGroupSpec("Subscription", a2, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        CostAndUsage value = outData.get(0, a2split);
        assertEquals("wrong value for account 2", 300.0 * 0.5, value.cost, .001);
        
        TagGroup a3split = new TagGroupSpec("Subscription", a3, "us-east-1", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        value = outData.get(0, a3split);
        assertEquals("wrong value for account 3", 300.0 * 0.3, value.cost, .001);
        
        a3split = new TagGroupSpec("Subscription", a3, "us-west-2", "GlobalFee", "Split", "Dollar", null).getTagGroup(as, ps);
        value = outData.get(0, a3split);
        assertEquals("wrong value for account 3", 300.0 * 0.2, value.cost, .001);
    }
}
