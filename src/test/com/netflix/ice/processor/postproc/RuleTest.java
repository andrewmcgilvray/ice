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
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;

public class RuleTest {
    static private ProductService ps;
	static private AccountService as;
	static private BasicResourceService rs;

	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
		rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
	}

	private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
		RuleConfig rc = new RuleConfig();
		return mapper.readValue(yaml, rc.getClass());
	}
	
	private String ruleTestYaml = "" +
			"name: ruleTest\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"operands:\n" + 
			"  accountAgg:\n" + 
			"    groupBy: [region,zone,product,operation,usageType]\n" +
			"    filter:\n" + 
			"      usageType: ['Usage']\n" + 
			"  accountFilterByList:\n" + 
			"    filter:\n" + 
			"      account: [1,2,3]\n" +
			"      usageType: ['Usage']\n" + 
			"  accountExFilter:\n" + 
			"    filter:\n" + 
			"      account: [1,2,3]\n" +
			"      usageType: ['Usage']\n" + 
			"      exclude: [account,usageType]\n" + 
			"in:\n" + 
			"  filter:\n" + 
			"    product: [Test]\n" + 
			"    usageType: ['..-Usage']\n" +
			"results:\n" +
			"- out:\n" + 
			"    product: Foo\n" + 
			"    usageType: Bar\n" + 
			"  cost: '0'\n";
	
	@Test
	public void testOperandHasAggregation() throws Exception {
		Rule rule = new Rule(getConfig(ruleTestYaml), as, ps, rs.getCustomTags());
		assertFalse("in operand incorrectly indicates that it has aggregation", rule.getIn().hasAggregation());
		assertTrue("accountAgg operand incorrectly indicates it has no aggregation", rule.getOperand("accountAgg").hasAggregation());
		assertFalse("accountAggByList operand incorrectly indicates it has no aggregation", rule.getOperand("accountFilterByList").hasAggregation());
		assertFalse("accountExAgg operand incorrectly indicates it has no aggregation", rule.getOperand("accountExFilter").hasAggregation());
	}
	
	private String patternTestYaml = "" +
			"name: patternTest\n" + 
			"start: 2019-11\n" + 
			"end: 2022-11\n" + 
			"in:\n" + 
			"  filter:\n" + 
			"    product: [Test]\n" + 
			"    usageType: ['..-Requests-Tier[12]']\n" +
			"patterns:\n" +
			"  region: '(..)-.*'\n" +
			"  tier: '.*([12])'\n" +
			"results:\n" +
			"- out:\n" + 
			"    product: Foo\n" + 
			"    usageType: '${region}-Bar-${tier}'\n" + 
			"  cost: '0'\n";
	@Test
	public void testGetTag() throws Exception {
		Rule rule = new Rule(getConfig(patternTestYaml), as, ps, rs.getCustomTags());
		String got = rule.getTag("${region}-Bar-${tier}", "US-Requests-Tier1");
		String expect = "US-Bar-1";
		assertEquals("wrong string from template replacement", expect, got);
	}

	@Test
	public void testTagGroupWithResources() throws Exception {
		String ruleYaml = "" +
				"name: getGroupTest\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"in:\n" + 
				"  filter:\n" + 
				"    product: [Test]\n" + 
				"results:\n" +
				"- out:\n" + 
				"    account: 123456789012\n" + 
				"    region: us-east-1\n" + 
				"    product: " + Product.Code.Ec2.toString() + "\n" + 
				"    operation: OP1\n" + 
				"    usageType: UT1\n" + 
				"    userTags:\n" + 
				"      Key1: tag1\n" + 
				"  cost: '0'\n";

		RuleConfig ruleConfig = getConfig(ruleYaml);
		Rule r = new Rule(ruleConfig, as, ps, rs.getCustomTags());
		
		TagGroup tg = r.getResult(0).tagGroup(null, as, ps, false);
		ResourceGroup expect = ResourceGroup.getResourceGroup(new String[]{"tag1", ""});
		assertEquals("incorrect resourceGroup string", expect, tg.resourceGroup);
	}


}
