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
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;

public class PostProcessorTest {
    static private ProductService ps;
	static private AccountService as;
	
	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
		ps.getProduct(Product.Code.CloudFront);
		ps.getProduct(Product.Code.CloudWatch);
	}
	    

	private RuleConfig getConfig(String yaml) throws JsonParseException, JsonMappingException, IOException {		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		RuleConfig rc = new RuleConfig();
		return mapper.readValue(yaml, rc.getClass());
	}
	
	class Datum {
		int num;
		Double value;
		TagGroup tg;
		
		public Datum(int num, Double value, TagGroup tg) {
			this.num = num;
			this.value = value;
			this.tg = tg;
		}
	}

	@Test
	public void testAggregateSummaryData() throws Exception {
		BasicResourceService rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
		String reportYaml = "" +
				"name: report-test\n" + 
				"start: 2019-11\n" + 
				"end: 2022-11\n" + 
				"report:\n" + 
				"  aggregate: [monthly]\n" + 
				"  includeCostType: true\n" + 
				"in:\n" + 
				"  type: cost\n" + 
				"  filter:\n" + 
				"    userTags:\n" +
				"      Key2: [compute]\n" +
				"  groupBy: [account]\n" +
				"";
		List<RuleConfig> rules = Lists.newArrayList(getConfig(reportYaml));
    	Account a = as.getAccountById("111111111111");
        Datum[] data = new Datum[]{
        		new Datum(0,  5.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","", "clusterA", "compute", ""}))),
        		new Datum(0, 25.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","extra1A", "clusterA", "twenty-five", "extra2A"}))),
        		new Datum(0, 30.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","extra1B", "clusterA", "thirty", "extra2A"}))),
        		new Datum(0, 40.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","extra1B", "clusterA", "forty", "extra2B"}))),
        		new Datum(1,  5.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","", "clusterA", "compute", ""}))),
        		new Datum(2,  5.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","", "clusterA", "compute", ""}))),
        		new Datum(24,  5.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","", "clusterA", "compute", ""}))),
        		new Datum(25,  5.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","", "clusterA", "compute", ""}))),
        		new Datum(742,  5.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","", "clusterA", "compute", ""}))),
        		new Datum(743,  5.0, TagGroup.getTagGroup(a, null, null, null, null, null, ResourceGroup.getResourceGroup(new String[]{"Recurring","", "clusterA", "compute", ""}))),
        };

		ReadWriteData rwData = new ReadWriteData();
	    for (int i = 0; i < data.length; i++) {
	    	Datum d = data[i];
	    	rwData.add(d.num, d.tg, d.value);	
	    }
		
        List<Map<TagGroup, Double>> daily = Lists.newArrayList();
        List<Map<TagGroup, Double>> monthly = Lists.newArrayList();
        
        PostProcessor pp = new PostProcessor(rules, as, ps, rs, null);
        pp.aggregateSummaryData(rwData, daily, monthly);
        
        assertEquals("wrong number of monthly entries", 1, monthly.size());
        assertEquals("wrong number of daily entries", 31, daily.size());
	}

}
