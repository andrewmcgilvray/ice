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

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.processor.postproc.Rule;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Zone.BadZone;

public class AggregationTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static AccountService as = new BasicAccountService();
	private static ProductService ps = new BasicProductService();
	
	private static Map<TagGroup, Double> data = Maps.newHashMap();
	static {
		// Generate test data
		List<String> permutations = getPermutations("12345");
		
		Double value = 1.0;
		for (String counts: permutations) {
			try {
					data.put(TagGroup.getTagGroup(
							"acct" + counts.charAt(0), 
							"us-east-1", 
							"us-east-1a", 
							"prod" + counts.charAt(1), 
							"op" + counts.charAt(2), 
							"ut" + counts.charAt(3), "",
							new String[]{"grp" + counts.charAt(4)}, as, ps), value);
			} catch (ResourceException e) {
				e.printStackTrace();
			} catch (BadZone e) {
				e.printStackTrace();
			}
		}
	}
	
	private static List<String> getPermutations(String counts) {
		List<String> ret = Lists.newArrayList();
		
		if (counts.length() == 1) {
			ret.add(counts);
		}
		else if (counts.length() > 1) {
			for (int i = 0; i < counts.length(); i++) {
				String c = counts.substring(i, i+1);
				String remaining = counts.substring(0,i) + counts.substring(i+1);
				for (String p: getPermutations(remaining))
					ret.add(c + p);
			}
		}
		return ret;
	}
	
	@Test
	public void test() throws Exception {
		List<Rule.TagKey> groupByTags = Lists.newArrayList(new Rule.TagKey[]{Rule.TagKey.account, Rule.TagKey.product, Rule.TagKey.region});
		List<Integer> groupByUserTagIndeces = Lists.newArrayList();
		
		Aggregation ag = new Aggregation(groupByTags, groupByUserTagIndeces);
		
		Map<AggregationTagGroup, Double> aggregationMap = Maps.newHashMap();
		for (TagGroup tg: data.keySet()) {
			AggregationTagGroup atg = ag.getAggregationTagGroup(tg);
			aggregationMap.put(atg, (aggregationMap.containsKey(atg) ? aggregationMap.get(atg) : 0.0) + data.get(tg));
		}
		
		// Should have 5! (5 factorial == 120) entries
		assertEquals("wrong number of tagGroups in test data", 120, data.size());
		
		assertEquals("wrong number of aggregations", 20, aggregationMap.size());
		// Each of the 20 aggregations should have the value 6.0
		for (AggregationTagGroup atg: aggregationMap.keySet())
			assertEquals("wrong value for aggregation of " + atg, 6.0, aggregationMap.get(atg), 0.001);
	}

}
