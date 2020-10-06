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

import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;

public class OperandTest {
	static private BasicAccountService as;
	static private BasicProductService ps;
	static private BasicResourceService rs;
	
	@BeforeClass
	public static void init() {
		List<Account> accts = Lists.newArrayList();
		accts.add(new Account("123456789012", "Account1", null));
		accts.add(new Account("234567890123", "Account2", null));
		as = new BasicAccountService(accts);
		ps = new BasicProductService();
		rs = new BasicResourceService(ps, new String[]{"Key1","Key2"}, false);
	}
	
	@Test
	public void testTagGroupWithResources() throws Exception {
		OperandConfig oc = new OperandConfig();
		oc.setAccount("123456789012");
		oc.setRegion("us-east-1");
		oc.setProduct(Product.Code.Ec2.toString());
		oc.setOperation("OP1");
		oc.setUsageType("UT1");
		Map<String, String> userTags = Maps.newHashMap();
		userTags.put("Key1", "tag1");
		oc.setUserTags(userTags);
		Operand o = new Operand(oc, as, rs.getCustomTags());
		
		TagGroup tg = o.tagGroup(null, as, ps, false);
		ResourceGroup expect = ResourceGroup.getResourceGroup(new String[]{"tag1", ""});
		assertEquals("incorrect resourceGroup string", expect, tg.resourceGroup);
	}

}
