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
package com.netflix.ice.reader;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UsageType;

public class TagListsTest {
	List<CostType> costTypes = Lists.newArrayList(CostType.recurring);
	Account[] accounts = new Account[]{
			new Account("123456789012", "Account1", null)
	};
	List<String> regions = Lists.newArrayList(new String[]{"us-east-1"});
	ProductService ps = new BasicProductService();


	@Test
	public void testContainsTagGroup() throws ResourceException {
		class Test {
			String[] resourceGroupName;
			boolean shouldBeTrue;
			List<ResourceGroup> resourceGroups;
			
			Test(String[] resourceGroupName, boolean shouldBeTrue, List<ResourceGroup> resourceGroups) {
				this.resourceGroupName = resourceGroupName;
				this.shouldBeTrue = shouldBeTrue;
				this.resourceGroups = resourceGroups;				
			}
			
			public void Run() throws ResourceException {
				TagLists tagLists = new TagLists(
						costTypes,
						Lists.newArrayList(accounts),
						Region.getRegions(regions),
						null, // zones
						null, // products
						null, // operations
						null, // usageTypes
						null
						);
				
				TagGroup tg = TagGroup.getTagGroup(CostType.recurring, accounts[0], Region.getRegionByName("us-east-1"), null, ps.getProduct("Product", "ProductCode"), Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
						ResourceGroup.getResourceGroup(resourceGroupName));
				ResourceGroup resourceGroupInTagLists = resourceGroups == null || resourceGroups.size() == 0 ? null : resourceGroups.get(0);
				if (shouldBeTrue)
					assertTrue("TagGroup not found in TagLists, resource group name: " + resourceGroupName + ", groups: " + resourceGroupInTagLists, tagLists.contains(tg));
				else
					assertFalse("TagGroup should not have been found in TagLists, resource group name: " + resourceGroupName + ", groups: " + resourceGroupInTagLists, tagLists.contains(tg));
			}
		}
		
		Test[] tests = new Test[]{
				// TagGroup with a match due to null entries in TagLists
				new Test(new String[]{"foo"}, true, null),
				// TagGroup with a match on single resource name
				new Test(new String[]{"foo"}, true, Lists.newArrayList(ResourceGroup.getResourceGroup(new String[]{"foo"}))),
				// TagGroup with a match on multi-field resource name
				new Test(new String[]{"foo","bar"}, true, Lists.newArrayList(ResourceGroup.getResourceGroup(new String[]{"foo","bar"}))),
				// TagGroup with a match on second-field of resource name
				new Test(new String[]{"","bar"}, true, Lists.newArrayList(ResourceGroup.getResourceGroup(new String[]{"","bar"}))),
				// TagGroup with a second-field matching to null list
				new Test(new String[]{"","bar"}, true, null),
		};
		
		for (Test t: tests) {
			t.Run();
		}
		
	}
	
}
