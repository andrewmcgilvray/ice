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
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;

public class TagListsWithUserTagsTest {
	Account[] accounts = new Account[]{
			new Account("123456789012", "Account1", null)
	};
	List<String> regions = Lists.newArrayList(new String[]{"us-east-1"});
	List<UserTag> tags0 = UserTag.getUserTags(Lists.newArrayList(new String[]{"", "t0v0"}));
	List<UserTag> tags1 = UserTag.getUserTags(Lists.newArrayList(new String[]{"", "t1v0", "t1v1", "t1v2", "t1v3"}));
	List<UserTag> tags2 = UserTag.getUserTags(Lists.newArrayList(new String[]{"t2v0"}));
	List<UserTag> tags3 = Lists.newArrayList();
	ProductService ps = new BasicProductService();

	@Test
	public void testContainsTagGroup() throws ResourceException {
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		userTagLists.add(tags0);
		userTagLists.add(tags1);
		userTagLists.add(tags2);
		userTagLists.add(tags3);
		
		TagListsWithUserTags tagLists = new TagListsWithUserTags(
				Lists.newArrayList(accounts),
				Region.getRegions(regions),
				null, // zones
				null, // products
				null, // operations
				null, // usageTypes
				userTagLists
				);
		
		Product product = ps.getProduct("AWS Product", "AWS Product Code");
		// TagGroup with a match on each user tag
		TagGroup tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
				ResourceGroup.getResourceGroup(new String[]{"t0v0","t1v1","t2v0",""}));
		assertTrue("TagGroup not found in TagLists", tagLists.contains(tg));

		// TagGroup with a match on first and last and an empty match in the middle
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
				ResourceGroup.getResourceGroup(new String[]{"t0v0","","t2v0",""}));
		assertTrue("TagGroup not found in TagLists", tagLists.contains(tg));

		// First two user tags should match, but not the third because we don't have an empty element in the tagLists
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
				ResourceGroup.getResourceGroup(new String[]{"","t1v1","",""}));
		assertFalse("TagGroup incorrectly found in TagLists", tagLists.contains(tg));

		// TagGroup with match on first and last, but second tag has non-empty non-matching value
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
				ResourceGroup.getResourceGroup(new String[]{"t0v0","t1v4","t2v0",""}));
		assertFalse("TagGroup incorrectly found in TagLists", tagLists.contains(tg));

		// TagGroup with null resourceGroup
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), null);
		assertTrue("TagGroup not found in TagLists", tagLists.contains(tg));
		assertFalse("TagGroup incorrectly found in TagLists", tagLists.contains(tg, true));

		// Add an empty value to tag2
		userTagLists.get(2).add(UserTag.empty);
		
		// TagGroup with null resourceGroup
		assertTrue("TagGroup not found in TagLists", tagLists.contains(tg, true));
		
		// TagGroup with a match on the first and second tags and an empty match on the third
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
				ResourceGroup.getResourceGroup(new String[]{"t0v0","t1v0","",""}));
		assertTrue("TagGroup not found in TagLists", tagLists.contains(tg));
		
		
		// TagGroup with only first user tag
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
				ResourceGroup.getResourceGroup(new String[]{"t0v0","","",""}));
		assertTrue("TagGroup not found in TagLists", tagLists.contains(tg, true));
		
		Product cloudWatch = ps.getProduct(Product.Code.CloudWatch);
		tagLists = new TagListsWithUserTags(
				Lists.newArrayList(accounts),
				Region.getRegions(regions),
				null, // zones
				Lists.newArrayList(cloudWatch), // products
				Lists.newArrayList(Operation.getOperation("MetricStorage:AWS/EC2")), // operations
				Lists.newArrayList(UsageType.getUsageType("CW:MetricMonitorUsage", "")), // usageTypes
				userTagLists
				);		
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, cloudWatch, Operation.getOperation("MetricStorage:AWS/EC2"), UsageType.getUsageType("CW:MetricMonitorUsage", ""), 
				ResourceGroup.getResourceGroup(new String[]{"","","t2v0",""}));
		assertTrue("TagGroup not found in TagLists", tagLists.contains(tg));
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, cloudWatch, Operation.getOperation("MetricStorage:AWS/EC2"), UsageType.getUsageType("CW:MetricMonitorUsage", ""), 
				ResourceGroup.getResourceGroup(new String[]{"","","",""}));
		assertTrue("TagGroup not found in TagLists", tagLists.contains(tg));
		tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, cloudWatch, Operation.getOperation("MetricStorage:AWS/EC2"), UsageType.getUsageType("CW:MetricMonitorUsage", ""), 
				ResourceGroup.getResourceGroup(new String[]{"","t1v2","t2v2",""}));
		assertFalse("TagGroup incorrectly found in TagLists", tagLists.contains(tg));

		
		List<UserTag> userTags = Lists.newArrayList(UserTag.empty);
		userTagLists = Lists.newArrayList();
		userTagLists.add(userTags);
		userTagLists.add(userTags);
		userTagLists.add(userTags);
		
		tagLists = new TagListsWithUserTags(
				Lists.newArrayList(accounts),
				Region.getRegions(regions),
				null, // zones
				null, // products
				null, // operations
				null, // usageTypes
				userTagLists
				);		
	}
	
	@Test
	public void testContainsTag() throws ResourceException {
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		userTagLists.add(tags0);
		userTagLists.add(tags1);
		userTagLists.add(tags2);
		userTagLists.add(tags3);
		
		TagListsWithUserTags tagLists = new TagListsWithUserTags(
				Lists.newArrayList(accounts),
				Region.getRegions(regions),
				null, // zones
				null, // products
				null, // operations
				null, // usageTypes
				userTagLists
				);
		
		class Test {
			public String userTag;
			public int tagIndex;
			public boolean shouldBeTrue;
			
			Test(String userTag, int tagIndex, boolean shouldBeTrue) {
				this.userTag = userTag;
				this.tagIndex = tagIndex;
				this.shouldBeTrue = shouldBeTrue;
			}
			
			void Run(TagLists tagLists) {
				if (shouldBeTrue)
					assertTrue("User tag " + userTag + ", " + tagIndex + " not found in TagLists", tagLists.contains(UserTag.get(userTag), TagType.Tag, tagIndex));
				else
					assertFalse("User tag " + userTag + ", " + tagIndex + " incorrectly found in TagLists", tagLists.contains(UserTag.get(userTag), TagType.Tag, tagIndex));
			}
		};

		Test[] tests = new Test[]{
				new Test("", 3, true), // Untagged resource, Check against user tag with empty tag list
				new Test("", 2, false), // Untagged resource, Check against user tag with no <none> entry in tag lists
				new Test("", 1, true), // Untagged resource, Check against user tag with a <none> entry in tag lists (but no none entry in the last user tag)
				new Test("", 0, true), // Untagged resource, Check against user tag with a <none> entry in tag lists (but no none entry in the last user tag)
		};
		for (Test t: tests)
			t.Run(tagLists);
		
		// Add an empty value to tag2
		userTagLists.get(2).add(UserTag.empty);
		
		tests = new Test[]{
				new Test("", 3, true), // Untagged resource, Check against user tag with a empty tag list
				new Test("", 2, true), // Untagged resource, Check against user tag with a <none> entry in all tag lists
				new Test("", 1, true), // Untagged resource, Check against user tag with a <none> entry in all tag lists
				new Test("", 0, true), // Untagged resource, Check against user tag with a <none> entry in all tag lists
		};
		for (Test t: tests)
			t.Run(tagLists);
		
		Product product = ps.getProduct("AWS Product", "AWS Product Code");
		
		TagGroup tg = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
				ResourceGroup.getResourceGroup(new String[]{"t1v1","t2v2"}));
		assertTrue("Account tag not found in TagLists", tagLists.contains(tg.account, TagType.Account, 0));
		
		tests = new Test[]{
				new Test("t0v0", 0, true),
				new Test("t0v1", 0, false),
				new Test("t1v1", 1, true),
				new Test("t0v0", 1, false), // tag 0 value in tag 1 slot
				new Test("t2v0", 2, true),
				new Test("t0v0", 2, false),
				new Test("", 2, true),
		};
		
		for (Test t: tests)
			t.Run(tagLists);
		
	}
	
	@Test
	public void testGetTagLists() throws ResourceException {
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		userTagLists.add(tags0);
		userTagLists.add(tags1);
		userTagLists.add(tags2);
		
		// Add an empty value to tag2
		userTagLists.get(2).add(UserTag.empty);
		
		TagListsWithUserTags tagLists = new TagListsWithUserTags(
				Lists.newArrayList(accounts),
				Region.getRegions(regions),
				null, // zones
				null, // products
				null, // operations
				null, // usageTypes
				userTagLists
				);
		Tag tag = Region.getRegionByName("us-east-1");
        TagLists tl = tagLists.getTagLists(tag, TagType.Region, 0);
        assertEquals("wrong number of user tags for resource", userTagLists.size(), ((TagListsWithUserTags) tl).resourceUserTagLists.size());
        assertEquals("wrong number of regions", 1, tl.regions.size());
        assertEquals("wrong region", tag, tl.regions.get(0));
        
		tag = ps.getProduct(Product.Code.CloudWatch);
        tl = tagLists.getTagLists(tag, TagType.Product, 0);
        assertEquals("wrong number of user tags for resource", userTagLists.size(), ((TagListsWithUserTags) tl).resourceUserTagLists.size());
        assertEquals("wrong number of products", 1, tl.products.size());
        assertEquals("wrong product", tag, tl.products.get(0));

        Product product = ps.getProduct("Product", "ProductCode");
		TagGroup userTagTagGroup = TagGroup.getTagGroup(accounts[0], Region.getRegionByName("us-east-1"), null, product, Operation.getOperation("Operation"), UsageType.getUsageType("UsageType", ""), 
				ResourceGroup.getResourceGroup(new String[]{"t0v0","","",""}));

		tag = UserTag.get("t0v0");
        tl = tagLists.getTagLists(tag, TagType.Tag, 0);
        assertEquals("wrong number of user tags for resource", 3, ((TagListsWithUserTags) tl).resourceUserTagLists.size());
        assertEquals("wrong number of tags", 1, ((TagListsWithUserTags) tl).resourceUserTagLists.get(0).size());
        assertEquals("wrong tag", tag, ((TagListsWithUserTags) tl).resourceUserTagLists.get(0).get(0));
        
        assertTrue("tagLists didn't contain userTagTagGroup", tl.contains(userTagTagGroup, true));
        
		tag = UserTag.empty;
        tl = tagLists.getTagLists(tag, TagType.Tag, 0);
        assertEquals("wrong number of user tags for product resource", 3, ((TagListsWithUserTags) tl).resourceUserTagLists.size());
        assertEquals("wrong number of tags", 1, ((TagListsWithUserTags) tl).resourceUserTagLists.get(0).size());
        assertEquals("wrong tag", tag, ((TagListsWithUserTags) tl).resourceUserTagLists.get(0).get(0));

        assertFalse("tagLists contains userTagTagGroup", tl.contains(userTagTagGroup, true));
        
	}
	
}
