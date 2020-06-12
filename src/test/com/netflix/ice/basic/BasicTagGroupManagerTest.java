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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.reader.TagListsWithUserTags;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone.BadZone;

public class BasicTagGroupManagerTest {
	private static ProductService productService = new BasicProductService();
	private static AccountService accountService = new BasicAccountService();
	public final static DateTime testMonth = new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC);
	private static Account a1;
	private static Product ec2;

	@BeforeClass
	public static void init() {
		a1 = accountService.getAccountByName("Account1");
		accountService.getAccountByName("Account2");
		ec2 = productService.getProduct(Product.Code.Ec2Instance);
	}
	
	private BasicTagGroupManager getTagGroupManager(TagGroup[] tagGroups) {
		TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups = Maps.newTreeMap();
		List<TagGroup> tagGroupList = Lists.newArrayList();
		for (TagGroup tg: tagGroups)
			tagGroupList.add(tg);
		tagGroupsWithResourceGroups.put(testMonth.getMillis(), tagGroupList);
		Interval interval = new Interval(testMonth.getMillis(), testMonth.plusMonths(1).getMillis());		
		
		return new BasicTagGroupManager(tagGroupsWithResourceGroups, interval, 2);
	}
	
	@Test
	public void testGetTagListsMap() throws Exception {
		TagGroup[] tagGroups = new TagGroup[]{
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"", ""}, accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"TagA", ""}, 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"TagB", ""}, 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"", "TagX"}, 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"", "TagY"}, 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"TagA", "TagX"}, accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"TagB", "TagY"}, accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"", ""}, 		accountService, productService),
				TagGroup.getTagGroup("Account2", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"", ""}, accountService, productService),
				TagGroup.getTagGroup("Account2", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"TagA", ""}, 	accountService, productService),
				TagGroup.getTagGroup("Account2", "us-east-1", "us-east-1a", "ProductA", "OperationA", "UsageTypeA", "", new String[]{"", "TagX"}, 	accountService, productService),
				// Savings operation tags that should be filtered if not for reservations
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "Savings - Spot", "m1.small", "hour", new String[]{"", ""}, 	accountService, productService),
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "Savings RIs - All Upfront", "m1.small", "hour", new String[]{"", ""}, 	accountService, productService),
				// Lent or Borrowed (but not both) should be filtered based on showLent flag
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "Lent RIs - All Upfront", "m1.small", "hour", new String[]{"", ""}, 	accountService, productService),				
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "Borrowed RIs - All Upfront", "m1.small", "hour", new String[]{"", ""}, 	accountService, productService),				
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "LentAmortized RIs - All Upfront", "m1.small", "hour", new String[]{"", ""}, 	accountService, productService),				
				TagGroup.getTagGroup("Account1", "us-east-1", "us-east-1a", "EC2 Instance", "BorrowedAmortized RIs - All Upfront", "m1.small", "hour", new String[]{"", ""}, 	accountService, productService),				
		};
		
		BasicTagGroupManager manager = getTagGroupManager(tagGroups);
		Interval interval = new Interval(testMonth.getMillis(), testMonth.plusMonths(1).getMillis());		
		
		//
		// Test non-resource tags with no tagLists filtering
		//
		TagLists tagLists = new TagLists();

		// Group by account
		List<Operation.Identity.Value> exclude = Operation.exclude(null, false, true, false);		
		Map<Tag, TagLists> groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Account, exclude);
		assertEquals("wrong number of groupBy tags for account", 2, groupByLists.size());
		
		// Group by region
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Region, exclude);
		assertEquals("wrong number of groupBy tags for region", 1, groupByLists.size());
		assertEquals("wrong number of operations in tagLists for groupBy region", 3, groupByLists.get(Region.US_EAST_1).operations.size());
		
		// Group by none
		groupByLists = manager.getTagListsMap(interval, tagLists, null, exclude);
		assertEquals("wrong number of groupBy tags for none", 1, groupByLists.size());
		assertEquals("wrong number of operations in tagLists for groupBy none", 3, groupByLists.get(Tag.aggregated).operations.size());
		
		// Group by operation for reservation with borrowed
		exclude = Operation.exclude(null, false, true, true);		
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Operation, exclude);
		assertEquals("wrong number of groupBy tags for operation", 5, groupByLists.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.spotInstanceSavings).operations.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.savingsAllUpfront).operations.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.borrowedInstancesAllUpfront).operations.size());
		
		// Group by operation for reservation with lent
		exclude = Operation.exclude(null, true, true, true);		
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Operation, exclude);
		assertEquals("wrong number of groupBy tags for operation", 5, groupByLists.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.spotInstanceSavings).operations.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.savingsAllUpfront).operations.size());
		assertEquals("wrong number of operations in tagLists for groupBy operation", 1, groupByLists.get(ReservationOperation.lentInstancesAllUpfront).operations.size());
		
		//
		// Test non-resource tags with tagLists filtering
		//
		exclude = Operation.exclude(null, false, true, false);
		tagLists = new TagLists(Lists.newArrayList(accountService.getAccountByName("Account1")));
		// Group by account one match
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Account, exclude);
		assertEquals("wrong number of groupBy tags for account", 1, groupByLists.size());
		
		tagLists = new TagLists(Lists.newArrayList(accountService.getAccountByName("Account3")));
		// Group by account - no matches
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Account, exclude);
		assertEquals("wrong number of groupBy tags for account", 0, groupByLists.size());

		//
		// Test Resources with user tags but no tagLists filtering
		//
    	List<List<UserTag>> resourceTagLists = Lists.newArrayList();
    	resourceTagLists.add(null);
    	resourceTagLists.add(null);
		tagLists = new TagListsWithUserTags(null, null, null, null, null, null, resourceTagLists);
		
		// Group by first user tag
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, exclude, 0);
		assertEquals("wrong number of groupBy tags for user tag 0", 3, groupByLists.size());
		
		// Group by second user tag
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, exclude, 1);
		assertEquals("wrong number of groupBy tags for user tag 1", 3, groupByLists.size());

	
		//
		// Test Resources with user tags and tagLists filtering
		//
		resourceTagLists = Lists.newArrayList();
		
		// Group by first tag and only return empties		
		resourceTagLists.add(Lists.<UserTag>newArrayList());
    	resourceTagLists.add(Lists.<UserTag>newArrayList());
    	// Add all the possible second user tag values so we only test filtering against the first
		resourceTagLists.get(1).add(UserTag.get("TagX"));
		resourceTagLists.get(1).add(UserTag.get("TagY"));		
		// Should now be: resourceTagLists[[],["TagX","TagY"]]
		
		tagLists = new TagListsWithUserTags(null, null, null, null, null, null, resourceTagLists);
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, exclude, 0);
		assertEquals("wrong number of groupBy tags for user tag - filter all but empties", 3, groupByLists.size());
		for (TagLists tl: groupByLists.values()) {
			assertTrue("wrong instance type for tagLists", tl instanceof TagListsWithUserTags);
		}
		
		// Now Add empty string to both the first and second tags
		resourceTagLists.get(0).add(UserTag.empty);
		resourceTagLists.get(1).add(UserTag.empty);
		// Should now be: resourceTagLists[[""],["","TagX","TagY"]]
    	
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, exclude, 0);
		assertEquals("wrong number of groupBy tags for user tag - filter all but empties", 1, groupByLists.size());
		for (TagLists tl: groupByLists.values()) {
			assertTrue("wrong instance type for tagLists", tl instanceof TagListsWithUserTags);
		}
		
		// Add one of the non-empty values
		resourceTagLists.get(0).add(UserTag.get("TagB"));
		// Should now be: resourceTagLists[["","TagB"],["","TagX","TagY"]]
		// Test for the first user tag
		groupByLists = manager.getTagListsMap(interval, tagLists, TagType.Tag, exclude, 0);
		assertEquals("wrong number of groupBy tags for user tag - wanted empties and TagB", 2, groupByLists.size());
		for (TagLists tl: groupByLists.values()) {
			assertTrue("wrong instance type for tagLists", tl instanceof TagListsWithUserTags);
		}
		// Make sure we have the three values for the second user tag on both group lists
		TagListsWithUserTags tl = (TagListsWithUserTags) groupByLists.get(UserTag.empty);
		assertEquals("wrong number of values in empty tag second user tags list", 3, tl.resourceUserTagLists.get(1).size());
		tl = (TagListsWithUserTags) groupByLists.get(UserTag.get("TagB"));
		assertEquals("wrong number of values in TagB tag second user tags list", 3, tl.resourceUserTagLists.get(1).size());
		
		
		for (Tag groupBy: groupByLists.keySet()) {
			tagLists = groupByLists.get(groupBy);
			if (groupBy.name.equals("TagB")) {
		        for (TagGroup tagGroup: tagGroups) {
					boolean contains = tagLists.contains(tagGroup, true);
					assertEquals("contains() returned wrong state on tag for: " + tagGroup, tagGroup.resourceGroup.getUserTags()[0].name.equals("TagB"), contains);
		        }
			}
			else if (groupBy.name.isEmpty()) {
		        for (TagGroup tagGroup: tagGroups) {
					boolean contains = tagLists.contains(tagGroup, true);
					boolean firstUserTagEmpty = tagGroup.resourceGroup.getUserTags()[0].name.isEmpty();
		        	if (tagGroup.operation.isLent() || tagGroup.operation.isSavings())
		        		assertEquals("contains returned wrong state on reservation op: " + tagGroup, false, contains);
		        	else
		        		assertEquals("contains returned wrong state on empty tag for: " + tagGroup, firstUserTagEmpty, contains);
		        }
			}
			else {
				fail("unexpected tag in groupBy map");
			}
		}
	}

	private TagGroup getTagGroup(Operation operation) {
		return TagGroup.getTagGroup(a1, Region.US_EAST_1, null, ec2, operation, UsageType.getUsageType("None", ""), null);
	}
	
	@Test
	public void testGetOperations() throws BadZone {
		List<TagGroup> tagGroups = Lists.newArrayList();
		int numLent = 0;
		int numAmort = 0;
		for (Operation op: Operation.getReservationOperations(true)) {
			if (op.isLent())
				numLent++;
			if (op.isAmortized())
				numAmort++;
			tagGroups.add(getTagGroup(op));
		}
		
		TagGroup[] tga = new TagGroup[tagGroups.size()];
		tagGroups.toArray(tga);
		BasicTagGroupManager manager = getTagGroupManager(tga);
		
		assertEquals("wrong number of lent operations", 11, numLent);
		assertEquals("wrong number of amortized operations", 15, numAmort);
		
		List<Operation.Identity.Value> exclude = Lists.newArrayList(Operation.Identity.Value.Amortized);
		Collection<Operation> ops = manager.getOperations(new TagLists(), exclude);
		assertEquals("wrong number of operations after excluding amortized", tagGroups.size() - numAmort, ops.size());
		
		exclude = Lists.newArrayList(Operation.Identity.Value.Lent);
		ops = manager.getOperations(new TagLists(), exclude);
		assertEquals("wrong number of operations after excluding amortized", tagGroups.size() - numLent, ops.size());
	}
}
