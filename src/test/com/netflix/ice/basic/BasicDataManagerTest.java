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

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.reader.AggregateType;
import com.netflix.ice.reader.ReadOnlyData;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.reader.TagListsWithUserTags;
import com.netflix.ice.reader.UsageUnit;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone.BadZone;

public class BasicDataManagerTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String dataDir = "src/test/data/";

	class TestDataFileCache extends BasicDataManager {
		private ReadOnlyData data;
		
		TestDataFileCache(DateTime startDate, final String dbName, ConsolidateType consolidateType, TagGroupManager tagGroupManager, boolean compress, int numUserTags,
	    		int monthlyCacheSize, WorkBucketConfig workBucketConfig, AccountService accountService, ProductService productService, ReadOnlyData data) {
			super(startDate, dbName, consolidateType, tagGroupManager, compress, numUserTags, monthlyCacheSize, workBucketConfig, accountService, productService, null);
			this.data = data;
		}
		
		@Override
		protected void buildCache(int monthlyCacheSize) {				
		}
		
		@Override
	    protected ReadOnlyData getReadOnlyData(DateTime key) throws ExecutionException {
	        return data;
	    }
	}
	
	@Test
	public void loadHourlyDataFromFile() throws Exception {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		BasicDataManager data = new TestDataFileCache(DateTime.now(), null, null, null, true, 0, 0, null, as, ps, null);
	    
	    File f = new File(dataDir + "cost_hourly_EC2_Instance_2018-06.gz");
	    if (!f.exists())
	    	return;
	    
	    ReadOnlyData rod = data.loadDataFromFile(f);
	    
	    logger.info("File: " + f + " has " + rod.getTagGroups().size() + " tag groups and "+ rod.getNum() + " hours of data");
	    
	    SortedSet<Product> products = new TreeSet<Product>();
	    for (TagGroup tg: rod.getTagGroups())
	    	products.add(tg.product);
	
	    logger.info("Products:");
	    for (Product p: products)
	    	logger.info("  " + p.getIceName());
	}
	
	@Test
	public void loadMonthlyDataFromFile() throws Exception {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		BasicDataManager data = new TestDataFileCache(DateTime.now(), null, null, null, true, 0, 0, null, as, ps, null);
	    
	    File f = new File(dataDir + "cost_monthly_all.gz");
	    if (!f.exists())
	    	return;
	    
	    ReadOnlyData rod = data.loadDataFromFile(f);
	    
	    logger.info("File: " + f + " has " + rod.getTagGroups().size() + " tag groups and "+ rod.getNum() + " months of data");
	    
	    SortedSet<Product> products = new TreeSet<Product>();
	    for (TagGroup tg: rod.getTagGroups())
	    	products.add(tg.product);
	
	    logger.info("Products:");
	    for (Product p: products)
	    	logger.info("  " + p.getIceName());
	    
	    for (int i = 0; i < rod.getNum(); i++) {
	    	List<TagGroup> tagGroups = (List<TagGroup>) rod.getTagGroups();
	    	double total = 0;
	    	double[] values = rod.getData(i);
	    	if (values == null) {
	    		logger.info("No data for month " + i);
	    		continue;
	    	}
	    	
	    	for (int j = 0; j < tagGroups.size(); j++) {
	    		if (tagGroups.get(j).product.isEc2Instance()) {
	    			total += values[j];
	    		}
	    	}
	    	logger.info("EC2 Instance total for month " + i + ": " + total);
	    	assertTrue("No data for month " + i, total > 0.0);
	    }
	    		
	}
	
	private TagGroupManager makeTagGroupManager(DateTime testMonth, Collection<TagGroup> tagGroups) {
		TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups = Maps.newTreeMap();
		List<TagGroup> tagGroupList = Lists.newArrayList();
		for (TagGroup tg: tagGroups)
			tagGroupList.add(tg);
		tagGroupsWithResourceGroups.put(testMonth.getMillis(), tagGroupList);
		
		return new BasicTagGroupManager(tagGroupsWithResourceGroups, new Interval(testMonth, testMonth.plusMonths(1)), 2);
	}
	
	@Test
	public void groupByUserTagAndFilterByUserTag() throws BadZone, ResourceException {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		double[][] rawData = new double[][]{
				new double[]{ 1.0, 2.0 },
		};
		List<TagGroup> tagGroups = Lists.newArrayList();
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "operation", "usgaeType", "usageTypeUnit", new String[]{"TagA","TagB"}, as, ps));
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "operation", "usgaeType", "usageTypeUnit", new String[]{"TagA", ""}, as, ps));
		
		
		ReadOnlyData rod = new ReadOnlyData(rawData, tagGroups, 2);
		DateTime testMonth = DateTime.parse("2018-01-01");
		TagGroupManager tagGroupManager = makeTagGroupManager(testMonth, tagGroups);
		
		BasicDataManager dataManager = new TestDataFileCache(testMonth, null, ConsolidateType.monthly, tagGroupManager, true, 0, 0, null, as, ps, rod);
		
		Interval interval = new Interval(testMonth, testMonth.plusMonths(1));
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		List<UserTag> listA = Lists.newArrayList();
		List<UserTag> listB = Lists.newArrayList();
		userTagLists.add(listA);
		userTagLists.add(listB);
		
		listA.add(UserTag.get("TagA"));
		
		TagLists tagLists = new TagListsWithUserTags(null, null, null, null, null, null, userTagLists);
		
		Map<Tag, double[]> data = dataManager.getData(interval, tagLists, TagType.Tag, AggregateType.data, null, UsageUnit.Instances, 1);
		
		for (Tag t: data.keySet()) {
			logger.info("Tag: " + t + ": " + data.get(t)[0]);
		}
		assertEquals("Wrong number of groupBy tags", 3, data.size());
		assertNotNull("No aggregated tag", data.get(Tag.aggregated));
		assertNotNull("No (none) tag", data.get(UserTag.get(UserTag.none)));
		assertNotNull("No TagB tag", data.get(UserTag.get("TagB")));
		
	}
	
	@Test
	public void groupByUserTagAndFilterByEmptyUserTag() throws BadZone, ResourceException {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		double[][] rawData = new double[][]{
				new double[]{ 1.0, 2.0 },
		};
		List<TagGroup> tagGroups = Lists.newArrayList();
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "operation", "usgaeType", "usageTypeUnit", new String[]{"TagA","TagB"}, as, ps));
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "operation", "usgaeType", "usageTypeUnit", null, as, ps));		
		
		ReadOnlyData rod = new ReadOnlyData(rawData, tagGroups, 2);
		DateTime testMonth = DateTime.parse("2018-01-01");
		TagGroupManager tagGroupManager = makeTagGroupManager(testMonth, tagGroups);
		
		BasicDataManager dataManager = new TestDataFileCache(testMonth, null, ConsolidateType.monthly, tagGroupManager, true, 0, 0, null, as, ps, rod);
		
		Interval interval = new Interval(testMonth, testMonth.plusMonths(1));
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		List<UserTag> listA = Lists.newArrayList();
		List<UserTag> listB = Lists.newArrayList();
		userTagLists.add(listA);
		userTagLists.add(listB);
		
		listB.add(UserTag.empty);
		
		TagLists tagLists = new TagListsWithUserTags(null, null, null, null, null, null, userTagLists);
		
		Map<Tag, double[]> data = dataManager.getData(interval, tagLists, TagType.Tag, AggregateType.data, null, UsageUnit.Instances, 1);
		
		for (Tag t: data.keySet()) {
			logger.info("Tag: " + t + ": " + data.get(t)[0]);
		}
		assertEquals("Wrong number of groupBy tags", 2, data.size());
		assertNotNull("No aggregated tag", data.get(Tag.aggregated));
		assertNotNull("No (none) tag", data.get(UserTag.get(UserTag.none)));
	}
	
	@Test
	public void groupByNoneWithUserTagFilters() throws BadZone, ResourceException {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		double[][] rawData = new double[][]{
				new double[]{ 1.0, 2.0 },
		};
		List<TagGroup> tagGroups = Lists.newArrayList();
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "On-Demand Instances", "usgaeType", "usageTypeUnit", new String[]{"TagA","TagB"}, as, ps));
		tagGroups.add(TagGroup.getTagGroup("account", "us-east-1", null, "product", "On-Demand Instances", "usgaeType", "usageTypeUnit", new String[]{"TagA", ""}, as, ps));
		
		
		ReadOnlyData rod = new ReadOnlyData(rawData, tagGroups, 2);
		DateTime testMonth = DateTime.parse("2018-01-01");
		TagGroupManager tagGroupManager = makeTagGroupManager(testMonth, tagGroups);
		
		BasicDataManager dataManager = new TestDataFileCache(testMonth, null, ConsolidateType.monthly, tagGroupManager, true, 0, 0, null, as, ps, rod);
		
		Interval interval = new Interval(testMonth, testMonth.plusMonths(1));
		List<List<UserTag>> userTagLists = Lists.newArrayList();
		List<UserTag> listA = Lists.newArrayList();
		List<UserTag> listB = Lists.newArrayList();
		userTagLists.add(listA);
		userTagLists.add(listB);
		
		listB.add(UserTag.get("TagB"));
		
		// First test with operations specified
		List<Operation> operations = Lists.newArrayList((Operation) Operation.ondemandInstances);
		TagLists tagLists = new TagListsWithUserTags(null, null, null, null, operations, null, userTagLists);
		
		Map<Tag, double[]> data = dataManager.getData(interval, tagLists, null, AggregateType.data, null, UsageUnit.Instances, 1);
		
		for (Tag t: data.keySet()) {
			logger.info("Tag: " + t + ": " + data.get(t)[0]);
		}
		assertEquals("With operation specified, wrong number of groupBy tags", 1, data.size());
		assertNotNull("With operation specified, has aggregated tag", data.get(Tag.aggregated));
		assertEquals("With operation specified, wrong value for aggregation", 1.0, data.get(Tag.aggregated)[0], 0.001);
		
		// Now test without operations specified
		tagLists = new TagListsWithUserTags(null, null, null, null, null, null, userTagLists);
		
		data = dataManager.getData(interval, tagLists, null, AggregateType.data, null, UsageUnit.Instances, 1);
		
		for (Tag t: data.keySet()) {
			logger.info("Tag: " + t + ": " + data.get(t)[0]);
		}
		assertEquals("Without operation specified, wrong number of groupBy tags", 1, data.size());
		assertNotNull("Without operation specified, has aggregated tag", data.get(Tag.aggregated));
		assertEquals("Without operation specified, wrong value for aggregation", 1.0, data.get(Tag.aggregated)[0], 0.001);
	}
	
	// Example for debugging getData()
	@Test
	public void testHourlyDataFromFile() throws Exception {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		BasicDataManager data = new TestDataFileCache(DateTime.now(), null, null, null, true, 0, 0, null, as, ps, null);
	    
	    File f = new File(dataDir + "cost_hourly_EC2Instance_2019-01.gz");
	    if (!f.exists())
	    	return;
	    
	    ReadOnlyData rod = data.loadDataFromFile(f);
	    
	    for (int i = 0; i < rod.getData(0).length; i++) {
	    	//TagGroup tg = rod.getTagGroups().get(i);
	    }
	    
	    logger.info("File: " + f + " has " + rod.getTagGroups().size() + " tag groups and "+ rod.getNum() + " hours of data");
	    
		DateTime testMonth = new DateTime("2020-01-01", DateTimeZone.UTC);
		TagGroupManager tagGroupManager = makeTagGroupManager(testMonth, rod.getTagGroups());
		
		BasicDataManager dataManager = new TestDataFileCache(testMonth, null, ConsolidateType.hourly, tagGroupManager, true, 0, 0, null, as, ps, rod);
		
		Interval interval = new Interval(testMonth, testMonth.plusHours(1));
		
		// First test with operations specified
		List<Operation> operations = Lists.newArrayList((Operation) Operation.amortizedAllUpfront);
		List<UsageType> usageTypes = Lists.newArrayList(UsageType.getUsageType("r4.2xlarge", "hours"));
		TagLists tagLists = new TagLists(null, null, null, null, operations, usageTypes, null);
		
		Map<Tag, double[]> results = dataManager.getData(interval, tagLists, TagType.Account, AggregateType.data, null, UsageUnit.Instances, 1);
		
		for (Tag t: results.keySet())
			logger.info(t + ", " + results.get(t)[0]);
	}

}
