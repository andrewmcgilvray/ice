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
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.TagCoverageMetrics;
import com.netflix.ice.reader.AggregateType;
import com.netflix.ice.reader.ReadOnlyTagCoverageData;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTagKey;

public class TagCoverageDataManagerTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String dataDir = "src/test/data/";

	class TestTagCoverageDataManager extends TagCoverageDataManager {
		TestTagCoverageDataManager() {
			super(DateTime.now(), "dummy", ConsolidateType.hourly, null, true, null, 0, null, null, null);
		}
		
		@Override
	    protected void buildCache(int monthlyCacheSize) {
	    }
	    
		@Override
		protected List<UserTagKey> getUserTagKeys() {
			List<UserTagKey> tags = Lists.newArrayList();
			tags.add(UserTagKey.get("Email"));
			tags.add(UserTagKey.get("Department"));
			tags.add(UserTagKey.get("Product"));
			return tags;
		}

	}
	
	@Test
	public void testProcessResult() {
		TagCoverageDataManager manager = new TestTagCoverageDataManager();
		
		Map<Tag, TagCoverageMetrics[]> data = Maps.newHashMap();
		
		TagCoverageMetrics metrics = new TagCoverageMetrics(manager.getUserTagKeys().size());
		                                            /* EMAIL, DEPARTMENT, PRODUCT */
		TagCoverageMetrics.add(metrics, new boolean[]{ true, true, false });
		TagCoverageMetrics.add(metrics, new boolean[]{ true, false, false });
		
		data.put(Tag.aggregated, new TagCoverageMetrics[]{ metrics });
		
		
		// insert tags in reverse order of that returned by getUserTags()
		List<UserTagKey> tagKeys = Lists.newArrayList();
		tagKeys.add(UserTagKey.get("Product"));
		tagKeys.add(UserTagKey.get("Email"));
		
		
		
		Map<Tag, double[]> result = manager.processResult(data, TagType.TagKey, AggregateType.stats, tagKeys);
		
		double email = result.get(UserTagKey.get("Email"))[0];
		assertEquals("email tag percentage is wrong", 100.0, email, 0.001);
		double product = result.get(UserTagKey.get("Product"))[0];
		assertEquals("product tag percentage is wrong", 0.0, product, 0.001);
		double aggregate = result.get(Tag.aggregated)[0];
		assertEquals("aggregate percentage is wrong", 75.0, aggregate, 0.001);
	}
	
	
	class TestDataFileCache extends TagCoverageDataManager {
		private final int userTagSize;
		
		TestDataFileCache(DateTime startDate, final String dbName, ConsolidateType consolidateType, boolean compress,
	    		int monthlyCacheSize, AccountService accountService, ProductService productService, int size) {
			super(startDate, dbName, consolidateType, null, compress, Lists.<UserTagKey>newArrayList(), monthlyCacheSize, null, accountService, productService);
			userTagSize = size;
		}
		
		@Override
		protected void buildCache(int monthlyCacheSize) {				
		}
		
		@Override
		protected int getUserTagKeysSize() {
			return userTagSize;
		}
	}

	@Test
	public void loadFile() throws Exception {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
		TagCoverageDataManager data = new TestDataFileCache(DateTime.now(), null, null, true, 0, as, ps, 12);
	    
	    File f = new File(dataDir + "coverage_hourly_all_2018-07.gz");
	    if (!f.exists())
	    	return;
	    
	    ReadOnlyTagCoverageData rod = data.loadDataFromFile(f);
	    
	    logger.info("File: " + f + " has " + rod.getTagGroups(null, null, 0).size() + " tag groups and "+ rod.getNum() + " hours of data");
	    
	    SortedSet<Product> products = new TreeSet<Product>();
	    SortedSet<Account> accounts = new TreeSet<Account>();
	    for (TagGroup tg: rod.getTagGroups(null, null, 0)) {
	    	products.add(tg.product);
	    	accounts.add(tg.account);
	    }
	
	    logger.info("Products:");
	    for (Product p: products)
	    	logger.info("  " + p.getIceName());
	    logger.info("Accounts:");
	    for (Account a: accounts)
	    	logger.info("  " + a.getIceName());
	}

}
