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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Instance;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.processor.Instances;

public class InstancesService implements DataCache {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
	private final Instances instances;
	private final AccountService accountService;
	private final ProductService productService;
	private Map<String, List<Instance>> instancesCache;

	public InstancesService(String localDir, String workS3BucketName, String workS3BucketPrefix, AccountService accountService, ProductService productService) {
		instances = new Instances(localDir, workS3BucketName, workS3BucketPrefix);
		this.accountService = accountService;
		this.productService = productService;
	}
	
	public Collection<Instance> getInstances(String id) {
		return instancesCache.get(id);
	}
	
	private Map<String, List<Instance>> buildCache() {
		Map<String, List<Instance>> newCache = Maps.newHashMap();
		
		for (Instance i: instances.values()) {
			// Store by full ID key (usually the ARN)
			List<Instance> instances = Lists.newArrayList(i);
			newCache.put(i.id, instances);
			
			// Store by the resource id portion of the ARN
			int separatorIndex = i.id.lastIndexOf(":");
			if (separatorIndex < 0) {
				separatorIndex = i.id.lastIndexOf("/");
			}
			if (separatorIndex >= 0 && i.id.length() > separatorIndex+1) {
				String key = i.id.substring(separatorIndex+1);
				instances = newCache.get(key);
				if (instances == null) {
					instances = Lists.newArrayList();
					newCache.put(key, instances);
				}
				instances.add(i);
			}
		}
		return newCache;
	}

	@Override
	public boolean refresh() {
        logger.info("Instances refresh...");
		// Ask for one day prior to make sure we've processed a report if at
		// start of month.
		instances.retrieve(DateTime.now().minusDays(1).getMillis(), accountService, productService);

		instancesCache = buildCache();
        return false;
	}

}
