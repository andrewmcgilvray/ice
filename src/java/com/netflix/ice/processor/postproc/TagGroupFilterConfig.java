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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * TagGroupFilterConfig specifies the set of tags used to filter usage or cost data when
 * performing a query to collect input for an input data set or operand.
 * 
 * The tags and userTags maps hold lists of regular expressions used to identify matching
 * records that can then be included or excluded from the data set.
 * 
 * The userTags map may only include valid user tag key names as defined by the ice.customTags property.
 * 
 * If any key is not present in the tags or userTags map, then no filtering on that dimension is done.
 * 
 * If the exclude attribute is provided, then for each list of regex expressions, the specified expression
 * is used to exclude matches rather than include them in the resulting data set.
 * 
 * Allowed exclude values for excludeTags: account, region, zone, product, operation, usageType
 * For excludeUserTags, the allowed values are the user tag key names as defined by the ice.customTags property.
 * 
 * In the case where the target input data has no user tags, the noUserTags flag may be set to true.
 * Use of this flag eliminates the need to provide a full set of user tags with empty values.
 */
public class TagGroupFilterConfig {
	private List<String> account;
	private List<String> region;
	private List<String> zone;
	private List<String> product;
	private List<String> operation;
	private List<String> usageType;
	private List<Rule.TagKey> excludeTags;
	
	private boolean noUserTags;
	private Map<String, List<String>> userTags;	
	private List<String> excludeUserTags;
	
	public TagGroupFilterConfig() {
	}
	
	public TagGroupFilterConfig(TagGroupFilterConfig copyMe) {
		account = copyMe.account == null ? null : Lists.newArrayList(copyMe.account);
		region = copyMe.region == null ? null : Lists.newArrayList(copyMe.region);
		zone = copyMe.zone == null ? null : Lists.newArrayList(copyMe.zone);
		product = copyMe.product == null ? null : Lists.newArrayList(copyMe.product);
		operation = copyMe.operation == null ? null : Lists.newArrayList(copyMe.operation);
		usageType = copyMe.usageType == null ? null : Lists.newArrayList(copyMe.usageType);
		excludeTags = copyMe.excludeTags == null ? null : Lists.newArrayList(copyMe.excludeTags);
		noUserTags = copyMe.noUserTags;
		userTags = null;
		if (copyMe.userTags != null) {
			userTags = Maps.newHashMap();
			for (String key: copyMe.userTags.keySet()) {
				List<String> values = copyMe.userTags.get(key);
				userTags.put(key, values == null ? null : Lists.newArrayList(values));
			}
		}
		excludeUserTags = copyMe.excludeUserTags == null ? null : Lists.newArrayList(copyMe.excludeUserTags);
	}

	/**
	 * Get the set of defined tags as a map.
	 * @return
	 */
	public Map<Rule.TagKey, List<String>> getTags() {
		Map<Rule.TagKey, List<String>> tags = Maps.newHashMap();
		if (account != null)
			tags.put(Rule.TagKey.account, account);
		if (region != null)
			tags.put(Rule.TagKey.region, region);
		if (zone != null)
			tags.put(Rule.TagKey.zone, zone);
		if (product != null)
			tags.put(Rule.TagKey.product, product);
		if (operation != null)
			tags.put(Rule.TagKey.operation, operation);
		if (usageType != null)
			tags.put(Rule.TagKey.usageType, usageType);
		return tags;
	}
	
	public List<String> getAccount() {
		return account;
	}
	public void setAccount(List<String> account) {
		this.account = account;
	}
	public List<String> getRegion() {
		return region;
	}
	public void setRegion(List<String> region) {
		this.region = region;
	}
	public List<String> getZone() {
		return zone;
	}
	public void setZone(List<String> zone) {
		this.zone = zone;
	}
	public List<String> getProduct() {
		return product;
	}
	public void setProduct(List<String> product) {
		this.product = product;
	}
	public List<String> getOperation() {
		return operation;
	}
	public void setOperation(List<String> operation) {
		this.operation = operation;
	}
	public List<String> getUsageType() {
		return usageType;
	}
	public void setUsageType(List<String> usageType) {
		this.usageType = usageType;
	}
	public List<Rule.TagKey> getExcludeTags() {
		return excludeTags;
	}
	public void setExcludeTags(List<Rule.TagKey> excludeTags) {
		this.excludeTags = excludeTags;
	}
	public boolean hasNoUserTags() {
		return noUserTags;
	}
	public void setNoUserTags(boolean noUserTags) {
		this.noUserTags = noUserTags;
	}
	public Map<String, List<String>> getUserTags() {
		return userTags;
	}
	public void setUserTags(Map<String, List<String>> userTags) {
		this.userTags = userTags;
	}
	public List<String> getExcludeUserTags() {
		return excludeUserTags;
	}
	public void setExcludeUserTags(List<String> excludeUserTags) {
		this.excludeUserTags = excludeUserTags;
	}
}
