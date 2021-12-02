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

import com.google.common.collect.Lists;

/**
 * QueryConfig specifies the set of tags used to filter and aggregate cost and usage data for
 * input to a post processor result.
 * 
 * The filter properties are used to filter on specific values and patterns for tags and userTags.
 * 
 * If the groupBy attribute is provided, any attribute names not listed will be merged and not broken
 * out as separate values in the result. If groupBy is not provided, no merging will be performed. If
 * groupBy is provided but is an empty list, all tag types will be merged.
 * 
 * Allowed groupBy values are: account, region, zone, product, operation, usageType
 * 
 * If the groupByUserTag attribute is provided, any user tag names not listed will be merged and not
 * broken out as separate values in the result. If groupByUserTag is not provided, no merging will be performed.
 * If groupByUserTag is provided but is an empty map, all user tags will be merged.
 * 
 * Allowed groupByUserTag values are the names specified for ice.customTags in the ice.properties configuration file.
 * 
 * The monthly attribute can be set to produce a single value for the month rather than per hour.
 */
public class QueryConfig {
	private TagGroupFilterConfig filter;
	private List<Rule.TagKey> groupBy;
	private List<String> groupByTags;
	private boolean monthly;
	
	public QueryConfig() {
	}
	
	public QueryConfig(QueryConfig copyMe) {
		filter = copyMe.filter == null ? null : new TagGroupFilterConfig(copyMe.filter);
		groupBy = copyMe.groupBy == null ? null : Lists.newArrayList(copyMe.groupBy);
		groupByTags = copyMe.groupByTags == null ? null : Lists.newArrayList(copyMe.groupByTags);
		monthly = copyMe.monthly;		
	}
	
	public boolean isMonthly() {
		return monthly;
	}
	
	public void setMonthly(boolean monthly) {
		this.monthly = monthly;
	}

	public TagGroupFilterConfig getFilter() {
		return filter;
	}

	public void setFilter(TagGroupFilterConfig filter) {
		this.filter = filter;
	}

	public List<Rule.TagKey> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<Rule.TagKey> groupBy) {
		this.groupBy = groupBy;
	}

	public List<String> getGroupByTags() {
		return groupByTags;
	}

	public void setGroupByTags(List<String> groupByTags) {
		this.groupByTags = groupByTags;
	}
}
