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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Aggregation;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone;


public class Query {
	private final Map<Rule.TagKey, TagFilters> tagFilters;
	private final Map<String, TagFilters> userTagFilters;
	private final Map<String, Integer> userTagFilterIndeces;
	private final int numUserTags;
	private final boolean singleTagGroup;
	private final boolean monthly;
	
	private final List<Rule.TagKey> groupBy;
	private final List<Integer> groupByTagsIndeces;
	private final List<String> groupByTags;
	protected final Aggregation aggregation;
	private final boolean aggregates;

	private final String[] emptyUserTags;
	
	private String string = null;

	public Query(QueryConfig queryConfig, List<String> userTagKeys) throws Exception {		
		TagGroupFilterConfig tgfc = queryConfig.getFilter();
		tagFilters = Maps.newHashMap();
		
		// If the tag filters all specify single values, the query could represent a single TagGroup
		// if all tags are present -or- the singleTagGroup flag is set to true in the config.
		// Presence of an exclude indicates it's not a singleTagGruop.
		// Get the initial state here, then we'll check for a single value in each filter as we walk through each.
		// For starters, we must have at least one tag specified, so make sure we have a list.
		boolean singleTagGroup = tgfc != null && tgfc.getTags() != null &&
								(tgfc.getExclude() == null || tgfc.getExclude().isEmpty()) &&
								(tgfc.getExcludeUserTags() == null || tgfc.getExcludeUserTags().isEmpty());
		
		// Get tags we're not aggregating. If null, we're grouping by everything, else use
		// the tags specified.
		groupBy = queryConfig.getGroupBy() == null ? Lists.<Rule.TagKey>newArrayList(Rule.TagKey.values()) : queryConfig.getGroupBy();

		if (tgfc != null && tgfc.getTags() != null) {
			for (Rule.TagKey key: tgfc.getTags().keySet()) {
				boolean exclude = tgfc.getExclude() != null && tgfc.getExclude().contains(key);
				List<String> filterValues = tgfc.getTags().get(key);
				if (singleTagGroup && filterValues.size() != 1)
					singleTagGroup = false;
				tagFilters.put(key, new TagFilters(exclude, filterValues));
			}
		}
		
		userTagFilters = Maps.newHashMap();
		userTagFilterIndeces = Maps.newHashMap();
		if (tgfc != null && tgfc.getUserTags() != null) {
			for (String key: tgfc.getUserTags().keySet()) {
				if (!userTagKeys.contains(key))
					throw new Exception("Invalid user tag key name: \"" + key + "\"");
				
				boolean exclude = tgfc.getExcludeUserTags() != null && tgfc.getExcludeUserTags().contains(key);
				List<String> filterValues = tgfc.getUserTags().get(key);
				if (singleTagGroup && filterValues.size() != 1)
					singleTagGroup = false;
				userTagFilters.put(key, new TagFilters(exclude, filterValues));
		    	userTagFilterIndeces.put(key, userTagKeys.indexOf(key));
			}
		}
		if (singleTagGroup) {
			if (tgfc.isSingleTagGroup()) {
				// Explicit single tag group.
				// Unspecified values will automatically be set to empty strings when processed.
				// Check for ones that are required.
				Set<Rule.TagKey> keys = tgfc.getTags().keySet();
				if (!singleTagGroup || !keys.contains(Rule.TagKey.costType) || !keys.contains(Rule.TagKey.account) || !keys.contains(Rule.TagKey.region) || !keys.contains(Rule.TagKey.product))
					throw new Exception("Query filter marked as single, but resolves to more than one tag group.");
			}
			else {
				// Possible implicit singleTagGroup
				// Make sure all tags and userTags have values before flagging as a single tag group.
				singleTagGroup = tgfc.getTags().size() == Rule.TagKey.values().length && tgfc.getUserTags().size() == userTagKeys.size();
			}
		}
		
		this.singleTagGroup = singleTagGroup;
		numUserTags = userTagKeys.size();
		
		boolean aggregates = groupBy.size() < Rule.TagKey.values().length;
		
		groupByTagsIndeces = Lists.newArrayList();
		groupByTags = Lists.newArrayList();
		if (queryConfig.getGroupByTags() == null) {
			// no aggregation, group by all user tag keys
			List<String> customTags = userTagKeys;
			for (int i = 0; i < customTags.size(); i++) {
				groupByTagsIndeces.add(i);
				groupByTags.add(customTags.get(i));
			}
		}
		else {
	    	for (String key: queryConfig.getGroupByTags()) {
	    		int tagIndex = userTagKeys.indexOf(key);
	    		groupByTagsIndeces.add(tagIndex);
	    		groupByTags.add(key);
	    	}
	    	aggregates |= groupByTagsIndeces.size() == userTagKeys.size();
		}
		
		this.monthly = queryConfig.isMonthly();		
		this.aggregation = new Aggregation(groupBy, groupByTagsIndeces);
		this.aggregates = aggregates;
		this.emptyUserTags = new String[numUserTags];
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Query other = (Query) o;
		return toString().equals(other.toString());		
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
		
	public List<Rule.TagKey> getGroupBy() {
		return this.groupBy;
	}
	
	public List<Integer> getGroupByTagsIndeces() {
		return this.groupByTagsIndeces;
	}
	
	public List<String> getGroupByTags() {
		return this.groupByTags;
	}
	
	public boolean hasAggregation() {
		return aggregates;
	}
	
	public Aggregation getAggregation() {
		return aggregation;
	}
	
	/**
	 * Indicates that each tag/userTag has a single explicit value that can be
	 * used to generate one TagGroup for direct lookup in a TagGroup map.
	 * @return
	 */
	public boolean isSingleTagGroup() {
		return singleTagGroup;
	}
	
	/**
	 * Indicates that one or more value sets will be aggregated into a single value set.
	 * i.e. the groupBy and groupByTag lists are both empty.
	 * @return
	 */
	public boolean isSingleAggregation() {
		return singleTagGroup || (groupBy.isEmpty() && groupByTags.isEmpty());
	}
	
	public boolean isMonthly() {
		return monthly;
	}
	
	public boolean hasGroupByTags() {
		return groupByTags.size() > 0;
	}
	
	public String toString() {
		if (string == null) {
			List<String> tags = Lists.newArrayList();
			tags.add(tagFilters.toString());
			tags.add(userTagFilters.toString());
			tags.add("groupBy:" + groupBy.toString());
			tags.add("groupByTags:" + groupByTags.toString());
									
			string = StringUtils.join(tags, ",");
		}
		return string;
	}

	/**
	 * Get the products that match the product regex group
	 * @param productService
	 * @return List of Product
	 */
	public Collection<Product> getProducts(ProductService productService) {
		List<Product> products = Lists.newArrayList();
		TagFilters productFilters = tagFilters.get(TagType.Product);
		if (productFilters == null || productFilters.isEmpty())
			return productService.getProducts();
		
		for (Product p: productService.getProducts()) {
			if (productFilters.matches(p.getServiceCode()))
				products.add(p);
		}
		return products;
	}
	
	/**
	 * Get the TagGroup based on the supplied tags and user tags - used for direct lookup of a single tag group.
	 * Values present in the input config are used to generate the TagGroup.
	 * There should only be one entry in each dimension list, so the first is always chosen.
	 */
	public TagGroup getSingleTagGroup(AccountService accountService, ProductService productService, boolean isNonResource) throws Exception {
		CostType costType = CostType.get(tagFilters.get(Rule.TagKey.costType).getFirst());
		Account account = accountService.getAccountById(tagFilters.get(Rule.TagKey.account).getFirst());
		Region region = Region.getRegionByName(tagFilters.get(Rule.TagKey.region).getFirst());
		Zone zone = tagFilters.containsKey(Rule.TagKey.zone) ? region.getZone(tagFilters.get(Rule.TagKey.zone).getFirst()) : null;
		Product product = productService.getProductByServiceCode(tagFilters.get(Rule.TagKey.product).getFirst());
		Operation operation = Operation.getOperation(tagFilters.containsKey(Rule.TagKey.operation) ? tagFilters.get(Rule.TagKey.operation).getFirst() : "");
		// TODO: Need way to specify usage type units in the config
		UsageType usageType = UsageType.getUsageType(tagFilters.containsKey(Rule.TagKey.usageType) ? tagFilters.get(Rule.TagKey.usageType).getFirst() : "", "");
				
		ResourceGroup resourceGroup = null;		
		if (!isNonResource) {
			if (resourceGroup == null && userTagFilters.size() == 0) {
				resourceGroup = ResourceGroup.getResourceGroup(emptyUserTags);
			}
			else {
				List<UserTag> userTags = Lists.newArrayListWithCapacity(numUserTags);
				for (int i = 0; i < numUserTags; i++)
					userTags.add(UserTag.empty);
				if (userTagFilters.size() > 0) {
					for (String key: userTagFilters.keySet()) {
						int i = userTagFilterIndeces.get(key);
						userTags.set(i, UserTag.get(userTagFilters.get(key).getFirst()));
					}
				}
				resourceGroup = ResourceGroup.getResourceGroup(userTags);
			}
		}

		return TagGroup.getTagGroup(costType, account, region, zone, product, operation, usageType, resourceGroup);
	}


	/**
	 * Used by RuleProcessor.runQuery() when aggregating the query data
	 */
	public AggregationTagGroup aggregateTagGroup(TagGroup tg, AccountService accountService, ProductService productService) throws Exception {		
		// Apply tag filters
		for (Rule.TagKey tk: tagFilters.keySet()) {
			TagFilters tf = tagFilters.get(tk);
			String value = null;
			switch (tk) {
			case costType:  value = tg.costType.name;				break;
			case account:	value = tg.account.getId();				break;				
			case region:	value = tg.region.name;					break;
			case zone:		value = tg.zone == null ? null : tg.zone.name;					break;
			case product:	value = tg.product.getServiceCode();	break;
			case operation:	value = tg.operation.name;				break;
			case usageType:	value = tg.usageType.name;				break;
			default:												break;
			}
			if (value == null || !tf.matches(value))
				return null;
		}
		  
		// Apply user tag filters
		UserTag[] userTags = tg.resourceGroup == null ? null : tg.resourceGroup.getUserTags();
		if (userTags != null) {
			for (String key: userTagFilters.keySet()) {
				TagFilters tf = userTagFilters.get(key);
				Integer userTagIndex = userTagFilterIndeces.get(key);
				if (userTagIndex < 0 || !tf.matches(userTags[userTagIndex].name))
					return null;
				
			}
		}
        
		return aggregation.getAggregationTagGroup(tg);
	}
	
	public class TagFilters {
		private boolean exclude;
		private List<TagFilter> filters;
		
		public TagFilters(boolean exclude, List<String> regularExpressions) {
			this.exclude = exclude;
			filters = Lists.newArrayList();
			for (String regex: regularExpressions) {
				filters.add(new TagFilter(regex));				
			}
			
		}
		
		public String toString() {
			return (exclude ? "exclude: " : "include: " + filters.toString());
		}
		
		public boolean isEmpty() {
			return filters.isEmpty();
		}
		
		public boolean matches(String name) {
			if (exclude) {
				for (TagFilter tf: filters) {
					if (tf.matches(name))
						return false;
				}
			}
			else {
				for (TagFilter tf: filters) {
					if (tf.matches(name))
						return true;
				}
			}
			return exclude;
		}
		
		public String getFirst() {
			return filters.get(0).toString();
		}
	}
	
	public class TagFilter {
		private String regex;
		private Pattern pattern = null; // lazy initialize
		
		public TagFilter(String regex) {
			this.regex = regex;
		}
		
		public String toString() {
			return regex;
		}
		
		public boolean matches(String name) {
			if (pattern == null)
				pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(name);
			return matcher.matches();
		}

	}

}
