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

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Aggregation;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone;

public class InputOperand extends Operand {
	private static final List<TagType> allTagTypes = Lists.newArrayList(new TagType[]{
			TagType.Account, TagType.Region, TagType.Zone, TagType.Product, TagType.Operation, TagType.UsageType});
	
	private final List<TagType> groupBy;
	private final List<Integer> groupByTagsIndeces;
	private final List<String> groupByTags;
	protected final Aggregation aggregation;
	private boolean excludeAccount = false;
	private boolean excludeRegion = false;
	private boolean excludeZone = false;
	private final boolean aggregates;

	@Override
	public String toString() {
		List<String> flags = Lists.newArrayList();
		if (excludeAccount)
			flags.add("excludeAccount");
		if (excludeRegion)
			flags.add("excludeRegion");
		if (aggregates)
			flags.add("aggregates");
		
		if (groupBy.size() > 0)
			flags.add("groupBy:" + groupBy.toString());
		if (groupByTags.size() > 0)
			flags.add("groupByTags:" + groupByTags.toString());
		
		return super.toString().split("}")[0] + (flags.size() > 0 ? "," + StringUtils.join(flags, ",") : "") + "}";
	}
	
	public InputOperand(OperandConfig opConfig, AccountService accountService, ResourceService resourceService) throws Exception {
		super(opConfig, accountService, resourceService);

		boolean aggregates = false;
		
		groupBy = opConfig.getGroupBy() == null ? allTagTypes : Lists.<TagType>newArrayList();
		if (opConfig.getGroupBy() != null) {
			groupBy.addAll(opConfig.getGroupBy());
			aggregates |= !groupBy.containsAll(allTagTypes);
		}
		
		groupByTagsIndeces = Lists.newArrayList();
		groupByTags = Lists.newArrayList();
		if (opConfig.getGroupByTags() == null) {
			// no aggregation, group by all user tag keys
			List<String> customTags = resourceService.getCustomTags();
			for (int i = 0; i < customTags.size(); i++) {
				groupByTagsIndeces.add(i);
				groupByTags.add(customTags.get(i));
			}
		}
		else {
	    	for (String key: opConfig.getGroupByTags()) {
	    		int tagIndex = resourceService.getUserTagIndex(key);
	    		groupByTagsIndeces.add(tagIndex);
	    		groupByTags.add(key);
	    	}
	    	aggregates |= groupByTagsIndeces.size() == resourceService.getCustomTags().size();
		}
		
		this.aggregation = new Aggregation(groupBy, groupByTagsIndeces);
		
		List<TagType> exclude = opConfig.getExclude();
		if (exclude != null) {
			// If we're using the exclude feature on any of the lists, then we're aggregating
			for (TagType tagType: exclude) {
				switch (tagType) {
				case Account: excludeAccount = true; aggregates = true; break;
				case Region: excludeRegion = true; aggregates = true; break;
				case Zone: excludeZone = true; aggregates = true; break;
				default: break;
				}
			}
		}
		
		// If any of the lists has more than one value, then we're aggregating
		if (accounts != null && accounts.size() > 1)
			aggregates = true;
		if (regions != null && regions.size() > 1)
			aggregates = true;
		if (zones != null && zones.size() > 1)
			aggregates = true;
		
		this.aggregates = aggregates;
	}
	
	public List<TagType> getGroupBy() {
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
	
	public boolean hasGroupByTags() {
		return groupByTags.size() > 0;
	}
	
	/**
	 * Used by the In operand when aggregating the input data
	 */
	public AggregationTagGroup aggregateTagGroup(TagGroup tg, AccountService accountService, ProductService productService) throws Exception {
		Account account = tg.account;
		Region region = tg.region;
		Zone zone = tg.zone;
		Product product = tg.product;
		Operation operation = tg.operation;
		UsageType usageType = tg.usageType;
		
		// Handle filtering
        if (accounts != null && accounts.size() > 0 && !(accounts.contains(tg.account) ^ excludeAccount)) {
        	return null;
        }
        if (regions != null && regions.size() > 0 && !(regions.contains(tg.region) ^ excludeRegion)) {
        	return null;
        }
        if (zones != null && zones.size() > 0 && !(zones.contains(tg.zone) ^ excludeZone)) {
        	return null;
        }
        if (accountTagFilter != null) {
        	String a = accountTagFilter.getGroup(tg.account.getId());
        	if (a == null)
        		return null;
        	account = a.isEmpty() ? null : accountService.getAccountById(a);
        }
        if (regionTagFilter != null) {
        	String r = regionTagFilter.getGroup(tg.region.name);
        	if (r == null)
        		return null;
        	region = r.isEmpty() ? null : Region.getRegionByName(r);
        }
        if (zoneTagFilter != null) {
        	String z = zoneTagFilter.getGroup(tg.zone.name);
        	if (z == null)
        		return null;
        	zone = z.isEmpty() ? null : region.getZone(z);
        }
        if (productTagFilter != null) {
        	String p = productTagFilter.getGroup(tg.product.getServiceCode());
        	if (p == null)
        		return null;
        	product = p.isEmpty() ? null : productService.getProductByServiceCode(p);
        }
        if (operationTagFilter != null) {
        	String op = operationTagFilter.getGroup(tg.operation.name);
        	if (op == null)
        		return null;
        	
        	operation = op.isEmpty() ? null : Operation.getOperation(op);
        }
        if (usageTypeTagFilter != null) {
        	String ut = usageTypeTagFilter.getGroup(tg.usageType.name);
        	if (ut == null)
        		return null;
        	
        	usageType = ut.isEmpty() ? null : UsageType.getUsageType(ut, tg.usageType.unit);
        }
        
		UserTag[] userTags = tg.resourceGroup == null ? null : tg.resourceGroup.getUserTags();
        
		if (userTags != null) {			
			for (String key: userTagFilters.keySet()) {
				TagFilter userTagFilter = userTagFilters.get(key);
				Integer userTagIndex = userTagFilterIndeces.get(key);
				if (userTagFilter != null) {
					String ut = userTagFilter.getGroup(userTags[userTagIndex].name);
					if (ut == null)
						return null;
					
					userTags[userTagIndex] = ut.isEmpty() ? null : UserTag.get(ut);
				}
			}
		}
        
		return aggregation.getAggregationTagGroup(account, region, zone, product, operation, usageType, userTags);
	}
	
	/**
	 * Get the TagGroup based on the AggregationTagGroup. Used when the operand is not an aggregation to directly
	 * determine the proper TagGroup.
	 */
	public TagGroup tagGroup(AggregationTagGroup atg, AccountService accountService, ProductService productService, boolean isNonResource) throws Exception {
		if (hasAggregation())
			throw new Exception("Cannot compute TagGroup directly with an aggregation operand");
		
		return super.tagGroup(atg, accountService, productService, isNonResource);
	}
	
	/**
	 *  Determine if the supplied TagGroup is a match for the given input aggregation.
	 */
	public boolean matches(AggregationTagGroup atg, TagGroup tg) {
        if (accounts != null && accounts.size() > 0 && !(accounts.contains(tg.account) ^ excludeAccount)) {
        	return false;
        }
        if (regions != null && regions.size() > 0 && !(regions.contains(tg.region) ^ excludeRegion)) {
        	return false;
        }
        if (zones != null && zones.size() > 0 && !(zones.contains(tg.zone) ^ excludeZone)) {
        	return false;
        }
        if (accountTagFilter != null) {
        	Account a = atg == null ? null : atg.getAccount();
        	if (a != null && !accountTagFilter.getTag(a.getId()).equals(tg.account.getId()))
        		return false;
        }
        else if (accounts == null && aggregation.groupBy(TagType.Account) && atg != null && atg.getAccount() != tg.account) {
        	return false;
        }
        if (regionTagFilter != null) {
        	Region r = atg == null ? null : atg.getRegion();
        	if (r != null && !regionTagFilter.getTag(r.name).equals(tg.region.name))
        		return false;
        }
        else if (regions == null && aggregation.groupBy(TagType.Region) && atg != null && atg.getRegion() != tg.region) {
        	return false;
        }
        if (zoneTagFilter != null) {
        	Zone z = atg == null ? null : atg.getZone();
        	if (z != null && !productTagFilter.getTag(z.name).equals(tg.zone.name))
        		return false;
        }
        else if (zones == null && aggregation.groupBy(TagType.Zone) && atg != null && atg.getZone() != tg.zone) {
        	return false;
        }
        if (productTagFilter != null) {
        	Product p = atg == null ? null : atg.getProduct();
        	if (p != null && !productTagFilter.getTag(p.getServiceCode()).equals(tg.product.getServiceCode()))
        		return false;
        	if (!productTagFilter.matches(tg.product.getServiceCode()))
        		return false;
        }
        else if (aggregation.groupBy(TagType.Product) && atg != null && atg.getProduct() != tg.product) {
        	return false;
        }
		if (operationTagFilter != null) {
			Operation o = atg == null ? null : atg.getOperation();
			if (o != null && !operationTagFilter.getTag(o.name).equals(tg.operation.name))
				return false;
			if (!operationTagFilter.matches(tg.operation.name))
				return false;
		}
		else if (aggregation.groupBy(TagType.Operation) && atg != null && atg.getOperation() != tg.operation) {
			return false;
		}
		if (usageTypeTagFilter != null) {
			UsageType ut = atg == null ? null : atg.getUsageType();
			if (ut != null && !usageTypeTagFilter.getTag(ut.name).equals(tg.usageType.name))
				return false;
			if (!usageTypeTagFilter.matches(tg.usageType.name))
				return false;
		}
		else if (aggregation.groupBy(TagType.UsageType) && atg != null && atg.getUsageType() != tg.usageType) {
			return false;
		}
		
		if (tg.resourceGroup != null) {
			UserTag[] userTags = tg.resourceGroup.getUserTags();
			
			for (String key: userTagFilters.keySet()) {
				TagFilter userTagFilter = userTagFilters.get(key);
				Integer userTagIndex = userTagFilterIndeces.get(key);
				if (userTagFilter != null) {
					UserTag ut = atg == null ? null : atg.getUserTag(userTagIndex);
					if (ut != null && !userTagFilter.getTag(ut.name).equals(userTags[userTagIndex].name))
						return false;
					if (!userTagFilter.matches(userTags[userTagIndex].name))
						return false;	
				}
				else if (aggregation.groupByUserTag(userTagIndex) && atg != null && atg.getUserTag(userTagIndex) != userTags[userTagIndex]) {
					return false;
				}
			}
			for (Integer userTagIndex: groupByTagsIndeces) {
				if (userTagFilterIndeces.containsValue(userTagIndex))
					continue; // already handled this one above
				if (aggregation.groupByUserTag(userTagIndex) && atg != null && atg.getUserTag(userTagIndex) != userTags[userTagIndex]) {
					return false;
				}
			}
		}
		
		return true;
	}
	
	public String cacheKey(AggregationTagGroup atg) {
		StringBuilder sb = new StringBuilder(32);
		sb.append(type.name() + ",");
        if (accounts != null && accounts.size() > 0) {
        	sb.append(accounts.toString() + "," + (excludeAccount ? "true," : "false,"));
        }
        else if (accountTagFilter != null) {
			sb.append(accountTagFilter.getTag(atg == null ? "" : atg.getAccount().getId()) + ",");
		}
        else if (aggregation.groupBy(TagType.Account) && atg.groupBy(TagType.Account)) {
        	sb.append(atg.getAccount().getId() + ",");
        }
        
        if (regions != null && regions.size() > 0) {
        	sb.append(regions.toString() + "," + (excludeRegion ? "true," : "false,"));
        }
        else if (regionTagFilter != null) {
			sb.append(regionTagFilter.getTag(atg == null ? "" : atg.getRegion().name) + ",");
		}
        else if (aggregation.groupBy(TagType.Region) && atg.groupBy(TagType.Region)) {
        	sb.append(atg.getRegion().name + ",");
        }
        
        if (zones != null && zones.size() > 0) {
        	sb.append(zones.toString() + "," + (excludeZone ? "true," : "false,"));
        }
        else if (zoneTagFilter != null) {
			sb.append(zoneTagFilter.getTag(atg == null ? "" : atg.getZone().name) + ",");
		}
        else if (aggregation.groupBy(TagType.Zone) && (atg == null || atg.groupBy(TagType.Zone))) { // zone can be null even if it's a groupBy
        	sb.append(atg == null || atg.getZone() == null ? "null," : atg.getZone().name + ",");
        }
        
		if (productTagFilter != null) {
			sb.append(productTagFilter.getTag(atg == null || atg.getProduct() == null ? "" : atg.getProduct().getIceName()) + ",");
		}
		else if (aggregation.groupBy(TagType.Product) && atg.groupBy(TagType.Product)) {
			sb.append(atg.getProduct().getServiceCode() + ",");
		}
		
		if (operationTagFilter != null) {
			sb.append(operationTagFilter.getTag(atg == null || atg.getOperation() == null ? "" : atg.getOperation().name) + ",");
		}
		else if (aggregation.groupBy(TagType.Operation) && atg.groupBy(TagType.Operation)) {
			sb.append(atg.getOperation().name + ",");
		}
		
		if (usageTypeTagFilter != null) {
			sb.append(usageTypeTagFilter.getTag(atg == null || atg.getUsageType() == null ? "" : atg.getUsageType().name) + ",");
		}
		else if (aggregation.groupBy(TagType.UsageType) && atg.groupBy(TagType.UsageType)) {
			sb.append(atg.getUsageType().name + ",");
		}
		
		for (String key: userTagFilters.keySet()) {
			TagFilter userTagFilter = userTagFilters.get(key);
			Integer userTagIndex = userTagFilterIndeces.get(key);
			if (userTagFilter != null) {
				sb.append(userTagFilter.getTag(atg == null || atg.getUserTag(userTagIndex) == null ? "" : atg.getUserTag(userTagIndex).name) + ",");
			}
			else if (aggregation.groupByUserTag(userTagIndex) && atg.groupByUserTag(userTagIndex)) {
				sb.append(atg.getUserTag(userTagIndex).name + ",");
			}
		}
		for (Integer userTagIndex: groupByTagsIndeces) {
			if (userTagFilterIndeces.containsValue(userTagIndex))
				continue; // already handled this one above
			if (aggregation.groupByUserTag(userTagIndex) && atg.groupByUserTag(userTagIndex)) { // user tags can be null
				sb.append(atg.getUserTag(userTagIndex).name + ",");
			}
		}
		
		return sb.toString();
	}
	
}
