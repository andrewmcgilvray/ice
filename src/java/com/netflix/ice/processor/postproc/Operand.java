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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.postproc.OperandConfig.OperandType;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone;

public class Operand {
	protected final String groupToken = "${group}";
	
	protected final OperandType type;
	protected final List<Account> accounts;
	protected final List<Region> regions;
	protected final List<Zone> zones;
	protected final TagFilter accountTagFilter;
	protected final TagFilter regionTagFilter;
	protected final TagFilter zoneTagFilter;
	protected final TagFilter productTagFilter;
	protected final TagFilter operationTagFilter;
	protected final TagFilter usageTypeTagFilter;
	protected final Map<String, TagFilter> userTagFilters;
	protected final Map<String, Integer> userTagFilterIndeces;
	protected final int numUserTags;
	private final boolean single;
	private final boolean monthly;
	private final String[] emptyUserTags;
	
	public String toString() {
		List<String> tags = Lists.newArrayList();
		tags.add(type.toString());
		if (single)
			tags.add("single");
		if (monthly)
			tags.add("monthly");
		
		String s = accounts.size() > 0 ? ("accounts:" + accounts + "") : accountTagFilter != null ? ("account:\"" + accountTagFilter + "\"") : null;
		if (s != null)
			tags.add(s);
		
		s = regions.size() > 0 ? ("regions:" + regions + "") : regionTagFilter != null ? ("region:\"" + regionTagFilter + "\"") : null;
		if (s != null)
			tags.add(s);

		s = zones.size() > 0 ? ("zones:" + zones + "") : zoneTagFilter != null ? ("zone:\"" + zoneTagFilter + "\"") : null;
		if (s != null)
			tags.add(s);
		
		if (productTagFilter != null)
			tags.add("product:\"" + productTagFilter + "\"");
		if (operationTagFilter != null)
			tags.add("operation:\"" + operationTagFilter + "\"");
		if (usageTypeTagFilter != null)
			tags.add("usageType:\"" + usageTypeTagFilter + "\"");
		for (String key: userTagFilters.keySet()) {
			tags.add(key + ":\"" + userTagFilters.get(key) + "\"");
		}
						
		return "{" + StringUtils.join(tags, ",") + "}";
	}
	
	public Operand(OperandConfig opConfig, AccountService accountService, ResourceService resourceService) throws Exception {
		type = opConfig.getType();
		accounts = Lists.newArrayList();
		for (String a: opConfig.getAccounts()) {
			accounts.add(accountService.getAccountById(a));
		}
		regions = Region.getRegions(opConfig.getRegions());
		zones = Zone.getZones(opConfig.getZones());
		accountTagFilter = opConfig.getAccount() == null ? null : new TagFilter(opConfig.getAccount());
		regionTagFilter = opConfig.getRegion() == null ? null : new TagFilter(opConfig.getRegion());
		zoneTagFilter = opConfig.getZone() == null ? null : new TagFilter(opConfig.getZone());
		productTagFilter = opConfig.getProduct() == null ? null : new TagFilter(opConfig.getProduct());
		operationTagFilter = opConfig.getOperation() == null ? null : new TagFilter(opConfig.getOperation());
		usageTypeTagFilter = opConfig.getUsageType() == null ? null : new TagFilter(opConfig.getUsageType());
		userTagFilters = Maps.newHashMap();
		userTagFilterIndeces = Maps.newHashMap();
		if (opConfig.getUserTags() != null) {
			List<String> customTags = resourceService.getCustomTags();
			for (String key: opConfig.getUserTags().keySet()) {
				if (!customTags.contains(key))
					throw new Exception("Invalid user tag key name: \"" + key + "\"");
				userTagFilters.put(key, new TagFilter(opConfig.getUserTags().get(key)));
		    	userTagFilterIndeces.put(key, resourceService.getUserTagIndex(key));
			}
		}
		numUserTags = resourceService.getCustomTags().size();
		single = opConfig.isSingle();
		monthly = opConfig.isMonthly();
		emptyUserTags = new String[numUserTags];
		
		validate();		
	}
	
	private void validate() throws Exception {
		if (accounts.size() > 0 && accountTagFilter != null)
			throw new Exception("only specify one of accounts or account filter");
		if (regions.size() > 0 && regionTagFilter != null)
			throw new Exception("only specify one of regions or region filter");
		if (zones.size() > 0 && zoneTagFilter != null)
			throw new Exception("only specify one of zones or zone filter");
		if (single) {
			if ((accountTagFilter != null && accountTagFilter.hasDependency()) ||
				(regionTagFilter != null && regionTagFilter.hasDependency()) ||
				(zoneTagFilter != null && zoneTagFilter.hasDependency()) ||
				(productTagFilter != null && productTagFilter.hasDependency()) ||
				(operationTagFilter != null && operationTagFilter.hasDependency()) ||
				(usageTypeTagFilter != null && usageTypeTagFilter.hasDependency())) {
				throw new Exception("single value operand should not have a tag filter with a dependency");
			}
			for (TagFilter tf: userTagFilters.values()) {
				if (tf.hasDependency())
					throw new Exception("single value operand should not have a tag filter with a dependency");
			}
		}
	}
	
	public OperandType getType() {
		return type;
	}
	
	public boolean isSingle() {
		return single;
	}

	public boolean isMonthly() {
		return monthly;
	}
	
	/**
	 * Get the products that match the product regex group
	 * @param productService
	 * @return List of Product
	 */
	public List<Product> getProducts(ProductService productService) {
		List<Product> products = Lists.newArrayList();
		for (Product p: productService.getProducts()) {
			if (productTagFilter == null || productTagFilter.matches(p.getServiceCode()))
				products.add(p);
		}
		return products;
	}
	
	public Product getProduct(ProductService productService) {
		// Result operands just use the regex directly.
		return productService.getProductByServiceCode(productTagFilter.regex);
	}
	
	/**
	 * Get the TagGroup based on the supplied AggregationTagGroup. Values present in the operand config are used to
	 * override the values in the supplied TagGroup. For tags that have lists, there should only be one entry, so
	 * the first is always chosen.
	 */
	public TagGroup tagGroup(AggregationTagGroup atg, AccountService accountService, ProductService productService, boolean isNonResource) throws Exception {
		Account account = (accounts != null && accounts.size() > 0) ? accounts.get(0) : atg == null ? null : atg.getAccount();
		Region region = (regions != null && regions.size() > 0) ? regions.get(0) : atg == null ? null : atg.getRegion();
		Zone zone = (zones != null && zones.size() > 0) ? zones.get(0) : atg == null ? null : atg.getZone();
		Product product = atg == null ? null : atg.getProduct();
		Operation operation = atg == null ? null : atg.getOperation();
		UsageType usageType = atg == null ? null : atg.getUsageType();		
		
		if (accountTagFilter != null) {
			account = accountService.getAccountById(accountTagFilter.getTag(account == null ? "" : account.name));
		}
		if (regionTagFilter != null) {
			region = Region.getRegionByName(regionTagFilter.getTag(region == null ? "" : region.name));
		}
		if (zoneTagFilter != null) {
			zone = region.getZone(zoneTagFilter.getTag(zone == null ? "" : zone.name));
		}
		if (productTagFilter != null) {
			product = productService.getProductByServiceCode(productTagFilter.getTag(product == null ? "" : product.getIceName()));
		}
		if (operationTagFilter != null) {
			operation = Operation.getOperation(operationTagFilter.getTag(operation == null ? "" : operation.name));
		}
		if (usageTypeTagFilter != null) {
			String ut = usageTypeTagFilter.getTag(usageType == null ? "" : usageType.name);
			usageType = UsageType.getUsageType(ut, usageType == null ? "" : usageType.unit);
		}
		
		ResourceGroup resourceGroup = null;		
		if (!isNonResource) {
			resourceGroup = atg == null ? null : atg.getResourceGroup(numUserTags);
			if (resourceGroup == null && userTagFilters.size() == 0) {
				resourceGroup = ResourceGroup.getResourceGroup(emptyUserTags);
			}
			else {
				List<UserTag> userTags = Lists.newArrayListWithCapacity(numUserTags);
				for (int i = 0; i < numUserTags; i++)
					userTags.add(atg == null ? UserTag.empty : atg.getUserTag(i));
				if (userTagFilters.size() > 0) {
					for (String key: userTagFilters.keySet()) {
						int i = userTagFilterIndeces.get(key);
						UserTag ut = userTags.get(i);
						userTags.set(i, UserTag.get(userTagFilters.get(key).getTag(ut == null ? "" : ut.name)));
					}
				}
				resourceGroup = ResourceGroup.getResourceGroup(userTags);
			}
		}

		return TagGroup.getTagGroup(account, region, zone, product, operation, usageType, resourceGroup);
	}

		
	public class TagFilter {
		String regex;
		private Pattern pattern;
		
		public TagFilter(String regex) {
			this.regex = regex;
		}
		
		public String getGroup(String name) {
			if (pattern == null)
				pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(name);
			if (matcher.matches()) {
				// If no group specified in the regex, return group 0 (the whole match)
				return matcher.groupCount() == 0 ? matcher.group(0) : matcher.group(1);
			}
			return null;
		}
		
		public boolean matches(String name) {
			if (pattern == null)
				pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(name);
			return matcher.matches();
		}
		
		/**
		 * Build the out tag by replacing any group reference with the group value provided.
		 */
		public String getTag(String group) {
			return regex.replace(groupToken, group);
		}
		
		public String toString() {
			return regex;
		}
		
		public boolean hasDependency() {
			return regex.contains("${");
		}
	}
}