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
 * OperandConfig specifies the set of tags used to filter and aggregate usage or cost data in a
 * post processor results value expression.
 * 
 * The accounts, regions, and zones attributes provide the ability to filter on a list
 * of values. For accounts the account ID is used, not the account name. If the values for these
 * attributes is left blank or not specified, then no filtering is done.
 * 
 * The account, region, zone, product, operation, and usageType fields are regex patterns used to both filter and capture
 * substrings which can be used to build result field values.
 * 
 * If the groupBy attribute is provided, any attribute names not listed will be merged and not broken
 * out as separate values in the result. If groupBy is not provided, no merging will be performed. If
 * groupBy is provided but is an empty list, all tag types will be merged.
 * 
 * Allowed groupBy values are: Account, Region, Zone, Product, Operation, UsageType
 * 
 * If the groupByUserTag attribute is provided, any user tag names not listed will be merged and not
 * broken out as separate values in the result. If groupByUserTag is not provided, no merging will be performed.
 * If groupByUserTag is provided but is an empty map, all user tags will be merged.
 * 
 * Allowed groupByUserTag values are the names specified for ice.customTags in the ice.properties configuration file.
 * 
 * If the exclude attribute is provided, then for each list of attributes, the specified values
 * for each of that attribute are excluded from the aggregation.
 * 
 * Allowed exclude values are: Account, Region, Zone
 * 
 * The monthly attribute can be set to produce a single value for the month rather than per hour.
 * 
 * The single attribute can be set to indicate that the operand has no dependencies on another operand and will produce
 * a single hourly or monthly value set.
 */
public class OperandConfig {
	public enum OperandType {
		cost,
		usage;
	}
	
	private OperandType type;
	private List<String> accounts;
	private List<String> regions;
	private List<String> zones;
	private String account;
	private String region;
	private String zone;
	private String product;
	private String operation;
	private String usageType;
	private Map<String, String> userTags;
	private List<String> groupBy;
	private List<String> groupByTags;
	private List<String> exclude;
	private boolean monthly;
	private boolean single;
	
	public OperandConfig() {
		accounts = Lists.newArrayList();
		regions = Lists.newArrayList();
		zones = Lists.newArrayList();
	}
	
	@Override
	public OperandConfig clone() {
		OperandConfig copy = new OperandConfig();
		copy.type = type;
		copy.accounts = accounts == null ? null : Lists.newArrayList(accounts);
		copy.regions = regions == null ? null : Lists.newArrayList(regions);
		copy.zones = zones == null ? null : Lists.newArrayList(zones);
		copy.account = account;
		copy.region = region;
		copy.zone = zone;
		copy.product = product;
		copy.operation = operation;
		copy.usageType = usageType;
		copy.userTags = userTags == null ? null : Maps.newHashMap(userTags);
		copy.groupBy = groupBy == null ? null : Lists.newArrayList(groupBy);
		copy.groupByTags = groupByTags == null ? null : Lists.newArrayList(groupByTags);
		copy.exclude = exclude == null ? null : Lists.newArrayList(exclude);
		copy.monthly = monthly;
		copy.single = single;
		
		return copy;
	}
	
	public OperandType getType() {
		return type;
	}
	public void setType(OperandType type) {
		this.type = type;
	}
	public List<String> getAccounts() {
		return accounts;
	}
	public void setAccounts(List<String> accounts) {
		this.accounts = accounts;
	}
	public List<String> getRegions() {
		return regions;
	}
	public void setRegions(List<String> regions) {
		this.regions = regions;
	}
	public List<String> getZones() {
		return zones;
	}
	public void setZones(List<String> zones) {
		this.zones = zones;
	}
	public String getAccount() {
		return account;
	}
	public void setAccount(String account) {
		this.account = account;
	}	
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getZone() {
		return zone;
	}

	public void setZone(String zone) {
		this.zone = zone;
	}

	public String getProduct() {
		return product;
	}
	public void setProduct(String product) {
		this.product = product;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getUsageType() {
		return usageType;
	}
	public void setUsageType(String usageType) {
		this.usageType = usageType;
	}
	public Map<String, String> getUserTags() {
		return userTags;
	}
	public void setUserTags(Map<String, String> userTags) {
		this.userTags = userTags;
	}

	public List<String> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<String> groupBy) {
		this.groupBy = groupBy;
	}

	public List<String> getGroupByTags() {
		return groupByTags;
	}

	public void setGroupByTags(List<String> groupByTags) {
		this.groupByTags = groupByTags;
	}

	public List<String> getExclude() {
		return exclude;
	}

	public void setExclude(List<String> exclude) {
		this.exclude = exclude;
	}

	public boolean isMonthly() {
		return monthly;
	}
	
	public void setMonthly(boolean monthly) {
		this.monthly = monthly;
	}

	public boolean isSingle() {
		return single;
	}

	public void setSingle(boolean single) {
		this.single = single;
	}
}
