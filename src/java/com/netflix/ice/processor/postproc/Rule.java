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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone;

public class Rule {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private final String[] emptyUserTags;

	public RuleConfig config;
	private List<String> userTagKeys;
	private Map<String, Query> operands;
	private Query in;
	private Map<String, Pattern> patterns;
	private List<Result> results;
	
	public enum TagKey {
		account,
		region,
		zone,
		product,
		operation,
		usageType;
	}
	
	private static Map<String, TagKey> allocationKeyMap = Maps.newHashMap();
	static {
		for (TagKey tk: TagKey.values())
			allocationKeyMap.put("_" + tk.toString(), tk);
	}
	
	public Rule(RuleConfig config, AccountService accountService, ProductService productService, List<String> userTagKeys) throws Exception {
		this.config = config;
		this.userTagKeys = userTagKeys;
		this.emptyUserTags = new String[userTagKeys.size()];
		this.patterns = Maps.newHashMap();
		
		// Check for mandatory values in the config
		if (StringUtils.isEmpty(config.getName()) ||
				StringUtils.isEmpty(config.getStart()) ||
				StringUtils.isEmpty(config.getEnd()) ||
				config.getIn() == null ||
				(config.getResults() == null && config.getAllocation() == null && !config.isReport()) || (config.getResults() != null && config.getAllocation() != null)) {
			String err = "Missing required parameters in post processor rule config for " + config.getName() + ". Must have: name, start, end, in, and either results or allocation, but not both";
			logger.error(err);
			throw new Exception(err);
		}
		
		operands = Maps.newHashMap();
		if (config.getOperands() != null) {
			for (String oc: config.getOperands().keySet()) {
				try {
				Query io = new Query(config.getOperand(oc), userTagKeys);
				operands.put(oc, io);
				logger.info("    operand " + oc + ": " + io);
				}
				catch (Exception e) {
					logger.error("Error with rule \"" + config.getName() + "\" and operand \"" + oc + "\", " + e.getMessage());
					throw e;
				}
			}
		}
		
		in = new Query(config.getIn(), userTagKeys);
		if (config.getResults() != null) {
			results = Lists.newArrayList();
			for (ResultConfig rc: config.getResults())
				results.add(new Result(rc));
		}
		
		if (config.getAllocation() != null) {
			// Make sure we're not allocating on a dimension that we've aggregated in the 'in' operand
			List<String> inAllocationTagKeys = Lists.newArrayList(config.getAllocation().getIn().keySet());
			List<TagKey> inTagKeys = config.getIn().getGroupBy();
			List<String> inUserTagKeys = config.getIn().getGroupByTags();
			
			for (String key: inAllocationTagKeys) {
				if (key.startsWith("_")) {
					TagKey tk = allocationKeyMap.get(key);
					if (inTagKeys != null && !inTagKeys.contains(tk)) {
						String err = "Post-processor rule " + config.getName() + " has allocation report that references aggregated tag key: " + tk;
						logger.error(err);
						throw new Exception(err);
					}
				}
				else {
					if (inUserTagKeys != null && !inUserTagKeys.contains(key)) {
						String err = "Post-processor rule " + config.getName() + " has allocation report that references aggregated user tag key: " + key;
						logger.error(err);
						throw new Exception(err);
					}
				}
				
			}
		}
	}
	
	public List<String> getOutUserTagKeys() {
		Set<String> keys = Sets.newHashSet();
		keys.addAll(in.getGroupByTags());
		if (config.getAllocation() != null) {
			keys.addAll(config.getAllocation().getOut().keySet());
		}
		
		List<String> sortedKeys = Lists.newArrayList(keys);
		Collections.sort(sortedKeys);
		return sortedKeys;
	}
	
	public Query getOperand(String name) {
		return operands.get(name);
	}
	
	public Map<String, Query> getOperands() {
		return operands;
	}
	
	public Query getIn() {
		return in;
	}
	
	public List<Result> getResults() {
		return results;
	}
	
	public Result getResult(int index) {
		return results.get(index);
	}
	
	public String getResultValue(int index) {
		return config.getResults().get(index).getValue();
	}
	
	/**
	 * Get the matching group 1 from the supplied string using the requested regex pattern.
	 * If no group is supplied in the regex and the string matches, then return group 0 (the entire string).
	 * 
	 * @param patternName
	 * @param string
	 * @return Return group 1 if present in the regex, or group 0 if no group specified. Return null if no match.
	 */
	public String getGroup(String patternName, String string) {
		Pattern pattern = patterns.get(patternName);
		if (pattern == null) {
			pattern = Pattern.compile(config.getPatterns().get(patternName));
			patterns.put(patternName, pattern);
		}
		Matcher matcher = pattern.matcher(string);
		if (matcher.matches()) {
			// If no group specified in the regex, return group 0 (the whole match)
			return matcher.groupCount() == 0 ? matcher.group(0) : matcher.group(1);
		}
		return null;
	}
	
	public class Result {
		private ResultConfig config;
		protected final Map<String, Integer> userTagIndeces;
		
		public Result(ResultConfig config) throws Exception {
			this.config = config;
			userTagIndeces = Maps.newHashMap();
			TagGroupConfig tgc = config.getOut();
			if (tgc.getUserTags() != null) {
				List<String> customTags = userTagKeys;
				for (String key: tgc.getUserTags().keySet()) {
					if (!customTags.contains(key))
						throw new Exception("Invalid user tag key name: \"" + key + "\"");
			    	userTagIndeces.put(key, userTagKeys.indexOf(key));
				}
			}
		}

		public String getProduct() {
			return config.getOut().getProduct();
		}
		
		public RuleConfig.DataType getType() {
			return config.getType();
		}

		public boolean isSingle() {
			return config.isSingle();
		}
		
		/**
		 * Get the TagGroup based on the supplied AggregationTagGroup. Values present in the operand config are used to
		 * override the values in the supplied TagGroup. For tags that have lists, there should only be one entry, so
		 * the first is always chosen.
		 */
		public TagGroup tagGroup(AggregationTagGroup atg, AccountService accountService, ProductService productService, boolean isNonResource) throws Exception {
			Account account = atg == null ? null : atg.getAccount();
			Region region = atg == null ? null : atg.getRegion();
			Zone zone = atg == null ? null : atg.getZone();
			Product product = atg == null ? null : atg.getProduct();
			Operation operation = atg == null ? null : atg.getOperation();
			UsageType usageType = atg == null ? null : atg.getUsageType();		
			
			TagGroupConfig tgc = config.getOut();
			if (tgc.getAccount() != null)
				account = accountService.getAccountById(getTag(tgc.getAccount(), account == null ? null : account.name));
			if (tgc.getRegion() != null)
				region = Region.getRegionByName(getTag(tgc.getRegion(), region == null ? null : region.name));
			if (tgc.getZone() != null)
				zone = region.getZone(getTag(tgc.getZone(), zone == null ? null : zone.name));
			if (tgc.getProduct() != null)
				product = productService.getProductByServiceCode(getTag(tgc.getProduct(), product == null ? null : product.getIceName()));
			if (tgc.getOperation() != null)
				operation = Operation.getOperation(getTag(tgc.getOperation(), operation == null ? null : operation.name));
			if (tgc.getUsageType() != null) {
				String ut = getTag(tgc.getUsageType(), usageType == null ? null : usageType.name);
				usageType = UsageType.getUsageType(ut, usageType == null ? "" : usageType.unit);
			}
			
			ResourceGroup resourceGroup = null;		
			if (!isNonResource) {
				resourceGroup = atg == null ? null : atg.getResourceGroup(userTagKeys.size());
				if (resourceGroup == null && (tgc.getUserTags() == null || tgc.getUserTags().size() == 0)) {
					resourceGroup = ResourceGroup.getResourceGroup(emptyUserTags);
				}
				else {
					List<UserTag> userTags = Lists.newArrayListWithCapacity(userTagKeys.size());
					for (int i = 0; i < userTagKeys.size(); i++)
						userTags.add(atg == null ? UserTag.empty : atg.getUserTag(i));
					if (tgc.getUserTags() != null && tgc.getUserTags().size() > 0) {
						for (String key: tgc.getUserTags().keySet()) {
							int i = userTagIndeces.get(key);
							UserTag ut = userTags.get(i);
							userTags.set(i, UserTag.get(getTag(tgc.getUserTags().get(key), ut == null ? "" : ut.name)));
						}
					}
					resourceGroup = ResourceGroup.getResourceGroup(userTags);
				}
			}

			return TagGroup.getTagGroup(account, region, zone, product, operation, usageType, resourceGroup);
		}
	}
	
	static final String templateVariableRegex = "\\$\\{([^\\}]+)\\}";
	static final Pattern templateVariablePattern = Pattern.compile(templateVariableRegex);
			
	/**
	 * Build the out tag by replacing any group reference with the group value provided.
	 */
	public String getTag(String template, String string) {
		StringBuffer sb = new StringBuffer();
		Matcher m = templateVariablePattern.matcher(template);
		while (m.find()) {
			String patternName = m.group(1);
			Pattern p = patterns.get(patternName);
			if (p == null) {
				p = Pattern.compile(config.getPatterns().get(patternName));
				patterns.put(patternName, p);
			}
			
			if (string == null) {
				m.appendReplacement(sb,  "");
			}
			else {
				Matcher matcher = p.matcher(string);
				String replacement = matcher.matches() ? matcher.groupCount() == 0 ? matcher.group(0) : matcher.group(1) : "";
				m.appendReplacement(sb, replacement);
			}
		}
		m.appendTail(sb);
		return sb.toString();
	}
				
}

