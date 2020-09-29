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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.tag.TagType;

public class Rule {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	public RuleConfig config;
	private Map<String, InputOperand> operands;
	private InputOperand in;
	private List<Operand> results;
	
	private static TagType[] tagTypes = new TagType[]{
		TagType.Account,
		TagType.Region,
		TagType.Zone,
		TagType.Product,
		TagType.Operation,
		TagType.UsageType,
	};
	private static Map<String, TagType> allocationKeyMap = Maps.newHashMap();
	static {
		for (TagType tt: tagTypes)
			allocationKeyMap.put("_" + tt.toString(), tt);
	}
	
	public Rule(RuleConfig config, AccountService accountService, ProductService productService, ResourceService resourceService) throws Exception {
		this.config = config;
		
		// Check for mandatory values in the config
		if (StringUtils.isEmpty(config.getName()) ||
				StringUtils.isEmpty(config.getStart()) ||
				StringUtils.isEmpty(config.getEnd()) ||
				config.getIn() == null ||
				(config.getResults() == null && config.getAllocation() == null) || (config.getResults() != null && config.getAllocation() != null)) {
			String err = "Missing required parameters in post processor rule config for " + config.getName() + ". Must have: name, start, end, in, and either results or allocation, but not both";
			logger.error(err);
			throw new Exception(err);
		}
		
		operands = Maps.newHashMap();
		if (config.getOperands() != null) {
			for (String oc: config.getOperands().keySet()) {
				InputOperand io = new InputOperand(config.getOperand(oc), accountService, resourceService);
				operands.put(oc, io);
				logger.info("    operand " + oc + ": " + io);
			}
		}
		
		in = new InputOperand(config.getIn(), accountService, resourceService);
		results = Lists.newArrayList();
		if (config.getResults() != null) {
			for (ResultConfig rc: config.getResults()) {
				Operand r = new Operand(rc.getOut(), accountService, resourceService);
				logger.info("    result " + results.size() + ": " + r);
				results.add(r);
			}
		}
		
		if (config.getAllocation() != null) {
			// Make sure we're not allocating on a dimension that we've aggregated in the 'in' operand
			List<String> inAllocationTagKeys = Lists.newArrayList(config.getAllocation().getIn().keySet());
			List<TagType> inTagKeys = config.getIn().getGroupBy();
			List<String> inUserTagKeys = config.getIn().getGroupByTags();
			
			for (String key: inAllocationTagKeys) {
				if (key.startsWith("_")) {
					TagType tt = allocationKeyMap.get(key);
					if (inTagKeys != null && !inTagKeys.contains(tt)) {
						String err = "Post-processor rule " + config.getName() + " has allocation report that references aggregated tag key: " + tt;
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
	
	public InputOperand getOperand(String name) {
		return operands.get(name);
	}
	
	public Map<String, InputOperand> getOperands() {
		return operands;
	}
	
	public InputOperand getIn() {
		return in;
	}
	
	public List<Operand> getResults() {
		return results;
	}
	
	public Operand getResult(int index) {
		return results.get(index);
	}
	
	public String getResultValue(int index) {
		return config.getResults().get(index).getValue();
	}
}

