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
package com.netflix.ice.common;

import java.util.List;
import java.util.Map;

/**
 * TagMappings defines a set of mapping rules that allow you to apply values to a
 * User Tag based on values of that tag or other tags. You can optionally specify what accounts
 * the rules apply to based on use of either <i>include</i> or <i>exclude</i> arrays.
 * The mappings may also specify a start date to indicate when the rules should take
 * effect. By default, only empty values are replaced by the mapping rules, but existing
 * values may be overwritten by setting <i>force</i> to true.
 * 
 * Previously defined rules may be stopped by specifying the mapped value as an empty string.
 * 
 * Rules are specified using a hierarchy of terms and associated operations.
 * Supported operations are:
 * 
 *   isOneOf - used to test if a tag key has one of the values in a list of supplied values
 *   isNotOneOf - used to test if a tag key is not one of the values in a list
 *   
 *   or - logical or of the evaluated results of all child terms
 *   and - logical and of the evaluated results of all child terms
 * 
 * Example yml config data for setting an Environment tag based on an Application tag
 * for account # 123456789012 starting on Feb. 1, 2020.
 *
 * <pre>
 * include: [123456789012]
 * start: 2020-02
 * force: true
 * maps:
 *   NonProd:
 *     key: Application
 *     operator: isOneOf
 *     values: [webServerTest, webServerStage]
 *   Prod:
 *     operator: or
 *     terms:
 *     - operator: and
 *       terms:
 *       - key: Application:
 *         operator: isOneOf
 *         values: [webServer]
 *       - key: Env:
 *         operator: isNotOneOf
 *         values: [test, qa, uat, stage]
 *     - key: Env
 *       operator: isOneOf
 *       values: [prod]
 * </pre>
 */
public class TagMappings {
	public String name; // name of tag mapping which if present allows it to be inherited by other TagMappings.
	public List<String> owners; // names of owners responsible for the mapping rule
	public Map<String, TagMappingTerm> maps;
	public List<String> include;
	public List<String> exclude;
	public String start;
	public Boolean force;
	public String parent; // Name of tag mappings to inherit from. Any parameters defined in this mappings will override values inherited.
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getOwners() {
		return owners;
	}
	public void setOwners(List<String> owners) {
		this.owners = owners;
	}
	public Map<String, TagMappingTerm> getMaps() {
		return maps;
	}
	public void setMaps(Map<String, TagMappingTerm> maps) {
		this.maps = maps;
	}
	public List<String> getInclude() {
		return include;
	}
	public void setInclude(List<String> include) {
		this.include = include;
	}
	public List<String> getExclude() {
		return exclude;
	}
	public void setExclude(List<String> exclude) {
		this.exclude = exclude;
	}
	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
	public Boolean isForce() {
		return force;
	}
	public void setForce(Boolean force) {
		this.force = force;
	}
	public String getParent() {
		return parent;
	}
	public void setParent(String parent) {
		this.parent = parent;
	}
	
	public void inherit(TagMappings parent) {
		if (name == null)
			name = parent.name;
		if (owners == null)
			owners = parent.owners;
		if (maps == null)
			maps = parent.maps;
		if (include == null)
			include = parent.include;
		if (exclude == null)
			exclude = parent.exclude;
		if (start == null)
			start = parent.start;
		if (force == null)
			force = parent.force;
	}
}
	
