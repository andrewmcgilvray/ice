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

import com.netflix.ice.processor.TagMapper;

import java.util.List;
import java.util.regex.Pattern;

public class TagMappingTerm {
	public List<TagMappingTerm> terms;
	public Operator operator;
	public String key;
	public List<String> values;
	
	// Place for the TagMapper to cache the key index and compiled patterns
	public int keyIndex;
	public TagGroupField tagGroupField;
	public List<Pattern> patterns;
	
	public enum Operator {
		and,
		or,
		isOneOf,
		isNotOneOf;
	}

	public enum TagGroupField {
		_CostType,
		_Account,
		_Region,
		_Zone,
		_Product,
		_Operation,
		_UsageType;
	}

	public List<TagMappingTerm> getTerms() {
		return terms;
	}

	public void setTerms(List<TagMappingTerm> terms) {
		this.terms = terms;
	}

	public Operator getOperator() {
		return operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public List<String> getValues() {
		return values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}
}
