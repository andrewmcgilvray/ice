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
package com.netflix.ice.processor;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.ice.common.TagGroup;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.common.TagMappingTerm;
import com.netflix.ice.common.TagMappings;

public class TagMapper {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private final int tagIndex;
	private final TagMappings config;
	private final long startMillis;
	private final boolean force;
	
	public static final String suspend = "<suspend>";

	public TagMapper(int tagIndex, TagMappings mappings, Map<String, Integer> tagKeyIndeces) {
		this.tagIndex = tagIndex;
		this.config = mappings;
		this.startMillis = config.start == null || config.start.isEmpty() ? 0 : new DateTime(config.start, DateTimeZone.UTC).getMillis();
		this.force = config.force == null ? false : config.force;

		for (String mappedValue: config.maps.keySet()) {
			TagMappingTerm term = config.maps.get(mappedValue);
			if (initTerm(mappedValue, term, tagKeyIndeces) != 0)
				continue;
		}
	}


	private int initTerm(String mappedValue, TagMappingTerm term, Map<String, Integer> tagKeyIndeces) {
		TagMappingTerm.Operator op = term.getOperator();
		if (op == null) {
			logger.error("Tag mapping term for \"" + mappedValue + "\" has no operator");
			return -1;
		}
	
		switch (op) {
		case or:
		case and:
			if (term.getTerms() == null) {
				logger.error("Tag mapping term for \"" + mappedValue + "\" with operator \"" + op + "\" has no terms");
				return -1;
			}
			for (TagMappingTerm t: term.getTerms()) {
				if (initTerm(mappedValue, t, tagKeyIndeces) != 0)
					return -1;
			}
			break;
			
		case isOneOf:
		case isNotOneOf:
			if (term.getKey() == null) {
				logger.error("Tag mapping term for \"" + mappedValue + "\" with operator \"" + op + "\" has no key");
				return -1;
			}
			if (term.getValues() == null || term.getValues().isEmpty()) {
				logger.error("Tag mapping term for \"" + mappedValue + "\" with operator \"" + op + "\" has no values");
				return -1;
			}
			String key = term.getKey();
			term.tagGroupField = null;
			if (key.startsWith("_")) {
				try {
					term.tagGroupField = TagMappingTerm.TagGroupField.valueOf(key);
				}
				catch (IllegalArgumentException e) {
					logger.error("Tag mapping term for \"" + mappedValue + "\" with operator \"" + op + "\" has invalid key");
					return -1;
				}
			}
			else if (!tagKeyIndeces.containsKey(term.getKey())) {
				logger.error("Tag mapping term for \"" + mappedValue + "\" with operator \"" + op + "\" has invalid key");
				return -1;
			}
			// Set tag key index for term
			term.keyIndex = term.tagGroupField != null ? -1 : tagKeyIndeces.get(term.getKey());

			// Create compiled patterns for values
			term.patterns = Lists.newArrayList();
			for (String regexTarget: term.getValues()) {
				term.patterns.add(Pattern.compile(regexTarget, Pattern.CASE_INSENSITIVE));
			}
			
			break;
		}
		return 0;
	}
	
	public long getStartMillis() {
		return startMillis;
	}
	
	public int getTagIndex() {
		return tagIndex;
	}

	private String getField(TagMappingTerm.TagGroupField field, TagGroup tg) {
		switch (field) {
			case _CostType:	return tg.costType.name;
			case _Account:	return tg.account.getId();
			case _Region:	return tg.region.name;
			case _Zone:		return tg.zone.name;
			case _Product:	return tg.product.getServiceCode();
			case _Operation: return tg.operation.name;
			case _UsageType: return tg.usageType.name;
		}
		return "";
	}
	
	private boolean eval(TagMappingTerm term, TagGroup tg, String[] tags) {
		TagMappingTerm.Operator op = term.getOperator();
		
		switch(op) {
		case or:
			for (TagMappingTerm t: term.getTerms()) {
				if (eval(t, tg, tags))
					return true;
			}
			return false;
			
		case and:
			for (TagMappingTerm t: term.getTerms()) {
				if (!eval(t, tg, tags))
					return false;
			}
			return true;
			
		case isOneOf:
		case isNotOneOf:
			String srcV = term.tagGroupField == null ? tags[term.keyIndex] : getField(term.tagGroupField, tg);
			if (srcV != null) {
				for (Pattern p: term.patterns) {
					Matcher m = p.matcher(srcV);
					if (m.matches())
						return op == TagMappingTerm.Operator.isOneOf;
				}
			}
			return op == TagMappingTerm.Operator.isNotOneOf;
		}
		return false;
	}
	
	public String apply(long startMillis, TagGroup tg, String[] tags, String value) {
		// Make sure the mapper is in effect and that we have maps
		if (startMillis < this.startMillis || config.maps.isEmpty())
			return value;
		
		// If we already have a value and we aren't set to force the map, just return current value.
		if (tags[tagIndex] != null && !tags[tagIndex].isEmpty() && !force)
			return value;

    	// If we have an include filter, make sure the account is in the list
    	if (config.include != null && !config.include.isEmpty() && !config.include.contains(tg.account.getId()))
    		return value;
    	
    	// If we have an exclude filter, make sure the account is not in the list
    	if (config.exclude != null && !config.exclude.isEmpty() && config.exclude.contains(tg.account.getId()))
    		return value;

    	// We'll accept the first term in the map that returns a value
		for (String v: config.maps.keySet()) {
			TagMappingTerm term = config.maps.get(v);
			if (eval(term, tg, tags)) {
				value = v;
				break;
			}
		}
		return value;
	}	
}
