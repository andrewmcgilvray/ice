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
package com.netflix.ice.processor.kubernetes;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.processor.config.KubernetesNamespaceMapping;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;

public class Tagger {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private final List<Rule> rules;
	private final List<String> tagsToCopy;
	private final List<String> tagKeys;
		
	public Tagger(List<String> tagsToCopy, List<KubernetesNamespaceMapping> namespaceMappings) {
		this.rules = Lists.newArrayList();
		List<String> keys = Lists.newArrayList();
		if (tagsToCopy != null)
			keys.addAll(tagsToCopy);
		
		if (namespaceMappings != null) {
			for (KubernetesNamespaceMapping m: namespaceMappings) {
				this.rules.add(new Rule(m));
				if (!keys.contains(m.getTag()))
					keys.add(m.getTag());
			}
		}
		this.tagsToCopy = tagsToCopy;
		this.tagKeys = keys;
	}
	
	public List<String> getTagKeys() {
		return tagKeys;
	}
	
	public List<String> getTagValues(KubernetesReport report, String[] item) {
		List<String> values = Lists.newArrayList();
		String namespace = report.getString(item, KubernetesColumn.Namespace);
		for (String key: tagKeys) {
			String v = "";
			if (tagsToCopy != null && tagsToCopy.contains(key)) {
				v = report.getUserTag(item, key);
			}
			for (Rule r: rules) {
				if (key.equals(r.getKey()) && r.matches(namespace) && v.isEmpty()) {
					v = r.getValue();
					break;
				}
			}
			values.add(v);
		}
		return values;
	}
			
	class Rule {
		private final KubernetesNamespaceMapping mapping;		
		private final String value;
		private final String key;
		private final List<Pattern> compiledPatterns;

		
		Rule(KubernetesNamespaceMapping mapping) {
			this.mapping = mapping;
			this.key = mapping.getTag();
			this.value = mapping.getValue();
			this.compiledPatterns = Lists.newArrayList();
			for (String p: mapping.getPatterns()) {
				if (p.isEmpty())
					continue;
				this.compiledPatterns.add(Pattern.compile(p));
			}
		}
		
		public boolean matches(String namespace) {
			for (Pattern p: compiledPatterns) {
				Matcher m = p.matcher(namespace);
				if (m.matches())
					return true;
			}
			return false;
		}
		
		public String getTagName() {
			return mapping.getTag();
		}
		
		public String getKey() {
			return key;
		}
		
		public String getValue() {
			return value;
		}
	}
	
}
