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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.ice.tag.UserTag;

public class ClusterNameBuilder {
	final private static String rulesSeparator = "\\+";
	final private List<List<Rule>> formulae;
	final private List<String> referencedTags;
	final private List<Integer> referencedTagIndeces;
	
	ClusterNameBuilder(List<String> formulae, List<String> tagNames) {
		this.formulae = Lists.newArrayList();
		
		List<String> refTags = Lists.newArrayList();
		List<Integer> refTagIndeces = Lists.newArrayList();
		
		for (String formula: formulae) {
			List<Rule> rules = Lists.newArrayList();
			
			String[] rulesStrs = formula.split(rulesSeparator);
			for (String rule: rulesStrs) {
				Rule r = new Rule(rule, tagNames);
				rules.add(r);
				if (r.tagIndex >= 0 && !refTagIndeces.contains(r.tagIndex)) {
					refTags.add(tagNames.get(r.tagIndex));
					refTagIndeces.add(r.tagIndex);
				}
			}
			this.formulae.add(rules);
		}
		this.referencedTags = refTags;
		this.referencedTagIndeces = refTagIndeces;
	}
	
	public Set<String> getClusterNames(UserTag[] userTags) {
		Set<String> ret = Sets.newHashSet();
		for (List<Rule> rules: formulae) {
			StringBuilder sb = new StringBuilder(32);
			for (Rule rule: rules) {
				sb.append(rule.call(userTags));
			}
			if (sb.length() > 0)
				ret.add(sb.toString());
		}
		return ret;
	}
	
	public List<String> getReferencedTags() {
		return referencedTags;
	}

	public List<String> getReferencedTagValues(UserTag[] userTags) {
		List<String> values = Lists.newArrayList();
		for (int i: referencedTagIndeces)
			values.add(userTags[i].name);
		return values;
	}

	class Rule {
		final private static char funcSeparator = '.';
		
		public final String literal;
		public final int tagIndex; // Tag to operate on
		public final List<Function> funcs; // operation to perform
		
		Rule(String ruleString, List<String> tagNames) {
			String literal = null;
			if (ruleString.startsWith("\"")) {
				literal = ruleString.replaceAll("\"", "");
			}
			this.literal = literal;
			if (literal != null) {
				this.tagIndex = -1;
				this.funcs = null;
				return;
			}
			
			String tagName = ruleString;
			int sep = ruleString.indexOf(funcSeparator);
			if (sep >= 0) {
				tagName = ruleString.substring(0, sep);
			}
			int index = -1;
			for (int i = 0; i < tagNames.size(); i++) {
				if (tagName.equals(tagNames.get(i))) {
					index = i;
					break;
				}
			}
			if (index == -1)
				throw(new IllegalArgumentException("Rule tag name not found"));
			
			this.tagIndex = index;
			this.funcs = Lists.newArrayList();
			
			if (sep < 0)
				return;
			
			String funcsString = ruleString.substring(ruleString.indexOf(funcSeparator) + 1);
			StringBuilder sb = new StringBuilder(16);
			boolean inQuote = false;
			for (int i = 0; i < funcsString.length(); i++) {
				char c = funcsString.charAt(i);
				if (c == '"') {
					inQuote = !inQuote;
					sb.append(c);
				}
				else if (c == funcSeparator && !inQuote) {
					funcs.add(new Function(sb.toString()));
					sb = new StringBuilder(16);
				}
				else {
					sb.append(c);
				}
			}
			if (sb.length() > 0)
				funcs.add(new Function(sb.toString()));
		}
		
		public String call(UserTag[] userTags) {
			String out = "";
			if (literal != null) {
				out = literal;
			}
			else if (userTags[tagIndex] != null) {
				out = userTags[tagIndex].name;

				for (Function f: funcs) {
					out = f.call(out);
				}
			}
			return out;
		}
	}
	
	enum Op {
		toLower, // convert to lower case string. no arguments
		toUpper, // convert to upper case string. no arguments
		regex,   // apply regular expression and return the first group
	}

	class Function {
		public Op op;
		public String[] args;
		private Pattern pattern = null;
		
		Function(String function) {
			// Examples: "toLower()", "regex(.*-(.*), $1)"
			op = Op.valueOf(function.substring(0, function.indexOf("(")));
			args = function.substring(0, function.lastIndexOf(")")).substring(function.indexOf("(")+1).split(",");
			for (int i = 0; i < args.length; i++) {
				args[i] = args[i].trim();
				// Strip the double quotes
				if (args[i].startsWith("\""))
					args[i] = args[i].substring(0, args[i].length()-1).substring(1);
			}
			if (args.length == 1 && args[0].isEmpty())
				args = new String[]{};
		}
		
		public String call(String in) {
			String out = "";
			
			switch (op) {
			case toLower:
				out = in.toLowerCase();
				break;
				
			case toUpper:
				out = in.toUpperCase();
				break;
				
			case regex:
				if (pattern == null)
					pattern = Pattern.compile(args[0]);
				Matcher matcher = pattern.matcher(in);
				if (matcher.matches())
					out = matcher.group(1);
				break;
			}
			return out;
		}
	}
}
