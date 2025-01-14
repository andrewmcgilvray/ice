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

/*
 * TagConfig holds the name of a user tag key and an optional set of aliases it may go by.
 * It also includes a list of values the tag key may take along with a set of aliases that each value may take.
 */
public class TagConfig {
	public enum Convert {
		none,
		toUpper,
		toLower;
	}

	public static class KeyAlias {
		public String name;
		public String filter;
		public TagConfig.Convert convert;

		public KeyAlias() {}

		public KeyAlias(String name, String filter, TagConfig.Convert convert) {
			this.name = name;
			this.filter = filter;
		}

		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getFilter() { return filter; }
		public void setFilter(String filter) { this.filter = filter; }
	}

	public String name;
	public List<KeyAlias> aliases;
	public List<String> displayAliases; // additional names to be shown in the UX
	public Map<String, List<String>> values;
	public String filter; // If not null, specifies a regex pattern that values must conform to, else value is set to 'other'
	public Convert convert;
	public List<TagMappings> mapped;
	
	public TagConfig() {}
	
	public TagConfig(String name, List<KeyAlias> aliases, List<String> displayAliases, Map<String, List<String>> values) {
		this.name = name;
		this.aliases = aliases;
		this.displayAliases = displayAliases;
		this.values = values;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<KeyAlias> getAliases() {
		return aliases;
	}

	public void setAliases(List<KeyAlias> aliases) {
		this.aliases = aliases;
	}

	public List<String> getDisplayAliases() {
		return displayAliases;
	}

	public void setDisplayAliases(List<String> displayAliases) {
		this.displayAliases = displayAliases;
	}

	public Map<String, List<String>> getValues() {
		return values;
	}

	public void setValues(Map<String, List<String>> values) {
		this.values = values;
	}

	public String getFilter() { return filter; }

	public void setFilter(String filter) { this.filter = filter; }

	public Convert getConvert() { return convert; }

	public void setConvert(Convert convert) { this.convert = convert; }

	public List<TagMappings> getMapped() {
		return mapped;
	}

	public TagMappings getTagMappings(String name) {
		for (TagMappings tm: mapped) {
			if (name.equals(tm.getName()))
				return tm;
		}
		return null;
	}
}
