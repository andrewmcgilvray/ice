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
package com.netflix.ice.tag;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class UserTagKey extends Tag {
	private static final long serialVersionUID = 1L;

    private static ConcurrentMap<String, UserTagKey> tagKeysByName = Maps.newConcurrentMap();
    
    public List<String> aliases;
    
	private UserTagKey(String name) {
		super(name);
		aliases = Lists.newArrayList();
	}
	
	public void addAlias(String alias) {
		aliases.add(alias);
	}
	
	public void addAllAliases(Collection<String> aliases) {
		this.aliases.addAll(aliases);
	}
	
	public static UserTagKey get(String name) {
		if (name == null)
			name = "";
        UserTagKey tag = tagKeysByName.get(name);
        if (tag == null) {
        	tagKeysByName.putIfAbsent(name, new UserTagKey(name));
        	tag = tagKeysByName.get(name);
        }
        return tag;
	}
	
	public static List<UserTagKey> getUserTagKeys(List<String> names) {
		List<UserTagKey> tags = Lists.newArrayList();
		for (String name: names) {
			tags.add(get(name));
		}
		return tags;
	}
}
