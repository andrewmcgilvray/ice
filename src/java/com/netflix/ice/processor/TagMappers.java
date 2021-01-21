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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.TagMappings;

public class TagMappers {
    /**
     * Holds the list of time-ordered TagMappers for a user tag key.
     * Map key is the milliseconds start time for the list of TagMappers
     * .
     *  <pre>
     *    <startMillis1>:
     *        - <tagMapperA>
     *        - <tagMapperB>
     *    <startMillis2>:
     *        - <tagMapperC>
     *        ...
     *  </pre>
     */
	final private int tagIndex;
	final private String tagKey;
    final private Map<Long, List<TagMapper>> tagMappers;

    public TagMappers(int tagIndex, String tagKey, List<TagMappings> tagMappings, Map<String, Integer> tagResourceGroupIndeces) {
    	this.tagIndex = tagIndex;
    	this.tagKey = tagKey;
		tagMappers = Maps.newTreeMap();
		
		for (TagMappings m: tagMappings) {
			TagMapper mapper = new TagMapper(tagIndex, m, tagResourceGroupIndeces);
			List<TagMapper> l = tagMappers.get(mapper.getStartMillis());
			if (l == null) {
				l = Lists.newArrayList();
				tagMappers.put(mapper.getStartMillis(), l);
			}
			l.add(mapper);
		}
    	
    }
    
    public int getTagIndex() {
    	return tagIndex;
    }
    
    public String getTagKey() {
    	return tagKey;
    }
    
    public String getMappedUserTagValue(long startMillis, String accountId, String[] tags, String value) {
    	// return the user tag value for the specified account if there is a mapping configured.
    	
    	// Get the time-ordered values
    	Collection<List<TagMapper>> timeOrderedListsOfTagMappers = tagMappers.values();
    	
    	for (List<TagMapper> tml: timeOrderedListsOfTagMappers) {
    		for (TagMapper tm: tml)
    			value = tm.apply(startMillis, accountId, tags, value);
    	}	
    	
    	return value;
    }
 
}
