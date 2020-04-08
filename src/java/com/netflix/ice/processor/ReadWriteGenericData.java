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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Zone.BadZone;

public abstract class ReadWriteGenericData<T> implements ReadWriteDataSerializer {
    protected List<Map<TagGroup, T>> data;
    // Cached set of tagGroup keys used throughout the list of data maps.
    // Post processing for reservations, savings plans, savings data, post processor, and data writing
    // all need an aggregated set of tagGroups and it's very expensive to walk the maps calling addAll().
    protected Set<TagGroup> tagGroups;

	public ReadWriteGenericData() {
        data = Lists.newArrayList();
        tagGroups = Sets.newHashSet();
    }

    public int getNum() {
        return data.size();
    }

    void cutData(int num) {
        if (data.size() > num)
            data = data.subList(0, num);
    }

    public Map<TagGroup, T> getData(int i) {
        return Collections.unmodifiableMap(getCreateData(i));
    }
    
    public T get(int i, TagGroup tagGroup) {
    	return getData(i).get(tagGroup);
    }

    public void put(int i, TagGroup tagGroup, T value) {
    	getCreateData(i).put(tagGroup, value);
    	tagGroups.add(tagGroup);
    }

    public T remove(int i, TagGroup tagGroup) {
    	if (i >= data.size())
    		return null;
    	T existing = data.get(i).remove(tagGroup);
    	if (existing != null) {
    		// See if we can purge the value from the cache
    		boolean found = false;
    		// Most remove calls are done by RI and SP processors which
    		// run through the data lists from 0 to the end.
    		// Assuming most values are present throughout the list, we
    		// can save time by checking from back to front since we'll
    		// bail quickly if we find the tagGroup at the end.
    		for (int j = data.size() - 1; j >= 0; j--) {
    			Map<TagGroup, T> d = data.get(j);
    			if (d.containsKey(tagGroup)) {
    				found = true;
    				break;
    			}
    		}
    		if (!found)
    			tagGroups.remove(tagGroup);
    	}
    	
    	return existing;
    }

    /**
     * Set the supplied data in the map. Called by the cost and usage data archiver to merge summary data.
     */
    void setData(List<Map<TagGroup, T>> newData, int startIndex) {
        for (int i = 0; i < newData.size(); i++) {
            int index = startIndex + i;

            if (index > data.size()) {
                getCreateData(index-1);
            }
            if (index >= data.size()) {
                data.add(newData.get(i));
            }
            else {
            	Map<TagGroup, T> removed = data.set(index, newData.get(i));
            	if (removed != null) {
	            	for (TagGroup tg: removed.keySet()) {
	            		// See if we can purge the value from the cache
	            		boolean found = false;
	            		for (int j = 0; j < data.size(); j++) {
	            			Map<TagGroup, T> d = data.get(j);
	            			if (d.containsKey(tg)) {
	            				found = true;
	            				break;
	            			}
	            		}
	            		if (!found)
	            			tagGroups.remove(tg);
	            	}
            	}
            }
            tagGroups.addAll(newData.get(i).keySet());
        }
    }
    
    abstract protected T add(T a, T b);
    
    /**
     * Merge all the data from the source into the existing destination.
     */
    void putAll(ReadWriteGenericData<T> srcData) {
    	List<Map<TagGroup, T>> newData = srcData.data;
        for (int i = 0; i < newData.size(); i++) {
            if (i > data.size()) {
                getCreateData(i-1);
            }
            if (i >= data.size()) {
                data.add(newData.get(i));
            }
            else {
                Map<TagGroup, T> existed = data.get(i);
                for (TagGroup tg: newData.get(i).keySet()) {
                	T existingValue = existed.get(tg);
                	T value = newData.get(i).get(tg);
                    existed.put(tg, existingValue == null ? value : add(existingValue, value));
                }
            }
        }
        tagGroups.addAll(srcData.tagGroups);
    }

    Map<TagGroup, T> getCreateData(int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, T>newHashMap());
            }
        }
        return data.get(i);
    }

    /**
     * Gets the aggregated set of TagGroups across all time intervals in the list of maps.
     * 
     * @return a set of TagGroups
     */
    public Collection<TagGroup> getTagGroups() {
        return tagGroups;
    }
    
    /**
     * Gets the tagGroup key set for the given hour
     */
    public Collection<TagGroup> getTagGroups(int i) {
    	return getData(i).keySet();
    }
    
    /**
     * Serialize data using standard Java serialization DataOutput methods in the following order:<br/>
     * 
     * 1. TagGroup count (int)<br/>
     * 2. TagGroup Array<br/>
     * 3. Number of hours/days/weeks/months of data (int)<br/>
     * 4. Data matrix:<br/>
     * 		4a. Data present for TagGroup flag (boolean)<br/>
     * 		4b. Data array for TagGroup (if flag is true)<br/>
     */
    public void serialize(DataOutput out, TagGroupFilter filter) throws IOException {
        Collection<TagGroup> keys = getTagGroups();
        if (filter != null)
        	keys = filter.getTagGroups(keys);
        
        out.writeInt(keys.size());
        for (TagGroup tagGroup: keys) {
            TagGroup.Serializer.serialize(out, tagGroup);
        }

        out.writeInt(data.size());
        for (int i = 0; i < data.size(); i++) {
            Map<TagGroup, T> map = getData(i);
            out.writeBoolean(map.size() > 0);
            if (map.size() > 0) {
                for (TagGroup tagGroup: keys) {
                    writeValue(out, map.get(tagGroup));
                }
            }
        }
    }

    abstract protected void writeValue(DataOutput out, T value) throws IOException;

    public void deserialize(AccountService accountService, ProductService productService, DataInput in) throws IOException, BadZone {

        int numKeys = in.readInt();
        List<TagGroup> keys = Lists.newArrayList();
        for (int j = 0; j < numKeys; j++) {
        	TagGroup tg = TagGroup.Serializer.deserialize(accountService, productService, in);
            keys.add(tg);
        	tagGroups.add(tg);
        }

        List<Map<TagGroup, T>> data = Lists.newArrayList();
        int num = in.readInt();
        for (int i = 0; i < num; i++)  {
            Map<TagGroup, T> map = Maps.newHashMap();
            boolean hasData = in.readBoolean();
            if (hasData) {
                for (int j = 0; j < keys.size(); j++) {
                    T v = readValue(in);
                    if (v != null) {
                        map.put(keys.get(j), v);
                    }
                }
            }
            data.add(map);
        }
        
        this.data = data;
    }
        
    abstract protected T readValue(DataInput in) throws IOException;        
}
