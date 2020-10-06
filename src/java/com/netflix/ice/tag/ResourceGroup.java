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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceGroup  implements Comparable<ResourceGroup>, Serializable {
	private static final long serialVersionUID = 1L;
	protected static Logger logger = LoggerFactory.getLogger(Tag.class);
	
	private final UserTag[] resourceTags;
    private final int hashcode;
	
    private static ConcurrentMap<ResourceGroup, ResourceGroup> resourceGroups = Maps.newConcurrentMap();

	public static class ResourceException extends Exception {
		private static final long serialVersionUID = 1L;

		ResourceException(String msg) {
			super(msg);
		}
	}


	protected ResourceGroup(UserTag[] tags) throws ResourceException {
		// Make sure there aren't any nulls and that we have at least one tag
		if (tags.length == 0)
			throw new ResourceException("Empty UserTag array");
		for (int i = 0; i < tags.length; i++)
			if (tags[i] == null)
				throw new ResourceException("UserTag array contains one or more null tags");
		resourceTags = tags;
		hashcode = genHashCode(resourceTags);
	}
	
    @Override
    public int hashCode() {
        return hashcode;
    }

    private int genHashCode(UserTag[] tags) {
        final int prime = 31;
        int result = 1;
        
        for (UserTag tag: tags) {
        	result = prime * result + tag.hashCode();
        }
        return result;
    }
    
    @Override
    public String toString() {
    	List<String> tags = Lists.newArrayListWithCapacity(resourceTags.length);
    	for (int i = 0; i < resourceTags.length; i++) {
    		String v = resourceTags[i].name;
    		if (v.contains(",")) {
    			// Quote the string
    			v = "\"" + v.replace("\"", "\\\"") + "\"";
    		}
    		tags.add(v);
    	}
        return String.join(",", tags);
    }
    
	public UserTag[] getUserTags() {
		return resourceTags;
	}
	
    public static ResourceGroup getResourceGroup(String[] tags) throws ResourceException {
    	if (tags == null)
    		return null;
    	
    	UserTag[] userTags = new UserTag[tags.length];
    	for (int i = 0; i < tags.length; i++)
    		userTags[i] = UserTag.get(tags[i]);
    	return getResourceGroup(userTags);
    }
    
    /**
     * Get a resource group that is not cached in the global ResourceGroup cache. This should
     * only be used for statistics processing.
     * 
     * @param tags
     * @return
     * @throws ResourceException
     */
    public static ResourceGroup getUncached(UserTag[] tags) throws ResourceException {
    	return new ResourceGroup(tags);
    }
    
    public static ResourceGroup getResourceGroup(UserTag[] tags) throws ResourceException {
    	if (tags == null || tags.length == 0)
    		return null;
    	
    	ResourceGroup rgNew = new ResourceGroup(tags);
    	ResourceGroup rgExisting = resourceGroups.get(rgNew);
    	if (rgExisting == null) {
    		resourceGroups.putIfAbsent(rgNew, rgNew);
    		rgExisting = resourceGroups.get(rgNew);
    	}
    	return rgExisting;
    }

    public static ResourceGroup getResourceGroup(List<UserTag> tags) throws ResourceException {
    	UserTag[] utArray = new UserTag[tags.size()];
    	tags.toArray(utArray);
    	return getResourceGroup(utArray);
    }

    public static List<ResourceGroup> getResourceGroups(List<String> names) {
        List<ResourceGroup> result = Lists.newArrayList();
        if (names != null) {
            for (String name: names) {
                ResourceGroup resourceGroup = resourceGroups.get(name);
                if (resourceGroup != null)
                    result.add(resourceGroup);
            }
        }
        return result;
    }

	@Override
	public int compareTo(ResourceGroup o) {
    	if (this == o)
    		return 0;
    	for (int i = 0; i < resourceTags.length; i++) {
            int result = resourceTags[i].compareTo(o.resourceTags[i]);
            if (result != 0)
            	return result;
    	}
		return 0;
	}
	
    @Override
    public boolean equals(Object o) {
    	if (this == o)
    		return true;
        if (o == null || !(o instanceof ResourceGroup))
            return false;
        
        ResourceGroup other = (ResourceGroup) o;
        
    	for (int i = 0; i < resourceTags.length; i++) {
    		if (resourceTags[i] != other.resourceTags[i])
    			return false;
    	}
    	return true;
    }
    
    public static void serialize(DataOutput out, ResourceGroup resourceGroup) throws IOException {
    	out.writeBoolean(resourceGroup != null);
    	if (resourceGroup == null)
    		return;
    	
    	// Write a bitmap to indicate which values are present
    	int bits = 0;
    	UserTag[] ut = resourceGroup.getUserTags();
    	for (int i = 0; i < ut.length; i++) {
    		if (ut[i] != null)
    			bits |= 1 << i;
    	}
    	out.writeInt(bits);
    	for (int i = 0; i < ut.length; i++) {
    		if (ut[i] != null)
    			out.writeUTF(ut[i].name);
    	}
    }

    public static void serializeCsv(Writer out, ResourceGroup resourceGroup) throws IOException {
    	if (resourceGroup == null)
    		return;
        out.write(resourceGroup.toString());
    }

    public static ResourceGroup deserialize(DataInput in, int numUserTags) throws IOException {
    	boolean present = in.readBoolean();
    	if (!present)
    		return null;
    	
        int bits = in.readInt();
        UserTag[] ut = new UserTag[numUserTags];
        for (int i = 0; i < numUserTags; i++) {
        	ut[i] = UserTag.get((bits & (1 << i)) == 0 ? "" : in.readUTF());
        }
        
        try {
			// We never use null entries, so should never throw
			return getResourceGroup(ut);
		} catch (ResourceException e) {
		}
        return null;
    }
}
