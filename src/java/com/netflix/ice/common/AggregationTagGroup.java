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

import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public class AggregationTagGroup {
	public List<Tag> tags;
	public List<TagType> types;
	private final int hashcode;
	
	protected AggregationTagGroup(List<Tag> tags, List<TagType> types) {
		this.tags = tags;
		this.types = types;
		this.hashcode = genHashCode();
	}
	
	public Account getAccount() {
		int index = types.indexOf(TagType.Account);
		return index < 0 ? null : (Account) tags.get(index); 
	}
	
	public Region getRegion() {
		int index = types.indexOf(TagType.Region);
		return index < 0 ? null : (Region) tags.get(index); 
	}
	
	public Zone getZone() {
		int index = types.indexOf(TagType.Zone);
		return index < 0 ? null : (Zone) tags.get(index); 
	}
	
	public Product getProduct() {
		int index = types.indexOf(TagType.Product);
		return index < 0 ? null : (Product) tags.get(index); 
	}
	
	public Operation getOperation() {
		int index = types.indexOf(TagType.Operation);
		return index < 0 ? null : (Operation) tags.get(index); 
	}
	
	public UsageType getUsageType() {
		int index = types.indexOf(TagType.UsageType);
		return index < 0 ? null : (UsageType) tags.get(index); 
	}
	
	public ResourceGroup getResourceGroup() {
		int index = types.indexOf(TagType.ResourceGroup);
		return index < 0 ? null : (ResourceGroup) tags.get(index); 
	}
	
    @Override
    public String toString() {
        return tags.toString();
    }

    public int compareTo(AggregationTagGroup atg) {
    	for (int i = 0; i < tags.size(); i++) {
    		int result = tags.get(i).compareTo(atg.tags.get(i));
    		if (result != 0)
    			return result;
    	}
    	return 0;
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o)
    		return true;
        if (o == null)
            return false;
        AggregationTagGroup other = (AggregationTagGroup)o;
        
        
    	for (int i = 0; i < tags.size(); i++) {
    		if (tags.get(i) != other.tags.get(i))
    			return false;
    	}
    	return true;
    }

    @Override
    public int hashCode() {
    	return hashcode;
    }
    
    private int genHashCode() {
        final int prime = 31;
        int result = 1;
        
    	for (int i = 0; i < tags.size(); i++) {
    		if (tags.get(i) != null)
    			result = prime * result + tags.get(i).hashCode();
    	}
        
        return result;
    }

}
