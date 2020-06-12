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
package com.netflix.ice.reader;

import java.util.List;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone;

public class TagListsWithUserTags extends TagLists {
    //private final Logger logger = LoggerFactory.getLogger(getClass());
	/*
	 * Broken-out lists of tags for resource groups
	 */
    public final List<List<UserTag>> resourceUserTagLists;

	public TagListsWithUserTags(List<Account> accounts, List<Region> regions,
			List<Zone> zones, List<Product> products,
			List<Operation> operations, List<UsageType> usageTypes,
			List<List<UserTag>> resourceUserTags) {
		super(accounts, regions, zones, products, operations, usageTypes,
				null);
    	this.resourceUserTagLists = resourceUserTags;
	}
    
    public TagLists copyWithOperations(List<Operation> operations) {
    	return new TagListsWithUserTags(accounts, regions, zones, products, operations, usageTypes, resourceUserTagLists);
    }

    /**
     * Compares the supplied TagGroup against the contents of the TagLists object
     * using the separate UserTag lists split out from the ResourceGroup.
     * If the TagLists doesn't have any values for a Tag, that Tag is passed over
     * and will be considered true. If the list for a Tag has values, then that list
     * is checked to see if it contains any values that match the one in the tagGroup.
     * if it does not, then return false.
     */
    @Override
    public boolean contains(TagGroup tagGroup, boolean useResource) {
        if (!super.contains(tagGroup))
        	return false;
        
        //logger.debug("           contains - resourceGroup: " + tagGroup.resourceGroup + ", " + resourceGroups + ", " + resourceUserTagLists);
        
        if (tagGroup.resourceGroup == null) {
        	if (useResource) {
        		// Check for empty tags in all the lists
    	        for (int i = 0; i < resourceUserTagLists.size(); i++) {
    	        	List<UserTag> resourceTags = resourceUserTagLists.get(i);
    	            if (resourceTags != null && resourceTags.size() > 0) {
    	            	if (!resourceUserTagLists.get(i).contains(UserTag.empty))
    	            		return false;
    	            }
    	        }        		
        	}        	
        }
        else {
	        UserTag[] userTags = tagGroup.resourceGroup.getUserTags();
	        
	        for (int i = 0; i < resourceUserTagLists.size(); i++) {
	        	List<UserTag> resourceTags = resourceUserTagLists.get(i);
	            if (resourceTags != null && resourceTags.size() > 0) {
	                if (!resourceTags.contains(userTags.length > i ? userTags[i] : UserTag.empty))
	                	return false;
	            }
	        }
        }
        return true;
    }
    
    @Override
    public boolean contains(TagGroup tagGroup) {
    	return contains(tagGroup, false);
    }

    
    /*
     * When groupBy is Tag, tag will be a UserTag.
     * userTagIndex will tell us which user tag we're grouping by.
     */
    @Override
    public boolean contains(Tag tag, TagType groupBy, int userTagGroupByIndex) {
    	if (groupBy == TagType.Tag) {
	        boolean result = true;
        	List<UserTag> resourceTags = resourceUserTagLists.get(userTagGroupByIndex);
            if (resourceTags != null && resourceTags.size() > 0) {
            	result = resourceTags.contains(tag.name.isEmpty() ? UserTag.empty : tag);
            }
	        
        	return result;
    	}
    	return super.contains(tag, groupBy, userTagGroupByIndex);
    }

    @Override
    public TagLists getTagLists(Tag tag, TagType groupBy, int userTagGroupByIndex) {
        TagLists result = null;

        switch (groupBy) {
            case Account:
                result = new TagListsWithUserTags(Lists.newArrayList((Account)tag), this.regions, this.zones, this.products, this.operations, this.usageTypes, this.resourceUserTagLists);
                break;
            case Region:
                result = new TagListsWithUserTags(this.accounts, Lists.newArrayList((Region)tag), this.zones, this.products, this.operations, this.usageTypes, this.resourceUserTagLists);
                break;
            case Zone:
                result = new TagListsWithUserTags(this.accounts, this.regions, Lists.newArrayList((Zone)tag), this.products, this.operations, this.usageTypes, this.resourceUserTagLists);
                break;
            case Product:
                result = new TagListsWithUserTags(this.accounts, this.regions, this.zones, Lists.newArrayList((Product)tag), this.operations, this.usageTypes, this.resourceUserTagLists);
                break;
            case Operation:
                result = new TagListsWithUserTags(this.accounts, this.regions, this.zones, this.products, Lists.newArrayList((Operation)tag), this.usageTypes, this.resourceUserTagLists);
                break;
            case UsageType:
                result = new TagListsWithUserTags(this.accounts, this.regions, this.zones, this.products, this.operations, Lists.newArrayList((UsageType)tag), this.resourceUserTagLists);
                break;
            case Tag:
    	        List<List<UserTag>> userTagLists = Lists.newArrayList();
    	        UserTag userTag = (UserTag) tag;
        		for (int i = 0; i < resourceUserTagLists.size(); i++) {
        			if (i == userTagGroupByIndex)
        				userTagLists.add(Lists.newArrayList(userTag));
        			else
        				userTagLists.add(resourceUserTagLists.get(i));
        		}
        		result = new TagListsWithUserTags(this.accounts, this.regions, this.zones, this.products, this.operations, this.usageTypes, userTagLists);
        		break;
            default:
            	result = null;
            	break;
        }
        return result;
    }
    
    @Override
    public TagLists getTagListsWithNullResourceGroup() {
    	List<List<UserTag>> newResourceTagLists = Lists.newArrayList();
    	for (int i = 0; i < resourceUserTagLists.size(); i++)
    		newResourceTagLists.add(null);
    	return new TagListsWithUserTags(accounts, regions, zones, products, operations, usageTypes, newResourceTagLists);
    }
    
    @Override
    public TagLists getTagListsWithOperations(List<Operation> operations) {
    	return new TagListsWithUserTags(this.accounts, this.regions, this.zones, this.products, operations, this.usageTypes, this.resourceUserTagLists);
    }
    
    @Override
    public String toString() {
    	return super.toString() + "," + (resourceUserTagLists == null ? "null" : resourceUserTagLists.toString());
    }

}
