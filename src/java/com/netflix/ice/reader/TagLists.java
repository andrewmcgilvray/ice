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

import com.google.common.collect.Lists;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.*;

import java.util.List;

/**
 * Holds a List of Values for each of the Tags in a TagGroup
 */
public class TagLists {
    public final List<Account> accounts;
    public final List<Region> regions;
    public final List<Zone> zones;
    public final List<Product> products;
    public final List<Operation> operations;
    public final List<UsageType> usageTypes;
    public final List<ResourceGroup> resourceGroups;

    public TagLists() {
        this.accounts = null;
        this.regions = null;
        this.zones = null;
        this.products = null;
        this.operations = null;
        this.usageTypes = null;
        this.resourceGroups = null;
    }

    public TagLists(List<Account> accounts) {
        this.accounts = accounts;
        this.regions = null;
        this.zones = null;
        this.products = null;
        this.operations = null;
        this.usageTypes = null;
        this.resourceGroups = null;
    }

    public TagLists(List<Account> accounts, List<Region> regions) {
        this.accounts = accounts;
        this.regions = regions;
        this.zones = null;
        this.products = null;
        this.operations = null;
        this.usageTypes = null;
        this.resourceGroups = null;
    }

    public TagLists(List<Account> accounts, List<Region> regions, List<Zone> zones) {
        this.accounts = accounts;
        this.regions = regions;
        this.zones = zones;
        this.products = null;
        this.operations = null;
        this.usageTypes = null;
        this.resourceGroups = null;
    }

    public TagLists(List<Account> accounts, List<Region> regions, List<Zone> zones, List<Product> products) {
        this.accounts = accounts;
        this.regions = regions;
        this.zones = zones;
        this.products = products;
        this.operations = null;
        this.usageTypes = null;
        this.resourceGroups = null;
    }

    public TagLists(List<Account> accounts, List<Region> regions, List<Zone> zones, List<Product> products, List<Operation> operations) {
        this.accounts = accounts;
        this.regions = regions;
        this.zones = zones;
        this.products = products;
        this.operations = operations;
        this.usageTypes = null;
        this.resourceGroups = null;
    }

    public TagLists(List<Account> accounts, List<Region> regions, List<Zone> zones, List<Product> products, List<Operation> operations, List<UsageType> usageTypes) {
        this.accounts = accounts;
        this.regions = regions;
        this.zones = zones;
        this.products = products;
        this.operations = operations;
        this.usageTypes = usageTypes;
        this.resourceGroups = null;
    }

    public TagLists(List<Account> accounts, List<Region> regions, List<Zone> zones, List<Product> products, List<Operation> operations, List<UsageType> usageTypes, List<ResourceGroup> resourceGroups) {
        this.accounts = accounts;
        this.regions = regions;
        this.zones = zones;
        this.products= products;
        this.operations = operations;
        this.usageTypes = usageTypes;
        this.resourceGroups = resourceGroups;
    }
    
    public TagLists copyWithOperations(List<Operation> operations) {
    	return new TagLists(accounts, regions, zones, products, operations, usageTypes, resourceGroups);
    }

    /**
     * Compares the supplied tagGroup against the contents of the TagLists object.
     * If the TagLists doesn't have any values for a Tag, that Tag is passed over
     * and will be considered true. If the list for a Tag has values, then that list
     * is checked to see if it contains any values that match the one in the tagGroup.
     * if it does not, then return false.
     */
    public boolean contains(TagGroup tagGroup) {
        boolean result = true;

        if (result && accounts != null && accounts.size() > 0) {
            result = accounts.contains(tagGroup.account);
        }
        if (result && regions != null && regions.size() > 0) {
            result = regions.contains(tagGroup.region);
        }
        if (result && zones != null && zones.size() > 0) {
            result = zones.contains(tagGroup.zone);
        }
        if (result && products != null && products.size() > 0) {
            result = products.contains(tagGroup.product);
        }
        if (result && operations != null && operations.size() > 0) {
            result = operations.contains(tagGroup.operation);
        }
        if (result && usageTypes != null && usageTypes.size() > 0) {
            result = usageTypes.contains(tagGroup.usageType);
        }
        if (result && resourceGroups != null && resourceGroups.size() > 0) {
            result = resourceGroups.contains(tagGroup.resourceGroup);
        }
        return result;
    }
    
    public boolean contains(TagGroup tagGroup, boolean useResource) {
    	return contains(tagGroup);
    }

    public boolean contains(Tag tag, TagType groupBy, int userTagGroupByIndex) {
        boolean result = true;

        switch (groupBy) {
            case Account:
                result = accounts == null || accounts.size() == 0 || accounts.contains(tag);
                break;
            case Region:
                result = regions == null || regions.size() == 0 || regions.contains(tag);
                break;
            case Zone:
                result = zones == null || zones.size() == 0 || zones.contains(tag);
                break;
            case Product:
                result = products == null || products.size() == 0 || products.contains(tag);
                break;
            case Operation:
                result = operations == null || operations.size() == 0 || operations.contains(tag);
                break;
            case UsageType:
                result = usageTypes == null || usageTypes.size() == 0 || usageTypes.contains(tag);
                break;
            default:
            	result = false;
            	break;
        }
        return result;
    }

    public TagLists getTagLists(Tag tag, TagType groupBy, int userTagGroupByIndex) {
        TagLists result = null;

        switch (groupBy) {
            case Account:
                result = new TagLists(Lists.newArrayList((Account)tag), this.regions, this.zones, this.products, this.operations, this.usageTypes, this.resourceGroups);
                break;
            case Region:
                result = new TagLists(this.accounts, Lists.newArrayList((Region)tag), this.zones, this.products, this.operations, this.usageTypes, this.resourceGroups);
                break;
            case Zone:
                result = new TagLists(this.accounts, this.regions, Lists.newArrayList((Zone)tag), this.products, this.operations, this.usageTypes, this.resourceGroups);
                break;
            case Product:
                result = new TagLists(this.accounts, this.regions, this.zones, Lists.newArrayList((Product)tag), this.operations, this.usageTypes, this.resourceGroups);
                break;
            case Operation:
                result = new TagLists(this.accounts, this.regions, this.zones, this.products, Lists.newArrayList((Operation)tag), this.usageTypes, this.resourceGroups);
                break;
            case UsageType:
                result = new TagLists(this.accounts, this.regions, this.zones, this.products, this.operations, Lists.newArrayList((UsageType)tag), this.resourceGroups);
                break;
            default:
            	result = null;
            	break;
        }
        return result;
    }
    
    public TagLists getTagListsWithNullResourceGroup() {
    	return new TagLists(this.accounts, this.regions, this.zones, this.products, this.operations, this.usageTypes, null);
    }
    
    public TagLists getTagListsWithProducts(List<Product> products) {
    	return new TagLists(this.accounts, this.regions, this.zones, products, this.operations, this.usageTypes, this.resourceGroups);
    }
    
    public TagLists getTagListsWithOperations(List<Operation> operations) {
    	return new TagLists(this.accounts, this.regions, this.zones, this.products, operations, this.usageTypes, this.resourceGroups);
    }
    
    public String toString() {
    	return  (accounts == null ? "null" : accounts.toString()) + "," +
    			(regions == null ? "null" : regions.toString()) + "," +
    			(zones == null ? "null" : zones.toString()) + "," +
        		(products == null ? "null" : products.toString()) + "," +
        		(operations == null ? "null" : operations.toString()) + "," +
        		(usageTypes == null ? "null" : usageTypes.toString()) + "," +
        		(resourceGroups == null ? "null" : resourceGroups.toString());
    }
}
