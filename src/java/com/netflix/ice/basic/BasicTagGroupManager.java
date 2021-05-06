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
package com.netflix.ice.basic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.TagGroupWriter;
import com.netflix.ice.reader.DataCache;
import com.netflix.ice.reader.TagGroupManager;
import com.netflix.ice.reader.TagLists;
import com.netflix.ice.tag.*;
import com.netflix.ice.tag.Zone.BadZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class BasicTagGroupManager implements TagGroupManager, DataCache {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    public static final String compressExtension = ".gz";

    private final WorkBucketConfig workBucketConfig;
    private final AccountService accountService;
    private final ProductService productService;
    private final int numUserTags;
    private final String dbName;
    private final File file;
    private TreeMap<Long, Collection<TagGroup>> tagGroups;
    private TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups;
    private Interval totalInterval;
    private boolean compress;

    BasicTagGroupManager(Product product, boolean compress, WorkBucketConfig workBucketConfig, AccountService accountService, ProductService productService, int numUserTags) {
    	this.compress = compress;
    	this.workBucketConfig = workBucketConfig;
    	this.accountService = accountService;
    	this.productService = productService;
    	this.numUserTags = numUserTags;
        this.dbName = TagGroupWriter.DB_PREFIX + (product == null ? "all" : product.getServiceCode());
        file = new File(workBucketConfig.localDir, dbName + (compress ? compressExtension : ""));
        
        refresh();
    }
    
    // For unit testing
    BasicTagGroupManager(TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups, Interval totalInterval, int numUserTags) {
    	this.tagGroupsWithResourceGroups = tagGroupsWithResourceGroups;
    	this.tagGroups = removeResourceGroups(tagGroupsWithResourceGroups);
    	this.workBucketConfig = null;
    	this.accountService = null;
    	this.productService = null;
    	this.numUserTags = numUserTags;
    	this.dbName = null;
    	this.file = null;
    	this.totalInterval = totalInterval;
    }
 
    public Collection<TagGroup> getTagGroups() {
    	Set<TagGroup> uniqueTagGroups = Sets.newHashSet();
    	for (Collection<TagGroup> tgs: tagGroups.values()) {
    		uniqueTagGroups.addAll(tgs);
    	}
    	return uniqueTagGroups;
    }
    
    public Collection<TagGroup> getTagGroupsWithResourceGroups() {
    	Set<TagGroup> uniqueTagGroups = Sets.newHashSet();
    	if (tagGroupsWithResourceGroups == null)
    		return uniqueTagGroups;
    	for (Collection<TagGroup> tgs: tagGroupsWithResourceGroups.values()) {
    		uniqueTagGroups.addAll(tgs);
    	}
    	return uniqueTagGroups;
    }
    
    public TreeMap<Long, Integer> getSizes() {
    	TreeMap<Long, Integer> sizes = Maps.newTreeMap();
    	if (tagGroupsWithResourceGroups == null)
    		return sizes;
    	
    	for (Long millis: tagGroupsWithResourceGroups.keySet())
    		sizes.put(millis, tagGroupsWithResourceGroups.get(millis).size());
    	
    	return sizes;
    }
    
    public TreeMap<Long, List<Integer>> getTagValueSizes(int numUserTags) {
    	TreeMap<Long, List<Integer>> sizes = Maps.newTreeMap();
    	if (tagGroupsWithResourceGroups == null)
    		return sizes;
    	
    	for (Long millis: tagGroupsWithResourceGroups.keySet()) {
    		Collection<TagGroup> tagGroups = tagGroupsWithResourceGroups.get(millis);
    		
    		Set<String> accountValues = Sets.newHashSet();
    		Set<String> regionValues = Sets.newHashSet();
    		Set<String> zoneValues = Sets.newHashSet();
    		Set<String> productValues = Sets.newHashSet();
    		Set<String> operationValues = Sets.newHashSet();
    		Set<String> usageTypeValues = Sets.newHashSet();
    		List<Set<String>> userTagValues = Lists.newArrayList();
    		for (int i = 0; i < numUserTags; i++)
    			userTagValues.add(Sets.<String>newHashSet());
    			
    		for (TagGroup tg: tagGroups) {
    			accountValues.add(tg.account.getIceName());
    			regionValues.add(tg.region.name);
    			if (tg.zone != null)
    				zoneValues.add(tg.zone.name);
    			productValues.add(tg.product.getIceName());
    			operationValues.add(tg.operation.name);
    			usageTypeValues.add(tg.usageType.name);
        		if (numUserTags > 0 && tg.resourceGroup != null) {
		    		UserTag[] userTags = tg.resourceGroup.getUserTags();
		    		for (int j = 0; j < numUserTags; j++) {
		    			if (!userTags[j].name.isEmpty())
		    				userTagValues.get(j).add(userTags[j].name);
		    		}
        		}
    		}
    		List<Integer> counts = Lists.newArrayList();
    		counts.add(accountValues.size());
    		counts.add(regionValues.size());
    		counts.add(zoneValues.size());
    		counts.add(productValues.size());
    		counts.add(operationValues.size());
    		counts.add(usageTypeValues.size());
    		for (int i = 0; i < numUserTags; i++)
    			counts.add(userTagValues.get(i).size());
    		sizes.put(millis, counts);    		
    	}    	
    	return sizes;
    }
    
    @Override
    public boolean refresh() {
        boolean downloaded = AwsUtils.downloadFileIfChanged(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, file);
        if (downloaded || tagGroups == null) {
	        logger.info("trying to read from " + file);
	        InputStream is = null;
	        DataInputStream in = null;
	        try {
	            is = new FileInputStream(file);
	            if (compress)
	            	is = new GZIPInputStream(is);
	            in = new DataInputStream(is);
	            
                TreeMap<Long, Collection<TagGroup>> tagGroupsWithResourceGroups = TagGroup.Serializer.deserializeTagGroups(accountService, productService, numUserTags, in);
                TreeMap<Long, Collection<TagGroup>> tagGroups = removeResourceGroups(tagGroupsWithResourceGroups);
                Interval totalInterval = null;
                if (tagGroups.size() > 0) {
                    totalInterval = new Interval(tagGroups.firstKey(), new DateTime(tagGroups.lastKey()).plusMonths(1).getMillis(), DateTimeZone.UTC);
                }
                this.totalInterval = totalInterval;
                this.tagGroups = tagGroups;
                this.tagGroupsWithResourceGroups = tagGroupsWithResourceGroups;
                logger.info("done reading " + file);
            }
            catch (IOException e) {
                logger.error("failed to download " + file, e);
            }
	        catch (BadZone e) {
                logger.error("failed to download " + file, e);
	        }
            finally {
            	if (in != null) {
	                try { in.close(); } catch (Exception e) {};
            	}
            	else if (is != null) {
	                try { is.close(); } catch (Exception e) {};
            	}
	        }
        }
        return false;
    }

    private TreeMap<Long, Collection<TagGroup>> removeResourceGroups(TreeMap<Long, Collection<TagGroup>> tagGroups) {
        TreeMap<Long, Collection<TagGroup>> result = Maps.newTreeMap();
        for (Long key: tagGroups.keySet()) {
            Collection<TagGroup> from = tagGroups.get(key);
            Set<TagGroup> to = Sets.newHashSet();
            for (TagGroup tagGroup: from) {
                if (tagGroup.resourceGroup != null)
                    to.add(TagGroup.getTagGroup(tagGroup.costType, tagGroup.account, tagGroup.region, tagGroup.zone, tagGroup.product, tagGroup.operation, tagGroup.usageType, null));
                else
                    to.add(tagGroup);
            }

            result.put(key, to);
        }
        return result;
    }

    private Set<TagGroup> getTagGroupsInRange(Collection<Long> monthMillis) {
        Set<TagGroup> tagGroupsInRange = Sets.newHashSet();
        for (Long monthMilli: monthMillis) {
            tagGroupsInRange.addAll(this.tagGroups.get(monthMilli));
        }
        return tagGroupsInRange;
    }

    private Set<TagGroup> getTagGroupsWithResourceGroupsInRange(Collection<Long> monthMillis) {
        Set<TagGroup> tagGroupsInRange = Sets.newHashSet();
    	if (tagGroupsWithResourceGroups == null)
    		return tagGroupsInRange;
    	
    	for (Long monthMilli: monthMillis) {
            tagGroupsInRange.addAll(this.tagGroupsWithResourceGroups.get(monthMilli));
        }
        return tagGroupsInRange;
    }

    private Collection<Long> getMonthMillis(Interval interval) {
        Set<Long> result = Sets.newTreeSet();
        for (Long milli: tagGroups.keySet()) {
            DateTime monthDate = new DateTime(milli, DateTimeZone.UTC);
            if (new Interval(monthDate, monthDate.plusMonths(1)).overlap(interval) != null)
                result.add(milli);
        }

        return result;
    }
    
    public Collection<CostType> getCostTypes(Set<TagGroup> tagGroupsInRange, TagLists tagLists) {
    	Set<CostType> costTypes = Sets.newHashSet();
    	
    	for (TagGroup tagGroup: tagGroupsInRange) {
    		if (tagLists.contains(tagGroup))
    			costTypes.add(tagGroup.costType);
    	}
    	
    	return costTypes;
    }

    public Collection<Account> getAccounts(Set<TagGroup> tagGroupsInRange, TagLists tagLists) {
        Set<Account> accounts = Sets.newHashSet();

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
            	accounts.add(tagGroup.account);
        }

        return accounts;
    }

    public Collection<Region> getRegions(Set<TagGroup> tagGroupsInRange, TagLists tagLists) {
        Set<Region> regions = Sets.newHashSet();

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
            	regions.add(tagGroup.region);
        }

        return regions;
    }

    public Collection<Zone> getZones(Set<TagGroup> tagGroupsInRange, TagLists tagLists) {
        Set<Zone> zones = Sets.newHashSet();

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup) && tagGroup.zone != null)
                zones.add(tagGroup.zone);
        }

        return zones;
    }

    public Collection<Product> getProducts(Set<TagGroup> tagGroupsInRange, TagLists tagLists) {
        Set<Product> products = Sets.newHashSet();

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
                products.add(tagGroup.product);
        }

        return products;
    }
    
    public Set<Operation> getOperations(Set<TagGroup> tagGroupsInRange, TagLists tagLists, Collection<Operation.Identity.Value> exclude) {
        Set<Operation> operations = Sets.newHashSet();
        int excludeBitSet = exclude == null ? 0 : Operation.Identity.getIdentitySet(exclude);
        
        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup)) {
            	if (excludeBitSet == 0 || !tagGroup.operation.isOneOf(excludeBitSet))
            		operations.add(tagGroup.operation);
            }
        }

        return operations;
    }

    public Collection<UsageType> getUsageTypes(Set<TagGroup> tagGroupsInRange, TagLists tagLists) {
        Set<UsageType> usageTypes = Sets.newHashSet();

        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup))
                usageTypes.add(tagGroup.usageType);
        }

        return usageTypes;
    }

    public Collection<ResourceGroup> getResourceGroups(Interval interval, TagLists tagLists) {
        Set<ResourceGroup> groups = Sets.newHashSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsWithResourceGroupsInRange(getMonthMillis(interval));

        // Add ResourceGroup tags that are non-nulls.
        for (TagGroup tagGroup: tagGroupsInRange) {
            if (tagLists.contains(tagGroup) && tagGroup.resourceGroup != null) {
                groups.add(tagGroup.resourceGroup);
            }
        }

        return groups;
    }

    public Collection<UserTag> getResourceGroupTags(Interval interval, TagLists tagLists, int userTagGroupByIndex) {
        Set<UserTag> userTags = Sets.newHashSet();
        Set<TagGroup> tagGroupsInRange = getTagGroupsWithResourceGroupsInRange(getMonthMillis(interval));
        
        // Add ResourceGroup tags that are null.
        for (TagGroup tagGroup: tagGroupsInRange) {
        	//logger.info("tag group <" + tagLists.contains(tagGroup) + ">: " + tagGroup);
            if (tagLists.contains(tagGroup)) {
            	try {
            		UserTag t = tagGroup.resourceGroup == null ? UserTag.empty : tagGroup.resourceGroup.getUserTags()[userTagGroupByIndex];
            		userTags.add(t);
            	}
            	catch (Exception e) {
            		logger.error("Bad resourceGroup: " + tagGroup.resourceGroup + ", " + e);
            	}
            }
        }

        return userTags;
    }

    public Collection<CostType> getCostTypes(TagLists tagLists) {
    	List<CostType> costTypes = Lists.newArrayList(getCostTypes(getTagGroupsInRange(getMonthMillis(totalInterval)), tagLists));
    	costTypes.sort(null);
        return costTypes;
    }

    public Collection<Account> getAccounts(TagLists tagLists) {
    	List<Account> accounts = Lists.newArrayList(getAccounts(getTagGroupsInRange(getMonthMillis(totalInterval)), tagLists));
    	accounts.sort(null);
        return accounts;
    }

    public Collection<Region> getRegions(TagLists tagLists) {
    	List<Region> regions = Lists.newArrayList(getRegions(getTagGroupsInRange(getMonthMillis(totalInterval)), tagLists));
    	regions.sort(null);
        return regions;
    }

    public Collection<Zone> getZones(TagLists tagLists) {
    	List<Zone> zones = Lists.newArrayList(getZones(getTagGroupsInRange(getMonthMillis(totalInterval)), tagLists));
    	zones.sort(null);
        return zones;
    }

    public Collection<Product> getProducts(TagLists tagLists) {
    	List<Product> products = Lists.newArrayList(getProducts(getTagGroupsInRange(getMonthMillis(totalInterval)), tagLists));
    	products.sort(null);
    	return products;
    }

    public Collection<Operation> getOperationsUnsorted(TagLists tagLists, Collection<Operation.Identity.Value> exclude) {
    	return getOperations(getTagGroupsInRange(getMonthMillis(totalInterval)), tagLists, exclude);
    }

    public Collection<Operation> getOperations(TagLists tagLists, Collection<Operation.Identity.Value> exclude) {
    	List<Operation> operations = Lists.newArrayList(getOperationsUnsorted(tagLists, exclude));
    	operations.sort(null);
    	return operations;
    }

    public Collection<UsageType> getUsageTypes(TagLists tagLists) {
    	List<UsageType> usageTypes = Lists.newArrayList(getUsageTypes(getTagGroupsInRange(getMonthMillis(totalInterval)), tagLists));
    	usageTypes.sort(null);
    	return usageTypes;
    }

    public Collection<ResourceGroup> getResourceGroups(TagLists tagLists) {
    	List<ResourceGroup> resourceGroups = Lists.newArrayList(getResourceGroups(totalInterval, tagLists));
    	resourceGroups.sort(null);
    	return resourceGroups;
    }

    public Interval getOverlapInterval(Interval interval) {
        return totalInterval == null ? null : totalInterval.overlap(interval);
    }

    public Map<Tag, TagLists> getTagListsMap(Interval interval, TagLists tagLists, TagType groupBy, List<Operation.Identity.Value> exclude) {
    	return getTagListsMap(interval, tagLists, groupBy, exclude, 0);
    }
    
    public Map<Tag, TagLists> getTagListsMap(Interval interval, TagLists tagLists, TagType groupBy, List<Operation.Identity.Value> exclude, int userTagGroupByIndex) {
        Map<Tag, TagLists> result = Maps.newHashMap();
        
        Set<TagGroup> tagGroupsInRange = getTagGroupsInRange(getMonthMillis(interval));
        
        // Get all the GroupBy tags. If we're not grouping by ResourceGroup or User Tag, then work with a TagLists that doesn't contain resourceGroups.
        // Filtering of results against resourceGroup values is handled later.
        TagLists tagListsForTag = tagLists;
        boolean tagListsHasResourceGroups = tagLists.resourceGroups != null && tagLists.resourceGroups.size() > 0;
        if ((groupBy == null || groupBy != TagType.Tag) && tagListsHasResourceGroups) {
        	//logger.info("getTagListsWithNullResourceGroup");
            tagListsForTag = tagLists.getTagListsWithNullResourceGroup();
        }
        
        // We must always specify all the operations so that we can choose between 
        // Borrowed and Lent Operations in order not to double count the cost/usage    	
    	List<Operation> ops = tagListsForTag.operations;
        if (ops == null || ops.size() == 0) {
        	ops = Lists.newArrayList(getOperations(tagGroupsInRange, tagListsForTag, exclude));
        }
        else {
        	ops = Operation.exclude(ops, exclude);
        }
        tagListsForTag = tagListsForTag.getTagListsWithOperations(ops);

        if (ops.isEmpty())
        	return result;
        
        if (groupBy == null || groupBy == TagType.TagKey) {
            result.put(Tag.aggregated, tagListsForTag);
        	//logger.info("groupBy == null || groupBy == TagType.TagKey");
            return result;
        }

        List<Tag> groupByTags = Lists.newArrayList();
        switch (groupBy) {
            case CostType:
        	    groupByTags.addAll(getCostTypes(tagGroupsInRange, tagListsForTag));
        	    break;
            case Account:
                groupByTags.addAll(getAccounts(tagGroupsInRange, tagListsForTag));
                break;
            case Region:
                groupByTags.addAll(getRegions(tagGroupsInRange, tagListsForTag));
                break;
            case Zone:
                groupByTags.addAll(getZones(tagGroupsInRange, tagListsForTag));
                break;
            case Product:
                groupByTags.addAll(getProducts(tagGroupsInRange, tagListsForTag));
                break;
            case Operation:
                groupByTags.addAll(getOperations(tagGroupsInRange, tagListsForTag, null));
                break;
            case UsageType:
                groupByTags.addAll(getUsageTypes(tagGroupsInRange, tagListsForTag));
                break;
            case Tag:
                groupByTags.addAll(getResourceGroupTags(interval, tagListsForTag, userTagGroupByIndex));
                break;
            default:
            	break;
        }
//        logger.info("TagLists: " + tagLists);
//        logger.info("found " + groupByTags.size() + " groupByTags, taglists instanceof " + (tagLists instanceof TagListsWithUserTags ? "TagListsWithUserTags" : "TagLists"));
//        if (tagLists instanceof TagListsWithUserTags) {
//        	for (Tag tag: groupByTags)
//        		logger.info("groupBy tag<" + tagLists.contains(tag, groupBy, userTagGroupByIndex) + ">: " + tag);
//        }
        
        // Get the TagLists with the ResourceGroups, but use the already filtered operations
        tagListsForTag = tagLists.getTagListsWithOperations(ops);

        for (Tag tag: groupByTags) {
            if (tagListsForTag.contains(tag, groupBy, userTagGroupByIndex)) {
                //logger.info("get tag lists for " + tag + ", " + groupByOperationOnReservationDashboard);
                TagLists tmp = tagListsForTag.getTagLists(tag, groupBy, userTagGroupByIndex);
        			
                result.put(tag, tmp);
            }
        }
        return result;
    }

}
