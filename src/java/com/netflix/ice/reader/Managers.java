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

import com.netflix.ice.common.*;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.ResourceGroup.ResourceException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.joda.time.Interval;

/**
 * Interface to manager all TagGroupManager and DataManager instances for different products
 */
public interface Managers {

    void init();

    /**
     *
     * @return collection of products
     */
	Collection<Product> getProducts();

    /**
     *
     * @param product
     * @return TagGroupManager instance for specified product
     */
    TagGroupManager getTagGroupManager(Product product);
    
    /**
     * @throws Exception 
     * 
     */
    Collection<UserTag> getUserTagValues(List<Account> accounts, List<Region> regions, List<Zone> zones, Collection<Product> products, int index) throws Exception;

    /**
     *
     * @param product
     * @param consolidateType
     * @return cost DataManager instance for specified product and consolidateType
     */
    DataManager getCostManager(Product product, ConsolidateType consolidateType);

    /**
     *
     * @param product
     * @param consolidateType
     * @return usage DataManager instance for specified product and consolidateType
     */
    DataManager getUsageManager(Product product, ConsolidateType consolidateType);

    /**
     * 
     */
    Map<Tag, double[]> getData(
    		Interval interval,
    		List<Account> accounts,
    		List<Region> regions,
    		List<Zone> zones,
    		List<Product> products,
    		List<Operation> operations,
    		List<UsageType> usageTypes,
    		boolean isCost,
    		ConsolidateType consolidateType,
    		TagType groupBy,
    		AggregateType aggregate,
    		List<Operation.Identity.Value> exclude,
    		UsageUnit usageUnit,
    		List<List<UserTag>> userTagLists,
    		int userTagGroupByIndex) throws Exception;
    
    /**
     * 
     * @return
     */
    DataManager getTagCoverageManager(Product product, ConsolidateType consolidateType);

    /**
     * 
     */
    Collection<Instance> getInstances(String id);
    
    /**
     * Get all operations that meet query in tagLists for the requested products from the resource-based data.
     * @param tagLists
     * @return collection of operations
     */
    Collection<Operation> getOperations(TagLists tagLists, Collection<Product> products, Collection<Operation.Identity.Value> exclude, boolean withUserTags);
    
    /**
     * shutdown all manager instances
     */
    void shutdown();
    
    String getStatistics(boolean csv) throws ExecutionException;
    
    public class UserTagStats {
    	public String key;
    	public int values;
    	public int caseVariations;
    	public int permutationContribution;
    	
    	public UserTagStats(String key, int values, int caseVariations, int permutationContribution) {
    		this.key = key;
    		this.values = values;
    		this.caseVariations = caseVariations;
    		this.permutationContribution = permutationContribution;
    	}
    };

    public class UserTagStatistics {
    	public int nonResourceTagGroups;
    	public int resourceTagGroups;
    	public List<UserTagStats> userTagStats;
    	
        public UserTagStatistics(int nonResourceTagGroups, int resourceTagGroups, List<UserTagStats> userTagStats) {
        	this.nonResourceTagGroups = nonResourceTagGroups;
        	this.resourceTagGroups = resourceTagGroups;
        	this.userTagStats = userTagStats;
        }
    }
    
    UserTagStatistics getUserTagStatistics() throws ResourceException;
    
    Collection<ProcessorStatus> getProcessorStatus();
    
    void reprocess(String month, boolean state);
    
    boolean startProcessor();
    String getProcessorState();

    public enum SubscriptionType {
    	RI,
    	SP;
    }
    public List<List<String>> getSubscriptions(SubscriptionType subscriptionType, String month);
    public String getSubscriptionsReport(SubscriptionType subscriptionType, String month);

    public Collection<String> getMonths();
}
