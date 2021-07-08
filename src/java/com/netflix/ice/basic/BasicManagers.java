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

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.*;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.processor.TagGroupWriter;
import com.netflix.ice.reader.*;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Operation.Identity.Value;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.UserTagKey;
import com.netflix.ice.tag.Zone;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

/**
 * This class manages all BasicTagGroupManager and BasicDataManager instances.
 */
public class BasicManagers extends Poller implements Managers {
    private ReaderConfig config;
    private boolean compress;

    private Set<Product> products = Sets.newHashSet();
    private LastProcessedPoller lastProcessedPoller = null;
    private Map<Product, BasicTagGroupManager> tagGroupManagers = Maps.newHashMap();
    private TreeMap<Key, BasicDataManager> dataManagers = Maps.newTreeMap();
    private TreeMap<Key, TagCoverageDataManager> tagCoverageManagers = Maps.newTreeMap();
    private InstanceMetricsService instanceMetricsService = null;
    private InstancesService instancesService = null;
    private Long lastPollMillis = 0L;
	private ExecutorService pool;
	private ExecutorService refreshPool;
    
    BasicManagers(boolean compress) {
    	this.compress = compress;
    }
    
    public void shutdown() {
    	lastProcessedPoller.shutdown();
    }

    public void init() {
        config = ReaderConfig.getInstance();
        lastProcessedPoller = new LastProcessedPoller(config.startDate, config.workBucketConfig);
        pool = Executors.newFixedThreadPool(config.numthreads);
        refreshPool = Executors.newFixedThreadPool(config.numthreads);
                		
        doWork();
        start(1*60, 1*60, false);
    }

    public Collection<Product> getProducts() {
        return products;
    }

    public TagGroupManager getTagGroupManager(Product product) {
        return tagGroupManagers.get(product);
    }

    public DataManager getDataManager(Product product, ConsolidateType consolidateType) {
        return dataManagers.get(new Key(product, consolidateType));
    }

    public DataManager getTagCoverageManager(Product product, ConsolidateType consolidateType) {
        return tagCoverageManagers.get(new Key(product, consolidateType));
    }
    
    public Collection<Instance> getInstances(String id) {
    	return instancesService.getInstances(id);
    }

    @Override
    protected void poll() throws Exception {
        doWork();
    }

    private void doWork() {
    	// Update the reader configuration from the work bucket data configuration
    	config.update();
    	
    	WorkBucketConfig wbc = config.workBucketConfig;
    	if (instancesService == null) {
            instanceMetricsService = new InstanceMetricsService(wbc.localDir, wbc.workS3BucketName, wbc.workS3BucketPrefix);
            instancesService = new InstancesService(wbc.localDir, wbc.workS3BucketName, wbc.workS3BucketPrefix, config.accountService, config.productService);
    	}
    	
    	if (lastPollMillis >= lastProcessedPoller.getLastProcessedMillis())
    		return;	// nothing to do
    	
       	lastPollMillis = lastProcessedPoller.getLastProcessedMillis();
       	    	
    	// Refresh all the data manager caches
    	refreshDataManagers(wbc);
    	    	
    	
        logger.info("trying to find new tag group and data managers...");
        Set<Product> products = Sets.newHashSet(this.products);
        Map<Product, BasicTagGroupManager> tagGroupManagers = Maps.newHashMap(this.tagGroupManagers);
        TreeMap<Key, BasicDataManager> dataManagers = Maps.newTreeMap(this.dataManagers);

        Set<Product> newProducts = Sets.newHashSet();
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        for (S3ObjectSummary s3ObjectSummary: s3Client.listObjects(wbc.workS3BucketName, wbc.workS3BucketPrefix + TagGroupWriter.DB_PREFIX).getObjectSummaries()) {
            String key = s3ObjectSummary.getKey();
            if (key.endsWith(BasicTagGroupManager.compressExtension)) {
            	key = key.substring(0, key.length() - BasicTagGroupManager.compressExtension.length());
            }
            Product product;
            if (key.endsWith("_all")) {
                product = null;
            }
            else {
                String serviceCode = key.substring((wbc.workS3BucketPrefix + TagGroupWriter.DB_PREFIX).length());
                product = config.productService.getProductByServiceCode(serviceCode);
            }
            if (!products.contains(product)) {
                products.add(product);
                newProducts.add(product);
            }
        }

        for (Product product: newProducts) {
        	BasicTagGroupManager tagGroupManager = new BasicTagGroupManager(product, true, config.workBucketConfig, config.accountService, config.productService, config.userTagKeys.size());
            tagGroupManagers.put(product, tagGroupManager);
            boolean loadTagCoverage = (product == null && config.getTagCoverage() != TagCoverage.none) || (product != null && config.getTagCoverage() == TagCoverage.withUserTags);
            for (ConsolidateType consolidateType: ConsolidateType.values()) {
                Key key = new Key(product, consolidateType);
                
            	boolean forReservations = consolidateType == ConsolidateType.hourly && !config.hourlyData;
            	
            	if (forReservations && product != null && !product.hasReservations() && !product.hasSavingsPlans()) {
            		// Create hourly cost and usage managers only for reservation and savings plan operations
            		continue;
            	}
            	
            	String partialDbName = consolidateType + "_" + (product == null ? "all" : product.getServiceCode());
            	int numUserTags = product == null ? 0 : config.userTagKeys.size();
            		
	               
                dataManagers.put(key, new BasicDataManager(config.startDate, partialDbName, consolidateType, tagGroupManager, compress, numUserTags,
                		config.monthlyCacheSize, config.workBucketConfig, config.accountService, config.productService, instanceMetricsService, forReservations));
                if (loadTagCoverage && consolidateType != ConsolidateType.hourly) {
    	            tagCoverageManagers.put(key, new TagCoverageDataManager(config.startDate, "coverage_" + partialDbName, consolidateType, tagGroupManager, compress, config.userTagKeys,
            				config.monthlyCacheSize, config.workBucketConfig, config.accountService, config.productService));
                }
            }
        }

        if (newProducts.size() > 0) {
            this.dataManagers = dataManagers;
            this.tagGroupManagers = tagGroupManagers;
            this.products = products;
        }
    }
    
    private void refreshDataManagers(WorkBucketConfig wbc) {
    	for (DataCache d: tagGroupManagers.values()) {
    		refresh(d);
    	}
    	for (DataCache d: dataManagers.values()) {
    		refresh(d);
    	}
    	for (DataCache d: tagCoverageManagers.values()) {
    		refresh(d);
    	}
    	
    	refresh(instancesService);
    	refresh(instanceMetricsService);
    }

    private Future<Void> refresh(final DataCache dataCache) {
    	return refreshPool.submit(new Callable<Void>() {
    		@Override
    		public Void call() throws Exception {
    			dataCache.refresh();
    			return null;
    		}
    	});    	
    }

    private static class Key implements Comparable<Key> {
        Product product;
        ConsolidateType consolidateType;
        Key(Product product, ConsolidateType consolidateType) {
            this.product = product;
            this.consolidateType = consolidateType;
        }

        public int compareTo(Key t) {
            int result = this.product == t.product ? 0 : (this.product == null ? 1 : (t.product == null ? -1 : t.product.compareTo(this.product)));
            if (result != 0)
                return result;
            return consolidateType.compareTo(t.consolidateType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            Key other = (Key)o;
            return
                    this.product == other.product &&
                    this.consolidateType == other.consolidateType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            if (product != null)
                result = prime * result + this.product.hashCode();
            result = prime * result + this.consolidateType.hashCode();

            return result;
        }
    }

    @Override
    public Collection<UserTag> getUserTagValues(List<Account> accounts, List<Region> regions, List<Zone> zones, Collection<Product> products, int index) throws Exception {
    	List<Future<Collection<ResourceGroup>>> futures = Lists.newArrayList();
    	
    	Set<UserTag> userTagValues = Sets.newTreeSet();

		// Add "None" entry
    	userTagValues.add(UserTag.get(UserTag.none));
    	
        for (Product product: products) {
            TagGroupManager tagGroupManager = getTagGroupManager(product);
			if (tagGroupManager == null) {
				//logger.error("No TagGroupManager for product " + product + ", products: " + getProducts().size());
				continue;
			}			
			futures.add(getUserTagValuesForProduct(new TagLists(accounts, regions, zones, Lists.newArrayList(product)), tagGroupManager));
        }
		// Wait for completion
		for (Future<Collection<ResourceGroup>> f: futures) {
			Collection<ResourceGroup> resourceGroups = f.get();
			for (ResourceGroup rg: resourceGroups) {
				UserTag[] tags = rg.getUserTags();
				if (tags.length > index && !StringUtils.isEmpty(tags[index].name))
					userTagValues.add(tags[index]);
			}
		}
		
    	return userTagValues;
    }

    private Future<Collection<ResourceGroup>> getUserTagValuesForProduct(final TagLists tagLists, final TagGroupManager tagGroupManager) {
    	return pool.submit(new Callable<Collection<ResourceGroup>>() {
    		@Override
    		public Collection<ResourceGroup> call() throws Exception {
    			Collection<ResourceGroup> rgs = tagGroupManager.getResourceGroups(tagLists);
    			return rgs;
    		}
    	});    	
    }

    @Override
    public Map<Tag, double[]> getData(
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
    		int userTagGroupByIndex) throws Exception {    	
    	
		StopWatch sw = new StopWatch();
		sw.start();
		
		if (products.size() == 0) {
	    	List<Future<Collection<Product>>> futures = Lists.newArrayList();
            TagLists tagLists = new TagLists(accounts, regions, zones);
            for (Product product: getProducts()) {
                if (product == null)
                    continue;

                futures.add(getFilteredProduct(tagLists, getTagGroupManager(product)));
            }
    		// Wait for completion
            Set<Product> productSet = Sets.newTreeSet();
    		for (Future<Collection<Product>> f: futures) {
                productSet.addAll(f.get());
    		}
            products = Lists.newArrayList(productSet);
		}
				
    	List<Future<Map<Tag, double[]>>> futures = Lists.newArrayList();
        for (Product product: products) {
            if (product == null)
                continue;

            DataManager dataManager = getDataManager(product, consolidateType);
			if (dataManager == null) {
				//logger.error("No DataManager for product " + product);
				continue;
			}
			TagLists tagLists = new TagListsWithUserTags(accounts, regions, zones, Lists.newArrayList(product), operations, usageTypes, userTagLists);
			logger.debug("-------------- Process product ----------------" + product);
            futures.add(getDataForProduct(
            		isCost,
                    interval,
                    tagLists,
                    groupBy,
                    aggregate,
                    exclude,
    				usageUnit,
    				userTagGroupByIndex,
    				dataManager));            
        }
        // Wait for completion
        Map<Tag, double[]> data = Maps.newTreeMap();
        
		for (Future<Map<Tag, double[]>> f: futures) {
			Map<Tag, double[]> dataOfProduct = f.get();
			
            if (groupBy == TagType.Product && dataOfProduct.size() > 0) {
                double[] currentProductValues = dataOfProduct.get(dataOfProduct.keySet().iterator().next());
                dataOfProduct.put(Tag.aggregated, Arrays.copyOf(currentProductValues, currentProductValues.length));
            } 
            
            merge(dataOfProduct, data);
		}
		
		logger.debug("getData() time to process: " + sw);

    	return data;
    }
    
    private Future<Collection<Product>> getFilteredProduct(final TagLists tagLists, final TagGroupManager tagGroupManager) {
    	return pool.submit(new Callable<Collection<Product>>() {
    		@Override
    		public Collection<Product> call() throws Exception {
                return tagGroupManager.getProducts(tagLists);
    		}
    	});    	
    }

    private Future<Map<Tag, double[]>> getDataForProduct(
    		final boolean isCost,
    		final Interval interval,
    		final TagLists tagLists,
    		final TagType groupBy,
    		final AggregateType aggregate,
    		final List<Operation.Identity.Value> exclude,
    		final UsageUnit usageUnit,
    		final int userTagGroupByIndex,
    		final DataManager dataManager) {
    	
    	return pool.submit(new Callable<Map<Tag, double[]>>() {
    		@Override
    		public Map<Tag, double[]> call() throws Exception {
    			Map<Tag, double[]> data = dataManager.getData(
    					isCost,
                        interval,
                        tagLists,
                        groupBy,
                        aggregate,
                        exclude,
        				usageUnit,
        				userTagGroupByIndex
                    );
    			return data;
    		}
    	});    	
    }

    
    private void merge(Map<Tag, double[]> from, Map<Tag, double[]> to) {
        for (Map.Entry<Tag, double[]> entry: from.entrySet()) {
            Tag tag = entry.getKey();
            double[] newValues = entry.getValue();
            if (to.containsKey(tag)) {
                double[] oldValues = to.get(tag);
                for (int i = 0; i < newValues.length; i++) {
                    oldValues[i] += newValues[i];
                }
            }
            else {
                to.put(tag, newValues);
            }
        }
    }

    public String getStatistics(boolean csv) throws ExecutionException {
    	StringBuilder sb = new StringBuilder();
    	TagGroupManager allTgm = tagGroupManagers.get(null);
		TreeMap<Long, Integer> allTgmSizes = allTgm.getSizes();
		DateTime month = new DateTime(allTgmSizes.lastKey());
		DateTime year = month.withMonthOfYear(1);
		int totalResourceTagGroups = 0;
    	
		if (csv) {
	    	sb.append("Product,TagGroups,Daily TagGroups,Accounts,Regions,Zones,Products,Operations,UsageTypes");
	    	for (UserTagKey utk: config.userTagKeys) {
	    		sb.append("," + utk.name);
	    	}
	    	sb.append("\n");
		}
		else {
	    	sb.append("<table><tr><td>Product</td><td>TagGroups</td><td>Daily TagGroups</td><td>Accounts</td><td>Regions</td><td>Zones</td><td>Products</td><td>Operations</td><td>UsageTypes</td>");
	    	for (UserTagKey utk: config.userTagKeys) {
	    		sb.append("<td>" + utk.name + "</td>");
	    	}
	    	sb.append("</tr>");
		}
    	for (Product p: tagGroupManagers.keySet()) {
    		TagGroupManager tgm = tagGroupManagers.get(p);
    		TreeMap<Long, Integer> sizes = tgm.getSizes();
    		TreeMap<Long, List<Integer>> tagValuesSizes = tgm.getTagValueSizes(config.userTagKeys.size());
    		BasicDataManager bdm = dataManagers.get(new Key(p, ConsolidateType.daily));
    		
    		if (csv) {
    			sb.append(p + "," + sizes.lastEntry().getValue() + "," + bdm.size(year));
    			for (Integer i: tagValuesSizes.lastEntry().getValue())
    				sb.append("," + i);
    	    	sb.append("\n");
    		}
    		else {
    			sb.append("<tr><td>" + p + "</td><td>" + sizes.lastEntry().getValue() + "</td><td>" + bdm.size(year) + "</td>");
    			for (Integer i: tagValuesSizes.lastEntry().getValue())
    				sb.append("<td>" + i + "</td>");
    	    	sb.append("</tr>");
    		}
    		
    		if (p != null) {
    			totalResourceTagGroups += sizes.lastEntry().getValue();
    		}
       	}
    	if (!csv)
    		sb.append("</table>");
    	
    	String intro = "TagGroupManagers: month=" + AwsUtils.monthDateFormat.print(month) + ", size=" + tagGroupManagers.size() + ", total resource TagGroups=" + totalResourceTagGroups;

		if (csv)
			intro += csv ? "\n" : "<br><br>";

    	return intro + sb.toString();
    }

	@Override
	public Collection<Operation> getOperations(TagLists tagLists, Collection<Product> products, Collection<Value> exclude, boolean withUserTags) {
		List<Operation> ops = null;
		
		if (withUserTags) {
	        Set<Operation> operations = Sets.newHashSet();
	        if (products.size() == 0) {
	            products = Lists.newArrayList(getProducts());
	        }
	        for (Product product: products) {
	            if (product == null)
	                continue;

	            List<Product> single = Lists.newArrayList(product);
	            TagGroupManager tagGroupManager = getTagGroupManager(product);
	            if (tagGroupManager == null)
	            	continue;
	            
	            Collection<Operation> tmp = tagGroupManager.getOperationsUnsorted(tagLists.getTagListsWithProducts(single), exclude);
	            operations.addAll(tmp);
	        }
	        ops = Lists.newArrayList(operations);
			ops.sort(null);
		}
		else {
			TagGroupManager tagGroupManager = getTagGroupManager(null);
			if (tagGroupManager == null)
				ops = Lists.newArrayList();
			else
				ops = Lists.newArrayList(tagGroupManager.getOperations(tagLists, exclude));
		}
		return ops;
	}

	@Override
	public UserTagStatistics getUserTagStatistics() throws ResourceException {
		List<UserTagStats> stats = Lists.newArrayList();
		
		// Build the full set of unique tagGroups across across all products and time
		Set<TagGroup> tagGroups = Sets.newHashSet();
		
		for (Product p: products) {
			if (p == null)
				continue;			
			tagGroups.addAll(tagGroupManagers.get(p).getTagGroupsWithResourceGroups());
		}
		
		// Extract the unique set of resource Groups
		Set<ResourceGroup> resourceGroups = Sets.newHashSet();
		for (TagGroup tg: tagGroups) {
			resourceGroups.add(tg.resourceGroup);
		}
		
		// Walk the list gathering stats for each user tag
		for (int i = 0; i < config.userTagKeys.size(); i++) {
			Set<String> values = Sets.newHashSet();
			Set<String> caseInsensitiveValues = Sets.newHashSet();
			Set<ResourceGroup> resourceGroupsWithoutCurrentTag = Sets.newHashSet();
			for (ResourceGroup rg: resourceGroups) {
				String v = rg.getUserTags()[i].name;
            	values.add(v);
            	caseInsensitiveValues.add(v.toLowerCase());
            	UserTag[] ut = rg.getUserTags().clone();
            	ut[i] = UserTag.empty;
            	resourceGroupsWithoutCurrentTag.add(ResourceGroup.getUncached(ut));
			}
			stats.add(new UserTagStats(config.userTagKeys.get(i).name, values.size(), values.size() - caseInsensitiveValues.size(), resourceGroups.size() - resourceGroupsWithoutCurrentTag.size()));
		}	
		return new UserTagStatistics(tagGroupManagers.get(null).getTagGroups().size(), tagGroups.size(), stats);
	}
	
	public Collection<ProcessorStatus> getProcessorStatus() {
		// Return sorted list - most recent month first
		List<ProcessorStatus> status = Lists.newArrayList(lastProcessedPoller.getStatus());
		Collections.sort(status, Collections.reverseOrder());
		return status;
	}
	
    public void reprocess(String month, boolean state) {
    	lastProcessedPoller.reprocess(month, state);
    }
    
    
    public boolean startProcessor() {
    	if (StringUtils.isEmpty(config.processorInstanceId) || StringUtils.isEmpty(config.processorRegion))
    		return false;
    	
    	boolean started = false;
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
        		.withRegion(config.processorRegion)
        		.withCredentials(AwsUtils.awsCredentialsProvider)
        		.withClientConfiguration(AwsUtils.clientConfig)
        		.build();

        try {
            StartInstancesRequest request = new StartInstancesRequest().withInstanceIds(new String[] { config.processorInstanceId });
            ec2.startInstances(request);
            started = true;
        }
        catch (Exception e) {
            logger.error("error in startInstances", e);
        }
        ec2.shutdown();
        return started;
    }
            
    public String getProcessorState() {
    	String state = "unknown";
    	
    	if (StringUtils.isEmpty(config.processorInstanceId) || StringUtils.isEmpty(config.processorRegion))
    		return state;
    	
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
        		.withRegion(config.processorRegion)
        		.withCredentials(AwsUtils.awsCredentialsProvider)
        		.withClientConfiguration(AwsUtils.clientConfig)
        		.build();

        try {
            DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(new String[] { config.processorInstanceId });
	        DescribeInstancesResult response = ec2.describeInstances(request);
	        // return state of first one we find. Should be only one since we specified the instance ID
	        for (com.amazonaws.services.ec2.model.Reservation reservation: response.getReservations()) {
	        	for (com.amazonaws.services.ec2.model.Instance instance: reservation.getInstances()) {
	        		return instance.getState().getName();
	        	}
	        }
        }
        catch (Exception e) {
            logger.error("error in describeInstances", e);
        }
        finally {
            ec2.shutdown();
        }
        return state;
    }

    private List<List<String>> getCsvData(String month, String prefix) {
        DateTime monthDateTime = new DateTime(month, DateTimeZone.UTC);
        File file = new File(config.workBucketConfig.localDir, prefix + AwsUtils.monthDateFormat.print(monthDateTime) + ".csv");
        
        try {
            AwsUtils.downloadFileIfChanged(config.workBucketConfig.workS3BucketName, config.workBucketConfig.workS3BucketPrefix + file.getName(), file);
        }
        catch (Exception e) {
            logger.error("error downloading " + file, e);
            return null;
        }
        if (file.exists()) {
            BufferedReader reader = null;
            List<List<String>> data = null;
            try {
            	InputStream is = new FileInputStream(file);
                reader = new BufferedReader(new InputStreamReader(is));

                
            	Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
            	
                data = Lists.newArrayList();
        	    for (CSVRecord record : records) {
        	    	List<String> row = Lists.newArrayListWithCapacity(record.size());
        	    	for (String col: record)
        	    		row.add(col);
        	    	data.add(row);
        	    }
            }
            catch (Exception e) {
            	logger.error("error in reading " + file, e);
            }
            finally {
                if (reader != null)
                    try {reader.close();} catch (Exception e) {}
            }
            return data;
        }
        return null;
    }
    
    private String getFileData(String month, String prefix) {
        DateTime monthDateTime = new DateTime(month, DateTimeZone.UTC);
        File file = new File(config.workBucketConfig.localDir, prefix + AwsUtils.monthDateFormat.print(monthDateTime) + ".csv");
        
        if (file.exists()) {
        	try {
        		return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        	}
        	catch (Exception e) {
        		logger.error("error reading " + file, e);
        	}
        }
        return null;
    }
    
    public List<List<String>> getSubscriptions(SubscriptionType subscriptionType, String month) {
    	switch (subscriptionType) {
    	case RI:
        	return getCsvData(month, "reservations_");
    	case SP:
        	return getCsvData(month, "savingsPlans_");
    	}
    	return null;
    }
    
    public String getSubscriptionsReport(SubscriptionType subscriptionType, String month) {
    	switch (subscriptionType) {
    	case RI:
        	return getFileData(month, "reservations_");
    	case SP:
        	return getFileData(month, "savingsPlans_");
    	}
    	return null;
    }
    
    public Collection<String> getMonths() {
    	List<String> months = Lists.newArrayList();
    	for (DateTime month = config.startDate; month.isBeforeNow(); month = month.plusMonths(1))
    		months.add(AwsUtils.monthDateFormat.print(month));
    	return months;
    }
    
    public List<List<String>> getPostProcessorStats(String month) {
    	if (month == null)
    		month = lastProcessedPoller.getLatestMonth();
    	return getCsvData(month, "postProcessorStats_");
    }
    
    public String getLatestProcessedMonth() {
    	return lastProcessedPoller.getLatestMonth();
    }

}
