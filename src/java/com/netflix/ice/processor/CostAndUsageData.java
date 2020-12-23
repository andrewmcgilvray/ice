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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.basic.BasicReservationService.Reservation;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Config;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.common.TagGroupRI;
import com.netflix.ice.common.TagGroupSP;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.processor.ProcessorConfig.JsonFileType;
import com.netflix.ice.processor.ReadWriteDataSerializer.TagGroupFilter;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ReservationArn;
import com.netflix.ice.tag.SavingsPlanArn;
import com.netflix.ice.tag.UserTagKey;

public class CostAndUsageData {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final long startMilli;
    private Map<Product, ReadWriteData> usageDataByProduct;
    private Map<Product, ReadWriteData> costDataByProduct;
    private final WorkBucketConfig workBucketConfig;
    private final AccountService accountService;
    private final ProductService productService;
    private Map<Product, ReadWriteTagCoverageData> tagCoverage;
    private List<UserTagKey> userTagKeys;
    private boolean collectTagCoverageWithUserTags;
    private Map<ReservationArn, Reservation> reservations;
    private Map<SavingsPlanArn, SavingsPlan> savingsPlans;
    private Set<Product> savingsPlanProducts;
    private List<PostProcessorStats> postProcessorStats;
    private List<String> userTagKeysAsStrings;
    private List<Status> archiveFailures;
    private boolean cacheTagGroups;
    
	public CostAndUsageData(long startMilli, WorkBucketConfig workBucketConfig, List<UserTagKey> userTagKeys, Config.TagCoverage tagCoverage, AccountService accountService, ProductService productService) {
		this.startMilli = startMilli;
        this.userTagKeys = userTagKeys;
		this.usageDataByProduct = Maps.newHashMap();
		this.costDataByProduct = Maps.newHashMap();
        this.usageDataByProduct.put(null, new ReadWriteData(0)); // Non-resource data has no user tags
        this.costDataByProduct.put(null, new ReadWriteData(0)); // Non-resource data has no user tags
        this.workBucketConfig = workBucketConfig;
        this.accountService = accountService;
        this.productService = productService;
        this.tagCoverage = null;
        this.collectTagCoverageWithUserTags = tagCoverage == TagCoverage.withUserTags;
        if (userTagKeys != null && tagCoverage != TagCoverage.none) {
    		this.tagCoverage = Maps.newHashMap();
        	this.tagCoverage.put(null, new ReadWriteTagCoverageData(getNumUserTags()));
        }
        this.reservations = Maps.newHashMap();
        this.savingsPlans = Maps.newHashMap();
        this.savingsPlanProducts = Sets.newHashSet();
        this.postProcessorStats = Lists.newArrayList();
        this.archiveFailures = Lists.newArrayList();
        this.cacheTagGroups = false;
	}
	
	/*
	 * Constructor that creates a new empty data set based on another data set. Used by the post processor for generating reports
	 */
	public CostAndUsageData(CostAndUsageData other, List<UserTagKey> userTagKeys) {
		this.startMilli = other.startMilli;
        this.userTagKeys = userTagKeys;
		this.usageDataByProduct = Maps.newHashMap();
		this.costDataByProduct = Maps.newHashMap();
        this.usageDataByProduct.put(null, new ReadWriteData(0)); // Non-resource data has no user tags
        this.costDataByProduct.put(null, new ReadWriteData(0)); // Non-resource data has no user tags
        this.workBucketConfig = other.workBucketConfig;
        this.accountService = other.accountService;
        this.productService = other.productService;
        this.tagCoverage = null;
        this.collectTagCoverageWithUserTags = false;
        this.reservations = null;
        this.savingsPlans = null;
        this.savingsPlanProducts = null;
        this.postProcessorStats = null;
        this.cacheTagGroups = false;
	}
	
	public DateTime getStart() {
		return new DateTime(startMilli, DateTimeZone.UTC);
	}
	
	public long getStartMilli() {
		return startMilli;
	}
	
	public int getNumUserTags() {
		return userTagKeys == null ? 0 : userTagKeys.size();
	}
	
	public boolean isCacheTagGroups() {
		return cacheTagGroups;
	}
	
	public void enableTagGroupCache(boolean cacheTagGroups) {
		this.cacheTagGroups = cacheTagGroups;
		for (ReadWriteData rwd: usageDataByProduct.values())
			rwd.enableTagGroupCache(cacheTagGroups);
		for (ReadWriteData rwd: costDataByProduct.values())
			rwd.enableTagGroupCache(cacheTagGroups);
		if (tagCoverage != null) {
			for (ReadWriteTagCoverageData tcd: tagCoverage.values())
				tcd.enableTagGroupCache(true);
		}
	}
	
	public List<String> getUserTagKeysAsStrings() {
        if (userTagKeys == null)
        	return null;
        
        if (userTagKeysAsStrings == null) {
        	userTagKeysAsStrings = Lists.newArrayList();
	        for (UserTagKey utk: userTagKeys)
	        	userTagKeysAsStrings.add(utk.name);
        }
        
		return userTagKeysAsStrings;
	}
	
	public ReadWriteData getUsage(Product product) {
		return usageDataByProduct.get(product);
	}
	
	public void putUsage(Product product, ReadWriteData data) {
		usageDataByProduct.put(product,  data);
		data.enableTagGroupCache(cacheTagGroups);
	}
	
	public ReadWriteData getCost(Product product) {
		return costDataByProduct.get(product);
	}
	
	public void putCost(Product product, ReadWriteData data) {
		costDataByProduct.put(product,  data);
		data.enableTagGroupCache(cacheTagGroups);
	}
	
	public ReadWriteTagCoverageData getTagCoverage(Product product) {
		return tagCoverage.get(product);
	}
	
	public void putTagCoverage(Product product, ReadWriteTagCoverageData data) {
		tagCoverage.put(product,  data);
		data.enableTagGroupCache(cacheTagGroups);
	}
	
	
	public void putAll(CostAndUsageData data) {
		// Add all the data from the supplied CostAndUsageData
		
		for (Entry<Product, ReadWriteData> entry: data.usageDataByProduct.entrySet()) {
			ReadWriteData usage = getUsage(entry.getKey());
			if (usage == null) {
				usageDataByProduct.put(entry.getKey(), entry.getValue());
				entry.getValue().enableTagGroupCache(cacheTagGroups);
			}
			else {
				usage.putAll(entry.getValue());
			}
		}
		for (Entry<Product, ReadWriteData> entry: data.costDataByProduct.entrySet()) {
			ReadWriteData cost = getCost(entry.getKey());
			if (cost == null) {
				costDataByProduct.put(entry.getKey(), entry.getValue());
				entry.getValue().enableTagGroupCache(cacheTagGroups);
			}
			else {
				cost.putAll(entry.getValue());
			}
		}
		if (tagCoverage != null && data.tagCoverage != null) {
			for (Entry<Product, ReadWriteTagCoverageData> entry: data.tagCoverage.entrySet()) {
				ReadWriteTagCoverageData tc = getTagCoverage(entry.getKey());
				if (tc == null) {
					tagCoverage.put(entry.getKey(), entry.getValue());
					entry.getValue().enableTagGroupCache(cacheTagGroups);
				}
				else {
					tc.putAll(entry.getValue());
				}
			}	
		}
		reservations.putAll(data.reservations);
		savingsPlans.putAll(data.savingsPlans);
		savingsPlanProducts.addAll(data.savingsPlanProducts);
	}
	
    public void cutData(int hours) {
        for (ReadWriteData data: usageDataByProduct.values()) {
            data.cutData(hours);
        }
        for (ReadWriteData data: costDataByProduct.values()) {
            data.cutData(hours);
        }
    }
    
    public int getMaxNum() {
    	// return the maximum number of hours represented in the underlying maps
    	int max = 0;
    	
        for (ReadWriteData data: usageDataByProduct.values()) {
            max = max < data.getNum() ? data.getNum() : max;
        }
        for (ReadWriteData data: costDataByProduct.values()) {
            max = max < data.getNum() ? data.getNum() : max;
        }
        return max;
    }
    
    public void addReservation(Reservation reservation) {
    	reservations.put(reservation.tagGroup.arn, reservation);
    }
    
    public Map<ReservationArn, Reservation> getReservations() {
    	return reservations;
    }
    
    public boolean hasReservations() {
    	return reservations != null && reservations.size() > 0;
    }
    
    public void addSavingsPlan(TagGroupSP tagGroup, PurchaseOption paymentOption, String term, String offeringType, long start, long end, String hourlyRecurringFee, String hourlyAmortization) {
    	if (savingsPlans.containsKey(tagGroup.arn))
    		return;
		SavingsPlan plan = new SavingsPlan(tagGroup,
				paymentOption, term, offeringType, start, end,
				Double.parseDouble(hourlyRecurringFee), 
				Double.parseDouble(hourlyAmortization));
    	savingsPlans.put(tagGroup.arn, plan);
    }
    
    public void addSavingsPlanProduct(Product product) {
    	savingsPlanProducts.add(product);
    }
    
    public Collection<Product> getSavingsPlanProducts() {
    	return savingsPlanProducts;
    }
    
    public Map<SavingsPlanArn, SavingsPlan> getSavingsPlans() {
    	return savingsPlans;
    }
    
    public boolean hasSavingsPlans() {
    	return savingsPlans != null && savingsPlans.size() > 0;
    }
    
    /**
     * Add an entry to the tag coverage statistics for the given TagGroup
     */
    public void addTagCoverage(Product product, int index, TagGroup tagGroup, boolean[] userTagCoverage) {
    	if (tagCoverage == null || !tagGroup.product.enableTagCoverage()) {
    		return;
    	}
    	
    	if (!collectTagCoverageWithUserTags && product != null) {
    		return;
    	}
    	
    	ReadWriteTagCoverageData data = tagCoverage.get(product);
    	if (data == null) {
    		data = new ReadWriteTagCoverageData(getNumUserTags());
    		tagCoverage.put(product, data);
    	}
    	
    	data.put(index, tagGroup, TagCoverageMetrics.add(data.get(index, tagGroup), userTagCoverage));
    }
    
    private String getProdName(Product product) {
        return product == null ? "all" : product.getServiceCode();
    }
    
    public class Status {
    	public boolean failed;
    	public String filename;
    	public Exception exception;
    	
    	Status(String filename, Exception exception) {
    		this.failed = true;
    		this.filename = filename;
    		this.exception = exception;
    	}
    	
    	Status(String filename) {
    		this.failed = false;
    		this.filename = filename;
    		this.exception = null;
    	}
    	
    	public String toString() {
    		return filename + ", " + exception;
    	}
    }

    // If archiveHourlyData is false, only archive hourly data used for reservations and savings plans
    public void archive(DateTime startDate, List<JsonFileType> jsonFiles, InstanceMetrics instanceMetrics, 
    		PriceListService priceListService, int numThreads, boolean archiveHourlyData) throws Exception {
    	
    	archiveFailures = Lists.newArrayList();
    	verifyTagGroups();
    	
    	ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    	List<Future<Status>> futures = Lists.newArrayList();
    	
    	for (JsonFileType jft: jsonFiles)
        	futures.add(archiveJson(jft, instanceMetrics, priceListService, pool));
    	
        for (Product product: costDataByProduct.keySet()) {
        	futures.add(archiveTagGroups(startMilli, product, costDataByProduct.get(product).getTagGroups(), pool));
        }
        
        Set<Product> products = Sets.newHashSet(costDataByProduct.keySet());
        products.addAll(usageDataByProduct.keySet());

        archiveHourly(archiveHourlyData, products, pool, futures);    
        archiveSummary(startDate, products, pool, futures);
        
        archiveSummaryTagCoverage(startDate, pool, futures);

        archiveReservations();
        archiveSavingsPlans();
        archivePostProcessorStats();
        
		// Wait for completion
		for (Future<Status> f: futures) {
			Status s = f.get();
			if (s.failed) {
				archiveFailures.add(s);
				logger.error("Error archiving file: " + s);
			}
		}
		
		shutdownAndAwaitTermination(pool);
    }
    
    public List<Status> getArchiveFailures() {
    	return archiveFailures;
    }
    
    private void shutdownAndAwaitTermination(ExecutorService pool) {
    	pool.shutdown(); // Disable new tasks from being submitted
    	try {
    		// Wait a while for existing tasks to terminate
    		if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
    			pool.shutdownNow(); // Cancel currently executing tasks
    			// Wait a while for tasks to respond to being cancelled
    			if (!pool.awaitTermination(60, TimeUnit.SECONDS))
    				System.err.println("Pool did not terminate");
    		}
    	} catch (InterruptedException ie) {
    		// (Re-)Cancel if current thread also interrupted
    		pool.shutdownNow();
    		// Preserve interrupt status
    		Thread.currentThread().interrupt();
    	}
	}
    
    private void verifyTagGroups() throws Exception {    	
        for (Product product: costDataByProduct.keySet()) {
        	boolean verify = product == null || product.isEc2Instance() || product.isRdsInstance() || product.isRedshift() || product.isElastiCache() || product.isElasticsearch();
        	if (!verify)
        		continue;
        	
            Collection<TagGroup> tagGroups = costDataByProduct.get(product).getTagGroups();            
        	verifyTagGroupsForProduct(tagGroups);

        	if (usageDataByProduct.get(product) != null) {
	            tagGroups = usageDataByProduct.get(product).getTagGroups();
	            verifyTagGroupsForProduct(tagGroups);
        	}
        }
    }
    
    /**
     * Make sure all SP and RI TagGroups have been aggregated back to regular TagGroups
     * @param tagGroups
     * @throws Exception
     */
    private void verifyTagGroupsForProduct(Collection<TagGroup> tagGroups) throws Exception {
    	int count = 0;
        for (TagGroup tg: tagGroups) {
    		// verify that all the tag groups are instances of TagGroup and not TagGroupArn
        	if (tg instanceof TagGroupRI || tg instanceof TagGroupSP) {
        		count++;
        		//throw new Exception("Non-baseclass tag group in archive cost data: " + tg);
        	}
        }
        if (count > 0)
        	logger.error("Non-baseclass tag groups in archive cost data. Found " + count + " TagGroupRI or TagGroupSP tagGroups");
    }
    
    private Future<Status> archiveJson(final JsonFileType writeJsonFiles, final InstanceMetrics instanceMetrics, final PriceListService priceListService, ExecutorService pool) {
    	return pool.submit(new Callable<Status>() {
    		@Override
    		public Status call() {
    	        logger.info("archiving " + writeJsonFiles.name() + " JSON data...");
    	        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
    	        String filename = writeJsonFiles.name() + "_all_" + AwsUtils.monthDateFormat.print(monthDateTime) + ".json";
    	        try {
	    	        DataJsonWriter writer = new DataJsonWriter(filename,
	    	        		monthDateTime, userTagKeys, writeJsonFiles, costDataByProduct, usageDataByProduct, instanceMetrics, priceListService, workBucketConfig);
	    	        writer.archive();
    	        }
    	        catch (Exception e) {
    				e.printStackTrace();
    	        	return new Status(filename, e);
    	        }
    	        return new Status(filename);
    		}
    	});        
    }
    
    private Future<Status> archiveTagGroups(final long startMilli, final Product product, final Collection<TagGroup> tagGroups, ExecutorService pool) {
    	return pool.submit(new Callable<Status>() {
    		@Override
    		public Status call() {
    			String name = getProdName(product);
    			try {
	                TagGroupWriter writer = new TagGroupWriter(name, true, workBucketConfig, accountService, productService, getNumUserTags());
	                writer.archive(startMilli, tagGroups);
    			}
    			catch (Exception e) {
    				logger.error("Failed to archive TagGroups: " + TagGroupWriter.DB_PREFIX + name + ", " + e);
    				e.printStackTrace();
    				return new Status(TagGroupWriter.DB_PREFIX + name, e);
    			}
    	        return new Status(TagGroupWriter.DB_PREFIX + name);
    		}
    	});        
    }
    
    private void archiveHourly(boolean archiveHourlyData, Set<Product> products, ExecutorService pool, List<Future<Status>> futures) {
        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
        
        for (Product product: products) {
        	if (!archiveHourlyData && product != null && !product.hasReservations() && !product.hasSavingsPlans())
        		continue;
        	
            String name = "hourly_" + getProdName(product) + "_" + AwsUtils.monthDateFormat.print(monthDateTime);
            ReadWriteData costData = costDataByProduct.get(product);
            ReadWriteData usageData = usageDataByProduct.get(product);
            
        	// Generate the full set of tag groups across both cost and usage
        	Set<TagGroup> tagGroups = Sets.newHashSet();
        	if (costData != null)
        		tagGroups.addAll(costData.getTagGroups());
        	if (usageData != null)
        		tagGroups.addAll(usageData.getTagGroups());
            
            futures.add(archiveHourlyFile(name, new DataSerializer(costData, usageData, tagGroups), archiveHourlyData, pool));
        }
    }
    
    public class RiSpTagGroupFilter implements TagGroupFilter {

		@Override
		public Collection<TagGroup> getTagGroups(Collection<TagGroup> tagGroups) {
			List<TagGroup> filtered = Lists.newArrayList();
			for (TagGroup tg: tagGroups) {
				if (tg.operation instanceof Operation.ReservationOperation || tg.operation instanceof Operation.SavingsPlanOperation)
					filtered.add(tg);
			}
			return filtered;
		}
    	
    }
    
    private Future<Status> archiveHourlyFile(final String name, final ReadWriteDataSerializer serializer, final boolean archiveHourlyData, ExecutorService pool) {
    	return pool.submit(new Callable<Status>() {
    		@Override
    		public Status call() {
    			try {
	                DataWriter writer = getDataWriter(name, serializer, false);
	                writer.archive(archiveHourlyData ? null : new RiSpTagGroupFilter());
	                writer.delete(); // delete local copy to save disk space since we don't need it anymore
    			}
    			catch (Exception e) {
    				e.printStackTrace();
    				return new Status(name, e);
    			}
                return new Status(name);
    		}
    	});
    }

    protected void addValue(List<Map<TagGroup, DataSerializer.CostAndUsage>> list, int index, TagGroup tagGroup, DataSerializer.CostAndUsage v) {
        Map<TagGroup, DataSerializer.CostAndUsage> map = DataSerializer.getCreateData(list, index);
        DataSerializer.CostAndUsage existedV = map.get(tagGroup);
        map.put(tagGroup, existedV == null ? v : new CostAndUsage(existedV.cost + v.cost, existedV.usage + v.usage));
    }


    private void archiveSummary(DateTime startDate, Set<Product> products, ExecutorService pool, List<Future<Status>> futures) {

        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);

        // Queue the non-resource version first because it takes longer and we
        // don't like to have it last with other threads idle.
        futures.add(archiveSummaryProductFuture(monthDateTime, startDate, null, costDataByProduct.get(null), usageDataByProduct.get(null), pool));
                
        for (Product product: products) {
        	if (product == null)
        		continue;
        	
            futures.add(archiveSummaryProductFuture(monthDateTime, startDate, product, costDataByProduct.get(product), usageDataByProduct.get(product), pool));
        }
    }
    
    protected void aggregateSummaryData(
    		DataSerializer data,
    		int daysFromLastMonth,
            List<Map<TagGroup, DataSerializer.CostAndUsage>> daily,
            List<Map<TagGroup, DataSerializer.CostAndUsage>> weekly,
            List<Map<TagGroup, DataSerializer.CostAndUsage>> monthly
    		) {
        // aggregate to daily, weekly and monthly
        for (int hour = 0; hour < data.getNum(); hour++) {
            // this month, add to weekly, monthly and daily
            Map<TagGroup, DataSerializer.CostAndUsage> map = data.getData(hour);

            for (TagGroup tagGroup: data.getTagGroups()) {
            	DataSerializer.CostAndUsage v = map.get(tagGroup);
                if (v != null) {
                    addValue(monthly, 0, tagGroup, v);
                    addValue(daily, hour/24, tagGroup, v);
                    addValue(weekly, (hour + daysFromLastMonth*24) / 24/7, tagGroup, v);
                }
            }
        }
    }
        
    protected void getPartialWeek(
    		DataSerializer dailyData,
    		int startDay,
    		int numDays,
    		int week,
    		Collection<TagGroup> tagGroups,
    		List<Map<TagGroup, DataSerializer.CostAndUsage>> weekly) {
    	
        for (int day = 0; day < numDays; day++) {
            Map<TagGroup, DataSerializer.CostAndUsage> prevData = dailyData.getData(startDay + day);
            for (TagGroup tagGroup: tagGroups) {
            	DataSerializer.CostAndUsage v = prevData.get(tagGroup);
                if (v != null) {
                    addValue(weekly, week, tagGroup, v);
                }
            }
        }
    }
    
    protected DataWriter getDataWriter(String name, ReadWriteDataSerializer data, boolean load) throws Exception {
        return new DataWriter(name, data, load, workBucketConfig, accountService, productService);
    }
    
    protected void archiveSummaryProduct(DateTime monthDateTime, DateTime startDate, Product product, ReadWriteData costData, ReadWriteData usageData) throws Exception {
    	// Generate the full set of tag groups across both cost and usage
    	Set<TagGroup> tagGroups = Sets.newHashSet();
    	if (costData != null)
    		tagGroups.addAll(costData.getTagGroups());
    	if (usageData != null)
    		tagGroups.addAll(usageData.getTagGroups());
    	
        // init daily, weekly and monthly
        List<Map<TagGroup, DataSerializer.CostAndUsage>> daily = Lists.newArrayList();
        List<Map<TagGroup, DataSerializer.CostAndUsage>> weekly = Lists.newArrayList();
        List<Map<TagGroup, DataSerializer.CostAndUsage>> monthly = Lists.newArrayList();

        // get last month data so we can properly update weekly data for weeks that span two months
        int year = monthDateTime.getYear();
        DataSerializer dailyData = null;
        DataWriter writer = null;
        int daysFromLastMonth = monthDateTime.getDayOfWeek() - 1; // Monday is first day of week == 1
        String prodName = getProdName(product);
        int numUserTags = product == null ? 0 : getNumUserTags(); // only resource data has user tags
        
        if (monthDateTime.isAfter(startDate)) {
            int lastMonthYear = monthDateTime.minusMonths(1).getYear();
            int lastMonthNumDays = monthDateTime.minusMonths(1).dayOfMonth().getMaximumValue();
            int lastMonthDayOfYear = monthDateTime.minusMonths(1).getDayOfYear();
            int startDay = lastMonthDayOfYear + lastMonthNumDays - daysFromLastMonth - 1;
            
            dailyData = new DataSerializer(numUserTags);
            writer = getDataWriter("daily_" + prodName + "_" + lastMonthYear, dailyData, true);
            getPartialWeek(dailyData, startDay, daysFromLastMonth, 0, tagGroups, weekly);
            if (year != lastMonthYear) {
            	writer.delete(); // don't need local copy of last month daily data any more
            	writer = null;
            	dailyData = null;
            }
        }
        int daysInNextMonth = 7 - (monthDateTime.plusMonths(1).getDayOfWeek() - 1);
        if (daysInNextMonth == 7)
        	daysInNextMonth = 0;
        
        DataSerializer data = new DataSerializer(costData, usageData, tagGroups);
        aggregateSummaryData(data, daysFromLastMonth, daily, weekly, monthly);
        
        if (daysInNextMonth > 0) {
        	// See if we have data processed for the following month that needs to be added to the last week of this month
        	if (monthDateTime.getMonthOfYear() < 12) {
        		if (writer == null) {
                    dailyData = new DataSerializer(numUserTags);
                    writer = getDataWriter("daily_" + prodName + "_" + year, dailyData, true);
                    int monthDayOfYear = monthDateTime.plusMonths(1).getDayOfYear() - 1;
                    if (dailyData.getNum() > monthDayOfYear)
                    	getPartialWeek(dailyData, monthDayOfYear, daysInNextMonth, weekly.size() - 1, tagGroups, weekly);
        		}
        	}
        	else {
        		DataSerializer nextYearDailyData = new DataSerializer(numUserTags);
                DataWriter nextYearWriter = getDataWriter("daily_" + prodName + "_" + (year + 1), nextYearDailyData, true);
                if (nextYearDailyData.getNum() > 0)
                	getPartialWeek(nextYearDailyData, 0, daysInNextMonth, weekly.size() - 1, tagGroups, weekly);
                nextYearWriter.delete();
        	}
        }
        
        // archive daily
        if (writer == null) {
            dailyData = new DataSerializer(numUserTags);
            writer = getDataWriter("daily_" + prodName + "_" + year, dailyData, true);
        }
        dailyData.setData(daily, monthDateTime.getDayOfYear() -1);
        writer.archive();

        // archive monthly
        DataSerializer monthlyData = new DataSerializer(numUserTags);
        int numMonths = Months.monthsBetween(startDate, monthDateTime).getMonths();            
        writer = getDataWriter("monthly_" + prodName, monthlyData, true);
        monthlyData.setData(monthly, numMonths);            
        writer.archive();

        // archive weekly
        DateTime weekStart = monthDateTime.withDayOfWeek(1);
        int index;
        if (!weekStart.isAfter(startDate))
            index = 0;
        else
            index = Weeks.weeksBetween(startDate, weekStart).getWeeks() + (startDate.dayOfWeek() == weekStart.dayOfWeek() ? 0 : 1);
        DataSerializer weeklyData = new DataSerializer(numUserTags);
        writer = getDataWriter("weekly_" + prodName, weeklyData, true);
        weeklyData.setData(weekly, index);
        writer.archive();
    }
    
    private Future<Status> archiveSummaryProductFuture(final DateTime monthDateTime, final DateTime startDate, final Product product,
    		final ReadWriteData costData, final ReadWriteData usageData, ExecutorService pool) {
    	return pool.submit(new Callable<Status>() {
    		@Override
    		public Status call() {
    			try {
    				archiveSummaryProduct(monthDateTime, startDate, product, costData, usageData);  
    			}
    			catch (Exception e) {
    				e.printStackTrace();
    				return new Status(getProdName(product), e);
    			}
				return new Status(getProdName(product));
    		}
        });
    }
    
    private void addTagCoverageValue(List<Map<TagGroup, TagCoverageMetrics>> list, int index, TagGroup tagGroup, TagCoverageMetrics v) {
        Map<TagGroup, TagCoverageMetrics> map = ReadWriteTagCoverageData.getCreateData(list, index);
        TagCoverageMetrics existedV = map.get(tagGroup);
        if (existedV == null)
        	map.put(tagGroup, v);
        else
        	existedV.add(v);
    }


    /**
     * Archive summary data for tag coverage. For tag coverage, we don't keep hourly data.
     */
    private void archiveSummaryTagCoverage(DateTime startDate, ExecutorService pool, List<Future<Status>> futures) {
    	if (tagCoverage == null) {
    		return;
    	}

        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);

        for (Product product: tagCoverage.keySet()) {

            ReadWriteTagCoverageData data = tagCoverage.get(product);
            
            futures.add(archiveSummaryTagCoverageProduct(monthDateTime, startDate, getProdName(product), data, pool));
        }
        
    }
    
    private Future<Status> archiveSummaryTagCoverageProduct(final DateTime monthDateTime, final DateTime startDate, final String prodName, 
    		final ReadWriteTagCoverageData data, ExecutorService pool) {
    	return pool.submit(new Callable<Status>() {
    		@Override
    		public Status call() {
    			try {
	    	        int numUserTags = getNumUserTags();
	                Collection<TagGroup> tagGroups = data.getTagGroups();
	
	                // init daily, weekly and monthly
	                List<Map<TagGroup, TagCoverageMetrics>> daily = Lists.newArrayList();
	                List<Map<TagGroup, TagCoverageMetrics>> weekly = Lists.newArrayList();
	                List<Map<TagGroup, TagCoverageMetrics>> monthly = Lists.newArrayList();
	
	                int dayOfWeek = monthDateTime.getDayOfWeek();
	                int daysFromLastMonth = dayOfWeek - 1;
	                DataWriter writer = null;
	                
	                if (daysFromLastMonth > 0) {
	                	// Get the daily data from last month so we can add it to the weekly data
	                	DateTime previousMonthStartDay = monthDateTime.minusDays(daysFromLastMonth);
	    	            int previousMonthYear = previousMonthStartDay.getYear();
	    	            
	    	            ReadWriteTagCoverageData previousDailyData = new ReadWriteTagCoverageData(numUserTags);
	    	            writer = getDataWriter("coverage_daily_" + prodName + "_" + previousMonthYear, previousDailyData, true);
	    	            
	    	            int day = previousMonthStartDay.getDayOfYear();
	    	            for (int i = 0; i < daysFromLastMonth; i++) {
	                        Map<TagGroup, TagCoverageMetrics> prevData = previousDailyData.getData(day);
	                        day++;
	                        for (TagGroup tagGroup: tagGroups) {
	                            TagCoverageMetrics v = prevData.get(tagGroup);
	                            if (v != null) {
	                            	addTagCoverageValue(weekly, 0, tagGroup, v);
	                            }
	                        }
	    	            }
	                }
	                
	                // aggregate to daily, weekly and monthly
	                for (int hour = 0; hour < data.getNum(); hour++) {
	                    // this month, add to weekly, monthly and daily
	                    Map<TagGroup, TagCoverageMetrics> map = data.getData(hour);
	
	                    for (TagGroup tagGroup: tagGroups) {
	                        TagCoverageMetrics v = map.get(tagGroup);
	                        if (v != null) {
	                        	addTagCoverageValue(monthly, 0, tagGroup, v);
	                        	addTagCoverageValue(daily, hour/24, tagGroup, v);
	                        	addTagCoverageValue(weekly, (hour + daysFromLastMonth*24) / 24/7, tagGroup, v);
	                        }
	                    }
	                }
	                
	                // archive daily
	                ReadWriteTagCoverageData dailyData = new ReadWriteTagCoverageData(numUserTags);
	                writer = getDataWriter("coverage_daily_" + prodName + "_" + monthDateTime.getYear(), dailyData, true);
	                dailyData.setData(daily, monthDateTime.getDayOfYear() -1);
	                writer.archive();
	
	                // archive monthly
	                ReadWriteTagCoverageData monthlyData = new ReadWriteTagCoverageData(numUserTags);
	                int numMonths = Months.monthsBetween(startDate, monthDateTime).getMonths();            
	                writer = getDataWriter("coverage_monthly_" + prodName, monthlyData, true);
	                monthlyData.setData(monthly, numMonths);            
	                writer.archive();
	
	                // archive weekly
	                DateTime weekStart = monthDateTime.withDayOfWeek(1);
	                int index;
	                if (!weekStart.isAfter(startDate))
	                    index = 0;
	                else
	                    index = Weeks.weeksBetween(startDate, weekStart).getWeeks() + (startDate.dayOfWeek() == weekStart.dayOfWeek() ? 0 : 1);
	                ReadWriteTagCoverageData weeklyData = new ReadWriteTagCoverageData(numUserTags);
	                writer = getDataWriter("coverage_weekly_" + prodName, weeklyData, true);
	                weeklyData.setData(weekly, index);
	                writer.archive();
	            }
	    		catch (Exception e) {
    				e.printStackTrace();
	        		return new Status("coverage_<interval>_" + prodName, e);
	    		}
	    		return new Status("coverage_<interval>_" + prodName);
    		}
    	});
    }

    private void archiveReservations() throws IOException {
        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
        File file = new File(workBucketConfig.localDir, "reservations_" + AwsUtils.monthDateFormat.print(monthDateTime) + ".csv");
        
    	OutputStream os = new FileOutputStream(file);
		Writer out = new OutputStreamWriter(os);
        
        try {
        	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(Reservation.header()));
        	for (Reservation ri: reservations.values()) {
        		printer.printRecord((Object[]) ri.values());
        	}
      	
        	printer.close(true);
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, workBucketConfig.localDir, file.getName());
        logger.info("uploaded " + file);
    }
 
    private void archiveSavingsPlans() throws IOException {
        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
        File file = new File(workBucketConfig.localDir, "savingsPlans_" + AwsUtils.monthDateFormat.print(monthDateTime) + ".csv");
        
    	OutputStream os = new FileOutputStream(file);
		Writer out = new OutputStreamWriter(os);
        
        try {
        	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(SavingsPlan.header()));
        	for (SavingsPlan sp: savingsPlans.values()) {
        		printer.printRecord((Object[]) sp.values());
        	}
      	
        	printer.close(true);
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, workBucketConfig.localDir, file.getName());
        logger.info("uploaded " + file);
    }
    
    private void archivePostProcessorStats() throws IOException {
        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
        File file = new File(workBucketConfig.localDir, "postProcessorStats_" + AwsUtils.monthDateFormat.print(monthDateTime) + ".csv");
        
    	OutputStream os = new FileOutputStream(file);
		Writer out = new OutputStreamWriter(os);
        
        try {
        	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(PostProcessorStats.header()));
        	for (PostProcessorStats pps: postProcessorStats) {
        		printer.printRecord((Object[]) pps.values());
        	}
      	
        	printer.close(true);
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, workBucketConfig.localDir, file.getName());
        logger.info("uploaded " + file);
    }
    
    public void addPostProcessorStats(PostProcessorStats stats) {
    	postProcessorStats.add(stats);
    }
    
	public enum RuleType {
		Fixed,
		Variable;
	}
	
    public static class PostProcessorStats {
    	private String ruleName;
    	private RuleType ruleType;
    	private int in;
    	private int out;
    	private boolean isNonResource;
    	private String info;
    	
    	public PostProcessorStats(String ruleName, RuleType ruleType, boolean isNonResource, int in, int out, String info) {
    		this.ruleName = ruleName;
    		this.ruleType = ruleType;
    		this.isNonResource = isNonResource;
    		this.in = in;
    		this.out = out;
    		this.info = info;
    	}
    	
        public static String[] header() {
    		return new String[] {"Name", "Rule Type", "User Tags", "In", "Out", "Info"};
        }
        
        public String[] values() {
    		return new String[]{ ruleName, ruleType.toString(), isNonResource ? "no" : "yes", Integer.toString(in), Integer.toString(out), info };
        }
    }
}



