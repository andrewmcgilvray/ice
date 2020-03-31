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
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import com.netflix.ice.processor.ProcessorConfig.JsonFileType;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ReservationArn;

public class CostAndUsageData {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final long startMilli;
    private Map<Product, ReadWriteData> usageDataByProduct;
    private Map<Product, ReadWriteData> costDataByProduct;
    private final WorkBucketConfig workBucketConfig;
    private final AccountService accountService;
    private final ProductService productService;
    private Map<Product, ReadWriteTagCoverageData> tagCoverage;
    private List<String> userTags;
    private boolean collectTagCoverageWithUserTags;
    private Map<ReservationArn, Reservation> reservations;
    private Map<String, SavingsPlan> savingsPlans;
    
	public CostAndUsageData(long startMilli, WorkBucketConfig workBucketConfig, List<String> userTags, Config.TagCoverage tagCoverage, AccountService accountService, ProductService productService) {
		this.startMilli = startMilli;
		this.usageDataByProduct = Maps.newHashMap();
		this.costDataByProduct = Maps.newHashMap();
        this.usageDataByProduct.put(null, new ReadWriteData());
        this.costDataByProduct.put(null, new ReadWriteData());
        this.workBucketConfig = workBucketConfig;
        this.accountService = accountService;
        this.productService = productService;
        this.tagCoverage = null;
        this.userTags = userTags;
        this.collectTagCoverageWithUserTags = tagCoverage == TagCoverage.withUserTags;
        if (userTags != null && tagCoverage != TagCoverage.none) {
    		this.tagCoverage = Maps.newHashMap();
        	this.tagCoverage.put(null, new ReadWriteTagCoverageData(userTags.size()));
        }
        this.reservations = Maps.newHashMap();
        this.savingsPlans = Maps.newHashMap();
	}
	
	public long getStartMilli() {
		return startMilli;
	}
	
	public ReadWriteData getUsage(Product product) {
		return usageDataByProduct.get(product);
	}
	
	public void putUsage(Product product, ReadWriteData data) {
		usageDataByProduct.put(product,  data);
	}
	
	public ReadWriteData getCost(Product product) {
		return costDataByProduct.get(product);
	}
	
	public void putCost(Product product, ReadWriteData data) {
		costDataByProduct.put(product,  data);
	}
	
	public ReadWriteTagCoverageData getTagCoverage(Product product) {
		return tagCoverage.get(product);
	}
	
	public void putTagCoverage(Product product, ReadWriteTagCoverageData data) {
		tagCoverage.put(product,  data);
	}
	
	
	public void putAll(CostAndUsageData data) {
		// Add all the data from the supplied CostAndUsageData
		
		for (Entry<Product, ReadWriteData> entry: data.usageDataByProduct.entrySet()) {
			ReadWriteData usage = getUsage(entry.getKey());
			if (usage == null) {
				usageDataByProduct.put(entry.getKey(), entry.getValue());
			}
			else {
				usage.putAll(entry.getValue());
			}
		}
		for (Entry<Product, ReadWriteData> entry: data.costDataByProduct.entrySet()) {
			ReadWriteData cost = getCost(entry.getKey());
			if (cost == null) {
				costDataByProduct.put(entry.getKey(), entry.getValue());
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
				}
				else {
					tc.putAll(entry.getValue());
				}
			}	
		}
		reservations.putAll(data.reservations);
		savingsPlans.putAll(data.savingsPlans);
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
    
    public void addSavingsPlan(String arn, PurchaseOption paymentOption, String hourlyRecurringFee, String hourlyAmortization) {
    	if (savingsPlans.containsKey(arn))
    		return;
		SavingsPlan plan = new SavingsPlan(arn,
				paymentOption,
				Double.parseDouble(hourlyRecurringFee), 
				Double.parseDouble(hourlyAmortization));
    	savingsPlans.put(arn, plan);
    }
    
    public Map<String, SavingsPlan> getSavingsPlans() {
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
    		data = new ReadWriteTagCoverageData(userTags == null ? 0 : userTags.size());
    		tagCoverage.put(product, data);
    	}
    	
    	data.put(index, tagGroup, TagCoverageMetrics.add(data.get(index, tagGroup), userTagCoverage));
    }
    
    class Status {
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
    	
    	Map<ReadWriteData, Collection<TagGroup>> tagGroupsCache = cacheTagGroups();
    	
    	ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    	List<Future<Status>> futures = Lists.newArrayList();
    	
    	for (JsonFileType jft: jsonFiles)
        	futures.add(archiveJson(jft, instanceMetrics, priceListService, pool));
    	
        for (Product product: costDataByProduct.keySet()) {
        	futures.add(archiveTagGroups(startMilli, product, tagGroupsCache.get(costDataByProduct.get(product)), pool));
        }

        archiveHourly(usageDataByProduct, "usage_", archiveHourlyData, pool, futures);
        archiveHourly(costDataByProduct, "cost_", archiveHourlyData, pool, futures);  
    
        archiveSummary(startDate, usageDataByProduct, "usage_", tagGroupsCache, pool, futures);
        archiveSummary(startDate, costDataByProduct, "cost_", tagGroupsCache, pool, futures);
        archiveSummaryTagCoverage(startDate, pool, futures);

		// Wait for completion
		for (Future<Status> f: futures) {
			Status s = f.get();
			if (s.failed) {
				logger.error("Error archiving file: " + s);
			}
		}
    }
    
    private Map<ReadWriteData, Collection<TagGroup>> cacheTagGroups() throws Exception {
    	Map<ReadWriteData, Collection<TagGroup>> tagGroupsCache = Maps.newHashMap();
    	
        for (Product product: costDataByProduct.keySet()) {
        	boolean verify = product == null || product.isEc2Instance() || product.isRdsInstance() || product.isRedshift() || product.isElastiCache() || product.isElasticsearch();
        	
            Collection<TagGroup> tagGroups = costDataByProduct.get(product).getSortedTagGroups();
            tagGroupsCache.put(costDataByProduct.get(product), tagGroups);
            
        	if (verify)
        		verifyTagGroups(tagGroups);

        	if (usageDataByProduct.get(product) != null) {
	            tagGroups = usageDataByProduct.get(product).getSortedTagGroups();
	            tagGroupsCache.put(usageDataByProduct.get(product), tagGroups);
        	}
            if (verify)
            	verifyTagGroups(tagGroups);
        }
        return tagGroupsCache;
    }
    
    /**
     * Make sure all SP and RI TagGroups have been aggregated back to regular TagGroups
     * @param tagGroups
     * @throws Exception
     */
    private void verifyTagGroups(Collection<TagGroup> tagGroups) throws Exception {
        for (TagGroup tg: tagGroups) {
    		// verify that all the tag groups are instances of TagGroup and not TagGroupArn
        	if (tg instanceof TagGroupRI || tg instanceof TagGroupSP)
        		throw new Exception("Non-baseclass tag group in archive cost data: " + tg);
        }
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
	    	        		monthDateTime, userTags, writeJsonFiles, costDataByProduct, usageDataByProduct, instanceMetrics, priceListService, workBucketConfig);
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
    			String name = product == null ? "all" : product.getServiceCode();
    			try {
	                TagGroupWriter writer = new TagGroupWriter(name, true, workBucketConfig, accountService, productService);
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
    
    private void archiveHourly(Map<Product, ReadWriteData> dataMap, String prefix, boolean archiveHourlyData, ExecutorService pool, List<Future<Status>> futures) {
        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);
        for (Product product: dataMap.keySet()) {
        	if (!archiveHourlyData && product != null)
        		continue;
        	
            String prodName = product == null ? "all" : product.getServiceCode();
            String name = prefix + "hourly_" + prodName + "_" + AwsUtils.monthDateFormat.print(monthDateTime);
            futures.add(archiveHourlyFile(name, dataMap.get(product), pool));
        }
    }
    
    private Future<Status> archiveHourlyFile(final String name, final ReadWriteDataSerializer data, ExecutorService pool) {
    	return pool.submit(new Callable<Status>() {
    		@Override
    		public Status call() {
    			try {
	                DataWriter writer = getDataWriter(name, data, false);
	                writer.archive();
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

    protected void addValue(List<Map<TagGroup, Double>> list, int index, TagGroup tagGroup, double v) {
        Map<TagGroup, Double> map = ReadWriteData.getCreateData(list, index);
        Double existedV = map.get(tagGroup);
        map.put(tagGroup, existedV == null ? v : existedV + v);
    }


    private void archiveSummary(DateTime startDate, Map<Product, ReadWriteData> dataMap, String prefix, Map<ReadWriteData, 
    		Collection<TagGroup>> tagGroupsCache, ExecutorService pool, List<Future<Status>> futures) {

        DateTime monthDateTime = new DateTime(startMilli, DateTimeZone.UTC);

        // Queue the non-resource version first because it takes longer and we
        // don't like to have it last with other threads idle.
        ReadWriteData data = dataMap.get(null);
        futures.add(archiveSummaryProductFuture(monthDateTime, startDate, "all", data, prefix, tagGroupsCache.get(data), pool));
                
        for (Product product: dataMap.keySet()) {
        	if (product == null)
        		continue;
        	
            data = dataMap.get(product);            
            futures.add(archiveSummaryProductFuture(monthDateTime, startDate, product.getServiceCode(), data, prefix, tagGroupsCache.get(data), pool));
        }
    }
    
    protected void aggregateSummaryData(
    		ReadWriteData data,
    		Collection<TagGroup> tagGroups,
    		int daysFromLastMonth,
            List<Map<TagGroup, Double>> daily,
            List<Map<TagGroup, Double>> weekly,
            List<Map<TagGroup, Double>> monthly
    		) {
        // aggregate to daily, weekly and monthly
        for (int hour = 0; hour < data.getNum(); hour++) {
            // this month, add to weekly, monthly and daily
            Map<TagGroup, Double> map = data.getData(hour);

            for (TagGroup tagGroup: tagGroups) {
                Double v = map.get(tagGroup);
                if (v != null && v != 0) {
                    addValue(monthly, 0, tagGroup, v);
                    addValue(daily, hour/24, tagGroup, v);
                    addValue(weekly, (hour + daysFromLastMonth*24) / 24/7, tagGroup, v);
                }
            }
        }
    }
    
    protected void getPartialWeek(
    		ReadWriteData dailyData,
    		int startDay,
    		int numDays,
    		int week,
    		Collection<TagGroup> tagGroups,
    		List<Map<TagGroup, Double>> weekly) {
    	
        for (int day = 0; day < numDays; day++) {
            Map<TagGroup, Double> prevData = dailyData.getData(startDay + day);
            for (TagGroup tagGroup: tagGroups) {
                Double v = prevData.get(tagGroup);
                if (v != null && v != 0) {
                    addValue(weekly, week, tagGroup, v);
                }
            }
        }
    }
    
    protected DataWriter getDataWriter(String name, ReadWriteDataSerializer data, boolean load) throws Exception {
        return new DataWriter(name, data, load, workBucketConfig, accountService, productService);
    }

    protected void archiveSummaryProduct(DateTime monthDateTime, DateTime startDate, String prodName, ReadWriteData data, String prefix, Collection<TagGroup> tagGroups) throws Exception {
        // init daily, weekly and monthly
        List<Map<TagGroup, Double>> daily = Lists.newArrayList();
        List<Map<TagGroup, Double>> weekly = Lists.newArrayList();
        List<Map<TagGroup, Double>> monthly = Lists.newArrayList();

        // get last month data so we can properly update weekly data for weeks that span two months
        int year = monthDateTime.getYear();
        ReadWriteData dailyData = null;
        DataWriter writer = null;
        int daysFromLastMonth = monthDateTime.getDayOfWeek() - 1; // Monday is first day of week == 1
        
        if (monthDateTime.isAfter(startDate)) {
            int lastMonthYear = monthDateTime.minusMonths(1).getYear();
            int lastMonthNumDays = monthDateTime.minusMonths(1).dayOfMonth().getMaximumValue();
            int lastMonthDayOfYear = monthDateTime.minusMonths(1).getDayOfYear();
            int startDay = lastMonthDayOfYear + lastMonthNumDays - daysFromLastMonth - 1;
            
            dailyData = new ReadWriteData();
            writer = getDataWriter(prefix + "daily_" + prodName + "_" + lastMonthYear, dailyData, true);
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
        
        aggregateSummaryData(data, tagGroups, daysFromLastMonth, daily, weekly, monthly);
        
        if (daysInNextMonth > 0) {
        	// See if we have data processed for the following month that needs to be added to the last week of this month
        	if (monthDateTime.getMonthOfYear() < 12) {
        		if (writer == null) {
                    dailyData = new ReadWriteData();
                    writer = getDataWriter(prefix + "daily_" + prodName + "_" + year, dailyData, true);
                    int monthDayOfYear = monthDateTime.plusMonths(1).getDayOfYear() - 1;
                    if (dailyData.getNum() > monthDayOfYear)
                    	getPartialWeek(dailyData, monthDayOfYear, daysInNextMonth, weekly.size() - 1, tagGroups, weekly);
        		}
        	}
        	else {
                ReadWriteData nextYearDailyData = new ReadWriteData();
                DataWriter nextYearWriter = getDataWriter(prefix + "daily_" + prodName + "_" + (year + 1), nextYearDailyData, true);
                if (nextYearDailyData.getNum() > 0)
                	getPartialWeek(nextYearDailyData, 0, daysInNextMonth, weekly.size() - 1, tagGroups, weekly);
                nextYearWriter.delete();
        	}
        }
        
        // archive daily
        if (writer == null) {
            dailyData = new ReadWriteData();
            writer = getDataWriter(prefix + "daily_" + prodName + "_" + year, dailyData, true);
        }
        dailyData.setData(daily, monthDateTime.getDayOfYear() -1);
        writer.archive();

        // archive monthly
        ReadWriteData monthlyData = new ReadWriteData();
        int numMonths = Months.monthsBetween(startDate, monthDateTime).getMonths();            
        writer = getDataWriter(prefix + "monthly_" + prodName, monthlyData, true);
        monthlyData.setData(monthly, numMonths);            
        writer.archive();

        // archive weekly
        DateTime weekStart = monthDateTime.withDayOfWeek(1);
        int index;
        if (!weekStart.isAfter(startDate))
            index = 0;
        else
            index = Weeks.weeksBetween(startDate, weekStart).getWeeks() + (startDate.dayOfWeek() == weekStart.dayOfWeek() ? 0 : 1);
        ReadWriteData weeklyData = new ReadWriteData();
        writer = getDataWriter(prefix + "weekly_" + prodName, weeklyData, true);
        weeklyData.setData(weekly, index);
        writer.archive();
    }
    
    private Future<Status> archiveSummaryProductFuture(final DateTime monthDateTime, final DateTime startDate, final String prodName,
    		final ReadWriteData data, final String prefix, final Collection<TagGroup> tagGroups, ExecutorService pool) {
    	return pool.submit(new Callable<Status>() {
    		@Override
    		public Status call() {
    			try {
    				archiveSummaryProduct(monthDateTime, startDate, prodName, data, prefix, tagGroups);  
    			}
    			catch (Exception e) {
    				e.printStackTrace();
    				return new Status(prefix + "<interval>_" + prodName, e);
    			}
				return new Status(prefix + "<interval>_" + prodName);
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

            String prodName = product == null ? "all" : product.getServiceCode();
            ReadWriteTagCoverageData data = tagCoverage.get(product);
            
            futures.add(archiveSummaryTagCoverageProduct(monthDateTime, startDate, prodName, data, pool));
        }
        
    }
    
    private Future<Status> archiveSummaryTagCoverageProduct(final DateTime monthDateTime, final DateTime startDate, final String prodName, 
    		final ReadWriteTagCoverageData data, ExecutorService pool) {
    	return pool.submit(new Callable<Status>() {
    		@Override
    		public Status call() {
    			try {
	    	        int numUserTags = userTags == null ? 0 : userTags.size();
	                Collection<TagGroup> tagGroups = data.getSortedTagGroups();
	
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

}



