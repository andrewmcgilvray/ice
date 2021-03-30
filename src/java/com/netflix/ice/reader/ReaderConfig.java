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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.common.*;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UserTagKey;
import com.netflix.ice.tag.Zone.BadZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Configuration class for reader/UI.
 */
public class ReaderConfig extends Config {
    private static ReaderConfig instance;
    public static final Logger logger = LoggerFactory.getLogger(ReaderConfig.class);
    
    private final int numThreads = 16;

    public final BasicAccountService accountService;
    public final DateTime startDate;
    public final String companyName;
    public final String currencySign;
    public final double currencyRate;
    public final String highstockUrl;
    public final ThroughputMetricService throughputMetricService;
    public final Managers managers;
    public final int monthlyCacheSize;
    public List<UserTagKey> userTagKeys;
    public final String dashboardNotice;
    public Map<String, Map<String, TagConfig>> tagConfigs;
    public String processorRegion;
    public String processorInstanceId;
    public final boolean enableReprocessRequests;

    /**
     *
     * @param properties (required)
     * @param managers (required)
     * @param productService (required)
     * @param applicationGroupService (optional)
     * @param throughputMetricService (optional)
     * @throws IOException 
     * @throws InterruptedException 
     * @throws UnsupportedEncodingException 
     * @throws BadZone 
     */
    public ReaderConfig(
            Properties properties,
            AWSCredentialsProvider credentialsProvider,
            Managers managers,
            ProductService productService,
            ThroughputMetricService throughputMetricService) throws UnsupportedEncodingException, InterruptedException, IOException, BadZone {
        super(properties, credentialsProvider, productService);
                
        WorkBucketDataConfig dataConfig = readWorkBucketDataConfig();
        this.startDate = new DateTime(dataConfig.getStartMonth(), DateTimeZone.UTC);
        this.userTagKeys = dataConfig.getUserTagKeys();
        this.tagConfigs = dataConfig.getTagConfigs();
        this.processorRegion = dataConfig.getProcessorRegion();
        this.processorInstanceId = dataConfig.getProcessorInstanceId();
        
        // update tagCoverage to level supported by processor
        switch (dataConfig.getTagCoverage()) {
        case none:
        	setTagCoverage(TagCoverage.none);
        	break;
        case basic:
        	if (getTagCoverage() == TagCoverage.withUserTags)
        		setTagCoverage(TagCoverage.basic);
        	break;
		default:
			break;        	
        }
        
        // Account service is initialized here and refreshed while running by the DataManager
        this.accountService = new BasicAccountService(dataConfig.getAccounts());
        
        updateZones(dataConfig.getZones());

        companyName = properties.getProperty(IceOptions.COMPANY_NAME, "");
        dashboardNotice = properties.getProperty(IceOptions.DASHBOARD_NOTICE, "");
        enableReprocessRequests = Boolean.parseBoolean(properties.getProperty(IceOptions.ENABLE_REPROCESS_REQUESTS, "false"));
        currencySign = properties.getProperty(IceOptions.CURRENCY_SIGN, "$");
        currencyRate = Double.parseDouble(properties.getProperty(IceOptions.CURRENCY_RATE, "1"));
        highstockUrl = properties.getProperty(IceOptions.HIGHSTOCK_URL, "http://code.highcharts.com/stock/highstock.js");

        this.managers = managers;
        this.throughputMetricService = throughputMetricService;
        this.monthlyCacheSize = Integer.parseInt(properties.getProperty(IceOptions.MONTHLY_CACHE_SIZE, "12"));

        ReaderConfig.instance = this;
        
        productService.initReader(workBucketConfig.localDir, workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix);

        if (throughputMetricService != null)
            throughputMetricService.init();
        managers.init();
    }

    /**
     *
     * @return singleton instance
     */
    public static ReaderConfig getInstance() {
        return instance;
    }

    public void start() throws InterruptedException, ExecutionException {

    	// Prime the data caches
        Managers managers = ReaderConfig.getInstance().managers;
        Collection<Product> products = managers.getProducts();        
    	ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    	List<Future<Void>> futures = Lists.newArrayList();

    	
        for (Product product: products) {
            TagGroupManager tagGroupManager = managers.getTagGroupManager(product);
            Interval interval = tagGroupManager.getOverlapInterval(new Interval(new DateTime(DateTimeZone.UTC).minusMonths(monthlyCacheSize), new DateTime(DateTimeZone.UTC)));
            if (interval == null)
                continue;
            boolean loadTagCoverage = (product == null && getTagCoverage() != TagCoverage.none) || (product != null && getTagCoverage() == TagCoverage.withUserTags);
            for (ConsolidateType consolidateType: ConsolidateType.values()) {
            	if (consolidateType == ConsolidateType.hourly && !hourlyData && product != null)
            		continue;
            	futures.add(readData(product, consolidateType, interval, managers.getDataManager(product, consolidateType), null, pool));
                // Prime the tag coverage cache
                if (loadTagCoverage && consolidateType != ConsolidateType.hourly) {
                	futures.add(readData(product, consolidateType, interval, managers.getTagCoverageManager(product, consolidateType), userTagKeys, pool));
                }
            }
        }
		// Wait for completion
		for (Future<Void> f: futures) {
			f.get();
		}
    }
        
    public Future<Void> readData(final Product product, final ConsolidateType consolidateType, final Interval interval, final DataManager dataManager, final List<UserTagKey> userTagList, ExecutorService pool) {
    	return pool.submit(new Callable<Void>() {
    		@Override
    		public Void call() throws Exception {
                readData(product, dataManager, interval, consolidateType, UsageUnit.Instances, userTagList);
                return null;
    		}
    	});
    }

    public void shutdown() {
        logger.info("Shutting down...");

        instance.managers.shutdown();
    }

    private void readData(Product product, DataManager dataManager, Interval interval, ConsolidateType consolidateType, UsageUnit usageUnit, List<UserTagKey> userTagList) {
        if (consolidateType == ConsolidateType.hourly) {
            DateTime start = interval.getStart().withDayOfMonth(1).withMillisOfDay(0);
            do {
                int hours = dataManager.getDataLength(start);
                logger.info("found " + hours + " hours data for " + product + " "  + interval);
                start = start.plusMonths(1);
            }
            while (start.isBefore(interval.getEnd()));
        }
        else if (consolidateType == ConsolidateType.daily) {
            DateTime start = interval.getStart().withDayOfYear(1).withMillisOfDay(0);
            do {
                dataManager.getDataLength(start);
                start = start.plusYears(1);
            }
            while (start.isBefore(interval.getEnd()));
        }
        else {
        	if (userTagList == null)
        		dataManager.getData(true, interval, new TagLists(), null, AggregateType.both, null, usageUnit, 0);
        	else
        		dataManager.getData(true, interval, new TagLists(), null, AggregateType.both, 0, userTagKeys);
        }
    }
    
	protected WorkBucketDataConfig readWorkBucketDataConfig() throws InterruptedException, UnsupportedEncodingException, IOException {
		// Try to download the work bucket data configuration.
		// Keep polling if file doesn't exist yet (Can happen if processor hasn't run yet for the first time)
		WorkBucketDataConfig config = null;
		for (config = downloadWorkBucketDataConfig(true); config == null; config = downloadWorkBucketDataConfig(true)) {
			Thread.sleep(60 * 1000L);
		}
		return config;    	
	}

    public void update() {
    	// Update the account list from the work bucket data configuration file
    	WorkBucketDataConfig config = downloadWorkBucketDataConfig(false);
    	if (config == null)
    		return; // No new configuration changes
    	
    	accountService.updateAccounts(config.getAccounts());
    	updateZones(config.getZones());
        userTagKeys = config.getUserTagKeys();
        tagConfigs = config.getTagConfigs();
        processorRegion = config.getProcessorRegion();
        processorInstanceId = config.getProcessorInstanceId();
        productService.updateReader(workBucketConfig.localDir, workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix);
    }
    
    private void updateZones(Map<String, List<String>> zones) {
    	for (String regionName: zones.keySet()) {
    		Region r = Region.getRegionByName(regionName);
    		if (r == null) {
    			logger.error("Unknown region: " + regionName);
    			continue;
    		}
    		for (String zoneName: zones.get(regionName)) {
    			try {
					r.getZone(zoneName);
				} catch (BadZone e) {
					logger.error("Bad zone " + zoneName + " for region " + r + ": " + e.getMessage());
				}
    		}
    	}
    }
}
