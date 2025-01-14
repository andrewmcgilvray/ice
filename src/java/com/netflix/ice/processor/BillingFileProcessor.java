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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.*;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.processor.postproc.PostProcessor;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Product;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.StopWatch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Class to process billing files and produce tag, usage, cost output files for reader/UI.
 */
public class BillingFileProcessor extends Poller {
    protected static Logger staticLogger = LoggerFactory.getLogger(BillingFileProcessor.class);

    private final ProcessorConfig config;
    private final WorkBucketConfig workBucketConfig;
    private Long startMilli;
    /**
     * The usageDataByProduct map holds both the usage data for each
     * individual product that has resourceIDs (if ResourceService is enabled) and a "null"
     * key entry for aggregated data for "all" services.
     * i.e. the null key means "all"
     */
    private CostAndUsageData costAndUsageData;
    private Instances instances;
    
    private final MonthlyReportProcessor cauProcessor;
    

    public BillingFileProcessor(ProcessorConfig config) throws Exception {
    	this.config = config;
    	this.workBucketConfig = config.workBucketConfig;
        
        cauProcessor = new CostAndUsageReportProcessor(config);
    }

    @Override
    protected void poll() {
    	try {
			processReports();
		} catch (Exception e1) {
			logger.error("Failed to process reports: " + e1);
			e1.printStackTrace();
		}
    	
        if (config.processOnce) {
        	// We're done. If we're running on an AWS EC2 instance, stop the instance
            logger.info("Stopping EC2 Instance " + config.processorInstanceId + " in region " + config.processorRegion);
            
            AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
            		.withRegion(config.processorRegion)
            		.withCredentials(AwsUtils.awsCredentialsProvider)
            		.withClientConfiguration(AwsUtils.clientConfig)
            		.build();

            try {
	            StopInstancesRequest request = new StopInstancesRequest().withInstanceIds(config.processorInstanceId);
	            ec2.stopInstances(request);
            }
            catch (Exception e) {
                logger.error("error in stopInstances", e);
            }
            ec2.shutdown();
        }
    }
    
    private void processReports() throws Exception {
        boolean wroteConfig = false;
        TreeMap<DateTime, List<MonthlyReport>> reportsToProcess = cauProcessor.getReportsToProcess();        
        
        for (DateTime dataTime: reportsToProcess.keySet()) {
        	try {
        		wroteConfig = processMonth(dataTime, reportsToProcess.get(dataTime), reportsToProcess.lastKey());
        	}
        	catch (Exception e) {
        		logger.error("Error processing report for month " + dataTime + ", " + e);
        		e.printStackTrace();
        	}
	    }
	    if (!wroteConfig) {
	    	// No reports to process. We still want to update the work bucket config in case
	    	// changes were made to the account configurations.
	        config.saveWorkBucketDataConfig();        	
	    }

	    logger.info("AWS usage processed.");
    }
    
    private boolean processMonth(DateTime month, List<MonthlyReport> reports, DateTime latestMonth) throws Exception {
    	StopWatch sw = new StopWatch();
    	sw.start();

    	Long endMilli = month.getMillis();
        startMilli = endMilli;
        init(startMilli);
        
        ProcessorStatus ps = getProcessorStatus(AwsUtils.monthDateFormat.print(month));

        long lastProcessed = ps == null || ps.reprocess ? 0 : new DateTime(ps.getLastProcessed(), DateTimeZone.UTC).getMillis();
        DateTime processTime = new DateTime(DateTimeZone.UTC);

        boolean hasTags = false;
        boolean hasNewFiles = false;
        for (MonthlyReport report: reports) {
        	hasTags |= report.hasTags();
        	
            if (report.getLastModifiedMillis() < lastProcessed) {
                logger.info("data has been processed. ignoring " + report.getReportKey() + "...");
                continue;
            }
            hasNewFiles = true;
        }
        
        if (!hasNewFiles) {
            logger.info("data has been processed. ignoring all files at " + AwsUtils.monthDateFormat.print(month));
            return false;
        }
        
        for (MonthlyReport report: reports) {
        	long end = report.getProcessor().downloadAndProcessReport(month, report, workBucketConfig.localDir, lastProcessed, costAndUsageData, instances);
            endMilli = Math.max(endMilli, end);
        }
    	
        if (month.equals(latestMonth)) {
            int hours = (int) ((endMilli - startMilli)/3600000L);
	        String start = LineItem.amazonBillingDateFormat.print(new DateTime(startMilli));
	        String end = LineItem.amazonBillingDateFormat.print(new DateTime(endMilli));

            logger.info("cut hours to " + hours + ", " + start + " to " + end);
            costAndUsageData.cutData(hours);
        }
        
        
        
        /* Debugging */
//            ReadWriteData costData = costDataByProduct.get(null);
//            Map<TagGroup, Double> costMap = costData.getData(0);
//            TagGroup redshiftHeavyTagGroup = new TagGroup(config.accountService.getAccountByName("IntegralReach"), Region.US_EAST_1, null, Product.redshift, Operation.reservedInstancesHeavy, UsageType.getUsageType("dc1.8xlarge", Operation.reservedInstancesHeavy, ""), null);
//            Double used = costMap.get(redshiftHeavyTagGroup);
//            logger.info("First hour cost is " + used + " for " + redshiftHeavyTagGroup + " before reservation processing");
        
        // now get reservation capacity to calculate upfront and un-used cost
        
        // Get the reservation processor from the first report
        ReservationProcessor reservationProcessor = reports.get(0).getProcessor().getReservationProcessor();
        ReservationService reservationService = config.reservationService;
        if (costAndUsageData.hasReservations()) {
        	// Use the reservations pulled from the CUR rather than those pulled by the capacity poller from the individual accounts.
        	logger.info("Process " + costAndUsageData.getReservations().size() + " reservations pulled from the CUR");
        	reservationService = new BasicReservationService(costAndUsageData.getReservations());
        }
        else {
        	logger.info("Process reservations pulled from the accounts");
        }
        SavingsPlanProcessor savingsPlanProcessor = new SavingsPlanProcessor(costAndUsageData, config.accountService);

		// Initialize the price lists
    	Map<Product, InstancePrices> prices = Maps.newHashMap();
    	for (ServiceCode sc: ServiceCode.values()) {
    		// EC2 and RDS Instances are broken out into separate products, so need to grab those
    		Product prod;
    		switch (sc) {
    		case AmazonEC2:
        		prod = config.productService.getProduct(Product.Code.Ec2Instance);
        		break;
    		case AmazonRDS:
    			prod = config.productService.getProduct(Product.Code.RdsInstance);
    			break;
    		default:
    			prod = config.productService.getProductByServiceCode(sc.name());
    			break;
    		}
    		
        	if (reservationService.hasReservations(prod)) {
        		if (!costAndUsageData.hasReservations()) {
        			// Using reservation data pulled from accounts. Need to also have pricing data
        			prices.put(prod, config.priceListService.getPrices(month, sc));
        		}
            	reservationProcessor.process(reservationService, costAndUsageData, prod, month, prices);
        	}
    	}
    	// Process resource version of data for products that SPs apply to
    	for (Product p: costAndUsageData.getSavingsPlanProducts()) {
        	if (costAndUsageData.get(p) != null)
        		savingsPlanProcessor.process(p);
    	}
    	
    	// Process non-resource version of data for RIs and SPs
    	reservationProcessor.process(reservationService, costAndUsageData, null, month, prices);
    	savingsPlanProcessor.process(null);
    	            
        logger.info("adding savings data for " + month + "...");
        addSavingsData(costAndUsageData, null, config.priceListService.getPrices(month, ServiceCode.AmazonEC2));
        addSavingsData(costAndUsageData, config.productService.getProduct(Product.Code.Ec2Instance), config.priceListService.getPrices(month, ServiceCode.AmazonEC2));
                
        // Run the post processor
        try {
            PostProcessor pp = new PostProcessor(config.startDate, config.postProcessorRules, config.reportSubPrefix,
                    config.accountService, config.productService, config.resourceService, config.workBucketConfig,
                    config.jsonFiles, config.parquetFiles, config.numthreads);
            pp.process(costAndUsageData);
        }
        catch (Exception e) {
        	logger.error("Error post processing reports" + e);
        	e.printStackTrace();
        }

        if (hasTags && config.resourceService != null)
            config.resourceService.commit();
        
        logger.info("archive product list...");
        config.productService.archive(workBucketConfig.localDir, workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix);

        logger.info("archiving results for " + month + (config.hourlyData ? " with" : " without") + " hourly data...");
        costAndUsageData.archive(config.jsonFiles, config.parquetFiles, config.priceListService.getInstanceMetrics(), config.priceListService, config.numthreads, config.hourlyData);
        
        logger.info("archiving instance data...");
        archiveInstances();
        
        logger.info("done archiving " + month);
        
        // Write out a new config each time we process a report. We may have added accounts or zones while processing.
        config.saveWorkBucketDataConfig();

        List<ProcessorStatus.Report> statusReports = Lists.newArrayList();
        for (MonthlyReport report: reports) {
        	String accountId = report.getS3BucketConfig().getAccountId();
        	String accountName = config.accountService.getAccountById(accountId).getIceName();
        	statusReports.add(new ProcessorStatus.Report(accountName, accountId, report.getReportKey(), new DateTime(report.getLastModifiedMillis(), DateTimeZone.UTC).toString()));
        }
        String monthStr = AwsUtils.monthDateFormat.print(month);
    	
    	sw.stop();
    	logger.info("Process time for month " + month + ": " + sw);
    	
        saveProcessorStatus(monthStr, new ProcessorStatus(monthStr, statusReports, processTime.toString(), sw.toString(), costAndUsageData.getArchiveFailures()));
        
        return true;
    }
    
    private void addSavingsData(CostAndUsageData data, Product product, InstancePrices ec2Prices) {
    	DataSerializer ds = data.get(product);
    	if (ds == null)
    		return;
    	
    	double edpDiscount = config.getDiscount(startMilli);
        
    	/*
    	 * Run through all the spot instance usage and add savings data
    	 */
    	for (TagGroup tg: ds.getTagGroups()) {
    		if (tg.operation == ReservationOperation.spotInstances) {
    			TagGroup savingsTag = TagGroup.getTagGroup(CostType.savings, tg.account, tg.region, tg.zone, tg.product, ReservationOperation.spotInstanceSavings, tg.usageType, tg.resourceGroup);
    			for (int i = 0; i < ds.getNum(); i++) {
    				// For each hour of usage...
    				DataSerializer.CostAndUsage cau = ds.get(i, tg);
    				if (cau != null) {
    					double onDemandRate = ec2Prices.getOnDemandRate(tg.region, tg.usageType);
    					// Don't include the EDP discount on top of the spot savings
    					double edpRate = onDemandRate * (1 - edpDiscount);
    					ds.put(i, savingsTag, new DataSerializer.CostAndUsage(edpRate * cau.usage - cau.cost, 0));
    				}
    			}
    		}
    	}
    }
    

    void init(long startMilli) {
    	costAndUsageData = new CostAndUsageData(config.startDate, startMilli, config.workBucketConfig, config.resourceService == null ? null : config.resourceService.getUserTagKeys(),
    			config.getTagCoverage(), config.accountService, config.productService);
    	costAndUsageData.enableTagGroupCache(true);
        instances = new Instances(workBucketConfig.localDir, workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix);
    }

    private void archiveInstances() throws Exception {
        instances.archive(startMilli); 	
    }

    private ProcessorStatus getProcessorStatus(String timeStr) {
    	String filename = ProcessorStatus.prefix + timeStr + ProcessorStatus.suffix;
    	
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        InputStream in = null;
        try {
            in = s3Client.getObject(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix + filename).getObjectContent();
            return new ProcessorStatus(IOUtils.toString(in, StandardCharsets.UTF_8));
        }
        catch (AmazonServiceException ase) {
        	if (ase.getStatusCode() == 404) {
            	logger.warn("file not found: " + filename);
        	}
        	else {
                logger.error("Error reading from file " + filename, ase);
        	}
            return null;
        }
        catch (Exception e) {
            logger.error("Error reading from file " + filename, e);
            return null;
        }
        finally {
            if (in != null)
                try {in.close();} catch (Exception e){}
        }
    }

    private void saveProcessorStatus(String timeStr, ProcessorStatus status) {
    	String filename = ProcessorStatus.prefix + timeStr + ProcessorStatus.suffix;
    	
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        String statusStr = status.toJSON();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(statusStr.length());

        s3Client.putObject(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix + filename, IOUtils.toInputStream(statusStr, StandardCharsets.UTF_8), metadata);
    }
}

