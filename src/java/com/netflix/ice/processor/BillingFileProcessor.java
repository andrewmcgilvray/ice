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
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicReservationService;
import com.netflix.ice.common.*;
import com.netflix.ice.processor.kubernetes.KubernetesProcessor;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.tag.Operation.ReservationOperation;
import com.netflix.ice.tag.Product;

import org.apache.commons.io.IOUtils;
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

    private ProcessorConfig config;
    private boolean compress;
    private Long startMilli;
    private Long endMilli;
    /**
     * The usageDataByProduct map holds both the usage data for each
     * individual product that has resourceIDs (if ResourceService is enabled) and a "null"
     * key entry for aggregated data for "all" services.
     * i.e. the null key means "all"
     */
    private CostAndUsageData costAndUsageData;
    private Instances instances;
    
    private MonthlyReportProcessor dbrProcessor;
    private MonthlyReportProcessor cauProcessor;
    

    public BillingFileProcessor(ProcessorConfig config, boolean compress) throws Exception {
    	this.config = config;
    	this.compress = compress;
        
        dbrProcessor = new DetailedBillingReportProcessor(config);
        cauProcessor = new CostAndUsageReportProcessor(config);
    }

    @Override
    protected void poll() throws Exception {
        TreeMap<DateTime, List<MonthlyReport>> reportsToProcess = dbrProcessor.getReportsToProcess();
        reportsToProcess.putAll(cauProcessor.getReportsToProcess());
                
        for (DateTime dataTime: reportsToProcess.keySet()) {
            startMilli = endMilli = dataTime.getMillis();
            init();
            
            long lastProcessed = lastProcessTime(AwsUtils.monthDateFormat.print(dataTime));
            long processTime = new DateTime(DateTimeZone.UTC).getMillis();

            boolean hasTags = false;
            boolean hasNewFiles = false;
            for (MonthlyReport report: reportsToProcess.get(dataTime)) {
            	hasTags |= report.hasTags();
            	
                if (report.getLastModifiedMillis() < lastProcessed) {
                    logger.info("data has been processed. ignoring " + report.getReportKey() + "...");
                    continue;
                }
                hasNewFiles = true;
            }
            
            if (!hasNewFiles) {
                logger.info("data has been processed. ignoring all files at " + AwsUtils.monthDateFormat.print(dataTime));
                continue;
            }
            
            for (MonthlyReport report: reportsToProcess.get(dataTime)) {
            	long end = report.getProcessor().downloadAndProcessReport(dataTime, report, config.localDir, lastProcessed, costAndUsageData, instances);
                endMilli = Math.max(endMilli, end);
            }
        	
            if (dataTime.equals(reportsToProcess.lastKey())) {
                int hours = (int) ((endMilli - startMilli)/3600000L);
    	        String start = LineItem.amazonBillingDateFormat.print(new DateTime(startMilli));
    	        String end = LineItem.amazonBillingDateFormat.print(new DateTime(endMilli));

                logger.info("cut hours to " + hours + ", " + start + " to " + end);
                costAndUsageData.cutData(hours);
            }
            
            
            
            /***** Debugging */
//            ReadWriteData costData = costDataByProduct.get(null);
//            Map<TagGroup, Double> costMap = costData.getData(0);
//            TagGroup redshiftHeavyTagGroup = new TagGroup(config.accountService.getAccountByName("IntegralReach"), Region.US_EAST_1, null, Product.redshift, Operation.reservedInstancesHeavy, UsageType.getUsageType("dc1.8xlarge", Operation.reservedInstancesHeavy, ""), null);
//            Double used = costMap.get(redshiftHeavyTagGroup);
//            logger.info("First hour cost is " + used + " for " + redshiftHeavyTagGroup + " before reservation processing");
            
            // now get reservation capacity to calculate upfront and un-used cost
            
            // Get the reservation processor from the first report
            ReservationProcessor reservationProcessor = reportsToProcess.get(dataTime).get(0).getProcessor().getReservationProcessor();
            ReservationService reservationService = costAndUsageData.hasReservations() ? new BasicReservationService(costAndUsageData.getReservations()) : config.reservationService;

    		// Initialize the price lists
        	Map<Product, InstancePrices> prices = Maps.newHashMap();
        	for (ServiceCode sc: ServiceCode.values()) {
        		// EC2 and RDS Instances are broken out into separate products, so need to grab those
        		Product prod = null;
        		switch (sc) {
        		case AmazonEC2:
            		prod = config.productService.getProductByName(Product.ec2Instance);
            		break;
        		case AmazonRDS:
        			prod = config.productService.getProductByName(Product.rdsInstance);
        			break;
        		default:
        			prod = config.productService.getProductByServiceCode(sc.name());
        			break;
        		}
        		
            	if (reservationService.hasReservations(prod)) {
            		if (!costAndUsageData.hasReservations()) {
            			// Using reservation data pulled from accounts. Need to also have pricing data
            			prices.put(prod, config.priceListService.getPrices(dataTime, sc));
            		}
                	reservationProcessor.process(reservationService, costAndUsageData, prod, dataTime, prices);
            	}
        	}
        	
        	reservationProcessor.process(reservationService, costAndUsageData, null, dataTime, prices);
        	            
            logger.info("adding savings data for " + dataTime + "...");
            addSavingsData(dataTime, costAndUsageData, null, config.priceListService.getPrices(dataTime, ServiceCode.AmazonEC2));
            addSavingsData(dataTime, costAndUsageData, config.productService.getProductByName(Product.ec2Instance), config.priceListService.getPrices(dataTime, ServiceCode.AmazonEC2));
            
            KubernetesProcessor kubernetesProcessor = new KubernetesProcessor(config, dataTime);
            kubernetesProcessor.downloadAndProcessReports(costAndUsageData);

            if (hasTags && config.resourceService != null)
                config.resourceService.commit();
            
            logger.info("archive product list...");
            config.productService.archive(config.localDir, config.workS3BucketName, config.workS3BucketPrefix);

            logger.info("archiving results for " + dataTime + "...");
            costAndUsageData.archive(startMilli, config.startDate, compress, config.writeJsonFiles, config.priceListService.getInstanceMetrics(), config.priceListService, config.numthreads);
            
            logger.info("archiving instance data...");
            archiveInstances();
            
            logger.info("done archiving " + dataTime);
            
            // Write out a new config in case we added accounts or zones while processing.
            config.saveWorkBucketDataConfig();

            updateProcessTime(AwsUtils.monthDateFormat.print(dataTime), processTime);
        }

        logger.info("AWS usage processed.");
        if (config.processOnce) {
        	// We're done. If we're running on an AWS EC2 instance, stop the instance
            logger.info("Stopping EC2 Instance " + config.processorInstanceId + " in region " + config.processorRegion);
            
            AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
            		.withRegion(config.processorRegion)
            		.withCredentials(AwsUtils.awsCredentialsProvider)
            		.withClientConfiguration(AwsUtils.clientConfig)
            		.build();

            try {
	            StopInstancesRequest request = new StopInstancesRequest().withInstanceIds(new String[] { config.processorInstanceId });
	            ec2.stopInstances(request);
            }
            catch (Exception e) {
                logger.error("error in stopInstances", e);
            }
            ec2.shutdown();
        }
    }
    
    private void addSavingsData(DateTime month, CostAndUsageData data, Product product, InstancePrices ec2Prices) throws Exception {
    	ReadWriteData usageData = data.getUsage(product);
    	ReadWriteData costData = data.getCost(product);
    	
    	double edpDiscount = config.getDiscount(startMilli);
        
    	/*
    	 * Run through all the spot instance usage and add savings data
    	 */
    	for (TagGroup tg: usageData.getTagGroups()) {
    		if (tg.operation == ReservationOperation.spotInstances) {
    			TagGroup savingsTag = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, ReservationOperation.spotInstanceSavings, tg.usageType, tg.resourceGroup);
    			for (int i = 0; i < usageData.getNum(); i++) {
    				// For each hour of usage...
    				Double usage = usageData.getData(i).get(tg);
    				Double cost = costData.getData(i).get(tg);
    				if (usage != null && cost != null) {
    					double onDemandRate = ec2Prices.getOnDemandRate(tg.region, tg.usageType);
    					// Don't include the EDP discount on top of the spot savings
    					double edpRate = onDemandRate * (1 - edpDiscount);
    					costData.getData(i).put(savingsTag, edpRate * usage - cost);
    				}
    			}
    		}
    	}
    }
    

    void init() {
    	costAndUsageData = new CostAndUsageData(config.resourceService == null ? null : config.resourceService.getUserTags(), config.getTagCoverage());
        instances = new Instances(config.localDir, config.workS3BucketName, config.workS3BucketPrefix);
    }

    private void archiveInstances() throws Exception {
        instances.archive(startMilli); 	
    }

    private void updateLastMillis(long millis, String filename) {
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        String millisStr = millis + "";
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(millisStr.length());

        s3Client.putObject(config.workS3BucketName, config.workS3BucketPrefix + filename, IOUtils.toInputStream(millisStr, StandardCharsets.UTF_8), metadata);
    }

    private Long getLastMillis(String filename) {
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        InputStream in = null;
        try {
            in = s3Client.getObject(config.workS3BucketName, config.workS3BucketPrefix + filename).getObjectContent();
            return Long.parseLong(IOUtils.toString(in, StandardCharsets.UTF_8));
        }
        catch (AmazonServiceException ase) {
        	if (ase.getStatusCode() == 404) {
            	logger.warn("file not found: " + filename);
        	}
        	else {
                logger.error("Error reading from file " + filename, ase);
        	}
            return 0L;
        }
        catch (Exception e) {
            logger.error("Error reading from file " + filename, e);
            return 0L;
        }
        finally {
            if (in != null)
                try {in.close();} catch (Exception e){}
        }
    }

    private Long lastProcessTime(String timeStr) {
        return getLastMillis("lastProcessMillis_" + timeStr);
    }

    private void updateProcessTime(String timeStr, long millis) {
        updateLastMillis(millis, "lastProcessMillis_" + timeStr);
    }
}

