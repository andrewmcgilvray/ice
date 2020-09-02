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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.csvreader.CsvReader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicLineItemProcessor;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.processor.config.BillingBucket;
import com.netflix.ice.processor.config.S3BucketConfig;

public class DetailedBillingReportProcessor implements MonthlyReportProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private ProcessorConfig config;
    private LineItemProcessor lineItemProcessor;
    private DetailedBillingReservationProcessor reservationProcessor;
    private long endMilli;
    private long reportMilli;

	public DetailedBillingReportProcessor(ProcessorConfig config) throws IOException {
		this.config = config;
		lineItemProcessor = new BasicLineItemProcessor(config.accountService, config.productService, config.reservationService, config.resourceService);
        reservationProcessor = new DetailedBillingReservationProcessor(
				config.accountService.getReservationAccounts().keySet(),
				config.productService,
				config.priceListService);
	}
	
	@Override
	public TreeMap<DateTime, List<MonthlyReport>> getReportsToProcess() {
        TreeMap<DateTime, List<MonthlyReport>> filesToProcess = Maps.newTreeMap();

        // list the tar.gz file in billing file folder
        for (BillingBucket bb: config.billingBuckets) {

            logger.info("trying to list objects in billing bucket " + bb.getName() +
            		" using assume role \"" + bb.getAccessRole() + "\", and external id \"" + bb.getExternalId() + "\"");
            List<S3ObjectSummary> objectSummaries = AwsUtils.listAllObjects(bb.getName(), bb.getRegion(), bb.getPrefix(),
                    bb.getAccountId(), bb.getAccessRole(), bb.getExternalId());
            logger.info("found " + objectSummaries.size() + " in billing bucket " + bb.getName());
            TreeMap<DateTime, S3ObjectSummary> filesToProcessInOneBucket = Maps.newTreeMap();

            for (S3ObjectSummary objectSummary : objectSummaries) {

                String fileKey = objectSummary.getKey();
                DateTime dataTime = AwsUtils.getDateTimeFromFileNameWithTags(fileKey);
                boolean withTags = true;
                if (dataTime == null) {
                    dataTime = AwsUtils.getDateTimeFromFileName(fileKey);
                    withTags = false;
                }

                if (dataTime == null)
                	continue; // Not a file we're interested in.
                
                if (dataTime.isBefore(config.startDate)) {
                    logger.info("ignoring previously processed file " + objectSummary.getKey());
                    continue;
                }
                
                if (!dataTime.isBefore(config.costAndUsageStartDate)) {
                    logger.info("ignoring old style billing report " + objectSummary.getKey());
                    continue;
                }

                if (!filesToProcessInOneBucket.containsKey(dataTime) ||
                    withTags && config.resourceService != null || !withTags && config.resourceService == null)
                    filesToProcessInOneBucket.put(dataTime, objectSummary);
                else
                    logger.info("ignoring file " + objectSummary.getKey());
            }

            for (DateTime key: filesToProcessInOneBucket.keySet()) {
                List<MonthlyReport> list = filesToProcess.get(key);
                if (list == null) {
                    list = Lists.newArrayList();
                    filesToProcess.put(key, list);
                }
                list.add(new BillingFile(filesToProcessInOneBucket.get(key), bb, this, bb.getRootName()));
            }
        }

        return filesToProcess;
	}
	
	protected long processReport(
			DateTime dataTime,
			MonthlyReport report,
			File file,
			CostAndUsageData costAndUsageData,
		    Instances instances) throws Exception {
		
		endMilli = dataTime.getMillis();
		reportMilli = report.getLastModifiedMillis();
		reservationProcessor.clearBorrowers();
		
        processBillingZipFile(dataTime, file, report.hasTags(), report.getRootName(), costAndUsageData, instances, report.getS3BucketConfig().getAccountId());
        
        return endMilli;
	}
	
	
    private void processBillingZipFile(
			DateTime dataTime,
    		File file,
    		boolean withTags,
    		String root,
    		CostAndUsageData costAndUsageData,
    		Instances instances,
    		String accountId) throws Exception {

        InputStream input = new FileInputStream(file);
        ZipArchiveInputStream zipInput = new ZipArchiveInputStream(input);

        try {
            ArchiveEntry entry;
            while ((entry = zipInput.getNextEntry()) != null) {
                if (entry.isDirectory())
                    continue;

                processBillingFile(dataTime, entry.getName(), zipInput, withTags, root, costAndUsageData, instances, accountId);
            }
        }
        catch (IOException e) {
            if (e.getMessage().equals("Stream closed"))
                logger.info("reached end of file.");
            else
                logger.error("Error processing " + file, e);
        }
        finally {
            try {
                zipInput.close();
            } catch (IOException e) {
                logger.error("Error closing " + file, e);
            }
            try {
                input.close();
            }
            catch (IOException e1) {
                logger.error("Cannot close input for " + file, e1);
            }
        }
    }
    
    private void processBillingFile(DateTime dataTime, String fileName, InputStream tempIn, boolean withTags, String root, CostAndUsageData costAndUsageData, Instances instances, String accountId) throws Exception {

        CsvReader reader = new CsvReader(new InputStreamReader(tempIn), ',');

        long lineNumber = 0;
        List<String[]> delayedItems = Lists.newArrayList();
        LineItem lineItem = null;
        try {
            reader.readRecord();
            String[] headers = reader.getValues();

            lineItem = new DetailedBillingReportLineItem(config.useBlended, withTags, headers);
            if (config.resourceService != null)
            	config.resourceService.initHeader(lineItem.getResourceTagsHeader(), accountId);

            while (reader.readRecord()) {
                String[] items = reader.getValues();
                try {
                	lineItem.setItems(items);
                    processOneLine(fileName, delayedItems, root, lineItem, costAndUsageData, instances);
                    String accountID = lineItem.getAccountId();
                    if (!accountID.isEmpty()) {
                        reservationProcessor.addBorrower(config.accountService.getAccountById(accountID));
                    }
                }
                catch (Exception e) {
                    logger.error(StringUtils.join(items, ","), e);
                }
                lineNumber++;

                if (lineNumber % 500000 == 0) {
                    logger.info("processed " + lineNumber + " lines...");
                }
//                if (lineNumber == 40000000) {//100000000      //
//                    break;
//                }
            }
        }
        catch (IOException e ) {
            logger.error("Error processing " + fileName + " at line " + lineNumber, e);
        }
        finally {
            try {
                reader.close();
            }
            catch (Exception e) {
                logger.error("Cannot close BufferedReader...", e);
            }
        }

        for (String[] items: delayedItems) {
        	lineItem.setItems(items);
            processOneLine(fileName, null, root, lineItem, costAndUsageData, instances);
        }
    }

    private void processOneLine(String fileName, List<String[]> delayedItems, String root, LineItem lineItem, CostAndUsageData costAndUsageData, Instances instances) {

        LineItemProcessor.Result result = lineItemProcessor.process(fileName, reportMilli, delayedItems == null, root, lineItem, costAndUsageData, instances, 0.0);

        if (result == LineItemProcessor.Result.delay) {
            delayedItems.add(lineItem.getItems());
        }
        else if (result == LineItemProcessor.Result.hourly) {
            endMilli = Math.max(endMilli, lineItem.getEndMillis());
        }
    }

	private File downloadReport(Report report, String localDir, long lastProcessed) {
        String fileKey = report.getS3ObjectSummary().getKey();
        S3BucketConfig bc = report.getS3BucketConfig();
        File file = new File(localDir, fileKey.substring(bc.getPrefix().length()));
        logger.info("trying to download " + fileKey + "...");
        boolean downloaded = AwsUtils.downloadFileIfChangedSince(report.getS3ObjectSummary().getBucketName(), bc.getRegion(), bc.getPrefix(), file, lastProcessed,
                bc.getAccountId(), bc.getAccessRole(), bc.getExternalId());
        if (downloaded)
            logger.info("downloaded " + fileKey);
        else {
            logger.info("file already downloaded " + fileKey + "...");
        }

        return file;
	}	
	

    class BillingFile extends MonthlyReport {
    	
		BillingFile(S3ObjectSummary s3ObjectSummary, S3BucketConfig s3BucketConfig, MonthlyReportProcessor processor, String rootName) {
			super(s3ObjectSummary, s3BucketConfig, processor, rootName);
		}

		/**
		 * Constructor used for testing only
		 */
		BillingFile(S3ObjectSummary s3ObjectSummary, MonthlyReportProcessor processor, String rootName) {
			super(s3ObjectSummary, new S3BucketConfig(), processor, rootName);
		}
		
		@Override
		public boolean hasTags() {
            return s3ObjectSummary.getKey().contains("with-resources-and-tags");
		}
		
		@Override
		public String[] getReportKeys() {
			return null;
		}
    }


	@Override
	public long downloadAndProcessReport(DateTime dataTime,
			MonthlyReport report, String localDir, long lastProcessed,
			CostAndUsageData costAndUsageData, Instances instances)
			throws Exception {
		
		File file = downloadReport(report, localDir, lastProcessed);
    	String fileKey = report.getReportKey();
        logger.info("processing " + fileKey + "...");
		long end = processReport(dataTime, report, file, costAndUsageData, instances);
        logger.info("done processing " + fileKey + ", end is " + LineItem.amazonBillingDateFormat.print(new DateTime(end)));
        return end;
	}

	@Override
	public ReservationProcessor getReservationProcessor() {
		return reservationProcessor;
	}
}
