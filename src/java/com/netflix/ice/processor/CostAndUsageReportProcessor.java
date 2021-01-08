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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.processor.config.BillingBucket;
import com.netflix.ice.processor.config.S3BucketConfig;

public class CostAndUsageReportProcessor implements MonthlyReportProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private ProcessorConfig config;
    private ReservationProcessor reservationProcessor = null;
    private LineItemProcessor lineItemProcessor;
    private static int MAX_DOWNLOAD_RETRIES = 4;

    private Instances instances;
    private long startMilli;
    private long reportMilli;

	private final ExecutorService pool;
    private volatile boolean aborting;

	// The following two keys can be added to ice.properties for debugging purposes.
	// For example:
	//     ice.debug.curMonth=20190101-20190201
	//     ice.debug.manifest=34d6d421-ef62-40f8-854c-b5181a123b1b
	//     ice.debug.reportKeys=report-01.csv.gz,report-02.csv.gz
    private final String debugMonthKey = "curMonth";
    private final String debugManifestKey = "manifest";
    private final String debugReportKeys = "reportKeys"; // report keys can be the full key or just the report name in which case the manifest path is used to build the key

    private static final DateTimeFormatter yearMonthDayFormat = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.UTC);

	public CostAndUsageReportProcessor(ProcessorConfig config) throws IOException {
		this.config = config;
		this.pool = Executors.newFixedThreadPool(config == null ? 5 : config.numthreads);
		if (config != null) {
	        reservationProcessor = new CostAndUsageReservationProcessor(
					config.accountService.getReservationAccounts().keySet(),
					config.productService,
					config.priceListService);
	        reservationProcessor.setDebugProperties(config.debugProperties);
	        lineItemProcessor = new CostAndUsageReportLineItemProcessor(config.accountService, config.productService, config.reservationService, config.resourceService);
		}
	}
	
    /*
     * Get the report name from the bucket prefix. Return null if no name found (is a DBR bucket for example)
     */
    protected static String reportName(String prefix) {
    	String[] parts = prefix.split("/");
    	if (parts.length < 2) {
    		// Can't be a cost-and-usage bucket, must be DBR
    		return null;
    	}
    	// could be a report name, else it's the last component of a DBR prefix
    	return parts[parts.length - 1];
    }

    private String getManifestKey(String prefix, String reportName, DateTime month) {
    	List<String> parts = Lists.newArrayList();
    	parts.add(prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix);
    	parts.add(month.toString(yearMonthDayFormat) + "-" + month.plusMonths(1).toString(yearMonthDayFormat));
    	parts.add(reportName + "-Manifest.json");
    	
    	return String.join("/", parts);
    }

	@Override
	public TreeMap<DateTime, List<MonthlyReport>> getReportsToProcess() {
        TreeMap<DateTime, List<MonthlyReport>> filesToProcess = Maps.newTreeMap();

        // list the cost and usage report manifest files in the billing report folder
        for (BillingBucket bb: config.billingBuckets) {
            String reportName = reportName(bb.getPrefix());
            if (reportName == null) {
            	// Must be a DBR bucket
            	continue; 
            }

            logger.info("trying to list relevant manifest files in cost and usage report bucket " + bb.getName() +
            		" using assume role \"" + bb.getAccessRole() + "\", and external id \"" + bb.getExternalId() + "\"");

            // S3 billing buckets can have lots of files, so we'll look specifically for the
            // manifest files we're interested in.
            for (DateTime month = config.startDate; month.isBefore(DateTime.now(DateTimeZone.UTC)); month = month.plusMonths(1)) {
            	String manifestKey = getManifestKey(bb.getPrefix(), reportName, month);
            	
                List<S3ObjectSummary> objectSummaries = AwsUtils.listAllObjects(bb.getName(), bb.getRegion(), manifestKey,
                        bb.getAccountId(), bb.getAccessRole(), bb.getExternalId());
                
                for (S3ObjectSummary manifest : objectSummaries) {
                    logger.info("using file " + manifest.getKey());
                    
                    List<MonthlyReport> list = filesToProcess.get(month);
                    if (list == null) {
                        list = Lists.newArrayList();
                        filesToProcess.put(month, list);
                    }
                    
                    // For debugging substitute alternate manifest (typically a short one from early in the month)
                    // property keys
                    String debugMonth = config.debugProperties.get(debugMonthKey);
                    if (debugMonth != null) {
    	                String fileKey = manifest.getKey();
    	                String debugManifest = config.debugProperties.get(debugManifestKey);
    	                if (fileKey.contains(debugMonth)) {
    	                	fileKey = fileKey.substring(0, fileKey.lastIndexOf("/")) + "/" + debugManifest + fileKey.substring(fileKey.lastIndexOf("/"));
    	                	manifest.setKey(fileKey);
    	                	
    	                    List<S3ObjectSummary> debugManifestSummary = AwsUtils.listAllObjects(bb.getName(), bb.getRegion(), fileKey,
    	                            bb.getAccountId(), bb.getAccessRole(), bb.getExternalId());

    	                	manifest.setLastModified(debugManifestSummary.get(0).getLastModified());
    	                }
                    }
                   
                    list.add(new CostAndUsageReport(manifest, bb, this, bb.getRootName()));
                }                	
            }
        }

        return filesToProcess;
	}
	
	class FileData {
		public CostAndUsageData costAndUsageData;
		public List<String[]> delayedItems;
		long endMilli;
		public Exception exception; // If not null, the file processor failed with this exception.
		
		FileData() {
			costAndUsageData = new CostAndUsageData(startMilli, config.workBucketConfig, config.resourceService == null ? null : config.resourceService.getUserTagKeys(), config.getTagCoverage(), config.accountService, config.productService);
			delayedItems = Lists.newArrayList();
			endMilli = startMilli;
		}
		
		FileData(Exception e) {
			costAndUsageData = null;
			delayedItems = null;
			endMilli = 0;
			exception = e;
		}
	}
	
	private Future<FileData> downloadAndProcessOneFile(final CostAndUsageReport report, final String localDir, final String fileKey, final long lastProcessed, final double edpDiscount) {
		return pool.submit(new Callable<FileData>() {
			@Override
			public FileData call() throws Exception {
				if (aborting)
					return null;
				
				String prefix = fileKey.substring(0, fileKey.lastIndexOf("/") + 1);
				String filename = fileKey.substring(prefix.length());
		        File file = new File(localDir, filename);
		        S3BucketConfig bc = report.getS3BucketConfig();
		        String fileKey = report.getS3ObjectSummary().getBucketName() + "/" + prefix + file.getName();
		        
		        try {
			        
			        // We delete files now once processed, so if it already exists it's probably not complete, so delete it
			        if (file.exists()) {
			        	logger.info("delete stale data file " + file.getName());
			        	file.delete();
			        }
			        
			        int retryCount = 0;
			        Exception error = null;
			        boolean downloaded = false;
			        
			        while (!downloaded && retryCount < MAX_DOWNLOAD_RETRIES) {		        	
				        logger.info("trying to download " + fileKey + "..." + (retryCount > 0 ? "retry " + retryCount : ""));
			        	retryCount++;
				        
				        try {
				        	downloaded = AwsUtils.downloadFileIfChangedSince(report.getS3ObjectSummary().getBucketName(), bc.getRegion(), prefix, file, lastProcessed,
				                bc.getAccountId(), bc.getAccessRole(), bc.getExternalId());
				        }
				        catch (com.amazonaws.SdkClientException e) {
				        	logger.error("Error trying to download " + fileKey + ": " + e);
				        	e.printStackTrace();
				        	error = e;
					        // Sleep for a while with some exponential back off
					        Thread.sleep(5*1000 * retryCount * retryCount);
				        }
			        }
			        if (error != null)
			        	return new FileData(error);
			        
			        FileData data = new FileData();
			        
			        // process the file
			        logger.info("processing " + file.getName() + "...");
			        
					CostAndUsageReportLineItem lineItem = new CostAndUsageReportLineItem(config.useBlended, config.costAndUsageNetUnblendedStartDate, report);
			        
					if (file.getName().endsWith(".zip"))
						data.endMilli = processReportZip(file, report.getRootName(), lineItem, data.delayedItems, data.costAndUsageData, edpDiscount);
					else
						data.endMilli = processReportGzip(file, report.getRootName(), lineItem, data.delayedItems, data.costAndUsageData, edpDiscount);
					
		            logger.info("done processing " + file.getName() + ", end is " + new DateTime(data.endMilli, DateTimeZone.UTC).toString() + ", " + data.costAndUsageData.getNum(null) + " hours");
			        file.delete();
			        return data;
		        }
		        catch (Exception e) {
		        	if (!aborting) {
		        		aborting = true;
		        		logger.error("Error processing " + fileKey);
		        		e.printStackTrace();
		        	}
		        	return new FileData(e);
		        }		        
			}
		});
	}
	
	@Override
	public long downloadAndProcessReport(
			DateTime dataTime,
			MonthlyReport report,
			String localDir,
			long lastProcessed,
			CostAndUsageData costAndUsageData,
		    Instances instances) throws Exception {

		this.instances = instances;
		startMilli = dataTime.getMillis();
		reportMilli = report.getLastModifiedMillis();
		aborting = false;
		
		CostAndUsageReport cau = (CostAndUsageReport) report; 
        
		String[] reportKeys = report.getReportKeys();
		
		logger.info("Process " + cau.getReportKey() + " - " + reportKeys.length + " files from " + new DateTime(report.getLastModifiedMillis(), DateTimeZone.UTC));
		
		if (reportKeys.length == 0)
			return dataTime.getMillis();

		CostAndUsageReportLineItem lineItem = new CostAndUsageReportLineItem(config.useBlended, config.costAndUsageNetUnblendedStartDate, cau);
        if (config.resourceService != null)
        	config.resourceService.initHeader(lineItem.getResourceTagsHeader(), report.getS3BucketConfig().getAccountId());
        long endMilli = startMilli;
        double edpDiscount = config.getDiscount(startMilli);
        
		// Queue up all the files
		List<Future<FileData>> fileData = Lists.newArrayList();
		
        String debugReportKeys = config.debugProperties.get(this.debugReportKeys);
		if (debugReportKeys != null) {
			// Queue up the debug reports
			String reportDir = reportKeys[0].substring(0, reportKeys[0].lastIndexOf("/") + 1);
			for (String reportKey: debugReportKeys.split(",")) {
				if (!reportKey.contains("/")) {
					// add the full key to the name
					reportKey =  reportDir + reportKey;
				}
		        fileData.add(downloadAndProcessOneFile(cau, localDir, reportKey, lastProcessed, edpDiscount));
			}
		}
		else {
			for (int i = 0; i < reportKeys.length; i++) {
				// Queue up the files for download and processing
		        fileData.add(downloadAndProcessOneFile(cau, localDir, reportKeys[i], lastProcessed, edpDiscount));
		    }
		}

		// Wait for completion and merge the results together
		for (Future<FileData> ffd: fileData) {
			FileData fd = ffd.get();
			if (fd == null)
				continue; // we get these when aborting
			
			if (fd.exception != null) {
				// We had an unrecoverable error, shut everything down
				aborting = true;
				logger.error("Unrecoverable error processing CUR file, abort processing the rest of the report");
				pool.shutdownNow();
				pool.awaitTermination(60, TimeUnit.SECONDS);
				throw new Exception("Unrecoverable error processing CUR file, abort");
			}
			costAndUsageData.putAll(fd.costAndUsageData);
            endMilli = Math.max(endMilli, fd.endMilli);			
		}
		
		// Process the delayed items		
		for (Future<FileData> ffd: fileData) {
			FileData fd = ffd.get();
	        for (String[] items: fd.delayedItems) {
	        	lineItem.setItems(items);
	            endMilli = processOneLine("<delayed items>", null, report.getRootName(), lineItem, costAndUsageData, endMilli, edpDiscount);
	        }
		}
        return endMilli;
	}

	// Used for unit testing only.
	protected long processReport(
			DateTime dataTime,
			MonthlyReport report,
			List<File> files,
			CostAndUsageData costAndUsageData,
		    Instances instances,
		    String payerAccountId) throws IOException {
		
		this.instances = instances;
		startMilli = dataTime.getMillis();
		reportMilli = report.getLastModifiedMillis();
		long endMilli = startMilli;
		double edpDiscount = config.getDiscount(startMilli);
		
		CostAndUsageReport cau = (CostAndUsageReport) report;
		
		CostAndUsageReportLineItem lineItem = new CostAndUsageReportLineItem(config.useBlended, config.costAndUsageNetUnblendedStartDate, cau);
        if (config.resourceService != null)
        	config.resourceService.initHeader(lineItem.getResourceTagsHeader(), payerAccountId);
        List<String[]> delayedItems = Lists.newArrayList();
        
		for (File file: files) {
            logger.info("processing " + file.getName() + "...");
			if (file.getName().endsWith(".zip"))
				endMilli = processReportZip(file, report.getRootName(), lineItem, delayedItems, costAndUsageData, edpDiscount);
			else
				endMilli = processReportGzip(file, report.getRootName(), lineItem, delayedItems, costAndUsageData, edpDiscount);
            logger.info("done processing " + file.getName() + ", end is " + new DateTime(endMilli, DateTimeZone.UTC).toString() + ", " + costAndUsageData.getNum(null) + " hours");
		}

        for (String[] items: delayedItems) {
        	lineItem.setItems(items);
            endMilli = processOneLine("<delayed items>", null, report.getRootName(), lineItem, costAndUsageData, endMilli, edpDiscount);
        }
        return endMilli;
	}
	
	private long processReportZip(File file, String root, CostAndUsageReportLineItem lineItem, List<String[]> delayedItems, CostAndUsageData costAndUsageData, double edpDiscount) throws IOException {
        InputStream input = new FileInputStream(file);
        ZipArchiveInputStream zipInput = new ZipArchiveInputStream(input);
        long endMilli = startMilli;

        try {
            ArchiveEntry entry;
            while ((entry = zipInput.getNextEntry()) != null) {
                if (entry.isDirectory())
                    continue;

                endMilli = processReportFile(entry.getName(), zipInput, root, lineItem, delayedItems, costAndUsageData, edpDiscount);
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
        return endMilli;
	}

	private long processReportGzip(File file, String root, CostAndUsageReportLineItem lineItem, List<String[]> delayedItems, CostAndUsageData costAndUsageData, double edpDiscount) {
        GZIPInputStream gzipInput = null;
        long endMilli = startMilli;
        
        try {
            InputStream input = new FileInputStream(file);
            gzipInput = new GZIPInputStream(input);
        	endMilli = processReportFile(file.getName(), gzipInput, root, lineItem, delayedItems, costAndUsageData, edpDiscount);
        }
        catch (IOException e) {
            if (e.getMessage().equals("Stream closed"))
                logger.info("reached end of file.");
            else
                logger.error("Error processing " + file, e);
        }
        finally {
        	try {
        		if (gzipInput != null)
        			gzipInput.close();
        	}
        	catch (IOException e) {
        		logger.error("Error closing " + file, e);
        	}
        }
        return endMilli;
	}

	private long processReportFile(String fileName, InputStream in, String root, CostAndUsageReportLineItem lineItem, List<String[]> delayedItems, CostAndUsageData costAndUsageData, double edpDiscount) {
    	CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
    	
        long endMilli = startMilli;
        long lineNumber = 0;
        CSVParser records = null;
        try {
        	records = format.parse(new InputStreamReader(in));
    	    for (CSVRecord record : records) {
                String[] items = new String[record.size()];
                for (int i = 0; i < record.size(); i++)
                	items[i] = record.get(i);
                	
                try {
                	lineItem.setItems(items);
                    endMilli = processOneLine(fileName, delayedItems, root, lineItem, costAndUsageData, endMilli, edpDiscount);
                }
                catch (Exception e) {
                    logger.error(StringUtils.join(items, ","), e);
                }
            }
        }
        catch (IOException e ) {
            logger.error("Error processing " + fileName + " at line " + lineNumber, e);
        }
        finally {
        	if (records != null)
        		try { records.close(); } catch (IOException e) { logger.error("Cannot close csv parser...", e); };
        }
        return endMilli;
	}

    private long processOneLine(String fileName, List<String[]> delayedItems, String root, CostAndUsageReportLineItem lineItem, CostAndUsageData costAndUsageData, long endMilli, double edpDiscount) {
        LineItemProcessor.Result result = lineItemProcessor.process(fileName, reportMilli, delayedItems == null, root, lineItem, costAndUsageData, instances, edpDiscount);

        if (result == LineItemProcessor.Result.delay) {
            delayedItems.add(lineItem.getItems());
        }
        else if (result == LineItemProcessor.Result.hourly) {
            endMilli = Math.max(endMilli, lineItem.getEndMillis());
        }
        
        return endMilli;
    }

	@Override
	public ReservationProcessor getReservationProcessor() {
		return reservationProcessor;
	}
}
