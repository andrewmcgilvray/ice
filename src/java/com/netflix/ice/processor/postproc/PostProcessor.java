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
package com.netflix.ice.processor.postproc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.Maps;
import com.netflix.ice.common.*;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.DataSerializer;
import com.netflix.ice.processor.CostAndUsageData.PostProcessorStats;
import com.netflix.ice.processor.CostAndUsageData.RuleType;
import com.netflix.ice.tag.UserTagKey;

public class PostProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected boolean debug = false;

    private final DateTime startDate;
	private List<RuleConfig> rules;
	private String reportSubPrefix;
	private AccountService accountService;
	private ProductService productService;
	private ResourceService resourceService;
	private WorkBucketConfig workBucketConfig;
	List<ProcessorConfig.JsonFileType> jsonFiles;
	private int numThreads;
	private ExecutorService pool;

	static final DateTimeFormatter yearMonth = DateTimeFormat.forPattern("yyyy-MM").withZone(DateTimeZone.UTC);

	public PostProcessor(DateTime startDate, List<RuleConfig> rules, String reportSubPrefix,
						 AccountService accountService, ProductService productService,
						 ResourceService resourceService, WorkBucketConfig workBucketConfig,
						 List<ProcessorConfig.JsonFileType> jsonFiles, int numThreads) {
		this.startDate = startDate;
		this.rules = rules;
		this.reportSubPrefix = reportSubPrefix;
		this.accountService = accountService;
		this.productService = productService;
		this.resourceService = resourceService;
		this.workBucketConfig = workBucketConfig;
		this.jsonFiles = Lists.newArrayList(jsonFiles);
		this.jsonFiles.remove(ProcessorConfig.JsonFileType.hourlyRI); // Don't generate hourlyRI for allocation reports
		this.numThreads = numThreads;
		this.pool = null; // lazy initialize
	}
	
	public void process(CostAndUsageData data) {
		logger.info("Post-process " + rules.size() + " rules");
		for (RuleConfig rc: rules) {
			try {
				processRule(rc, data);
			} catch (Exception e) {
				logger.error("Error post-processing cost and usage data for rule " + rc.getName() + ": " + e);
				e.printStackTrace();
			}
		}
		if (pool != null)
			shutdownAndAwaitTermination(pool);
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
	
	
	private boolean isActive(RuleConfig rc, long startMilli) {
		long ruleStart = new DateTime(rc.getStart(), DateTimeZone.UTC).getMillis();
		long ruleEnd = new DateTime(rc.getEnd(), DateTimeZone.UTC).getMillis();
		return startMilli >= ruleStart && startMilli < ruleEnd;
	}
	
	protected void processRule(RuleConfig rc, CostAndUsageData data) throws Exception {
		logger.info("-------- Process rule: \"" + rc.getName() + "\" --------");
		// Make sure the rule is in effect for the start date
		if (!isActive(rc, data.getStartMilli())) {
			logger.info("Post-process rule " + rc.getName() + " is not active for this month, start=" + rc.getStart() + ", end=" + rc.getEnd());
			return;
		}
		
		Rule rule = new Rule(rc, accountService, productService, resourceService.getCustomTags());
		if (rc.getAllocation() != null && !rc.isReport() && rule.getIn().hasAggregation()) {
			// We don't currently support allocating aggregated costs because we don't track the source tagGroups that were aggregated
			// by the getInData() method. So if the input data set is the same as the out data set, we fail.
			String info = "In-place allocation with aggregation is currently unsupported, in: " + rule.getIn().toString();
			logger.error(info);
			data.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Variable, false, 0, 0, info));
			return;
		}
		
		if (rc.getResults() != null) {
			if (rc.isReport()) {
				String info = "Post processing does not support report generation for results rules: " + rc.getName();
				logger.error(info);
				data.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Fixed, false, 0, 0, info));
				return;
			}
			RuleProcessor rp = new FixedRuleProcessor(rule, accountService, productService);
			rp.process(data);
		}
		else {
			CostAndUsageData outData = null;
			List<String> outUserTagKeys = null;
			
			if (rc.isReport()) {
				outUserTagKeys = rule.getOutUserTagKeys();
				WorkBucketConfig reportWorkBucketConfig = null;
				if (rc.getReport().isArchiveToWorkBucket()) {
					File file = new File(workBucketConfig.localDir, rc.getName());
					if (!file.exists()) {
						boolean success = file.mkdir();
						if (!success) {
							logger.error("Unable to create local directory for report work bucket files: " + file.getPath());
						}
					}
					reportWorkBucketConfig = new WorkBucketConfig(workBucketConfig.workS3BucketName,
							workBucketConfig.workS3BucketRegion,
							workBucketConfig.workS3BucketPrefix + rc.getName() + "/",
							file.getPath());
				}
				outData = new CostAndUsageData(data, reportWorkBucketConfig, UserTagKey.getUserTagKeys(outUserTagKeys));
			}
			
			if (pool == null && numThreads > 0)
	    		pool = Executors.newFixedThreadPool(numThreads);

			VariableRuleProcessor rp = new VariableRuleProcessor(rule, outData, accountService, productService, resourceService, workBucketConfig, pool);
			boolean processed = rp.process(data);
			if (processed && rc.isReport()) {
				outData.enableTagGroupCache(true);
				writeReports(rule, outData);

				WorkBucketConfig reportWorkBucketConfig = outData.getWorkBucketConfig();

				if (reportWorkBucketConfig != null) {
					// Normalize the cost and usage data for use by a work bucket
					outData.normalize();

					outData.archive(jsonFiles, numThreads);
					// Create work bucket data config for the report data
					String monthStr = startDate.toString(yearMonth);
					saveWorkBucketDataConfig(monthStr, outData.getUserTagKeys(), reportWorkBucketConfig);

					List<ProcessorStatus.Report> statusReports = Lists.newArrayList();
					ProcessorStatus ps = new ProcessorStatus(monthStr, statusReports, new DateTime(DateTimeZone.UTC).toString(), "", outData.getArchiveFailures());
					saveInstanceMetrics(reportWorkBucketConfig);
					saveProcessorStatus(monthStr, ps, reportWorkBucketConfig);
					productService.archive(reportWorkBucketConfig.localDir, reportWorkBucketConfig.workS3BucketName, reportWorkBucketConfig.workS3BucketPrefix);
				}
			}
		}		
	}
				
	protected void writeReports(Rule rule, CostAndUsageData cauData) throws Exception {
		DataSerializer data = cauData.get(null);
		Query in = rule.getIn();
		
		Collection<RuleConfig.Aggregation> aggregate = rule.config.getReport().getAggregate();
		
		if (aggregate.contains(RuleConfig.Aggregation.hourly)) {
			String filename = reportName(cauData.getStart(), rule.config.getName(), RuleConfig.Aggregation.hourly);
			ReportWriter writer = new ReportWriter(reportSubPrefix, filename, rule.config.getReport(), workBucketConfig.localDir, cauData.getStart(), 
										in.getGroupBy(), cauData.getUserTagKeysAsStrings(), data, RuleConfig.Aggregation.hourly);		
			writer.archive();
		}
		if (aggregate.contains(RuleConfig.Aggregation.monthly) || aggregate.contains(RuleConfig.Aggregation.daily)) {
			List<Map<TagGroup, DataSerializer.CostAndUsage>> monthly = Lists.newArrayList();
			List<Map<TagGroup, DataSerializer.CostAndUsage>> daily = Lists.newArrayListWithCapacity(744);
			
			aggregateSummaryData(data, daily, monthly);
			if (aggregate.contains(RuleConfig.Aggregation.monthly)) {
				writeReport(cauData.getStart(), cauData.getUserTagKeysAsStrings(), rule, monthly, RuleConfig.Aggregation.monthly);
			}
			if (aggregate.contains(RuleConfig.Aggregation.daily)) {
				writeReport(cauData.getStart(), cauData.getUserTagKeysAsStrings(), rule, daily, RuleConfig.Aggregation.daily);
			}
		}
	}

	private void saveWorkBucketDataConfig(String startMonth, List<UserTagKey> userTagKeys, WorkBucketConfig workBucketConfig) throws IOException {
		Map<String, List<String>> zones = Maps.newHashMap();
		for (Region r: Region.getAllRegions()) {
			List<String> zlist = Lists.newArrayList();
			for (Zone z: r.getZones())
				zlist.add(z.name);
			zones.put(r.name, zlist);
		}
		WorkBucketDataConfig wbdc = new WorkBucketDataConfig(startMonth, null, null,
				accountService.getAccounts(), zones, userTagKeys, Config.TagCoverage.none, null);
		File file = new File(workBucketConfig.localDir, Config.workBucketDataConfigFilename);
		OutputStream os = new FileOutputStream(file);
		OutputStreamWriter writer = new OutputStreamWriter(os);
		writer.write(wbdc.toJSON());
		writer.close();

		logger.info("Upload work bucket data config file");
		AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, file);
	}

	private void saveProcessorStatus(String timeStr, ProcessorStatus status, WorkBucketConfig workBucketConfig) {
		String filename = ProcessorStatus.prefix + timeStr + ProcessorStatus.suffix;

		AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
		String statusStr = status.toJSON();
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(statusStr.length());

		s3Client.putObject(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix + filename, IOUtils.toInputStream(statusStr, StandardCharsets.UTF_8), metadata);
	}

	private void saveInstanceMetrics(WorkBucketConfig reportWorkBucketConfig) throws IOException {
		File instanceMetrics = new File(workBucketConfig.localDir, InstanceMetrics.dbName);
		File localCopy = new File(reportWorkBucketConfig.localDir, InstanceMetrics.dbName);
		FileUtils.copyFile(instanceMetrics, localCopy);
		logger.info("Upload instance metrics");
		AwsUtils.upload(reportWorkBucketConfig.workS3BucketName, reportWorkBucketConfig.workS3BucketPrefix, localCopy);
	}

	protected String reportName(DateTime month, String ruleName, RuleConfig.Aggregation aggregation) {
		return "report-" + ruleName + "-" + aggregation.toString() + "-" + month.toString(yearMonth) + ".csv.gz";
	}
	
	protected void writeReport(DateTime month, List<String> userTagKeys, Rule rule, List<Map<TagGroup, DataSerializer.CostAndUsage>> data, RuleConfig.Aggregation aggregation) throws Exception {
		Query in = rule.getIn();
		String filename = reportName(month, rule.config.getName(), aggregation);
        DataSerializer rwData = new DataSerializer(userTagKeys.size());
        rwData.enableTagGroupCache(true);
        rwData.setData(data, 0);
		ReportWriter writer = new ReportWriter(reportSubPrefix, filename, rule.config.getReport(), workBucketConfig.localDir, 
									month, in.getGroupBy(), userTagKeys, rwData, aggregation);		
		writer.archive();		
	}
	
    protected void aggregateSummaryData(
    		DataSerializer data,
            List<Map<TagGroup, DataSerializer.CostAndUsage>> daily,
            List<Map<TagGroup, DataSerializer.CostAndUsage>> monthly
    		) {
    	
    	Collection<TagGroup> tagGroups = data.getTagGroups();
    	
        // aggregate to daily and monthly
        for (int hour = 0; hour < data.getNum(); hour++) {
            // this month, add to weekly, monthly and daily
            Map<TagGroup, DataSerializer.CostAndUsage> map = data.getData(hour);

            for (TagGroup tagGroup: tagGroups) {
            	DataSerializer.CostAndUsage v = map.get(tagGroup);
            	
                if (v != null) {
                    addValue(monthly, 0, tagGroup, v);
                    addValue(daily, hour/24, tagGroup, v);
                }
            }
        }
    }
    
    protected void addValue(List<Map<TagGroup, DataSerializer.CostAndUsage>> list, int index, TagGroup tagGroup, DataSerializer.CostAndUsage v) {
        Map<TagGroup, DataSerializer.CostAndUsage> map = DataSerializer.getCreateData(list, index);
        map.put(tagGroup, v.add(map.get(tagGroup)));
    }


}
