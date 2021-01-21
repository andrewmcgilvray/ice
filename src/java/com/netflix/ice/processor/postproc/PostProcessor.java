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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.CostAndUsageData.PostProcessorStats;
import com.netflix.ice.processor.CostAndUsageData.RuleType;
import com.netflix.ice.tag.UserTagKey;

public class PostProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected boolean debug = false;

	private List<RuleConfig> rules;
	private String reportSubPrefix;
	private AccountService accountService;
	private ProductService productService;
	private ResourceService resourceService;
	private WorkBucketConfig workBucketConfig;
	private int numThreads;
	private ExecutorService pool;
		
	public PostProcessor(List<RuleConfig> rules, String reportSubPrefix, AccountService accountService, ProductService productService, ResourceService resourceService, WorkBucketConfig workBucketConfig, int numThreads) {
		this.rules = rules;
		this.reportSubPrefix = reportSubPrefix;
		this.accountService = accountService;
		this.productService = productService;
		this.resourceService = resourceService;
		this.workBucketConfig = workBucketConfig;
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
				outData = new CostAndUsageData(data, UserTagKey.getUserTagKeys(outUserTagKeys));
			}
			
			if (pool == null && numThreads > 0)
	    		pool = Executors.newFixedThreadPool(numThreads);

			VariableRuleProcessor rp = new VariableRuleProcessor(rule, outData, accountService, productService, resourceService, workBucketConfig, pool);
			boolean processed = rp.process(data);
			if (processed && rc.isReport()) {
				outData.enableTagGroupCache(true);
				writeReports(rule, outData);
			}
		}		
	}
				
	protected void writeReports(Rule rule, CostAndUsageData cauData) throws Exception {
		ReadWriteData data = rule.config.getIn().getType() == RuleConfig.DataType.cost ? cauData.getCost(null) : cauData.getUsage(null);
		Query in = rule.getIn();
		
		Collection<RuleConfig.Aggregation> aggregate = rule.config.getReport().getAggregate();
		
		if (aggregate.contains(RuleConfig.Aggregation.hourly)) {
			String filename = reportName(cauData.getStart(), rule.config.getName(), RuleConfig.Aggregation.hourly);
			ReportWriter writer = new ReportWriter(reportSubPrefix, filename, rule.config.getReport(), workBucketConfig.localDir, cauData.getStart(), 
										rule.config.getIn().getType(), in.getGroupBy(), cauData.getUserTagKeysAsStrings(), data, RuleConfig.Aggregation.hourly);		
			writer.archive();
		}
		if (aggregate.contains(RuleConfig.Aggregation.monthly) || aggregate.contains(RuleConfig.Aggregation.daily)) {
			List<Map<TagGroup, Double>> monthly = Lists.newArrayList();
			List<Map<TagGroup, Double>> daily = Lists.newArrayList();
			
			aggregateSummaryData(data, daily, monthly);
			if (aggregate.contains(RuleConfig.Aggregation.monthly)) {
				writeReport(cauData.getStart(), cauData.getUserTagKeysAsStrings(), rule, monthly, RuleConfig.Aggregation.monthly);
			}
			if (aggregate.contains(RuleConfig.Aggregation.daily)) {
				writeReport(cauData.getStart(), cauData.getUserTagKeysAsStrings(), rule, daily, RuleConfig.Aggregation.daily);
			}
		}
	}
	
	protected String reportName(DateTime month, String ruleName, RuleConfig.Aggregation aggregation) {
        DateTimeFormatter yearMonth = DateTimeFormat.forPattern("yyyy-MM").withZone(DateTimeZone.UTC);

		return "report-" + ruleName + "-" + aggregation.toString() + "-" + month.toString(yearMonth) + ".csv.gz";
	}
	
	protected void writeReport(DateTime month, List<String> userTagKeys, Rule rule, List<Map<TagGroup, Double>> data, RuleConfig.Aggregation aggregation) throws Exception {
		Query in = rule.getIn();
		String filename = reportName(month, rule.config.getName(), aggregation);
        ReadWriteData rwData = new ReadWriteData(userTagKeys.size());
        rwData.enableTagGroupCache(true);
        rwData.setData(data, 0);
		ReportWriter writer = new ReportWriter(reportSubPrefix, filename, rule.config.getReport(), workBucketConfig.localDir, 
									month, rule.config.getIn().getType(), in.getGroupBy(), userTagKeys, rwData, aggregation);		
		writer.archive();		
	}
	
    protected void aggregateSummaryData(
    		ReadWriteData data,
            List<Map<TagGroup, Double>> daily,
            List<Map<TagGroup, Double>> monthly
    		) {
    	
    	Collection<TagGroup> tagGroups = data.getTagGroups();
    	
        // aggregate to daily and monthly
        for (int hour = 0; hour < data.getNum(); hour++) {
            // this month, add to weekly, monthly and daily
            Map<TagGroup, Double> map = data.getData(hour);

            for (TagGroup tagGroup: tagGroups) {
                Double v = map.get(tagGroup);
                if (v != null && v != 0) {
                    addValue(monthly, 0, tagGroup, v);
                    addValue(daily, hour/24, tagGroup, v);
                }
            }
        }
    }
    
    protected void addValue(List<Map<TagGroup, Double>> list, int index, TagGroup tagGroup, double v) {
        Map<TagGroup, Double> map = ReadWriteData.getCreateData(list, index);
        Double existedV = map.get(tagGroup);
        map.put(tagGroup, existedV == null ? v : existedV + v);
    }


}
