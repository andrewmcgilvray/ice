package com.netflix.ice.processor.postproc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.Aggregation;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.DataSerializer;
import com.netflix.ice.processor.CostAndUsageData.PostProcessorStats;
import com.netflix.ice.processor.CostAndUsageData.RuleType;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.kubernetes.KubernetesReport;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTag;

public class VariableRuleProcessor extends RuleProcessor {
	private CostAndUsageData outCauData;
	private ResourceService resourceService;
	private WorkBucketConfig workBucketConfig;
	private ExecutorService pool;

	public VariableRuleProcessor(Rule rule, CostAndUsageData outCauData,
			AccountService accountService, ProductService productService, 
			ResourceService resourceService, WorkBucketConfig workBucketConfig, ExecutorService pool) {
		super(rule, accountService, productService);
		this.outCauData = outCauData;
		this.resourceService = resourceService;
		this.workBucketConfig = workBucketConfig;
		this.pool = pool;
	}
		
	private int[] getIndeces(List<String> outUserTagKeys) {
		if (outUserTagKeys == null)
			return new int[]{};
		
		int[] indeces = new int[outUserTagKeys.size()];
		
		List<String> inUserTagKeys = resourceService.getCustomTags();
		for (int i = 0; i < outUserTagKeys.size(); i++) {
			indeces[i] = inUserTagKeys.indexOf(outUserTagKeys.get(i));
		}
		return indeces;
	}
	
	@Override
	public boolean process(CostAndUsageData inCauData) throws Exception {
		int maxNum = inCauData.getMaxNum();
		String logMsg = "Post-process rule " + getConfig().getName() + " on " + maxNum + " hours of resource data";
		if (getConfig().getAllocation() != null)
			logMsg += " with allocation report";
		if (getConfig().isReport())
			logMsg += " and generate report";
			
		if (getConfig().getAllocation() == null && !getConfig().isReport()) {
			logger.error("Post-process rule " + getConfig().getName() + " has nothing to do.");
			return false;
		}
		logger.info(logMsg);

		boolean copy = outCauData != null;
		boolean isAllocation = getConfig().getAllocation() != null;

		AllocationReport allocationReport = null;
		if (isAllocation)
			allocationReport = getAllocationReport(inCauData);
		
		// Only process if it's a copy without allocation -or- we have an allocation report
		if (!(copy && !isAllocation) && allocationReport == null) {
			logger.warn("No allocation report to process for rule " + getConfig().getName());
			inCauData.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Variable, false, 0, 0, "No allocation report found"));
			return false;
		}
		StopWatch sw = new StopWatch();
		sw.start();
		
		Map<AggregationTagGroup, CostAndUsage[]> inDataGroups = runQuery(rule.getIn(), inCauData, false, maxNum, rule.config.getName());
		
		int numSourceUserTags = resourceService.getCustomTags().size();		

		if (copy)
			inDataGroups = copyAndReduce(inDataGroups, maxNum, numSourceUserTags);
		
		// Keep some statistics
		Set<TagGroup> allocatedTagGroups = ConcurrentHashMap.newKeySet();
		
		if (allocationReport != null) {
			
			CostAndUsageData cauData = copy ? outCauData : inCauData;
			performAllocation(cauData, inDataGroups, maxNum, allocationReport, allocatedTagGroups);
		}
		
		String info = "";
		if (allocationReport != null) {
			Map<AllocationReport.Key, Double> overAllocatedKeys = allocationReport.overAllocatedKeys();
			if (!overAllocatedKeys.isEmpty()) {
				info = "Allocations exceeded 100% for keys " + allocationReport.getInTagKeys() + " with values: "+ overAllocatedKeys.toString();
				logger.warn(info);
			}
		}
		sw.stop();
		info = "Elapsed time: " + sw.toString() + (!info.isEmpty() ? ", " + info : "");
		
		logger.info("  -- data for rule " + rule.config.getName() + " -- in data size = " + inDataGroups.size() + ", --- allocated size = " + allocatedTagGroups.size());
		inCauData.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Variable, false, inDataGroups.size(), allocatedTagGroups.size(), info));
		
		return true;
	}
	
	private void performAllocation(CostAndUsageData cauData, Map<AggregationTagGroup, CostAndUsage[]> inDataGroups, int maxNum, AllocationReport allocationReport, Set<TagGroup> allocatedTagGroups) throws Exception {
		StopWatch sw = new StopWatch();
		sw.start();
		
		if (pool != null) {
	    	List<Future<Void>> futures = Lists.newArrayListWithCapacity(maxNum);
	
			for (int hour = 0; hour < maxNum; hour++) {
				allocateHour(cauData, hour, inDataGroups, maxNum, allocationReport, allocatedTagGroups, pool, futures);
				// Wait for completion
			}
			for (Future<Void> f: futures) {
				f.get();
			}
		}
		else {
			for (int hour = 0; hour < maxNum; hour++) {
				allocateHour(cauData, hour, inDataGroups, maxNum, allocationReport, allocatedTagGroups);
			}
		}
		logger.info("  -- performAllocation elapsed time: " + sw);
	}
	
	protected void allocateHour(CostAndUsageData cauData, int hour, Map<AggregationTagGroup, CostAndUsage[]> inDataGroups, int maxNum, AllocationReport allocationReport, Set<TagGroup> allocatedTagGroups, ExecutorService pool, List<Future<Void>> futures) {
		futures.add(submitAllocateHour(cauData, hour, inDataGroups, maxNum, allocationReport, allocatedTagGroups, pool));
	}
	
	protected Future<Void> submitAllocateHour(final CostAndUsageData cauData, final int hour, final Map<AggregationTagGroup, CostAndUsage[]> inDataGroups, final int maxNum, final AllocationReport allocationReport, final Set<TagGroup> allocatedTagGroups, ExecutorService pool) {
    	return pool.submit(new Callable<Void>() {
    		@Override
    		public Void call() {
    			try {
    				allocateHour(cauData, hour, inDataGroups, maxNum, allocationReport, allocatedTagGroups);
    			}
    			catch (Exception e) {
    				logger.error("allocation for hour " + hour + " failed, " + e.getMessage());
    				e.printStackTrace();
    				return null;
    			}
                return null;
    		}
    	});
	}
		
	private void allocateHour(CostAndUsageData cauData, int hour, Map<AggregationTagGroup, CostAndUsage[]> inDataGroups, int maxNum, AllocationReport allocationReport, Set<TagGroup> allocatedTagGroups) throws Exception {
		boolean copy = outCauData != null;
		int numUserTags = cauData.getNumUserTags();

		for (AggregationTagGroup atg: inDataGroups.keySet()) {
			CostAndUsage[] inValues = inDataGroups.get(atg);
			if (hour >= inValues.length)
				continue;
			
			TagGroup tagGroup = atg.getTagGroup(numUserTags);
						
			// Get the input and output data sets. If generating a report, put all of the data on the "null" product key.
			Product p = copy ? null : tagGroup.product;
			DataSerializer data = cauData.get(p);
		
			if (inValues[hour] == null || inValues[hour].isZero())
				continue;
			
			processHourData(allocationReport, data, hour, tagGroup, inValues[hour], allocatedTagGroups);
		}			
	}
	
	/**
	 * Copy the query data to the report data set and if requested, generate CostType from
	 * the operation tag. If not grouping by operation, aggregate the data to remove the operation dimension.
	 */
	private Map<AggregationTagGroup, CostAndUsage[]> copyAndReduce(Map<AggregationTagGroup, CostAndUsage[]> inDataGroups, int maxNum, int numSourceUserTags) throws Exception {
		StopWatch sw = new StopWatch();
		sw.start();
		
 		Map<AggregationTagGroup, CostAndUsage[]> aggregatedInDataGroups = Maps.newHashMap();
		int[] indeces = getIndeces(outCauData.getUserTagKeysAsStrings());
		
	    List<Rule.TagKey> groupByTags = rule.getGroupBy();
	    
	    List<Integer> groupByUserTagIndeces = Lists.newArrayList();
	    for (int i = 0; i < outCauData.getNumUserTags(); i++)
	    	groupByUserTagIndeces.add(i);

		Aggregation outAggregation = new Aggregation(groupByTags, groupByUserTagIndeces);
		
		for (AggregationTagGroup atg: inDataGroups.keySet()) {
			CostAndUsage[] inValues = inDataGroups.get(atg);
			if (inValues == null)
				continue;
						
			TagGroup tagGroup = atg.getTagGroup(numSourceUserTags);
			
			// Map the input user tags to the output user tags
			UserTag[] inUserTags = tagGroup.resourceGroup.getUserTags();
			UserTag[] outUserTags = new UserTag[indeces.length];
			for (int i = 0; i < indeces.length; i++) {
				outUserTags[i] = indeces[i] < 0 ? UserTag.empty : inUserTags[indeces[i]];
			}
			tagGroup = tagGroup.withResourceGroup(ResourceGroup.getResourceGroup(outUserTags));
			
			// Get the new AggregationTagGroup that operates on the output data set
			AggregationTagGroup newAtg = outAggregation.getAggregationTagGroup(tagGroup);
			CostAndUsage[] inAggregated = aggregatedInDataGroups.get(newAtg);
			if (inAggregated == null) {
				inAggregated = new CostAndUsage[maxNum];
				for (int i = 0; i < maxNum; i++)
					inAggregated[i] = new CostAndUsage();
				aggregatedInDataGroups.put(newAtg, inAggregated);
			}
			
			// Generating a report so put all of the data on the "null" product key.
			DataSerializer data = outCauData.get(null);
			
			for (int hour = 0; hour < inValues.length; hour++) {
				inAggregated[hour] = inAggregated[hour].add(inValues[hour]);
				
				// Copy the data to the output report
				data.add(hour, tagGroup, inValues[hour]);
			}			
		}
		logger.info("  -- copyAndReduce elapsed time: " + sw + ", aggregated groups: " + aggregatedInDataGroups.keySet().size());
		return aggregatedInDataGroups;
	}

	protected AllocationReport getAllocationReport(CostAndUsageData data) throws Exception {
		RuleConfig rc = getConfig();
		if (rc.getAllocation() == null)
			return null;
		
		// Prepare the allocation report
		AllocationReport ar = null;
		String info = "";
				
		KubernetesConfig kc = rule.config.getAllocation().getKubernetes();
		if (kc != null) {
			// Make sure product list is limited to the four products we support
			if (rc.getIn().getFilter().getProduct() == null) {
				// load the supported products into the input filter
				rc.getIn().getFilter().setProduct(Lists.newArrayList("^(" + StringUtils.join(KubernetesReport.productServiceCodes, "|") + ")$"));
			}
			
			// Pre-process the K8s report to produce an allocation report
			KubernetesReport kr = new KubernetesReport(rc.getAllocation(), data.getStart(), resourceService);
			if (kr.loadReport(workBucketConfig.localDir)) {
				Set<String> unprocessedClusters = Sets.newHashSet(kr.getClusters());
				Set<String> unprocessedAtgs = Sets.newHashSet();

				ar = generateAllocationReport(kr, data, unprocessedClusters, unprocessedAtgs);
				
				String reportName = rc.getName() + "-" + AwsUtils.monthDateFormat.print(data.getStart()) + ".csv";
				ar.archiveReport(data.getStart(), reportName, workBucketConfig);
				
				if (!unprocessedClusters.isEmpty()) {
					info = "unprocessed clusters in Kubernetes report: " + unprocessedClusters.toString();
					logger.warn("unprocessed clusters in Kubernetes report for rule " + rule.config.getName() + ": " + unprocessedClusters);
				}
				if (!unprocessedAtgs.isEmpty()) {
					info += (info.isEmpty() ? "" : "; ") + "unprocessed aggregation tag groups due to no matching cluster names in report: " + unprocessedAtgs.toString();
					logger.warn("unprocessed aggregation tag groups due to no matching cluster names in report for rule " + rule.config.getName() + ": " + unprocessedAtgs);
				}
			}
		}
		else {
			ar = new AllocationReport(rc.getAllocation(), data.getStartMilli(), rc.isReport(), 
					outCauData == null ? resourceService.getCustomTags() : outCauData.getUserTagKeysAsStrings(), resourceService);
			// Download the allocation report and load it.
			if (!ar.loadReport(data.getStart(), workBucketConfig.localDir))
				ar = null; // No report to process
		}
		return ar;
	}	
	
	protected void processHourData(AllocationReport report, DataSerializer data, int hour, TagGroup tg, CostAndUsage total, Set<TagGroup> allocatedTagGroups) throws Exception {
		Map<AllocationReport.Key, Double> hourData = report.getData(hour, tg);
		if (hourData == null || hourData.isEmpty()) {
			return;
		}
		
		// Remove the source value - we'll add any unallocated back at the end
		data.remove(hour, tg);

		CostAndUsage unAllocated = total;
		for (AllocationReport.Key key: hourData.keySet()) {
			CostAndUsage allocated = total.mul(hourData.get(key));
			if (allocated.isZero())
				continue;
			
			TagGroup allocatedTagGroup = report.getOutputTagGroup(key, tg);
			
			allocatedTagGroups.add(allocatedTagGroup);
			
			data.add(hour,  allocatedTagGroup, allocated);
			
			unAllocated = unAllocated.sub(allocated);
		}
		
		double threshold = 0.000000001;
		// Unused cost can go negative if, for example, a K8s cluster is over-subscribed, so test the absolute value.
		if (Math.abs(unAllocated.cost) > threshold || Math.abs(unAllocated.usage) > threshold) {
			// Add the remaining cost on the original tagGroup
			data.add(hour, tg, unAllocated);
		}
		//boolean overAllocated = unAllocated < -threshold;
		//if (overAllocated)
		//	logger.warn("Over allocation at hour " + hour + " for tag group: " + tg + " --- amount: " + unAllocated);
	}

	protected AllocationReport generateAllocationReport(KubernetesReport report, CostAndUsageData data,
			Set<String> unprocessedClusters, Set<String> unprocessedAtgs) throws Exception {
		// Copy the inConfig so remaining changes aren't carried to the Allocation Report processing
		QueryConfig inConfig = new QueryConfig(rule.config.getIn());
				
		// Set aggregations based on the input tags. Group only by tags used to compute the cluster names.
		// We only want one atg for each report item.
		List<String> groupByTags = Lists.newArrayList();
		for (String key: rule.config.getAllocation().getIn().keySet()) {
			if (!key.startsWith("_"))
				groupByTags.add(key);
		}
		List<Rule.TagKey> groupBy = Lists.newArrayList(Rule.TagKey.product);
		if (report.hasUsageType())
			groupBy.add(Rule.TagKey.usageType);
		
		inConfig.setGroupBy(groupBy);

		inConfig.setGroupByTags(groupByTags);
		
		Query query = new Query(inConfig, resourceService.getCustomTags());
		
		int maxNum = data.getMaxNum();
		Map<AggregationTagGroup, CostAndUsage[]> inData = runQuery(query, data, false, maxNum, rule.config.getName());
		
		AllocationReport allocationReport = new AllocationReport(rule.config.getAllocation(), data.getStartMilli(), rule.config.isReport(), 
				outCauData == null ? resourceService.getCustomTags() : outCauData.getUserTagKeysAsStrings(), resourceService);
		int numUserTags = resourceService.getCustomTags().size();
		
		for (AggregationTagGroup atg: inData.keySet()) {
			CostAndUsage[] inValues = inData.get(atg);

			int maxHours = inValues == null ? maxNum : inValues.length;			
			
			UserTag[] ut = atg.getResourceGroup(numUserTags).getUserTags();
			if (ut == null)
				continue;	
			
			// Get the cluster name for this tag group.
			String clusterName = report.getClusterName(ut);
			if (clusterName == null) {
				unprocessedAtgs.add(atg.userTags.toString());
				continue;
			}
			
			unprocessedClusters.remove(clusterName);
			
			List<String> inTags = getInTags(allocationReport, ut, atg.getProduct(), atg.getUsageType());
			String ec2UsageType = atg.getProduct().isEc2Instance() ? atg.getUsageType() != null ? atg.getUsageType().name : null : null;
			
			for (int hour = 0; hour < maxHours; hour++) {						
				List<String[]> hourClusterData = report.getData(clusterName, hour, ec2UsageType);
				if (hourClusterData != null && !hourClusterData.isEmpty()) {
					addHourClusterRecords(allocationReport, hour, atg.getProduct(), inTags, clusterName, report, hourClusterData);
				}
			}
		}
		
		return allocationReport;
	}
	
	private List<String> getInTags(AllocationReport allocationReport, UserTag[] userTags, Product product, UsageType usageType) {
		List<String> tags = Lists.newArrayList();
		List<String> inTagKeys = allocationReport.getInTagKeys();
		for (int i = 0; i < inTagKeys.size(); i++) {
			String key = inTagKeys.get(i);
			String v = null;
			if (key.equals("_product"))
				v = product.getServiceCode();
			else if (key.equals("_usageType"))
				v = usageType.getName();
			else
				v = userTags[allocationReport.getInTagIndeces().get(i)].name;
			tags.add(v);
		}
		return tags;
	}
	
	protected void addHourClusterRecords(AllocationReport allocationReport, int hour, Product product, List<String> inTags, String clusterName, KubernetesReport report, List<String[]> hourClusterData) {
		double remainingAllocation = 1.0;
		
		for (String[] item: hourClusterData) {
			double allocation = report.getAllocationFactor(product, item);
			if (allocation == 0.0)
				continue;
			
			List<String> outTags = report.getTagValues(item, allocationReport.getOutTagKeys());
			remainingAllocation -= allocation;			
			allocationReport.add(hour, allocation, inTags, outTags);			
		}
		// Assign any unused to the unused type, resource, and namespace
		// If we have any overallocation, this will go negative
		if (Math.abs(remainingAllocation) > 0.0001) {
			List<String> outTags = report.getUnusedTagValues(allocationReport.getOutTagKeys());
			allocationReport.add(hour, remainingAllocation, inTags, outTags);			
		}
	}	

}
