package com.netflix.ice.processor.postproc;

import java.util.Collection;
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
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.CostAndUsageData.PostProcessorStats;
import com.netflix.ice.processor.CostAndUsageData.RuleType;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.kubernetes.KubernetesReport;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
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
	public void process(CostAndUsageData inCauData) throws Exception {
		int maxNum = inCauData.getMaxNum();
		String logMsg = "Post-process rule " + getConfig().getName() + " on " + maxNum + " hours of resource data";
		if (getConfig().getAllocation() != null)
			logMsg += " with allocation report";
		if (getConfig().isReport())
			logMsg += " and generate report";
			
		if (getConfig().getAllocation() == null && !getConfig().isReport()) {
			logger.error("Post-process rule " + getConfig().getName() + " has nothing to do.");
			return;
		}
		logger.info(logMsg);

		boolean copy = outCauData != null;
		AllocationReport allocationReport = getAllocationReport(inCauData);
		
		if (!copy && allocationReport == null) {
			logger.warn("No allocation report to process for rule " + getConfig().getName());
			inCauData.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Variable, false, 0, 0, "No allocation report found"));
			return;
		}
		StopWatch sw = new StopWatch();
		sw.start();
		
		Map<AggregationTagGroup, Double[]> inDataGroups = runQuery(rule.getIn(), inCauData, false, maxNum, rule.config.getName());
		
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
			Collection<AllocationReport.Key> overAllocatedKeys = allocationReport.overAllocatedKeys();
			if (!overAllocatedKeys.isEmpty()) {
				info = "Allocations exceeded 100% for keys " + allocationReport.getInTagKeys() + " with values: "+ overAllocatedKeys.toString();
				logger.error(info);
			}
		}
		sw.stop();
		info = "Elapsed time: " + sw.toString() + (info.isEmpty() ? ", " + info : "");
		
		logger.info("  -- data for rule " + rule.config.getName() + " -- in data size = " + inDataGroups.size() + ", --- allocated size = " + allocatedTagGroups.size());
		inCauData.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Variable, false, inDataGroups.size(), allocatedTagGroups.size(), info));
	}
	
	private void performAllocation(CostAndUsageData cauData, Map<AggregationTagGroup, Double[]> inDataGroups, int maxNum, AllocationReport allocationReport, Set<TagGroup> allocatedTagGroups) throws Exception {
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
	
	protected void allocateHour(CostAndUsageData cauData, int hour, Map<AggregationTagGroup, Double[]> inDataGroups, int maxNum, AllocationReport allocationReport, Set<TagGroup> allocatedTagGroups, ExecutorService pool, List<Future<Void>> futures) {
		futures.add(submitAllocateHour(cauData, hour, inDataGroups, maxNum, allocationReport, allocatedTagGroups, pool));
	}
	
	protected Future<Void> submitAllocateHour(final CostAndUsageData cauData, final int hour, final Map<AggregationTagGroup, Double[]> inDataGroups, final int maxNum, final AllocationReport allocationReport, final Set<TagGroup> allocatedTagGroups, ExecutorService pool) {
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
		
	private void allocateHour(CostAndUsageData cauData, int hour, Map<AggregationTagGroup, Double[]> inDataGroups, int maxNum, AllocationReport allocationReport, Set<TagGroup> allocatedTagGroups) throws Exception {
		boolean copy = outCauData != null;
		int numUserTags = cauData.getNumUserTags();

		for (AggregationTagGroup atg: inDataGroups.keySet()) {
			Double[] inValues = inDataGroups.get(atg);
			if (hour >= inValues.length)
				continue;
			
			TagGroup tagGroup = atg.getTagGroup(numUserTags);
						
			// Get the input and output data sets. If generating a report, put all of the data on the "null" product key.
			Product p = copy ? null : tagGroup.product;
			ReadWriteData data = rule.getIn().getType() == RuleConfig.DataType.cost ? cauData.getCost(p) : cauData.getUsage(p);
		
			if (inValues[hour] == null || inValues[hour] == 0.0)
				continue;
			
			processHourData(allocationReport, data, hour, tagGroup, inValues[hour], allocatedTagGroups);
		}			
	}
	
	/**
	 * Copy the query data to the report data set and if requested, generate CostType from
	 * the operation tag. If not grouping by operation, aggregate the data to remove the operation dimension.
	 */
	private Map<AggregationTagGroup, Double[]> copyAndReduce(Map<AggregationTagGroup, Double[]> inDataGroups, int maxNum, int numSourceUserTags) throws Exception {
		StopWatch sw = new StopWatch();
		sw.start();
		
 		Map<AggregationTagGroup, Double[]> aggregatedInDataGroups = Maps.newHashMap();
 		List<String> userTagKeys = outCauData.getUserTagKeysAsStrings();
		int costTypeIndex = userTagKeys == null ? -1 : userTagKeys.indexOf("CostType");
		boolean addedOperationForCostType = rule.getIn().addedOperationForCostType();
		int[] indeces = getIndeces(outCauData.getUserTagKeysAsStrings());
		
	    List<Rule.TagKey> groupByTags = rule.getGroupBy();
	    
	    List<Integer> groupByUserTagIndeces = Lists.newArrayList();
	    for (int i = 0; i < outCauData.getNumUserTags(); i++)
	    	groupByUserTagIndeces.add(i);

		Aggregation outAggregation = new Aggregation(groupByTags, groupByUserTagIndeces);
		
		for (AggregationTagGroup atg: inDataGroups.keySet()) {
			Double[] inValues = inDataGroups.get(atg);
			if (inValues == null)
				continue;
						
			TagGroup tagGroup = atg.getTagGroup(numSourceUserTags);
			
			// Map the input user tags to the output user tags
			UserTag[] inUserTags = tagGroup.resourceGroup.getUserTags();
			UserTag[] outUserTags = new UserTag[indeces.length];
			for (int i = 0; i < indeces.length; i++) {
				outUserTags[i] = indeces[i] < 0 ? UserTag.empty : inUserTags[indeces[i]];
			}
			if (costTypeIndex >= 0) {
				outUserTags[costTypeIndex] = UserTag.get(CostType.getCostType(tagGroup.operation).name);
				if (addedOperationForCostType)
					tagGroup = tagGroup.withOperation(null);
			}
			tagGroup = tagGroup.withResourceGroup(ResourceGroup.getResourceGroup(outUserTags));
			
			// Get the new AggregationTagGroup that operates on the output data set
			AggregationTagGroup newAtg = outAggregation.getAggregationTagGroup(tagGroup);
			Double[] inAggregated = aggregatedInDataGroups.get(newAtg);
			if (inAggregated == null) {
				inAggregated = new Double[maxNum];
				for (int i = 0; i < maxNum; i++)
					inAggregated[i] = 0.0;
				aggregatedInDataGroups.put(newAtg, inAggregated);
			}
			
			// Generating a report so put all of the data on the "null" product key.
			ReadWriteData data = rule.getIn().getType() == RuleConfig.DataType.cost ? outCauData.getCost(null): outCauData.getUsage(null);
			
			for (int hour = 0; hour < inValues.length; hour++) {
				if (inValues[hour] == null || inValues[hour] == 0.0)
					continue;
				
				inAggregated[hour] += inValues[hour];
				
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
				
				String reportName = rc.getName() + "-" + AwsUtils.monthDateFormat.print(data.getStart()) + ".csv.gz";
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
			ar = new AllocationReport(rc.getAllocation(), rc.isReport(), outCauData == null ? resourceService.getCustomTags() : outCauData.getUserTagKeysAsStrings());
			// Download the allocation report and load it.
			ar.loadReport(data.getStart(), workBucketConfig.localDir);
		}
		return ar;
	}	
	
	protected void processHourData(AllocationReport report, ReadWriteData data, int hour, TagGroup tg, Double total, Set<TagGroup> allocatedTagGroups) throws Exception {
		if (total == null || total == 0.0)
			return;
				
		Map<AllocationReport.Key, Double> hourData = report.getData(hour, tg);
		if (hourData == null || hourData.isEmpty()) {
			return;
		}
		
		// Remove the source value - we'll add any unallocated back at the end
		data.remove(hour, tg);

		double unAllocated = total;
		for (AllocationReport.Key key: hourData.keySet()) {
			double allocated = total * hourData.get(key);
			if (allocated == 0.0)
				continue;
			
			TagGroup allocatedTagGroup = report.getOutputTagGroup(key, tg);
			
			allocatedTagGroups.add(allocatedTagGroup);
			
			Double existing = data.get(hour, allocatedTagGroup);
			data.put(hour, allocatedTagGroup,  allocated + (existing == null ? 0.0 : existing));
			
			unAllocated -= allocated;
		}
		
		double threshold = 0.000000001;
		// Unused cost can go negative if, for example, a K8s cluster is over-subscribed, so test the absolute value.
		if (Math.abs(unAllocated) > threshold) {
			// Put the remaining cost on the original tagGroup
			data.put(hour, tg, unAllocated);
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
		inConfig.setGroupBy(Lists.<Rule.TagKey>newArrayList(Rule.TagKey.product));
		inConfig.setGroupByTags(groupByTags);
		
		Query query = new Query(inConfig, resourceService.getCustomTags(), false);
		
		int maxNum = data.getMaxNum();
		Map<AggregationTagGroup, Double[]> inData = runQuery(query, data, false, maxNum, rule.config.getName());
		
		AllocationReport allocationReport = new AllocationReport(rule.config.getAllocation(), rule.config.isReport(), outCauData == null ? resourceService.getCustomTags() : outCauData.getUserTagKeysAsStrings());
		int numUserTags = resourceService.getCustomTags().size();
		
		for (AggregationTagGroup atg: inData.keySet()) {
			Double[] inValues = inData.get(atg);

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
			
			List<String> inTags = getInTags(allocationReport, ut, atg.getProduct());
			
			for (int hour = 0; hour < maxHours; hour++) {						
				List<String[]> hourClusterData = report.getData(clusterName, hour);
				if (hourClusterData != null && !hourClusterData.isEmpty()) {
					addHourClusterRecords(allocationReport, hour, atg.getProduct(), inTags, clusterName, report, hourClusterData);
				}
			}
		}
		
		return allocationReport;
	}
	
	private List<String> getInTags(AllocationReport allocationReport, UserTag[] userTags, Product product) {
		List<String> tags = Lists.newArrayList();
		for (Integer i: allocationReport.getInTagIndeces()) {
			if (i < 0)
				tags.add(product.getServiceCode());
			else
				tags.add(userTags[i].name);
		}
		return tags;
	}
	
	protected void addHourClusterRecords(AllocationReport allocationReport, int hour, Product product, List<String> inTags, String clusterName, KubernetesReport report, List<String[]> hourClusterData) {
		double remainingAllocation = 1.0;
		String t = report.getConfig().getKubernetes().getType();
		// Default to Namespace if not specified
		KubernetesReport.Type reportItemType = (t == null || t.isEmpty()) ? KubernetesReport.Type.Namespace : KubernetesReport.Type.valueOf(report.getConfig().getKubernetes().getType());

		
		for (String[] item: hourClusterData) {
			double allocation = report.getAllocationFactor(product, item);
			if (allocation == 0.0)
				continue;
			
			KubernetesReport.Type type = report.getType(item);
			
			// If we have a Type column, the config type flag determines
			// the scope to use for breaking out the cost.
			// Each scope duplicates the data set, so we only want to process one.
			if (type != KubernetesReport.Type.None && type != reportItemType)
				continue;
			
			List<String> outTags = report.getTagValues(item, allocationReport.getOutTagKeys());
			remainingAllocation -= allocation;			
			allocationReport.add(hour, allocation, inTags, outTags);			
		}
		// Assign any unused to the unused type, resource, and namespace
		if (remainingAllocation > 0.0001) {
			List<String> outTags = report.getUnusedTagValues(allocationReport.getOutTagKeys());
			allocationReport.add(hour, remainingAllocation, inTags, outTags);			
		}
	}	

}
