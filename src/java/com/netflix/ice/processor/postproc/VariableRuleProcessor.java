package com.netflix.ice.processor.postproc;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
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
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTag;

public class VariableRuleProcessor extends RuleProcessor {
	private CostAndUsageData outCauData;
	private ResourceService resourceService;
	private WorkBucketConfig workBucketConfig;

	public VariableRuleProcessor(Rule rule, CostAndUsageData outCauData,
			AccountService accountService, ProductService productService, 
			ResourceService resourceService, WorkBucketConfig workBucketConfig) {
		super(rule, accountService, productService);
		this.outCauData = outCauData;
		this.resourceService = resourceService;
		this.workBucketConfig = workBucketConfig;
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
		String logMsg = "Post-process rule " + getConfig().getName() + " on resource data";
		if (getConfig().getAllocation() != null)
			logMsg += " with allocation report";
		if (getConfig().isReport())
			logMsg += " and generate report";
			
		if (getConfig().getAllocation() == null && !getConfig().isReport()) {
			logger.error("Post-process rule " + getConfig().getName() + " has nothing to do.");
			return;
		}
		logger.info(logMsg);

		
		int maxNum = inCauData.getMaxNum();
		Map<AggregationTagGroup, Double[]> inDataGroups = getInData(rule.getIn(), inCauData, false, maxNum, rule.config.getName());
		
		int numSourceUserTags = resourceService.getCustomTags().size();
		
		AllocationReport allocationReport = getAllocationReport(inCauData);
		
		boolean copy = outCauData != null;
		int[] indeces = copy ? getIndeces(outCauData.getUserTagKeysAsStrings()) : null;

		// Keep some statistics
		Set<TagGroup> allocatedTagGroups = Sets.newHashSet();
		TagGroup overAllocationTagGroup = null;
		int firstOverAllocationHour = 0;

		for (AggregationTagGroup atg: inDataGroups.keySet()) {
			Double[] inValues = inDataGroups.get(atg);
			int maxHours = inValues == null ? maxNum : inValues.length;
			
			TagGroup tagGroup = atg.getTagGroup(numSourceUserTags);
			if (tagGroup.resourceGroup == null)
				continue;
			
			if (copy) {
				// Map the input user tags to the output user tags
				UserTag[] inUserTags = tagGroup.resourceGroup.getUserTags();
				UserTag[] outUserTags = new UserTag[indeces.length];
				for (int i = 0; i < indeces.length; i++) {
					outUserTags[i] = indeces[i] < 0 ? UserTag.empty : inUserTags[indeces[i]];
				}
				tagGroup = tagGroup.withResourceGroup(ResourceGroup.getResourceGroup(outUserTags));
			}
			
			// Get the input and output data sets. If generating a report, put all of the data on the "null" product key.
			ReadWriteData data = null;
			if (rule.getIn().getType() == OperandConfig.OperandType.cost) {
				data = (copy ? outCauData : inCauData).getCost(copy ? null : tagGroup.product);
			}
			else {
				data = (copy ? outCauData : inCauData).getUsage(copy ? null : tagGroup.product);
			}
			
			for (int hour = 0; hour < maxHours; hour++) {
				if (inValues[hour] == null || inValues[hour] == 0.0)
					continue;
				
				if (copy) {
					// Copy the data to the output report
					data.put(hour, tagGroup, inValues[hour]);
				}
				
				if (allocationReport != null) {
					if (processHourData(allocationReport, data, hour, tagGroup, inValues[hour], allocatedTagGroups) && overAllocationTagGroup == null) {
						overAllocationTagGroup = tagGroup;
						firstOverAllocationHour = hour;
					}
				}
			}			
		}
		String info = "";
		
		if (overAllocationTagGroup != null) {
			info = "Allocations exceeded 100% at hour " + Integer.toString(firstOverAllocationHour) + ". first over allocated tag group: " + overAllocationTagGroup.toString();
		}
		logger.info("  -- data for rule " + rule.config.getName() + " -- in data size = " + inDataGroups.size() + ", --- allocated size = " + allocatedTagGroups.size());
		inCauData.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Variable, false, inDataGroups.size(), allocatedTagGroups.size(), info));
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
			if (rc.getIn().getProduct() == null) {
				// load the supported products into the input filter
				rc.getIn().setProduct("^(" + StringUtils.join(KubernetesReport.productServiceCodes, "|") + ")$");
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
		
	// returns true if any allocation set exceeds 100%
	protected boolean processHourData(AllocationReport report, ReadWriteData data, int hour, TagGroup tg, Double total, Set<TagGroup> allocatedTagGroups) {
		if (total == null || total == 0.0)
			return false;
				
		AllocationReport.Key key = report.getKey(tg);
		List<AllocationReport.Value> hourClusterData = report.getData(hour, key);
		if (hourClusterData == null || hourClusterData.isEmpty())
			return false;
		
		// Remove the source value - we'll add any unallocated back at the end
		data.remove(hour, tg);

		double unAllocated = total;
		for (AllocationReport.Value value: hourClusterData) {
			double allocated = total * value.getAllocation();
			if (allocated == 0.0)
				continue;
			
			TagGroup allocatedTagGroup = value.getOutputTagGroup(tg);
			
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
		boolean overAllocated = unAllocated < -threshold;
		if (overAllocated)
			logger.warn("Over allocation at hour " + hour + " for tag group: " + tg);
		return overAllocated;
	}

	protected AllocationReport generateAllocationReport(KubernetesReport report, CostAndUsageData data,
			Set<String> unprocessedClusters, Set<String> unprocessedAtgs) throws Exception {
		// Clone the inConfig so remaining changes aren't carried to the Allocation Report processing
		OperandConfig inConfig = rule.config.getIn().clone();
				
		// Set aggregations based on the input tags. Group only by tags used to compute the cluster names.
		// We only want one atg for each report item.
		List<String> groupByTags = Lists.newArrayList();
		for (String key: rule.config.getAllocation().getIn().keySet()) {
			if (!key.startsWith("_"))
				groupByTags.add(key);
		}
		inConfig.setGroupBy(Lists.<TagType>newArrayList(TagType.Product));
		inConfig.setGroupByTags(groupByTags);
		
		InputOperand inOperand = new InputOperand(inConfig, accountService, resourceService.getCustomTags());
		
		int maxNum = data.getMaxNum();
		Map<AggregationTagGroup, Double[]> inData = getInData(inOperand, data, false, maxNum, rule.config.getName());
		
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
		for (String[] item: hourClusterData) {
			double allocation = report.getAllocationFactor(product, item);
			if (allocation == 0.0)
				continue;
			
			remainingAllocation -= allocation;
			
			String type = report.getString(item, KubernetesColumn.Type);
			
			// If we have Type and Resource columns and the type is Namespace, ignore it because we
			// process the items at a more granular level of Deployment, DaemonSet, and StatefulSet.
			if (type.equals("Namespace"))
				continue;
			
			List<String> outTags = report.getTagValues(item, allocationReport.getOutTagKeys());
			allocationReport.add(hour, allocation, inTags, outTags);			
		}
		// Assign any unused to the unused type, resource, and namespace
		if (remainingAllocation > 0.0001) {
			List<String> outTags = report.getUnusedTagValues(allocationReport.getOutTagKeys());
			allocationReport.add(hour, remainingAllocation, inTags, outTags);			
		}
	}	

}
