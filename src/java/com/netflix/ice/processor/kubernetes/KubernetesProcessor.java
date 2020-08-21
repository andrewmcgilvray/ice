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
package com.netflix.ice.processor.kubernetes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.processor.postproc.AllocationReport;
import com.netflix.ice.processor.postproc.InputOperand;
import com.netflix.ice.processor.postproc.Operand;
import com.netflix.ice.processor.postproc.OperandConfig;
import com.netflix.ice.processor.postproc.PostProcessor;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.UserTag;

public class KubernetesProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    public static final String reportPrefix = "kubernetes-";
    public static final double vCpuToMemoryCostRatio = 10.9;
    
	public static final Product.Code[] productCodes = new Product.Code[]{
			Product.Code.Ec2Instance,
			Product.Code.CloudWatch,
			Product.Code.Ebs,
			Product.Code.DataTransfer,
		};
	public static final List<String> productServiceCodes = Lists.newArrayList();
	{
		for (Product.Code c: productCodes)
			productServiceCodes.add(c.serviceCode);
	}
	
	private static final String k8sType = "K8sType";
	private static final String k8sResource = "K8sResource";
	private static final String k8sNamespace = "K8sNamespace";
    
    protected final ProcessorConfig config;
	protected final DateTime start;
    protected final List<KubernetesReport> reports;
    protected final int numUserTags;

	public KubernetesProcessor(ProcessorConfig config, DateTime start) throws IOException {
		this.config = config;
		this.start = start;
		
		List<KubernetesReport> reports = null;
		reports = getReportsToProcess();
		this.reports = reports;
		this.numUserTags = config.resourceService.getCustomTags().size();
	}	
	
	private boolean isActive(KubernetesConfig kc) {
		DateTime ruleStart = new DateTime(kc.getStart(), DateTimeZone.UTC);
		DateTime ruleEnd = new DateTime(kc.getEnd(), DateTimeZone.UTC);
		return !ruleStart.isAfter(start) && start.isBefore(ruleEnd);
	}
	
	protected List<KubernetesReport> getReportsToProcess() throws IOException {
        List<KubernetesReport> filesToProcess = Lists.newArrayList();

        // Compile list of reports from all the configured buckets
        for (KubernetesConfig kc: config.kubernetesConfigs) {        	
            if (!isActive(kc) || kc.getBucket().isEmpty())
            	continue;
                        
            String prefix = kc.getPrefix();
            if (!prefix.isEmpty() && !prefix.endsWith("/"))
            	prefix += "/";

            String fileKey = prefix + reportPrefix + AwsUtils.monthDateFormat.print(start);

            logger.info("trying to list objects in kubernetes bucket " + kc.getBucket() +
            		" using assume role \"" + kc.getAccountId() + ":" + kc.getAccessRole() + "\", and external id \"" + kc.getExternalId() + "\" with key " + fileKey);
            
            List<S3ObjectSummary> objectSummaries = AwsUtils.listAllObjects(kc.getBucket(), kc.getRegion(), fileKey,
            		kc.getAccountId(), kc.getAccessRole(), kc.getExternalId());
            logger.info("found " + objectSummaries.size() + " report(s) in kubernetes bucket " + kc.getBucket());
            
            if (objectSummaries.size() > 0) {
	            filesToProcess.add(new KubernetesReport(objectSummaries.get(0), kc, start, config.resourceService));
            }
        }

        return filesToProcess;
	}

	
	public void downloadAndProcessReports(CostAndUsageData data) throws Exception {
		if (reports == null || reports.isEmpty()) {
			logger.info("No kubernetes reports to process");
			return;
		}
				
		for (KubernetesReport report: reports) {
			report.loadReport(config.workBucketConfig.localDir);
			process(report, data);
		}
	}
	
	protected void process(KubernetesReport report, CostAndUsageData data) throws Exception {
		AllocationReport ar = generateAllocationReport(report, data);
		processAllocationReport(report.getConfig().getIn(), ar, data);
	}
	
	protected AllocationReport generateAllocationReport(KubernetesReport report, CostAndUsageData data) throws Exception {
		OperandConfig inConfig = report.getConfig().getIn().clone();
		// Make sure product list is limited to the four we support
		if (inConfig.getProduct() == null) {
			// load the supported products into the input filter
			inConfig.setProduct("^(" + StringUtils.join(productServiceCodes, "|") + ")$");
		}
		
		// Set aggregations based on the input tags. Group only by tags used to compute the cluster names.
		// We only want one atg for each report item.
		List<String> groupByTags = report.getClusterNameBuilder().getReferencedTags();
		inConfig.setGroupBy(Lists.<String>newArrayList("Product"));
		inConfig.setGroupByTags(groupByTags);
		
		InputOperand inOperand = new InputOperand(inConfig, config.accountService, config.resourceService);
		
		int maxNum = data.getMaxNum();
		PostProcessor pp = new PostProcessor(null, config.accountService, config.productService, config.resourceService);
		Map<AggregationTagGroup, Double[]> inData = pp.getInData(inOperand, data, false, maxNum);
		
		AllocationReport allocationReport = newAllocationReport(report);
		
		// Keep track of the cluster names we've processed so we only do it once for each
		Map<String, UserTag[]> processedClusterNames = Maps.newHashMap();
		
		for (AggregationTagGroup atg: inData.keySet()) {
			Double[] inValues = inData.get(atg);

			int maxHours = inValues == null ? maxNum : inValues.length;			
			
			UserTag[] ut = atg.getResourceGroup(numUserTags).getUserTags();
			if (ut == null)
				continue;	
			
			// Get the list of possible cluster names for this tag group.
			String clusterName = report.getClusterName(ut);
			if (clusterName == null)
				continue;
			
			if (processedClusterNames.containsKey(clusterName)) {
				List<UserTag> previous = Lists.newArrayList(processedClusterNames.get(clusterName));
				List<UserTag> current = Lists.newArrayList(ut);
				logger.error("Multiple user tag sets produce the same cluster name: " + clusterName + ", tag sets: {" + previous + " == " + current + "}");
				continue;
			}
			
			processedClusterNames.put(clusterName, ut);
			
			for (int hour = 0; hour < maxHours; hour++) {						
				List<String[]> hourClusterData = report.getData(clusterName, hour);
				if (hourClusterData != null && !hourClusterData.isEmpty()) {
					addHourClusterRecords(allocationReport, hour, atg.getProduct(), ut, clusterName, report, hourClusterData);
				}
			}
		}
		
		return allocationReport;
	}
	
	protected AllocationReport newAllocationReport(KubernetesReport report) {
		// Get the input dimensions
		List<String> inTagKeys = report.getClusterNameBuilder().getReferencedTags();
		inTagKeys.add("Product");
		// Set the K8s output dimensions
		List<String> outTagKeys = Lists.newArrayList(new String[]{k8sType, k8sResource, k8sNamespace});
		// Add Namespace Mappings and Label-to-UserTag mappings
		outTagKeys.addAll(report.getTagger().getTagKeys());
		return new AllocationReport(start, inTagKeys, outTagKeys, config.resourceService);
	}
	
	protected void addHourClusterRecords(AllocationReport allocationReport, int hour, Product product, UserTag[] userTags, String clusterName, KubernetesReport report, List<String[]> hourClusterData) {
		List<String> inTags = report.getClusterNameBuilder().getReferencedTagValues(userTags);
		// Add entry for Product tag
		inTags.add(product.getServiceCode());
		
		double remainingAllocation = 1.0;
		for (String[] item: hourClusterData) {
			double allocation = getAllocationFactor(product, report, item);
			if (allocation == 0.0)
				continue;
			
			remainingAllocation -= allocation;
			
			String type = report.getString(item, KubernetesColumn.Type);
			String resource = report.getString(item, KubernetesColumn.Resource);
			String namespace = report.getString(item, KubernetesColumn.Namespace);
			
			// If we have Type and Resource columns and the type is Namespace, ignore it because we
			// process the items at a more granular level of Deployment, DaemonSet, and StatefulSet.
			if (type.equals("Namespace"))
				continue;
			
			List<String> outTags = Lists.newArrayList();
			outTags.add(type);
			outTags.add(resource);
			outTags.add(namespace);
			outTags.addAll(report.getTagger().getTagValues(report, item));
			allocationReport.add(hour, allocation, inTags, outTags);			
		}
		// Assign any unused to the unused type, resource, and namespace
		if (remainingAllocation > 0.0001) {
			List<String> outTags = Lists.newArrayList();
			outTags.add("unused");
			outTags.add("unused");
			outTags.add("unused");
			for (int i = 0; i < report.getTagger().getTagKeys().size(); i++)
				outTags.add("");
			allocationReport.add(hour, remainingAllocation, inTags, outTags);			
		}
	}
	
	private double getAllocationFactor(Product product, KubernetesReport report, String[] item) {
		if (product.isEc2Instance() || product.isCloudWatch()) {
			double cpuCores = report.getDouble(item, KubernetesColumn.RequestsCPUCores);
			double clusterCores = report.getDouble(item, KubernetesColumn.ClusterCPUCores);
			double memoryGiB = report.getDouble(item, KubernetesColumn.RequestsMemoryGiB);
			double clusterMemoryGiB = report.getDouble(item, KubernetesColumn.ClusterMemoryGiB);
			double unitsPerCluster = clusterCores * vCpuToMemoryCostRatio + clusterMemoryGiB;
			return (cpuCores * vCpuToMemoryCostRatio + memoryGiB) / unitsPerCluster;
		}
		else if (product.isEbs()) {
			double pvcGiB = report.getDouble(item, KubernetesColumn.PersistentVolumeClaimGiB);
			double clusterPvcGiB = report.getDouble(item, KubernetesColumn.ClusterPersistentVolumeClaimGiB);
			return pvcGiB / clusterPvcGiB;
		}
		else if (product.isDataTransfer()) {
			double networkGiB = report.getDouble(item, KubernetesColumn.NetworkInGiB) + report.getDouble(item, KubernetesColumn.NetworkOutGiB);
			double clusterNetworkGiB = report.getDouble(item, KubernetesColumn.ClusterNetworkInGiB) + report.getDouble(item, KubernetesColumn.ClusterNetworkOutGiB);
			return networkGiB / clusterNetworkGiB;
		}
		return 0;
	}
	
	
	
	
	protected void processAllocationReport(OperandConfig inConfig, AllocationReport allocationReport, CostAndUsageData data) throws Exception {
		// Make sure product list is limited to the four we support
		if (inConfig.getProduct() == null) {
			// load the supported products into the input filter
			inConfig.setProduct("^(" + StringUtils.join(productServiceCodes, "|") + ")$");
		}
		
		InputOperand inOperand = new InputOperand(inConfig, config.accountService, config.resourceService);
		OperandConfig resultConfig = new OperandConfig();
		Operand result = new Operand(resultConfig, config.accountService, config.resourceService);
		
		int maxNum = data.getMaxNum();
		PostProcessor pp = new PostProcessor(null, config.accountService, config.productService, config.resourceService);
		Map<AggregationTagGroup, Double[]> inData = pp.getInData(inOperand, data, false, maxNum);
		
		for (AggregationTagGroup atg: inData.keySet()) {
			Double[] inValues = inData.get(atg);

			int maxHours = inValues == null ? maxNum : inValues.length;
			TagGroup tg = result.tagGroup(atg, config.accountService, config.productService, false);
			
			
			if (tg.resourceGroup == null)
				return;
			
			for (int hour = 0; hour < maxHours; hour++) {						
				processHourClusterData(allocationReport, data.getCost(tg.product), hour, tg);
			}
		}
	}
		
	protected void processHourClusterData(AllocationReport report, ReadWriteData costData, int hour, TagGroup tg) {
		Double totalCost = costData.get(hour, tg);
		if (totalCost == null)
			return;
				
		AllocationReport.Key key = report.getKey(tg);
		List<AllocationReport.Value> hourClusterData = report.getData(hour, key);
		if (hourClusterData == null || hourClusterData.isEmpty())
			return;

		double unAllocatedCost = totalCost;
		for (AllocationReport.Value value: hourClusterData) {
			double allocatedCost = totalCost * value.getAllocation();
			if (allocatedCost == 0.0)
				continue;
			
			
			TagGroup allocated = value.getOutputTagGroup(tg);
						
			costData.put(hour, allocated,  allocatedCost);
			
			unAllocatedCost -= allocatedCost;
		}
		
		if (unAllocatedCost < 0.0001) {
			costData.remove(hour, tg);
		}
		else {
			// Put the remaining cost on the original tagGroup
			costData.put(hour, tg, unAllocatedCost);
		}
	}

}
