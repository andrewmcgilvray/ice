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
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.processor.postproc.InputOperand;
import com.netflix.ice.processor.postproc.Operand;
import com.netflix.ice.processor.postproc.OperandConfig;
import com.netflix.ice.processor.postproc.PostProcessor;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
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
    
    protected final ProcessorConfig config;
    protected final List<KubernetesReport> reports;

	public KubernetesProcessor(ProcessorConfig config, DateTime start) throws IOException {
		this.config = config;
		
		List<KubernetesReport> reports = null;
		reports = getReportsToProcess(start);
		this.reports = reports;
		
	}	
	
	private boolean isActive(KubernetesConfig kc, DateTime start) {
		DateTime ruleStart = new DateTime(kc.getStart(), DateTimeZone.UTC);
		DateTime ruleEnd = new DateTime(kc.getEnd(), DateTimeZone.UTC);
		return !ruleStart.isAfter(start) && start.isBefore(ruleEnd);
	}
	
	protected List<KubernetesReport> getReportsToProcess(DateTime start) throws IOException {
        List<KubernetesReport> filesToProcess = Lists.newArrayList();

        // Compile list of reports from all the configured buckets
        for (KubernetesConfig kc: config.kubernetesConfigs) {        	
            if (!isActive(kc, start) || kc.getBucket().isEmpty())
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
		OperandConfig inConfig = report.getConfig().getIn();
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
			
			UserTag[] ut = tg.resourceGroup.getUserTags();
			
			for (int hour = 0; hour < maxHours; hour++) {						
				// Get the list of possible cluster names for this tag group.
				// Only one report entry should match.
				List<String> clusterNames = report.getClusterNameBuilder().getClusterNames(ut);
				if (clusterNames.size() > 0) {
					for (String clusterName: clusterNames) {
						List<String[]> hourClusterData = report.getData(clusterName, hour);
						if (hourClusterData != null) {
							processHourClusterData(data.getCost(tg.product), hour, tg, clusterName, report, hourClusterData);
						}
					}
				}
			}
		}
	}
		
	protected void processHourClusterData(ReadWriteData costData, int hour, TagGroup tg, String cluster, KubernetesReport report, List<String[]> hourClusterData) {		
		Double totalCost = costData.get(hour, tg);
		if (totalCost == null)
			return;
		
		int namespaceIndex = report.getNamespaceIndex();
		double unusedCost = totalCost;
		Tagger tagger = report.getTagger();
		for (String[] item: hourClusterData) {
			double allocatedCost = getAllocatedCost(tg, totalCost, report, item);
			if (allocatedCost == 0.0)
				continue;
			
			UserTag[] userTags = tg.resourceGroup.getUserTags().clone();
			
			String namespace = report.getString(item, KubernetesColumn.Namespace);
			if (namespaceIndex >= 0)
				userTags[namespaceIndex] = UserTag.get(namespace);
			
			if (tagger != null)
				tagger.tag(report, item, userTags);
			
			ResourceGroup rg = null;
			try {
				rg = ResourceGroup.getResourceGroup(userTags);
			} catch (ResourceException e) {
				// should never throw because no user tags are null
				logger.error("error creating resource group from user tags: " + e);
			}
			
			TagGroup allocated = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, rg);
			
			costData.put(hour, allocated,  allocatedCost);
			
			unusedCost -= allocatedCost;
		}
		
		// Put the remaining cost on the original tagGroup with namespace set to "unused"
		UserTag[] userTags = tg.resourceGroup.getUserTags().clone();
		userTags[namespaceIndex] = UserTag.get("unused");
		ResourceGroup rg = null;
		try {
			rg = ResourceGroup.getResourceGroup(userTags);
		} catch (ResourceException e) {
			// should never throw because no user tags are null
			logger.error("error creating resource group from user tags: " + e);
		}
		TagGroup unused = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, rg);
		costData.remove(hour, tg);
		costData.put(hour, unused, unusedCost);
	}
	
	private double getAllocatedCost(TagGroup tg, double cost, KubernetesReport report, String[] item) {
		Product product = tg.product;
		if (product.isEc2Instance() || product.isCloudWatch()) {
			double cpuCores = report.getDouble(item, KubernetesColumn.RequestsCPUCores);
			double clusterCores = report.getDouble(item, KubernetesColumn.ClusterCPUCores);
			double memoryGiB = report.getDouble(item, KubernetesColumn.RequestsMemoryGiB);
			double clusterMemoryGiB = report.getDouble(item, KubernetesColumn.ClusterMemoryGiB);
			double unitsPerCluster = clusterCores * vCpuToMemoryCostRatio + clusterMemoryGiB;
			double ratePerUnit = cost / unitsPerCluster;
			return ratePerUnit * (cpuCores * vCpuToMemoryCostRatio + memoryGiB);
		}
		else if (product.isEbs()) {
			double pvcGiB = report.getDouble(item, KubernetesColumn.PersistentVolumeClaimGiB);
			double clusterPvcGiB = report.getDouble(item, KubernetesColumn.ClusterPersistentVolumeClaimGiB);
			return cost * pvcGiB / clusterPvcGiB;
		}
		else if (product.isDataTransfer()) {
			double networkGiB = report.getDouble(item, KubernetesColumn.NetworkInGiB) + report.getDouble(item, KubernetesColumn.NetworkOutGiB);
			double clusterNetworkGiB = report.getDouble(item, KubernetesColumn.ClusterNetworkInGiB) + report.getDouble(item, KubernetesColumn.ClusterNetworkOutGiB);
			return cost * networkGiB / clusterNetworkGiB;
		}
		return 0;
	}
	
}
