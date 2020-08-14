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
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UserTag;

public class KubernetesProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    public static final String reportPrefix = "kubernetes-";
    public static final double vCpuToMemoryCostRatio = 10.9;
    
    protected final ProcessorConfig config;
    private final List<KubernetesReport> reports;

	public KubernetesProcessor(ProcessorConfig config, DateTime start) throws IOException {
		this.config = config;
		
		List<KubernetesReport> reports = null;
		reports = getReportsToProcess(start);
		this.reports = reports;
		
	}	
	
	protected List<KubernetesReport> getReportsToProcess(DateTime start) throws IOException {
        List<KubernetesReport> filesToProcess = Lists.newArrayList();

        // Compile list of reports from all the configured buckets
        for (KubernetesConfig kc: config.kubernetesConfigs) {
            if (kc.getBucket().isEmpty())
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
		}
		
		Product.Code[] productCodes = new Product.Code[]{
			Product.Code.Ec2Instance,
			Product.Code.CloudWatch,
			Product.Code.Ebs,
			Product.Code.DataTransfer,
		};
		
		for (Product.Code productCode: productCodes) {
			Product p = config.productService.getProduct(productCode);
			logger.info("Process kubernetes data for " + productCode.serviceCode);
			process(data.getCost(p));
		}
	}
	
	protected void process(ReadWriteData costData) {
		// Process each hour of data
		for (int i = 0; i < costData.getNum(); i++) {
			// Get a copy of the key set since we'll be updating the map
			Set<TagGroup> tagGroups = Sets.newHashSet(costData.getTagGroups(i));
			for (TagGroup tg: tagGroups) {
				if (tg.resourceGroup == null)
					continue;
				
				UserTag[] ut = tg.resourceGroup.getUserTags();
								
				for (KubernetesReport report: reports) {
					if (report.isCompute(ut)) {
						// Get the list of possible cluster names for this tag group.
						// Only one report entry should match.
						List<String> clusterNames = report.getClusterNameBuilder().getClusterNames(ut);
						if (clusterNames.size() > 0) {
							for (String clusterName: clusterNames) {
								List<String[]> hourClusterData = report.getData(clusterName, i);
								if (hourClusterData != null) {
									processHourClusterData(costData, i, tg, clusterName, report, hourClusterData);
								}
							}
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
