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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.Report;
import com.netflix.ice.processor.config.S3BucketConfig;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.postproc.AllocationConfig;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.UserTag;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class KubernetesReport extends Report {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final double vCpuToMemoryCostRatio = 10.9;
    
	public static final Product.Code[] productCodes = new Product.Code[]{
			Product.Code.Ec2Instance,
			Product.Code.CloudWatch,
			Product.Code.Ebs,
			Product.Code.DataTransfer,
		};
	public static final List<String> productServiceCodes = Lists.newArrayList();
	static {
		for (Product.Code c: productCodes)
			productServiceCodes.add(c.serviceCode);
	}

    private final DateTime month;
    private final long startMillis;
    private final AllocationConfig allocationConfig;
    private final ClusterNameBuilder clusterNameBuilder;

    public enum KubernetesColumn {
    	Cluster,
    	Type,
    	Resource,
    	Namespace,
    	StartDate,
    	EndDate,
    	RequestsCPUCores,
    	UsedCPUCores,
    	LimitsCPUCores,
    	ClusterCPUCores,
    	RequestsMemoryGiB,
    	UsedMemoryGiB,
    	LimitsMemoryGiB,
    	ClusterMemoryGiB,
    	NetworkInGiB,
    	ClusterNetworkInGiB,
    	NetworkOutGiB,
    	ClusterNetworkOutGiB,
    	PersistentVolumeClaimGiB,
    	ClusterPersistentVolumeClaimGiB,
    	UsageType,
    }
    
    public enum Type {
    	DaemonSet,
    	Deployment,
    	Namespace,
    	Pod,
    	StatefulSet,
    	None;
    }
    
    private Map<KubernetesColumn, Integer> reportIndeces = null;
    private Map<String, Integer> userTagIndeces = null;
    // Map of clusters with hourly data for the month - index will range from 0 to 743
    private Map<String, List<List<String[]>>> data = null;
    // Map of output tag keys to Kubernetes deployment parameters
    private Map<String, KubernetesColumn> deployParams;

	public KubernetesReport(AllocationConfig allocationConfig, DateTime month, ResourceService resourceService) throws Exception {
    	super();
    	S3BucketConfig bucket = allocationConfig.getS3Bucket();
    	// Make sure we have a valid bucket configuration
    	String missingParams = getMissingParams(bucket);
    	if (!missingParams.isEmpty())
    		throw new Exception("Missing s3Bucket configuration parameters: " + missingParams);
    	withS3BucketConfig(new S3BucketConfig()
    								.withName(bucket.getName())
    								.withRegion(bucket.getRegion())
    								.withPrefix(bucket.getPrefix())
    								.withAccountId(bucket.getAccountId())
    								.withAccessRole(bucket.getAccessRole())
    								.withExternalId(bucket.getExternalId())
    								);
    	this.month = month;
    	this.startMillis = month.getMillis();
		this.allocationConfig = allocationConfig;
		
		KubernetesConfig config = allocationConfig.getKubernetes();
		List<String> clusterNameFormulae = config.getClusterNameFormulae();
		clusterNameBuilder = clusterNameFormulae == null || clusterNameFormulae.isEmpty() ? null : new ClusterNameBuilder(config.getClusterNameFormulae(), resourceService.getCustomTags());
		if (clusterNameBuilder != null && !allocationConfig.getIn().keySet().containsAll(clusterNameBuilder.getReferencedTags()))
			throw new Exception("Cluster name formulae refer to tags not in the input tag key list");
		
		deployParams = Maps.newHashMap();
		if (config.getOut() != null) {
			for (String deployParam: config.getOut().keySet()) {
				deployParams.put(config.getOut().get(deployParam), KubernetesColumn.valueOf(deployParam));
			}
		}
	}
	
	public boolean hasUsageType() {
		return reportIndeces.containsKey(KubernetesColumn.UsageType);
	}
	
	private String getMissingParams(S3BucketConfig bucket) {
		List<String> errors = Lists.newArrayList();		
    	if (bucket.getName() == null)
    		errors.add("name");
    	if (bucket.getRegion() == null)
    		errors.add("region");
    	if (bucket.getAccountId() == null)
    		errors.add("accountId");
    	return String.join(", ", errors);
	}

	public boolean loadReport(String localDir)
			throws Exception {
		
		S3BucketConfig bucket = allocationConfig.getS3Bucket();
        if (bucket.getName().isEmpty())
        	return false;
                    
        String prefix = bucket.getPrefix();

        String fileKey = prefix + AwsUtils.monthDateFormat.print(month);

        logger.info("trying to list objects in allocation report bucket " + bucket.getName() +
        		" using assume role \"" + bucket.getAccountId() + ":" + bucket.getAccessRole() + "\", and external id \"" + bucket.getExternalId() + "\" with key " + fileKey);
        
        List<S3ObjectSummary> objectSummaries = AwsUtils.listAllObjects(bucket.getName(), bucket.getRegion(), fileKey,
        		bucket.getAccountId(), bucket.getAccessRole(), bucket.getExternalId());
        logger.info("found " + objectSummaries.size() + " allocation report(s) in bucket " + bucket.getName());
        
        if (objectSummaries.size() == 0)
            return false;
        
        withS3ObjectSummary(objectSummaries.get(0));
		
		File file = download(localDir);
        logger.info("loading " + fileKey + "...");
		long end = readFile(file);
        logger.info("done loading " + fileKey + ", end is " + LineItem.amazonBillingDateFormat.print(new DateTime(end)) + ", clusters: " + getClusters());
        return true;
	}

	private File download(String localDir) {
        String fileKey = getS3ObjectSummary().getKey();
		String prefix = fileKey.substring(0, fileKey.lastIndexOf("/") + 1);
		String filename = fileKey.substring(prefix.length());
        File file = new File(localDir, filename);

        // kubernetes report files all have the same name for the same month, so remove any
        // local copy and download
        if (file.exists())
        	file.delete();
        
        logger.info("trying to download " + getS3ObjectSummary().getBucketName() + "/" + prefix + file.getName() + 
        		" from account " + s3BucketConfig.getAccountId() + " using role " + s3BucketConfig.getAccessRole() + 
        		(StringUtils.isEmpty(s3BucketConfig.getExternalId()) ? "" : " with exID: " + s3BucketConfig.getExternalId()) + "...");
        boolean downloaded = AwsUtils.downloadFileIfChangedSince(getS3ObjectSummary().getBucketName(), s3BucketConfig.getRegion(), prefix, file, file.lastModified(),
        		s3BucketConfig.getAccountId(), s3BucketConfig.getAccessRole(), s3BucketConfig.getExternalId());
        if (downloaded)
            logger.info("downloaded " + fileKey);
        else {
            logger.info("file already downloaded " + fileKey + "...");
        }

        return file;
	}
		
	protected long readFile(File file) {
		InputStream input = null;
        long endMilli = month.getMillis();
        
        try {
            input = new FileInputStream(file);
            if (file.getName().endsWith(".gz")) {
            	input = new GZIPInputStream(input);
            }
        	endMilli = readFile(file.getName(), input);
        }
        catch (IOException e) {
            if (e.getMessage().equals("Stream closed"))
                logger.info("reached end of file.");
            else
                logger.error("Error processing " + file, e);
        }
        finally {
        	try {
        		if (input != null)
        			input.close();
        	}
        	catch (IOException e) {
        		logger.error("Error closing " + file, e);
        	}
        }
        return endMilli;
	}

	protected long readFile(String fileName, InputStream in) {
		CsvParserSettings settings = new CsvParserSettings();
		settings.setHeaderExtractionEnabled(false);
		settings.setNullValue("");
		settings.setEmptyValue("");
		CsvParser parser = new CsvParser(settings);
		

        data = Maps.newHashMap();
        
        long endMilli = month.getMillis();
        long lineNumber = 0;
        try {
    		parser.beginParsing(in);
    		String[] row;
    		            
            // load the header
            initIndecies(parser.parseNext());
            lineNumber++;

            while ((row = parser.parseNext()) != null) {
                lineNumber++;
                try {
                    long end = processOneLine(row);
                    if (end > endMilli)
                    	endMilli = end;
                }
                catch (Exception e) {
                    logger.error(StringUtils.join(row, ","), e);
                }

                if (lineNumber % 500000 == 0) {
                    logger.info("processed " + lineNumber + " lines...");
                }
            }
			parser.stopParsing();
        }
        catch (Exception e ) {
            logger.error("Error processing " + fileName + " at line " + lineNumber, e);
        }
        logger.info("processed " + lineNumber + " lines from file: " + fileName);
        return endMilli;
	}
	
	private void initIndecies(String[] header) {
		reportIndeces = Maps.newHashMap();
		userTagIndeces = Maps.newHashMap();
		
		List<String> unreferenced = Lists.newArrayList();
		List<Integer> empty = Lists.newArrayList();
		for (int i = 0; i < header.length; i++) {
			if (allocationConfig.getOut() != null && allocationConfig.getOut().containsValue(header[i])) {
				for (Entry<String, String> e: allocationConfig.getOut().entrySet()) {
					if (e.getValue().equals(header[i])) {
						userTagIndeces.put(e.getKey(), i);
						break;
					}
				}
			}
			else {
				try {
					KubernetesColumn col = KubernetesColumn.valueOf(header[i]);
					reportIndeces.put(col, i);
				}
				catch (IllegalArgumentException e) {
					if (header[i].isEmpty())
						empty.add(i);
					else
						unreferenced.add(header[i]);
				}
			}
		}
		if (!empty.isEmpty())
			logger.warn("Empty columns in Kubernetes report: " + empty);
		if (!unreferenced.isEmpty())
			logger.info("Unreferenced columns in Kubernetes report: " + unreferenced);
		
		// Check that we have all the columns we expect
		List<String> optional = Lists.newArrayList();
		List<String> mandatory = Lists.newArrayList();
		for (KubernetesColumn col: KubernetesColumn.values()) {
			if (!reportIndeces.containsKey(col)) {
				if (col == KubernetesColumn.Type || col == KubernetesColumn.Resource || col == KubernetesColumn.UsageType)
					optional.add(col.toString());
				else
					mandatory.add(col.toString());
			}
		}		
		if (!optional.isEmpty())
			logger.info("Kubernetes report does not have columns for optional fields: " + optional);
		if (!mandatory.isEmpty())
			logger.error("Kubernetes report does not have columns for mandatory fields: " + mandatory);
	}
	
	private long processOneLine(String[] item) {
		DateTime startDate = new DateTime(item[reportIndeces.get(KubernetesColumn.StartDate)], DateTimeZone.UTC);
		long millisStart = startDate.getMillis();
		DateTime endDate = new DateTime(item[reportIndeces.get(KubernetesColumn.EndDate)], DateTimeZone.UTC);
		long millisEnd = endDate.getMillis();
        int startIndex = (int)((millisStart - startMillis)/ AwsUtils.hourMillis);
        int endIndex = (int)((millisEnd + 1000 - startMillis)/ AwsUtils.hourMillis);
        
        if (startIndex < 0 || startIndex > 31 * 24) {
        	logger.error("StartDate outside of range for month. Month start=" + month.getYear() + "-" + month.getDayOfMonth() + ", StartDate=" + startDate.getYear() + "-" + startDate.getDayOfMonth());
        	return startMillis;
        }
        if (endIndex > startIndex + 1) {
        	logger.error("EndDate more than one hour after StartDate. StartDate=" + startDate.getYear() + "-" + startDate.getDayOfMonth() + ", EndDate=" + endDate.getYear() + "-" + endDate.getDayOfMonth());
        	return startMillis;
        }
        
        String cluster = item[reportIndeces.get(KubernetesColumn.Cluster)];
        List<List<String[]>> clusterData = data.get(cluster);
        if (clusterData == null) {
        	clusterData = Lists.newArrayList();
        	data.put(cluster, clusterData);
        }
        // Expand the data lists if not long enough
        for (int i = clusterData.size(); i < startIndex + 1; i++) {
        	List<String[]> hourData = Lists.newArrayList();
        	clusterData.add(hourData);
        }
        
        List<String[]> hourData = clusterData.get(startIndex);
        hourData.add(item);
        
		return millisEnd;
	}
	
	public Set<String> getClusters() {
		return data.keySet();
	}

	public boolean hasData(Collection<String> possibleClusterNames) {
		for (String cluster: possibleClusterNames) {
			if (data.containsKey(cluster))
				return true;
		}
		return false;
	}
	
	public String getClusterName(UserTag[] userTags) {
		// return the first matching cluster name
		for (String name: clusterNameBuilder.getClusterNames(userTags)) {
			if (data.containsKey(name))
				return name;
		}
		
		return null;
	}
	
	public List<List<String[]>> getData(String cluster) {
		return data.get(cluster);
	}
	
	public List<String[]> getData(String cluster, int hour, String usageType) {
		List<List<String[]>> clusterData = data.get(cluster);
		if (clusterData == null || clusterData.size() <= hour)
			return null;
		
		List<String[]> hourData = clusterData.get(hour);
		if (usageType != null && !usageType.isEmpty()) {
			// Pull items with matching usage type
			List<String[]> all = hourData;
			hourData = Lists.newArrayListWithCapacity(all.size());
			for (String[] item: all) {
				if (usageType.equals(getString(item, KubernetesColumn.UsageType)))
					hourData.add(item);
			}
			
		}
		return hourData;
	}
	
	public Type getType(String[] item) {
		String t = getString(item, KubernetesColumn.Type);
		return t.isEmpty() ? Type.None : Type.valueOf(t);
	}
	
	public String getString(String[] item, KubernetesColumn col) {
		Integer i = reportIndeces.get(col);
		return i == null ? "" : item[i];
	}
	
	public double getDouble(String[] item, KubernetesColumn col) {
		String s = getString(item, col);
		
		return s.isEmpty() || s.equalsIgnoreCase("nan") || s.equalsIgnoreCase("inf") ? 0 : Double.parseDouble(s);
	}
	
	public String getUserTag(String[] item, String col) {
		return userTagIndeces.get(col) == null ? "" : item[userTagIndeces.get(col)];
	}

	public List<String> getTagValues(String[] item, List<String> tagKeys) {
		List<String> values = Lists.newArrayList();
		for (String key: tagKeys) {
			values.add(deployParams.containsKey(key) ? getString(item, deployParams.get(key)) : getUserTag(item, key));
		}
		return values;
	}
	
	// Return an empty set of tag values with the deploy parameters set to "unused"
	public List<String> getUnusedTagValues(List<String> tagKeys) {
		List<String> values = Lists.newArrayList();
		for (String key: tagKeys) {
			values.add(deployParams.containsKey(key) ? "unused" : "");
		}
		return values;
	}
	
	public ClusterNameBuilder getClusterNameBuilder() {
		return clusterNameBuilder;
	}
		
	public AllocationConfig getConfig() {
		return allocationConfig;
	}
	
	public double getAllocationFactor(Product product, String[] item) {
		if (product.isEc2Instance() || product.isCloudWatch()) {
			double cpuCores = Math.max(getDouble(item, KubernetesColumn.RequestsCPUCores), getDouble(item, KubernetesColumn.UsedCPUCores));
			double clusterCores = getDouble(item, KubernetesColumn.ClusterCPUCores);
			double memoryGiB = Math.max(getDouble(item, KubernetesColumn.RequestsMemoryGiB), getDouble(item, KubernetesColumn.UsedMemoryGiB));
			double clusterMemoryGiB = getDouble(item, KubernetesColumn.ClusterMemoryGiB);
			double unitsPerCluster = clusterCores * vCpuToMemoryCostRatio + clusterMemoryGiB;
			return unitsPerCluster <= 0 ? 0 : ((cpuCores * vCpuToMemoryCostRatio + memoryGiB) / unitsPerCluster);
		}
		else if (product.isEbs()) {
			double pvcGiB = getDouble(item, KubernetesColumn.PersistentVolumeClaimGiB);
			double clusterPvcGiB = getDouble(item, KubernetesColumn.ClusterPersistentVolumeClaimGiB);
			return clusterPvcGiB <= 0 ? 0 : (pvcGiB / clusterPvcGiB);
		}
		else if (product.isDataTransfer()) {
			double networkGiB = getDouble(item, KubernetesColumn.NetworkInGiB) + getDouble(item, KubernetesColumn.NetworkOutGiB);
			double clusterNetworkGiB = getDouble(item, KubernetesColumn.ClusterNetworkInGiB) + getDouble(item, KubernetesColumn.ClusterNetworkOutGiB);
			return clusterNetworkGiB <= 0 ? 0 : (networkGiB / clusterNetworkGiB);
		}
		return 0;
	}

	public DateTime getMonth() {
		return month;
	}
}

