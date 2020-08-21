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
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.csvreader.CsvReader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.BillingBucket;
import com.netflix.ice.processor.Report;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.tag.UserTag;

public class KubernetesReport extends Report {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    private final DateTime month;
    private final long startMillis;
    private final KubernetesConfig config;
    private final int computeIndex;
    private final int namespaceIndex;
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
    }
    
    private Map<KubernetesColumn, Integer> reportIndecies = null;
    private Map<String, Integer> userTagIndecies = null;
    // Map of clusters with hourly data for the month - index will range from 0 to 743
    private Map<String, List<List<String[]>>> data = null;
    private final Tagger tagger;

	public KubernetesReport(S3ObjectSummary s3ObjectSummary, KubernetesConfig config,
			DateTime month, ResourceService resourceService) {
    	super(s3ObjectSummary, new BillingBucket()
    								.withS3BucketName(config.getBucket())
    								.withS3BucketRegion(config.getRegion())
    								.withS3BucketPrefix(config.getPrefix())
    								.withAccountId(config.getAccountId())
    								.withAccessRoleName(config.getAccessRole())
    								.withAccessExternalId(config.getExternalId())
    								);
    	this.month = month;
    	this.startMillis = month.getMillis();
		this.config = config;
		this.computeIndex = StringUtils.isEmpty(config.getComputeTag()) ? -1 : resourceService.getUserTagIndex(config.getComputeTag());
		this.namespaceIndex = StringUtils.isEmpty(config.getNamespaceTag()) ? -1 : resourceService.getUserTagIndex(config.getNamespaceTag());
		
		List<String> clusterNameFormulae = config.getClusterNameFormulae();
		this.clusterNameBuilder = clusterNameFormulae == null || clusterNameFormulae.isEmpty() ? null : new ClusterNameBuilder(config.getClusterNameFormulae(), resourceService.getCustomTags());		
    	this.tagger = new Tagger(config.getTags(), config.getNamespaceMappings());
	}

	public long loadReport(String localDir)
			throws Exception {
		
		File file = download(localDir);
    	String fileKey = getReportKey();
        logger.info("loading " + fileKey + "...");
		long end = readFile(file);
        logger.info("done loading " + fileKey + ", end is " + LineItem.amazonBillingDateFormat.print(new DateTime(end)));
        return end;
	}

	private File download(String localDir) {
        String fileKey = getS3ObjectSummary().getKey();
		String prefix = fileKey.substring(0, fileKey.lastIndexOf("/") + 1);
		String filename = fileKey.substring(prefix.length());
        File file = new File(localDir, filename);

        if (getS3ObjectSummary().getLastModified().getTime() > file.lastModified()) {
	        logger.info("trying to download " + getS3ObjectSummary().getBucketName() + "/" + prefix + file.getName() + 
	        		" from account " + billingBucket.accountId + " using role " + billingBucket.accessRoleName + 
	        		(StringUtils.isEmpty(billingBucket.accessExternalId) ? "" : " with exID: " + billingBucket.accessExternalId) + "...");
	        boolean downloaded = AwsUtils.downloadFileIfChangedSince(getS3ObjectSummary().getBucketName(), billingBucket.s3BucketRegion, prefix, file, file.lastModified(),
	                billingBucket.accountId, billingBucket.accessRoleName, billingBucket.accessExternalId);
	        if (downloaded)
	            logger.info("downloaded " + fileKey);
	        else {
	            logger.info("file already downloaded " + fileKey + "...");
	        }
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

	private long readFile(String fileName, InputStream in) {

        CsvReader reader = new CsvReader(new InputStreamReader(in), ',');
        data = Maps.newHashMap();
        
        long endMilli = month.getMillis();
        long lineNumber = 0;
        try {
            reader.readRecord();
            
            // load the header
            initIndecies(reader.getValues());

            while (reader.readRecord()) {
                String[] items = reader.getValues();
                try {
                    long end = processOneLine(items);
                    if (end > endMilli)
                    	endMilli = end;
                }
                catch (Exception e) {
                    logger.error(StringUtils.join(items, ","), e);
                }
                lineNumber++;

                if (lineNumber % 500000 == 0) {
                    logger.info("processed " + lineNumber + " lines...");
                }
            }
        }
        catch (IOException e ) {
            logger.error("Error processing " + fileName + " at line " + lineNumber, e);
        }
        finally {
            try {
                reader.close();
            }
            catch (Exception e) {
                logger.error("Cannot close BufferedReader...", e);
            }
        }
        return endMilli;
	}
	
	private void initIndecies(String[] header) {
		reportIndecies = Maps.newHashMap();
		userTagIndecies = Maps.newHashMap();
		
		for (int i = 0; i < header.length; i++) {
			if (config.getTags().contains(header[i])) {
				userTagIndecies.put(header[i], i);
			}
			else {
				try {
					KubernetesColumn col = KubernetesColumn.valueOf(header[i]);
					reportIndecies.put(col, i);
				}
				catch (IllegalArgumentException e) {
					logger.warn("Undefined column in Kubernetes report: " + header[i]);
				}
			}
		}
		
		// Check that we have all the columns we expect
		for (KubernetesColumn col: KubernetesColumn.values()) {
			if (!reportIndecies.containsKey(col)) {
				if (col == KubernetesColumn.Type || col == KubernetesColumn.Resource)
					logger.info("Kubernetes report does not have column for " + col);
				else
					logger.error("Kubernetes report does not have column for " + col);
			}
		}		
	}
	
	private long processOneLine(String[] item) {
		DateTime startDate = new DateTime(item[reportIndecies.get(KubernetesColumn.StartDate)], DateTimeZone.UTC);
		long millisStart = startDate.getMillis();
		DateTime endDate = new DateTime(item[reportIndecies.get(KubernetesColumn.EndDate)], DateTimeZone.UTC);
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
        
        String cluster = item[reportIndecies.get(KubernetesColumn.Cluster)];
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
	
	public List<String[]> getData(String cluster, int i) {
		List<List<String[]>> clusterData = data.get(cluster);
		return clusterData == null || clusterData.size() <= i ? null : clusterData.get(i);
	}
	
	public String getString(String[] item, KubernetesColumn col) {
		Integer i = reportIndecies.get(col);
		return i == null ? "" : item[i];
	}
	
	public double getDouble(String[] item, KubernetesColumn col) {
		String s = getString(item, col);
		return s.isEmpty() || s.equalsIgnoreCase("nan") || s.equalsIgnoreCase("inf") ? 0 : Double.parseDouble(s);
	}
	
	public String getUserTag(String[] item, String col) {
		return userTagIndecies.get(col) == null ? "" : item[userTagIndecies.get(col)];
	}

	public Tagger getTagger() {
		return tagger;
	}
	
	public boolean isCompute(UserTag[] userTags) {
		return userTags[computeIndex] != null && userTags[computeIndex].name.equals(config.getComputeValue());
	}
	
	public ClusterNameBuilder getClusterNameBuilder() {
		return clusterNameBuilder;
	}
	
	public int getNamespaceIndex() {
		return namespaceIndex;
	}
	
	public KubernetesConfig getConfig() {
		return config;
	}
}

