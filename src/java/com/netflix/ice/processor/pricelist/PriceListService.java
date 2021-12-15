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
package com.netflix.ice.processor.pricelist;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.processor.pricelist.Index.Offer;
import com.netflix.ice.processor.pricelist.InstancePrices.Key;
import com.netflix.ice.processor.pricelist.InstancePrices.OfferingClass;
import com.netflix.ice.processor.pricelist.InstancePrices.OperatingSystem;
import com.netflix.ice.processor.pricelist.InstancePrices.Product;
import com.netflix.ice.processor.pricelist.InstancePrices.Rate;
import com.netflix.ice.processor.pricelist.InstancePrices.RateKey;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.processor.pricelist.InstancePrices.Tenancy;
import com.netflix.ice.processor.pricelist.VersionIndex.Version;
import com.netflix.ice.reader.InstanceMetrics;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class PriceListService {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String domain = "https://pricing.us-east-1.amazonaws.com";
	private static final String priceListIndexUrl = "/offers/v1.0/aws/index.json";

	// Add other Tenancy values when needed - must also add to Key if more than one or have the tenancy reflected in the usage type
	public static Set<Tenancy> tenancies = Sets.newHashSet(new Tenancy[]{Tenancy.Shared, Tenancy.Dedicated});
	
	private final String localDir;
	private final String workS3BucketName;
	private final String workS3BucketPrefix;

	/**
	 * A generic cached item with a timestamp indicating when it was last read
	 * An item older than one hour is assumed out-of-date.
	 *
	 * @param <I>
	 */
	class CachedItem<I> {
		public I item;
		public DateTime lastReadAt;
		
		public CachedItem(I item, DateTime lastReadAt) {
			this.item = item;
			this.lastReadAt = lastReadAt;
		}
		
		public boolean isCurrent() {
			return lastReadAt.isAfter(DateTime.now().minusHours(1));
		}
	}
	private CachedItem<Index> index = null;
	
	private Map<String, CachedItem<VersionIndex>> versionIndecies; // Key is URL to version Index
	
	private Map<ServiceCode, Map<String, InstancePrices>> versionedPriceLists; // Keyed by service code. Second key is version ID
	protected InstanceMetrics instanceMetrics;
	
	
	public PriceListService(String localDir, String workS3BucketName, String workS3BucketPrefix) throws Exception {
		this.localDir = localDir;
		this.workS3BucketName = workS3BucketName;
		this.workS3BucketPrefix = workS3BucketPrefix;
		
		versionIndecies = Maps.newHashMap();
		versionedPriceLists = Maps.newHashMap();
		for (ServiceCode code: ServiceCode.values()) {
			Map<String, InstancePrices> versionedPrices = Maps.newHashMap();
			versionedPriceLists.put(code, versionedPrices);
		}
		instanceMetrics = null;
	}
	
	public void init() throws Exception {
		// Build the instance metrics from the latest price lists for EC2 and Redshift.
		// (RDS doesn't contribute anything that EC2 doesn't have)
		// Load our cached copy if we have one and then see what price list versions
		// it was built from.
		loadInstanceMetrics();
		
		ServiceCode[] serviceCodes = { ServiceCode.AmazonEC2, ServiceCode.AmazonRedshift };
		boolean current = true;
		DateTime now = DateTime.now();
		Index index = getIndex();
		
		for (ServiceCode sc: serviceCodes) {
			VersionIndex vi = getVersionIndex(index, sc);
			String id = vi.getVersionId(now);
			if (!StringUtils.equals(instanceMetrics.getPriceListVersion(sc), id)) {
				current = false;
				break;
			}
		}
		
		if (current) {
			// Price lists haven't changed since we last generated the metrics
			return;
		}
		
		// Regenerate metrics from scratch
		instanceMetrics = new InstanceMetrics();
		
		// Load latest price lists
		for (ServiceCode sc: serviceCodes) {
			InstancePrices prices = getPrices(now, sc);
			// scan all the products and grab the metrics
        	for (Product p: prices.getPrices().values()) {
	        	// Add stats to InstanceMetrics
        		if (sc == ServiceCode.AmazonEC2 && !p.operatingSystem.equals(OperatingSystem.Linux.toString()))
        			continue;
	        	instanceMetrics.add(p.instanceType, p.vcpu, p.ecu, p.normalizationSizeFactor);
        	}
		}
       	archiveInstanceMetrics();
	}
	
	public InstanceMetrics getInstanceMetrics() throws IOException {
		return instanceMetrics;
	}
	
	private Index getIndex() throws Exception {
		if (index != null && index.isCurrent() ) {
			// Current cached copy is less than an hour old, so use it.
			return index.item;
		}
		DateTime readTime = DateTime.now();
		InputStream stream = new URL(domain + priceListIndexUrl).openStream();
        index = new CachedItem<Index>(new Index(stream), readTime);
        stream.close();
        return index.item;
	}
	
	private VersionIndex getVersionIndex(Index index, ServiceCode serviceCode) throws Exception {
        Offer offer = index.getOffer(serviceCode.name());
        
        // See if we can use the one in the cache
        CachedItem<VersionIndex> vi = versionIndecies.get(offer.versionIndexUrl);
        if (vi != null && vi.isCurrent()) {
        	return vi.item;
        }
                
		DateTime readTime = DateTime.now();
        InputStream stream = new URL(domain + offer.versionIndexUrl).openStream();        
        VersionIndex versionIndex = new VersionIndex(stream);
        stream.close();
        versionIndecies.put(offer.versionIndexUrl, new CachedItem<VersionIndex>(versionIndex, readTime));
        return versionIndex;
	}
	
    public InstancePrices getPrices(DateTime start, ServiceCode serviceCode) throws Exception {
        VersionIndex versionIndex = getVersionIndex(getIndex(), serviceCode);
	       
        String id = versionIndex.getVersionId(start);
        Version version = versionIndex.getVersion(id);
        
        // Is the current version in our cache?
        InstancePrices prices = versionedPriceLists.get(serviceCode).get(id);
        if (prices == null) {        
	        // See if we've already processed the current version
	        prices = load(serviceCode, id, version);
        }
        
       	return prices;
    }
    
    private String getFilename(ServiceCode serviceCode, String versionId) {
    	return "prices_" + serviceCode + "_" + versionId;
    }
    
    protected InstancePrices load(ServiceCode serviceCode, String versionId, Version version) throws Exception {
        InstancePrices ip = null;
        
    	if (localDir != null) {    	
	    	String name = getFilename(serviceCode, versionId);
	        File file = new File(localDir, name + ".gz");
	        
	        if (workS3BucketName != null) {
		        logger.info("downloading " + file + "...");
		        AwsUtils.downloadFileIfNotExist(workS3BucketName, workS3BucketPrefix + file.getName(), file);
	        	logger.info("downloaded " + file);
	        }
	
	        if (file.exists()) {
	        	InputStream is = new FileInputStream(file);
	        	is = new GZIPInputStream(is);
	            DataInputStream in = new DataInputStream(is);
	            try {
	                ip = InstancePrices.Serializer.deserialize(in);
	            }
	            finally {
	                if (in != null)
	                    in.close();
	            }
	           	versionedPriceLists.get(serviceCode).put(versionId, ip);
	            return ip;
	        }
    	}

        ip = fetchCSV(serviceCode, versionId, version);
       	versionedPriceLists.get(serviceCode).put(versionId, ip);
        return ip;
    }
    
    protected InstancePrices fetchCSV(ServiceCode serviceCode, String versionId, Version version) throws Exception {
    	final String domain = "https://pricing.us-east-1.amazonaws.com";
    	String offerVersionUrl = version.offerVersionUrl.replace(".json", ".csv");
        logger.info("fetching price list for " + serviceCode + " from " + domain + offerVersionUrl + "...");
        InputStream stream = new URL(domain + offerVersionUrl).openStream();
        
       	InstancePrices prices = new InstancePrices(serviceCode, versionId, version.getBeginDate(), version.getEndDate());
        if (importPriceList(stream, prices))
        	archive(prices, getFilename(serviceCode, versionId));

        return prices;
    }
    
    protected InstancePrices fetchJSON(ServiceCode serviceCode, String versionId, Version version) throws Exception {
        logger.info("fetching price list for " + serviceCode + " from " + domain + version.offerVersionUrl + "...");
        InputStream stream = new URL(domain + version.offerVersionUrl).openStream();
        PriceList priceList = new PriceList(stream);
        
       	InstancePrices prices = new InstancePrices(serviceCode, versionId, version.getBeginDate(), version.getEndDate());
       	prices.importPriceList(priceList, tenancies);
       	
       	archive(prices, getFilename(serviceCode, versionId));
       	
       	return prices;
    }
    
    protected void archive(InstancePrices prices, String name) throws IOException {
    	if (localDir == null)
    		return;
    	
        logger.info("archiving price list " + name + "...");
        File file = new File(localDir, name + ".gz");
    	OutputStream os = new FileOutputStream(file);
		os = new GZIPOutputStream(os);
        DataOutputStream out = new DataOutputStream(os);
        try {
            InstancePrices.Serializer.serialize(out, prices);
        }
        finally {
            out.close();
        }
        
        if (workS3BucketName != null) {
	        logger.info(name + " uploading to s3...");
	        AwsUtils.upload(workS3BucketName, workS3BucketPrefix + file.getName(), file);
	        logger.info(name + " uploading done.");
        }
    }
    
	protected void loadInstanceMetrics() throws IOException {
		instanceMetrics = new InstanceMetrics();
		if (localDir == null)
			return;
		
		File file = new File(localDir, InstanceMetrics.dbName);
		
		if (workS3BucketName != null) {
			logger.info("downloading " + file + "...");
			AwsUtils.downloadFileIfNotExist(workS3BucketName, workS3BucketPrefix + file.getName(), file);
		}
	
		if (file.exists()) {
		    logger.info("trying to load " + file);
	        DataInputStream in = new DataInputStream(new FileInputStream(file));
	        try {
	            instanceMetrics = InstanceMetrics.Serializer.deserialize(in);
	            logger.info("done loading " + file);
	        }
	        finally {
	            in.close();
	        }
	    }
	}
	
    protected void archiveInstanceMetrics() throws IOException {
    	if (instanceMetrics == null || localDir == null)
    		return;
    	
       	logger.info("archiving instance metrics " + InstanceMetrics.dbName + "...");
        File file = new File(localDir, InstanceMetrics.dbName);
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        try {
        	InstanceMetrics.Serializer.serialize(out, instanceMetrics);
        }
        finally {
            out.close();
        }
        
        if (workS3BucketName != null) {
	        logger.info(InstanceMetrics.dbName + " uploading to s3...");
	        AwsUtils.upload(workS3BucketName, workS3BucketPrefix + file.getName(), file);
	        logger.info(InstanceMetrics.dbName + " uploading done.");
        }
    }
    
	// Price list columns for EC2, RDS, ES, ElastiCache and Redshift price list CSV files
	public enum Column {
		SKU,
		OfferTermCode,
		RateCode,
		TermType,
		PriceDescription,
		EffectiveDate,
		StartingRange,
		EndingRange,
		Unit,
		PricePerUnit,
		Currency,
		RelatedTo, // RDS, ElastiCache and Redshift only
		LeaseContractLength,
		PurchaseOption,
		OfferingClass,
		ProductFamily("Product Family"),
		ServiceCode("serviceCode"),
		Description, // Redshift only
		Location,
		LocationType("Location Type"),
		InstanceType("Instance Type"),
		CurrentGeneration("Current Generation"),
		InstanceFamily("Instance Family"), // EC2 and RDS only
		vCPU,
		PhysicalProcessor("Physical Processor"), // EC2 and RDS only
		ClockSpeed("Clock Speed"),  // EC2 and RDS only
		Memory, // EC2, RDS, ElastiCache and Redshift only
		Storage, // EC2, RDS, ES and Redshift only
		NetworkPerformance("Network Performance"), // EC2, ElastiCache, and RDS only
		ProcessorArchitecture("Processor Architecture"), // EC2 and RDS only
		CacheEngine("Cache Engine"), // ElastiCache only
		IO("I/O"), // Redshift only
		StorageMedia("Storage Media"),
		VolumeType("Volume Type"), // EC2 and RDS only
		MinVolumeSize("Min Volume Size"), // RDS only
		MaxVolumeSize("Max Volume Size"), // EC2 and RDS only
		MaxIopsPerVolume("Max IOPS/volume"), // EC2 only
		MaxIopsBurstPerformance("Max IOPS Burst Performance"), // EC2 only
		MaxThroughputPerVolume("Max throughput/volume"), // EC2 only
		Provisioned, // EC2 only
		Tenancy, // EC2 only
		EbsOptimized("EBS Optimized"), // EC2 only
		OperatingSystem("Operating System"), // EC2 only
		EngineCode("Engine Code"), // RDS only
		DatabaseEngine("Database Engine"), // RDS only
		DatabaseEdition("Database Edition"), // RDS only
		LicenseModel("License Model"), // EC2 and RDS only
		DeploymentOption("Deployment Option"), // RDS only
		EngineMediaType("Engine Media Type"), // RDS only
		Group, // EC2, RDS, Redshift, ElastiCache only
		GroupDescription("Group Description"), // EC2, RDS, Redshift, ElastiCache only
		TransferType("Transfer Type"), // EC2 only
		FromLocation("From Location"), // EC2 only
		FromLocationType("From Location Type"), // EC2 only
		ToLocation("To Location"), // EC2 only
		ToLocationType("To Location Type"), // EC2 only
		UsageType("usageType"),
		Operation("operation"),
		AvailabilityZone, // EC2 only
		CapacityStatus, // EC2 only
		ClassicNetworkingSupport, // EC2 only
		ConcurrencyScalingFreeUsage, // Redshift only
		DedicatedEbsThroughput("Dedicated EBS Throughput"), // EC2 and RDS only
		DeploymentModel("Deployment Model"), // RDS only
		ECU, // EC2 and Redshift only
		PricingUnit("Pricing Unit"), // Redshift only
		ElasticGraphicsType("Elastic Graphics Type"), // EC2 only
		MemoryGiB("Memory (GiB)"), // ES only
		EnhancedNetworkingSupported("Enhanced Networking Supported"), // EC2 and RDS only
		FromRegionCode("From Region Code"), // EC2 only
		GPU, // EC2 only
		InstanceTypeFamily("Instance Type Family"), // RDS only
		
		// EC2 only
		GpuMemory("GPU Memory"),
		Instance,
		InstanceCapacity10xlarge("Instance Capacity - 10xlarge"),
		InstanceCapacity12xlarge("Instance Capacity - 12xlarge"),
		InstanceCapacity16xlarge("Instance Capacity - 16xlarge"),
		InstanceCapacity18xlarge("Instance Capacity - 18xlarge"),
		InstanceCapacity24xlarge("Instance Capacity - 24xlarge"),
		InstanceCapacity2xlarge("Instance Capacity - 2xlarge"),		
		InstanceCapacity32xlarge("Instance Capacity - 32xlarge"),
		InstanceCapacity4xlarge("Instance Capacity - 4xlarge"),
		InstanceCapacity8xlarge("Instance Capacity - 8xlarge"),
		InstanceCapacity9xlarge("Instance Capacity - 9xlarge"),
		InstanceCapacityLarge("Instance Capacity - large"),
		InstanceCapacityMedium("Instance Capacity - medium"),
		InstanceCapacityXlarge("Instance Capacity - xlarge"),
		InstanceCapacityMetal("Instance Capacity - metal"),
		InstanceSKU("instanceSKU"),
		IntelAvxAvailable("Intel AVX Available"),
		IntelAvx2Available("Intel AVX2 Available"),
		IntelTurboAvailable("Intel Turbo Available"),
		MarketOption,
		
		NormalizationSizeFactor("Normalization Size Factor"), // EC2 and RDS only
		PhysicalCores("Physical Cores"), // EC2 only
		PreInstalledSw("Pre Installed S/W"), // EC2 only
		ProcessorFeatures("Processor Features"), // EC2 and RDS only
		ProductType("Product Type"), // EC2 only
		RegionCode("Region Code"),
		ResourceType("Resource Type"), // EC2 only
		ServiceName("serviceName"),
		SnapshotArchiveFeeType, // EC2 only
		ToRegionCode("To Region Code"), // EC2 only
		VolumeName("Volume Name"), // RDS only
		VolumeApiName("Volume API Name"), // EC2 only
		VPCNetworkingSupport, // EC2 only
		UsageFamily("Usage Family"), // Redshift only
		
		// Old EC2 columns
		ElasticGpuType("Elastic GPU Type"),
		Comments,
		Sockets;
		
		private final String header;
		private static Map<String, Column> byHeaderName;
		
		static {
			byHeaderName = Maps.newHashMap();
			for (Column v: Column.values()) {
				byHeaderName.put(v.header, v);
			}
		}
		
		Column() {
			this.header = this.name();
		}
		Column(String header) {
			this.header = header;
		}
		
		static Column get(String header) {
			return byHeaderName.get(header);
		}
	}
	
	public class Getter {
		private int[] index;
		
		public Getter(String[] headerNames) {
	    	index = new int[Column.values().length];
	    	
	    	for (int i = 0; i < index.length; i++) {
	    		index[i] = -1;
	    	}
	    	
	    	for (int i = 0; i < headerNames.length; i++) {
	    		Column col = Column.get(headerNames[i]);
	    		if (col == null) {
	    			logger.error("No enum for column header: " + headerNames[i]);
	    			continue;
	    		}
	    		index[col.ordinal()] = i;
	    	}
		}
		
		public String value(String[] items, Column col) {
			int i = index[col.ordinal()];
	    	return i < 0 ? null : items[i];
		}
	}

	private void checkHeader(int index, String[] items) throws Exception {
		String[] lines = new String[]{
				"FormatVersion",
				"Disclaimer",
				"Publication Date",
				"Version",
				"OfferCode",
		};
		
		if (items.length != 2) {
			throw new Exception("Wrong number of items in header line. Expected 2: " + Lists.newArrayList(items));
		}
		if (!lines[index].equals(items[0])) {
			throw new Exception("Wrong header line. Expected " + lines[index] + ", got: " + Lists.newArrayList(items));
		}
		switch(index) {
		case 0:
			if (!items[1].equals("v1.0")) {
				throw new Exception("Wrong pricelist file version, should be v1.0, got: " + items[1]);
			}
			break;
		default:
			break;
		}
	}
	
    private InstancePrices.Product getProduct(InstancePrices prices, Getter getter, String[] items) {
    	// Get the product key
    	String location = getter.value(items, Column.Location);
    	String productFamily = getter.value(items, Column.ProductFamily);
    	String tenancy = getter.value(items, Column.Tenancy);
    	String operation = getter.value(items, Column.Operation);
    	String operatingSystem = getter.value(items, Column.OperatingSystem);
    	String usageType = getter.value(items, Column.UsageType);
    	String deploymentOption = getter.value(items, Column.DeploymentOption);
    	String instanceType = getter.value(items, Column.InstanceType);
    	String capacityStatus = getter.value(items, Column.CapacityStatus);
    	
    	//if (capacityStatus )
    	
    	Key key = prices.getKey(location, productFamily, tenancy, operation, operatingSystem, usageType, instanceType, deploymentOption, PriceListService.tenancies, capacityStatus);
    	if (key == null)
    		return null;
    	
    	// Create the Product
    	String sku = getter.value(items, Column.SKU);
    	
    	Product product = prices.getProduct(key);
    	if (product == null) {
	    	String memory = getter.value(items, Column.Memory);
			if (memory != null) {
				String[] memoryParts = memory.split(" ");
				if (!memoryParts[1].toLowerCase().equals("gib") && !memoryParts[1].toLowerCase().equals("gb")) {
					logger.error("Found PriceList entry with product memory using non-GiB/GB units: " + memoryParts[1] + ", usageType: " + usageType);
					prices.setErrors(true);
				}
				memory = memoryParts[0].replace(",", "");
			}
			else {
				memory = getter.value(items, Column.MemoryGiB);
			}

	    	String ecu = getter.value(items, Column.ECU);
	    	String nsf = getter.value(items, Column.NormalizationSizeFactor);
	    	String vcpu = getter.value(items, Column.vCPU);
	    	String preinstalledSw = getter.value(items, Column.PreInstalledSw);
	    	String databaseEngine = getter.value(items, Column.DatabaseEngine);
	    	String databaseEdition = getter.value(items, Column.DatabaseEdition);
	    	
	    	product = new InstancePrices.Product(sku, memory, ecu, nsf, vcpu, instanceType, operatingSystem, operation, usageType, preinstalledSw, databaseEngine, databaseEdition);
	    	if (key.usageType.name.endsWith(".others")) {
	    		logger.error("Pricelist entry with unknown usage type: " + product);
				prices.setErrors(true);
	    	}
    		prices.setProduct(key, product);
    	}
    	else if (!product.sku.equals(sku)) {
    		logger.error("Pricelist product has two SKUs with same product key: " + product.sku + ", " + sku + ". Existing product: " + product + ", Ignored conflicting product: " + String.join(",", items));
			prices.setErrors(true);
    		product = null;
    	}
    	return product;
    }
    
    private void addTerm(Product product, Getter getter, String[] items) {
    	String termType = getter.value(items, Column.TermType);
    	if (termType.equals("OnDemand")) {
    		product.setOnDemandRate(Double.parseDouble(getter.value(items, Column.PricePerUnit)));
    	}
    	else if (termType.equals("Reserved")) {
    		String leaseContractLength = getter.value(items, Column.LeaseContractLength);
    		String purchaseOption = getter.value(items, Column.PurchaseOption);
    		
    		String offeringClass = getter.value(items, Column.OfferingClass);
    		if (offeringClass.isEmpty()) {
				// only standard offering class was available before March 2017
				offeringClass = OfferingClass.standard.name();
    		}
    		
			RateKey rateKey = new RateKey(leaseContractLength, purchaseOption, offeringClass);
			
			Rate rate = product.getReservationRate(rateKey);
			if (rate == null) {
				rate = new Rate();
				product.setReserationRate(rateKey, rate);
			}
			String unit = getter.value(items, Column.Unit);
			double pricePerUnit = Double.parseDouble(getter.value(items, Column.PricePerUnit));
			if (unit.equals("Quantity"))
				rate.fixed = pricePerUnit;
			else if (unit.equals("Hrs"))
				rate.hourly = pricePerUnit;				
    	}
    }

	/**
	 * return true if successful
	 */
	private boolean importPriceList(InputStream stream, InstancePrices prices) {
		CsvParserSettings settings = new CsvParserSettings();
		settings.setHeaderExtractionEnabled(false);
		settings.setNullValue("");
		settings.setEmptyValue("");
		CsvParser parser = new CsvParser(settings);
		
		try {
			parser.beginParsing(stream);
			String[] row;
	        // process multi-line header
	        for (int i = 0; i < 5; i++) {
	            row = parser.parseNext();
	        	checkHeader(i, row);
	        }
	        
	        Getter getter = new Getter(parser.parseNext());
	        long lineNumber = 6;
			
			while ((row = parser.parseNext()) != null) {
				lineNumber++;
	            try {
	            	// Only process USD prices
	            	if (!getter.value(row, Column.Currency).equals("USD"))
	            		continue;
	            	
	            	Product product = getProduct(prices, getter, row);
	            	if (product == null)
	            		continue;
	            	
	            	addTerm(product, getter, row);
	            }
	            catch (Exception e) {
	                logger.error("Error on line " + lineNumber + ": " + StringUtils.join(row, ","), e);
	            }
			}
			parser.stopParsing();
		}
	    catch (Exception e ) {
	        logger.error("Error processing price list data: ", e);
	        return false;
	    }			
        
        return true;
    }
    
    
}
