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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.processor.pricelist.PriceList.Product.Attributes;
import com.netflix.ice.processor.pricelist.PriceList.Term;
import com.netflix.ice.tag.InstanceCache;
import com.netflix.ice.tag.InstanceDb;
import com.netflix.ice.tag.InstanceOs;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class InstancePrices implements Comparable<InstancePrices> {
	protected static Logger logger = LoggerFactory.getLogger(PriceListService.class);

	private final String versionId;
	private final DateTime effectiveBeginDate;
	private final DateTime effectiveEndDate;
	private boolean hasErrors;
	private Map<Key, Product> prices = Maps.newHashMap();

	public enum ServiceCode {
		AmazonEC2,
		AmazonRDS,
		AmazonRedshift,
		AmazonElastiCache,
		AmazonES;
	}

	private final ServiceCode serviceCode;
	
	public InstancePrices(ServiceCode serviceCode, String versionId, DateTime begin, DateTime end) {
		this.serviceCode = serviceCode;
		this.versionId = versionId;
		effectiveBeginDate = begin;
		effectiveEndDate = end;
		hasErrors = false;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		sb.append("\tserviceCode: " + serviceCode + "\n");
		sb.append("\tversionId: " + versionId + "\n");
		sb.append("\teffectiveBeginDate: " + effectiveBeginDate + "\n");
		sb.append("\teffectiveEndDate: " + effectiveEndDate + "\n");
		sb.append("\tprices: {\n");
		sb.append("\t\tsize: " + prices.size() + "\n");
		for (Entry<Key, Product> entry: prices.entrySet()) {
			sb.append("\t\t" + entry.getKey() + ": " + entry.getValue() + "\n");
		}
		sb.append("\t}\n");
		sb.append("}\n");
		return sb.toString();
	}
	
	public boolean hasErrors() {
		return hasErrors;
	}

	public void setErrors(boolean hasErrors) {
		this.hasErrors = hasErrors;
	}
	
    public Map<Key, Product> getPrices() {
		return prices;
	}
    
    public ServiceCode getServiceCode() {
		return serviceCode;
	}

    public String getVersionId() {
		return versionId;
	}

	public DateTime getEffectiveBeginDate() {
		return effectiveBeginDate;
	}

	public DateTime getEffectiveEndDate() {
		return effectiveEndDate;
	}
	
	public Product getProduct(Key productKey) {
		return prices.get(productKey);
	}
	
	public void setProduct(Key key, Product product) {
		prices.put(key, product);
	}
	
	public Product getProduct(Region region, UsageType usageType) {
		Key key = new Key(region, usageType);
		Product product = getProduct(key);
		if (product == null) {
			logger.error("No product for key: " + key);
			return null;
		}
		return product;
	}
	
	public double getOnDemandRate(Region region, UsageType usageType) {
		Product product = getProduct(region, usageType);
		return product == null ? 0.0 : product.getOnDemandRate();
	}
	
	public Rate getReservationRate(Region region, UsageType usageType, LeaseContractLength lcl, PurchaseOption po, OfferingClass oc) {
		return getReservationRate(new Key(region, usageType), new RateKey(lcl, po, oc));
	}
	
	public Rate getReservationRate(Key productKey, RateKey rateKey) {
		Product product = getProduct(productKey);
		if (product == null) {
			logger.error("No product for key: " + productKey);
			return null;
		}
		return product.getReservationRate(rateKey);
	}
	
	protected Key getKey(
			String location, 
			String productFamily, 
			String tenancyStr, 
			String operation, 
			String operatingSystem, 
			String usageTypeStr,
			String instanceType,
			String deploymentOption, 
			Set<Tenancy> tenancies,
			String capacityStatusStr) {
    	// There is one entry for RDS db.t1.micro with location of "Any". We'll ignore that one.
    	// Also skip GovCloud and non-instance SKUs.
    	if (productFamily == null || !productFamily.contains("Instance") || location.contains("GovCloud") || location.equals("Any"))
    		return null;
    	
    	Tenancy tenancy = StringUtils.isEmpty(tenancyStr) ? Tenancy.NA : Tenancy.valueOf(tenancyStr);
    	switch (serviceCode) {
        	case AmazonEC2:
            	if (productFamily.equals("Compute Instance") && !tenancyStr.isEmpty()) {
                	if (!tenancies.contains(tenancy))
                		return null;
            	}
            	// Skip anything related to reserved capacity if attribute is available
            	if (!StringUtils.isEmpty(capacityStatusStr)) {
            		// As of July 2021, EC2 pricelists started to contain capacity status strings with
					// Fleet codes tacked on the end: e.g. "UnusedCapacityReservationFFP-C42T22-1"
					// so now we just look for a prefix
	            	//CapacityStatus capacityStatus = CapacityStatus.valueOf(capacityStatusStr);
					//if (capacityStatus == CapacityStatus.AllocatedCapacityReservation || capacityStatus == CapacityStatus.UnusedCapacityReservation)
					//	return null;
					if (capacityStatusStr.startsWith(CapacityStatus.AllocatedCapacityReservation.name()) ||
							capacityStatusStr.startsWith(CapacityStatus.UnusedCapacityReservation.name()))
						return null;
            	}
            	
        		if (!operation.startsWith("RunInstances") ||
        				operatingSystem.equals("NA")) {
        			return null;
        		}
        		// Check that Dedicated Host and Dedicated Instance tenancies are enabled
        		if ((usageTypeStr.contains("HostBoxUsage") && !tenancies.contains(Tenancy.Host))
        				|| (usageTypeStr.contains("DedicatedUsage") && !tenancies.contains(Tenancy.Dedicated))) {
        			return null;
        		}
    			break;
			case AmazonElastiCache:
				if (usageTypeStr.contains("Outpost-"))
					return null;
				break;
    		default:
    			break;
    	}
    
    	Region region = Region.US_EAST_1;
		if (usageTypeStr.contains(":")) {
			// UsageTypeStr contains a colon and region must appear before
			usageTypeStr = usageTypeStr.substring(0, usageTypeStr.indexOf(":"));
		}
		if (usageTypeStr.contains("-") && !usageTypeStr.startsWith("Multi-AZ")) {
    		Region r = Region.getRegionByShortName(usageTypeStr.substring(0, usageTypeStr.indexOf("-")));
    		if (r != null)
    			region = r;
    		else {
    			logger.error("Pricelist entry for unknown region. Usage type found: " + usageTypeStr + ", location: " + location);
    			hasErrors = true;
    			return null; // unsupported region
    		}
    	}
    	UsageType usageType = getUsageType(instanceType, operation, deploymentOption, tenancy);
		return new Key(region, usageType);
	}
	
	public void importPriceList(PriceList priceList, Set<Tenancy> tenancies) {
        Map<String, PriceList.Product> products = priceList.getProducts();
        for (String sku: products.keySet()) {
        	PriceList.Product p = products.get(sku);
        	String location = p.getAttribute(Attributes.location);
        	String t = p.getAttribute(Attributes.tenancy);
        	String operation = p.getAttribute(Attributes.operation);
        	String operatingSystem = p.getAttribute(Attributes.operatingSystem);
        	String usageTypeStr = p.getAttribute(Attributes.usagetype);
        	String deploymentOption = p.getAttribute(Attributes.deploymentOption);
        	String instanceType = p.getAttribute(Attributes.instanceType);
        	String capacityStatusStr = p.getAttribute(Attributes.capacitystatus);
        	
        	Key key = getKey(location, p.productFamily, t, operation, operatingSystem, usageTypeStr, instanceType, deploymentOption, tenancies, capacityStatusStr);
        	if (key == null)
        		continue;
        	
        	if (key.usageType.name.endsWith(".others")) {
        		logger.error("Pricelist entry with unknown usage type: " + p);
        	}
    		PriceList.Terms terms = priceList.getTerms();

        	// Find the OnDemand Rate
    		// First check the OnDemand price
    		PriceList.Rate onDemandRate = null;
    		Map<String, PriceList.Term> offerTerms = terms.OnDemand.get(p.sku);
    		if (offerTerms != null) {
	    		// Get what should be the only term
	    		PriceList.Term term = offerTerms.entrySet().iterator().next().getValue();
	    		// Get what should be the only rate
	    		onDemandRate = term.priceDimensions.entrySet().iterator().next().getValue();
    		}
    		else {
    			logger.info("No OnDemand rate for SKU " + p.sku + ", " + key.region + ", " + key.usageType + ", " + 
    					p.getAttribute(Attributes.operation) + " " + 
    					p.getAttribute(Attributes.operatingSystem) +
    					p.getAttribute(Attributes.databaseEngine));
    		}
    		
        	// Find the Reservation Offers
			Map<String, Term> reservationOfferTerms = terms.Reserved.get(p.sku);

        	Product product = new Product(p, onDemandRate, reservationOfferTerms);
        	
        	// Check that we're not overwriting a product we already added - usually indicates
        	// new pricelist changes with additional options that we're not looking for.
        	if (prices.containsKey(key)) {
        		logger.error("Multiple products with same key: " + key + " Previous: " + prices.get(key) + " Current: " + product);
    			hasErrors = true;
        	}
        	
        	prices.put(key, product);
        }
    }
    
    private UsageType getUsageType(String instanceType, String operation, String deploymentOption, Tenancy tenancy) {
    	String usageTypeStr = instanceType;
    	
        int index = operation.indexOf(":");
        String osStr = index > 0 ? operation.substring(index) : "";

        if (operation.startsWith("RunInstances")) {
        	// EC2 instance
            usageTypeStr = usageTypeStr + InstanceOs.withCode(osStr).usageType;
            if (tenancy == Tenancy.Dedicated)
            	usageTypeStr += ".dedicated";
        }
        else if (operation.startsWith("CreateDBInstance")) {
        	// RDS instance
        	if (deploymentOption.startsWith("Multi-AZ"))
        		usageTypeStr += UsageType.multiAZ;
            InstanceDb db = InstanceDb.withCode(osStr);
            usageTypeStr = usageTypeStr + "." + db;
        }
        else if (operation.startsWith("CreateCacheCluster")) {
        	// ElastiCache
        	usageTypeStr = usageTypeStr + InstanceCache.withCode(osStr).usageType;
        }
    	
    	return UsageType.getUsageType(usageTypeStr, "hours");
    }

	@Override
	public int compareTo(InstancePrices o) {
		return effectiveBeginDate.compareTo(o.effectiveBeginDate);
	}
	
    public static class Serializer {
    	
        public static void serialize(DataOutput out, InstancePrices ip) throws IOException {
        	out.writeUTF(ip.serviceCode.name());
        	out.writeUTF(ip.versionId);
        	out.writeLong(ip.effectiveBeginDate.getMillis());
        	if (ip.effectiveEndDate == null)
        		out.writeLong(0);
        	else
        		out.writeLong(ip.effectiveEndDate.getMillis());
        	
        	// Write product map
        	out.writeInt(ip.prices.size());
        	for (Entry<Key, Product> entry: ip.prices.entrySet()) {
        		Key.Serializer.serialize(out, entry.getKey());
        		Product.Serializer.serialize(out, entry.getValue());
        	}
        }
    	
        public static InstancePrices deserialize(DataInput in) throws IOException {
        	ServiceCode sc = ServiceCode.valueOf(in.readUTF());
        	String versionId = in.readUTF();
        	DateTime begin = new DateTime(in.readLong(), DateTimeZone.UTC);
        	Long endMillis = in.readLong();
        	DateTime end = endMillis == 0 ? null : new DateTime(endMillis, DateTimeZone.UTC);
        	InstancePrices ip = new InstancePrices(sc, versionId, begin, end);
        	
        	// Read Product map
        	int size = in.readInt();
        	for (int i = 0; i < size; i++) {
	        	Key key = Key.Serializer.deserialize(in);
	        	Product product = Product.Serializer.deserialize(in);
	        	ip.prices.put(key, product);
        	}
        	
        	return ip;
        }
    }	
	
	
    public static class Rate {
    	public double fixed;
    	public double hourly;
    	
    	public Rate() {
    		fixed = 0.0;
    		hourly = 0.0;
    	}
    	
    	public Rate(double fixed, double hourly) {
    		this.fixed = fixed;
    		this.hourly = hourly;
    	}
    	
    	public double getHourlyUpfrontAmortized(LeaseContractLength lcl) {
            int hours = 365 * 24 * lcl.years;
            return fixed / hours;

    	}
    	
        public static class Serializer {
            public static void serialize(DataOutput out, Rate rate) throws IOException {
                out.writeDouble(rate.fixed);
                out.writeDouble(rate.hourly);
            }

            public static Rate deserialize(DataInput in) throws IOException {
                 return new Rate(in.readDouble(), in.readDouble());
            }
        }
    }
    
    public static class RateKey {
    	public final LeaseContractLength leaseContractLength;
    	public final PurchaseOption purchaseOption;
    	public final OfferingClass offeringClass;
    	
    	public RateKey(LeaseContractLength leaseContractLength, PurchaseOption purchaseOption, OfferingClass offeringClass) {
    		this.leaseContractLength = leaseContractLength;
    		this.purchaseOption = purchaseOption;
    		this.offeringClass = offeringClass;
    	}
    	
    	public RateKey(String leaseContractLength, String purchaseOption, String offeringClass) {
    		this.leaseContractLength = LeaseContractLength.getByName(leaseContractLength);
    		this.purchaseOption = PurchaseOption.get(purchaseOption);
    		this.offeringClass = OfferingClass.valueOf(offeringClass);
    	}
    	
    	public String toString() {
    		return leaseContractLength.toString() + "|" + purchaseOption.toString() + "|" + offeringClass.toString();
    	}
    	
        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            RateKey other = (RateKey)o;
            return toString().equals(other.toString());
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        public static class Serializer {
            public static void serialize(DataOutput out, RateKey key) throws IOException {
                out.writeUTF(key.leaseContractLength.name);
                out.writeUTF(key.purchaseOption.name);
                out.writeUTF(key.offeringClass.toString());
            }

            public static RateKey deserialize(DataInput in) throws IOException {
                 return new RateKey(in.readUTF(), in.readUTF(), in.readUTF());
            }
        }
    }
    
	public static class Product {
		public final double memory; // in GiB
		public final double ecu; // 0 == Variable (t family)
		public final double normalizationSizeFactor;
		public final int vcpu;
		public final String instanceType;
		public final String operatingSystem;
		public final String operation;
		private double onDemandRate;
		private Map<RateKey, Rate> reservationRates;
		
		// fields used to debug ingest
		public final String usagetype;
		public final String preInstalledSw;
		public final String databaseEngine;
		public final String databaseEdition;
		
		// sku used only for ingest to reconcile csv records and catch duplicate keys
		public final String sku;
		
		// Constructor used by deserializer
		private Product(double memory, double ecu, double normalizationSizeFactor,
				int vcpu, String instanceType, String operatingSystem, String operation, double onDemandRate,
				Map<RateKey, Rate> reservationRates) {
			this.sku = null;
			this.memory = memory;
			this.ecu = ecu;
			this.normalizationSizeFactor = normalizationSizeFactor;
			this.vcpu = vcpu;
			this.instanceType = instanceType;
			this.operatingSystem = operatingSystem;
			this.operation = operation;
			this.onDemandRate = onDemandRate;
			this.reservationRates = reservationRates;
			this.usagetype = null;
			this.preInstalledSw = null;
			this.databaseEngine = null;
			this.databaseEdition = null;
		}
		
		// Constructor used by CSV reader
		public Product(String sku, String memory, String ecu, String normalizationSizeFactor,
				String vcpu, String instanceType, String operatingSystem, String operation, String usageType,
				String preInstalledSw, String databaseEngine, String databaseEdition) {
			
			this.memory = Double.parseDouble(memory);
			this.vcpu = Integer.parseInt(vcpu);
			this.ecu = StringUtils.isEmpty(ecu) ? 0 : ecu.toLowerCase().equals("variable") ? 3 * this.vcpu : (ecu.equals("NA") || ecu.equals("N/A")) ? 0 : Double.parseDouble(ecu);
			this.normalizationSizeFactor = StringUtils.isEmpty(normalizationSizeFactor) ? 1.0 : normalizationSizeFactor.equals("NA") ? 1.0 : Double.parseDouble(normalizationSizeFactor);
			this.instanceType = instanceType == null ? "" : instanceType;
			this.operatingSystem = operatingSystem == null ? "" : operatingSystem;
			this.operation = operation;
			this.usagetype = usageType;
			this.preInstalledSw = preInstalledSw == null ? "" : preInstalledSw;
			this.databaseEngine = databaseEngine == null ? "" : databaseEngine;
			this.databaseEdition = databaseEdition == null ? "" : databaseEdition;
			this.sku = sku;
			this.reservationRates = Maps.newHashMap();
		}
				
		// Constructor used by JSON reader
		public Product(PriceList.Product product, PriceList.Rate onDemandRate, Map<String, PriceList.Term> reservationOfferTerms) {
			String memory = null;
			if (product.hasAttribute(Attributes.memory)) {
				String[] memoryParts = product.getAttribute(Attributes.memory).split(" ");
				if (!memoryParts[1].toLowerCase().equals("gib")) {
					logger.error("Found PriceList entry with product memory using non-GiB units: " + memoryParts[1] + ", usageType: " + product.getAttribute(Attributes.usagetype));
				}
				memory = memoryParts[0].replace(",", "");
			}
			else if (product.hasAttribute(Attributes.memoryGib)) {
				memory = product.getAttribute(Attributes.memoryGib);
			}
			this.memory = Double.parseDouble(memory);
					
			String ecu = product.getAttribute(Attributes.ecu);
			this.vcpu = Integer.parseInt(product.getAttribute(Attributes.vcpu));
			this.ecu = ecu.isEmpty() ? 0 : ecu.toLowerCase().equals("variable") ? 3 * this.vcpu : (ecu.equals("NA") || ecu.equals("N/A")) ? 0 : Double.parseDouble(ecu);
			String nsf = product.getAttribute(Attributes.normalizationSizeFactor);
			this.normalizationSizeFactor = nsf.isEmpty() ? 1.0 : nsf.equals("NA") ? 1.0 : Double.parseDouble(nsf);
			this.instanceType = product.getAttribute(Attributes.instanceType);
			this.operatingSystem = product.getAttribute(Attributes.operatingSystem);
			this.operation = product.getAttribute(Attributes.operation);
			this.onDemandRate = onDemandRate == null ? 0.0 : Double.parseDouble(onDemandRate.pricePerUnit.get("USD"));
			this.reservationRates = Maps.newHashMap();
			if (reservationOfferTerms != null) {
				for (String skuOfferCode: reservationOfferTerms.keySet()) {
					Term term = reservationOfferTerms.get(skuOfferCode);
					
					Double quantity = 0.0;
					Double hrs = 0.0;
					for (String skuOfferCodeRateCode: term.priceDimensions.keySet()) {
						PriceList.Rate rate = term.priceDimensions.get(skuOfferCodeRateCode);
						if (rate.unit.equals("Quantity"))
							quantity = Double.parseDouble(rate.pricePerUnit.get("USD"));
						if (rate.unit.equals("Hrs"))
							hrs = Double.parseDouble(rate.pricePerUnit.get("USD"));
					}
					
					// only standard offering class was available before March 2017
					String offeringClass = term.termAttributes.OfferingClass.isEmpty() ? OfferingClass.standard.name() : term.termAttributes.OfferingClass;
					RateKey key = new RateKey(term.termAttributes.LeaseContractLength, term.termAttributes.PurchaseOption, offeringClass);
					this.reservationRates.put(key, new Rate(quantity, hrs));
				}
			}
			this.usagetype = product.getAttribute(Attributes.usagetype);
			this.preInstalledSw = product.getAttribute(Attributes.preInstalledSw);
			this.databaseEngine = product.getAttribute(Attributes.databaseEngine);
			this.databaseEdition = product.getAttribute(Attributes.databaseEdition);
			this.sku = product.sku;
		}
		
		void setOnDemandRate(double onDemandRate) {
			this.onDemandRate = onDemandRate;
		}
		
		void setReserationRate(RateKey key, Rate rate) {
			reservationRates.put(key, rate);
		}
		
        @Override
        public String toString() {
        	StringBuilder sb = new StringBuilder();
        	sb.append("{");
			sb.append("memory:" + memory);
			sb.append(",ecu:" + ecu);
			sb.append(",normalizationSizeFactor:" + normalizationSizeFactor);
			sb.append(",vcpu:" + vcpu);
			sb.append(",instanceType:" + instanceType);
			sb.append(",operatingSystem:" + operatingSystem);
			sb.append(",operation:" + operation);
			sb.append(",onDemandRate:" + onDemandRate);
			sb.append(",reservationRates:" + reservationRates.keySet().toString());
			sb.append(",usagetype:" + usagetype);
			sb.append(",preInstalledSw:" + preInstalledSw);
			sb.append(",databaseEngine:" + databaseEngine);
			sb.append(",databaseEdition:" + databaseEdition);
        	sb.append("}");
            return sb.toString();
        }

		public double getOnDemandRate() {
			return onDemandRate;
		}
		
		public Rate getReservationRate(RateKey key) {
			return reservationRates.get(key);
		}
		
        public static class Serializer {
            public static void serialize(DataOutput out, Product product) throws IOException {
            	out.writeDouble(product.memory);
            	out.writeDouble(product.ecu);
            	out.writeDouble(product.normalizationSizeFactor);
            	out.writeInt(product.vcpu);
            	out.writeUTF(product.instanceType);
            	out.writeUTF(product.operatingSystem);
            	out.writeUTF(product.operation);
            	out.writeDouble(product.onDemandRate);
            	
            	// Write reservationsRates map
            	out.writeInt(product.reservationRates.size());
            	for (Entry<RateKey, Rate> rate: product.reservationRates.entrySet()) {
            		RateKey.Serializer.serialize(out, rate.getKey());
            		Rate.Serializer.serialize(out, rate.getValue());
            	}
            }

            public static Product deserialize(DataInput in) throws IOException {
        		double memory = in.readDouble();
        		double ecu = in.readDouble();
        		double normalizationSizeFactor = in.readDouble();
        		int vcpu = in.readInt();
        		String instanceType = in.readUTF();
        		String operatingSystem = in.readUTF();
        		String operation = in.readUTF();
        		double onDemandRate = in.readDouble();
        		
        		Map<RateKey, Rate> reservationRates = Maps.newHashMap();
        		int size = in.readInt();
        		for (int i = 0; i < size; i++) {
        			RateKey k = RateKey.Serializer.deserialize(in);
        			Rate r = Rate.Serializer.deserialize(in);
        			reservationRates.put(k, r);
        		}
        		
        		return new Product(memory, ecu, normalizationSizeFactor, vcpu, instanceType, operatingSystem, operation, onDemandRate, reservationRates);            	
            }
        }
	}

	public static class Key implements Comparable<Key> {
        public final Region region;
        public final UsageType usageType;
        // public final Tenancy; // Add this if we ever start using Host or Dedicated instances
        
        public Key(Region region, UsageType usageType) {
            this.region = region;
            this.usageType = usageType;
        }

        public int compareTo(Key t) {
            int result = this.region.compareTo(t.region);
            if (result != 0)
                return result;
            result = this.usageType.compareTo(t.usageType);
            return result;
        }

        @Override
        public String toString() {
            return region.toString() + "|" + usageType;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            Key other = (Key)o;
            return
                this.region == other.region &&
                this.usageType == other.usageType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + this.region.hashCode();
            result = prime * result + this.usageType.hashCode();
            return result;
        }

        public static class Serializer {
            public static void serialize(DataOutput out, Key key) throws IOException {
                out.writeUTF(key.region.toString());
                UsageType.serialize(out, key.usageType);
            }

            public static Key deserialize(DataInput in) throws IOException {
                Region region = Region.getRegionByName(in.readUTF());
                UsageType usageType = UsageType.deserialize(in);                

                return new Key(region, usageType);
            }
        }
	}
	
    public static enum LeaseContractLength {
    	none(0, ""),
        oneyear(1, "1yr"),
        threeyear(3, "3yr");

        public final int years;
        public final String name;

        private LeaseContractLength(int years, String name) {
            this.years = years;
            this.name = name;
        }
        
        public static LeaseContractLength getByName(String name) {
        	for (LeaseContractLength lcl: LeaseContractLength.values()) {
        		if (lcl.name.equals(name))
        			return lcl;
        	}
        	return none;
        }
        
        public static LeaseContractLength getByYears(int years) {
        	for (LeaseContractLength lcl: LeaseContractLength.values()) {
        		if (lcl.years == years)
        			return lcl;
        	}
        	return none;
        }
    }
    
    public static enum OfferingClass {
    	standard,
    	convertible;
    }
    
    public static enum Tenancy {
    	Dedicated,
    	Host,
    	Shared,
    	Reserved,	// Deprecated?
    	NA;
    }
    
    public static enum OperatingSystem {
    	Linux,
    	RHEL,
    	SUSE,
    	Windows;
    }

	public enum CapacityStatus {
		AllocatedCapacityReservation,
		UnusedCapacityReservation,
		Used,
		None,
		NA;
	}
			        
}
