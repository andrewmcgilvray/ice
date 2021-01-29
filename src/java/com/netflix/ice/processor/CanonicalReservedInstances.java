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
package com.netflix.ice.processor;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.amazonaws.services.ec2.model.ReservedInstances;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.elasticache.model.ReservedCacheNode;
import com.amazonaws.services.elasticsearch.model.ReservedElasticsearchInstance;
import com.amazonaws.services.rds.model.ReservedDBInstance;
import com.amazonaws.services.redshift.model.ReservedNode;
import com.google.common.collect.Lists;
import com.netflix.ice.tag.Product;

/*
 * CanonicalReservedInstances is a universal representation of a reserved instance.
 * This class unifies the representation of reserved instances across EC2, RDS, and Redshift so
 * that client code can treat them in a consistent and uniform way.
 */
public class CanonicalReservedInstances {
	public static final String hourly = "Hourly";
	
	/**
	 * The ID of the account owning this reservation
	 */
	private String accountId;
	
	/**
	 * Product for this Reservation - EC2, RDS, Redshift, Elasticsearch, ElastiCache
	 */
	private String product;
	
	/**
	 * Region where reservation applies
	 */
	private String region;
	
    /**
     * The ID of the Reservation for EC2 Instances, DB Instances, or Redshift Nodes
     */
	private String reservationId;

    /**
     * The offering identifier. RDS and Redshift only.
     */
	private String reservationOfferingId;

    /**
     * The reservation instance, class, or node type.
     */
	private String instanceType;
	
	/**
	 * The scope of the EC2 reservation AZ or Region
	 */
	private String scope;

    /**
     * The Availability Zone in which the EC2 Reserved Instance can be used.
     * Does not apply to RDS or Redshift
     */
	private String availabilityZone;

    /**
     * Indicates if the reservation applies to Multi-AZ deployments. (RDS only)
     */
	private Boolean multiAZ;

    /**
     * The date and time the Reserved Instance started.
     */
	private java.util.Date start;

    /**
     * The time when the Reserved Instance expires.
     */
	private java.util.Date end;

    /**
     * The duration of the Reserved Instance, in seconds.
     */
	private Long duration;

    /**
     * The usage price of the Reserved Instance, per hour.
     */
	private Double usagePrice;

    /**
     * The purchase price of the Reserved Instance.
     */
	private Double fixedPrice;

    /**
     * The number of Reserved EC2 Instances, DB Instances, or Nodes purchased.
     */
	private Integer instanceCount;

    /**
     * The Reservation description.
     */
	private String productDescription;

    /**
     * The state of the Reserved Instance purchase.
     * <p>
     * <b>Constraints:</b><br/>
     * <b>Allowed Values: </b>payment-pending, active, payment-failed, retired
     */
	private String state;

    /**
     * The currency of the Reserved Instance. It's specified using ISO 4217
     * standard currency codes. At this time, the only supported currency is
     * <code>USD</code>.
     * <p>
     * <b>Constraints:</b><br/>
     * <b>Allowed Values: </b>USD
     */
	private String currencyCode;

    /**
     * The Reserved Instance offering type.
     * <p>
     * <b>Constraints:</b><br/>
     * <b>Allowed Values: </b>No Upfront, Partial Upfront, All Upfront
     */
	private String offeringType;
	
	/**
	 * The Reserved Instance offering class.
	 * <p>
	 * <b>Contraints:</b><br/>
	 * <b>Allowed Values: </b>standard, convertible
	 */
	private String offeringClass;
	
    public class RecurringCharge {
    	final public String frequency;
    	final public Double cost;
    	
    	public RecurringCharge(String frequency, Double cost) {
    		this.frequency = frequency;
    		this.cost = cost;
    	}
    	
    	public RecurringCharge(String charge) {
        	String[] rc = charge.split(":");
        	this.frequency = rc[0];
        	this.cost = Double.parseDouble(rc[1]);
    	}
    	
    	public String toString() {
    		return frequency + ":" + cost.toString();
    	}
    }

    /**
     * The recurring charge tag assigned to the resource.
     */
    private List<RecurringCharge> recurringCharges;
    
    /*
     * parent reservation ID if this was modified
     */
    private String parentReservationId;
    
    /*
     * tags
     */
    private List<Tag> tags;

    public CanonicalReservedInstances(String accountId, String region, ReservedInstances ri, String parentReservationId) {
		this.accountId = accountId;
        this.product = Product.Code.Ec2.serviceCode;
        this.region = region;
        this.reservationId = ri.getReservedInstancesId();
        this.reservationOfferingId = "";
        this.instanceType = ri.getInstanceType();
        this.scope = ri.getScope();
        this.availabilityZone = ri.getAvailabilityZone();
        this.multiAZ = false;
        this.start = ri.getStart();
        this.end = ri.getEnd();
        this.duration = ri.getDuration();
        this.usagePrice = new Double(ri.getUsagePrice());
        this.fixedPrice = new Double(ri.getFixedPrice());
        this.instanceCount = ri.getInstanceCount();
        this.productDescription = ri.getProductDescription();
        this.state = ri.getState();
        this.currencyCode = ri.getCurrencyCode();
        this.offeringType = ri.getOfferingType();
        this.offeringClass = ri.getOfferingClass();
        this.recurringCharges = new ArrayList<RecurringCharge>();
        for (com.amazonaws.services.ec2.model.RecurringCharge rc: ri.getRecurringCharges()) {
        	this.recurringCharges.add(new RecurringCharge(rc.getFrequency(), rc.getAmount()));
        }
        this.parentReservationId = parentReservationId;
        this.tags = ri.getTags();
	}
    
    public CanonicalReservedInstances(String accountId, String region, ReservedDBInstance ri) {
		this.accountId = accountId;
        this.product = Product.Code.Rds.serviceCode;
        this.region = region;
        this.reservationId = ri.getReservedDBInstanceId();
        this.reservationOfferingId = ri.getReservedDBInstancesOfferingId();
        this.instanceType = ri.getDBInstanceClass();
        this.scope = "";
        this.availabilityZone = "";
        this.multiAZ = ri.getMultiAZ();
        this.start = ri.getStartTime();
        this.end = new Date(start.getTime() + ri.getDuration() * 1000L);
        this.duration = new Long(ri.getDuration());
        this.usagePrice = ri.getUsagePrice();
        this.fixedPrice = ri.getFixedPrice();
        this.instanceCount = ri.getDBInstanceCount();
        this.productDescription = ri.getProductDescription();
        this.state = ri.getState();
        this.currencyCode = ri.getCurrencyCode();
        this.offeringType = ri.getOfferingType();
        this.offeringClass = null;
        this.recurringCharges = Lists.newArrayList();
        for (com.amazonaws.services.rds.model.RecurringCharge rc: ri.getRecurringCharges()) {
        	this.recurringCharges.add(new RecurringCharge(rc.getRecurringChargeFrequency(), rc.getRecurringChargeAmount()));
        }
        this.parentReservationId = null;
        this.tags = Lists.newArrayList();
    }

    public CanonicalReservedInstances(String accountId, String region, ReservedNode ri) {
		this.accountId = accountId;
        this.product = Product.Code.Redshift.serviceCode;
        this.region = region;
        this.reservationId = ri.getReservedNodeId();
        this.reservationOfferingId = ri.getReservedNodeOfferingId();
        this.instanceType = ri.getNodeType();
        this.scope = "";
        this.availabilityZone = "";
        this.multiAZ = false;
        this.start = ri.getStartTime();
        this.end = new Date(start.getTime() + ri.getDuration() * 1000L);
        this.duration = new Long(ri.getDuration());
        this.usagePrice = ri.getUsagePrice();
        this.fixedPrice = ri.getFixedPrice();
        this.instanceCount = ri.getNodeCount();
        this.productDescription = "";
        this.state = ri.getState();
        this.currencyCode = ri.getCurrencyCode();
        this.offeringType = ri.getOfferingType();
        this.offeringClass = null;
        this.recurringCharges = Lists.newArrayList();
        for (com.amazonaws.services.redshift.model.RecurringCharge rc: ri.getRecurringCharges()) {
        	this.recurringCharges.add(new RecurringCharge(rc.getRecurringChargeFrequency(), rc.getRecurringChargeAmount()));
        }
        this.parentReservationId = null;
        this.tags = Lists.newArrayList();
    }
    
    public CanonicalReservedInstances(String accountId, String region, ReservedElasticsearchInstance ri) {
    	this.accountId = accountId;
    	this.product = Product.Code.Elasticsearch.serviceCode;
    	this.region = region;
    	this.reservationId = ri.getReservedElasticsearchInstanceId();
    	this.instanceType = ri.getElasticsearchInstanceType();
    	this.scope = "";
    	this.availabilityZone = "";
    	this.multiAZ = false;
    	this.start = ri.getStartTime();
    	this.end = new Date(start.getTime() + ri.getDuration() * 1000L);
    	this.duration = new Long(ri.getDuration());
    	this.usagePrice = ri.getUsagePrice();
    	this.fixedPrice = ri.getFixedPrice();
    	this.instanceCount = ri.getElasticsearchInstanceCount();
    	this.productDescription = "";
    	this.state = ri.getState();
    	this.currencyCode = ri.getCurrencyCode();
    	this.offeringType = "";
    	this.offeringClass = null;
    	this.recurringCharges = Lists.newArrayList();
    	for (com.amazonaws.services.elasticsearch.model.RecurringCharge rc: ri.getRecurringCharges()) {
    		this.recurringCharges.add(new RecurringCharge(rc.getRecurringChargeFrequency(), rc.getRecurringChargeAmount()));
    	}
    	this.parentReservationId = null;
    	this.tags = Lists.newArrayList();
    }
    
    public CanonicalReservedInstances(String accountId, String region, ReservedCacheNode ri) {
    	this.accountId = accountId;
    	this.product = Product.Code.ElastiCache.serviceCode;
    	this.region = region;
    	this.reservationId = ri.getReservedCacheNodeId();
    	this.instanceType = ri.getCacheNodeType();
    	this.scope = "";
    	this.availabilityZone = "";
    	this.multiAZ = false;
    	this.start = ri.getStartTime();
    	this.end = new Date(start.getTime() + ri.getDuration() * 1000L);
    	this.duration = new Long(ri.getDuration());
    	this.usagePrice = ri.getUsagePrice();
    	this.fixedPrice = ri.getFixedPrice();
    	this.instanceCount = ri.getCacheNodeCount();
    	this.productDescription = ri.getProductDescription();
    	this.state = ri.getState();
    	this.currencyCode = "";
    	this.offeringType = ri.getOfferingType();
    	this.offeringClass = null;
    	this.recurringCharges = Lists.newArrayList();
    	for (com.amazonaws.services.elasticache.model.RecurringCharge rc: ri.getRecurringCharges()) {
    		this.recurringCharges.add(new RecurringCharge(rc.getRecurringChargeFrequency(), rc.getRecurringChargeAmount()));
    	}
    	this.parentReservationId = null;
    	this.tags = Lists.newArrayList();
    }

    public CanonicalReservedInstances(String csv) {
        String[] tokens = csv.split(",", -1);
        accountId = tokens[0];
        product = tokens[1];
        region = tokens[2];
        reservationId = tokens[3];
        reservationOfferingId = tokens[4];
        instanceType = tokens[5];
        scope = tokens[6];
        availabilityZone = tokens[7];
        multiAZ = Boolean.parseBoolean(tokens[8]);
        start = new Date(LineItem.amazonBillingDateFormat.parseMillis(tokens[9]));
        end = new Date(LineItem.amazonBillingDateFormat.parseMillis(tokens[10]));
        duration = Long.parseLong(tokens[11]);
        usagePrice = Double.parseDouble(tokens[12]);
        fixedPrice = Double.parseDouble(tokens[13]);
        instanceCount = Integer.parseInt(tokens[14]);
        productDescription = tokens[15];
        state = tokens[16];
        currencyCode = tokens[17];
        offeringType = tokens[18];
        // Recurring charges in the form "f:c|f:c"
        recurringCharges = Lists.newArrayList();
        if (tokens.length > 19 && !tokens[19].isEmpty()) {
	        String[] charges = tokens[19].split("\\|");
	        for (String charge: charges) {
	        	recurringCharges.add(new RecurringCharge(charge));
	        }
        }
    	if (tokens.length > 20)
    		parentReservationId = tokens[20];
    	if (tokens.length > 21)
    		offeringClass = tokens[21];
    	tags = Lists.newArrayList();
    	if (tokens.length > 22 && !tokens[22].isEmpty()) {
    		String[] tagArray = tokens[22].split("\\|");
    		for (String t: tagArray) {
    			String[] parts = t.split(":");
    			tags.add(new Tag(parts[0], parts[1]));
    		}
    	}
    }
    
	public String getAccountId() {
		return accountId;
	}

	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getReservationId() {
		return reservationId;
	}

	public void setReservationId(String reservationId) {
		this.reservationId = reservationId;
	}

	public String getReservationOfferingId() {
		return reservationOfferingId;
	}

	public void setReservationOfferingId(String reservationOfferingId) {
		this.reservationOfferingId = reservationOfferingId;
	}

	public String getInstanceType() {
		return instanceType;
	}

	public void setInstanceType(String instanceType) {
		this.instanceType = instanceType;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public String getAvailabilityZone() {
		return availabilityZone;
	}

	public void setAvailabilityZone(String availabilityZone) {
		this.availabilityZone = availabilityZone;
	}

	public Boolean getMultiAZ() {
		return multiAZ;
	}

	public void setMultiAZ(Boolean multiAZ) {
		this.multiAZ = multiAZ;
	}

	public java.util.Date getStart() {
		return start;
	}

	public void setStart(java.util.Date start) {
		this.start = start;
	}

	public java.util.Date getEnd() {
		return end;
	}

	public void setEnd(java.util.Date end) {
		this.end = end;
	}

	public Long getDuration() {
		return duration;
	}

	public void setDuration(Long duration) {
		this.duration = duration;
	}

	public Double getUsagePrice() {
		return usagePrice;
	}

	public void setUsagePrice(Double usagePrice) {
		this.usagePrice = usagePrice;
	}

	public Double getFixedPrice() {
		return fixedPrice;
	}

	public void setFixedPrice(Double fixedPrice) {
		this.fixedPrice = fixedPrice;
	}

	public Integer getInstanceCount() {
		return instanceCount;
	}

	public void setInstanceCount(Integer instanceCount) {
		this.instanceCount = instanceCount;
	}

	public String getProductDescription() {
		return productDescription;
	}

	public void setProductDescription(String productDescription) {
		this.productDescription = productDescription;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCurrencyCode() {
		return currencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
	}

	public String getOfferingType() {
		return offeringType;
	}

	public void setOfferingType(String offeringType) {
		this.offeringType = offeringType;
	}

	public List<RecurringCharge> getRecurringCharges() {
		return recurringCharges;
	}

	public void setRecurringCharges(List<RecurringCharge> recurringCharges) {
		this.recurringCharges = recurringCharges;
	}
	
	/*
	 * Generate a header for a CSV file
	 */
	public static String header() {
		String[] cols = new String[] {
			"Account",
			"Product",
			"Region",
			"ReservationId",
			"ReservationOfferingId",
			"InstanceType",
			"Scope",
			"AvailabilityZone",
			"MultiAZ",
			"Start",
			"End",
			"Duration",
			"UsagePrice",
			"FixedPrice",
			"InstanceCount",
			"ProductDescription",
			"State",
			"CurrencyCode",
			"OfferingType",
			"RecurringCharges",
			"ParentReservationId",
			"OfferingClass",
			"Tags",
		};
		return StringUtils.join(cols, ",");
	}

	public String toString() {
    	// First prep the recurring charges
    	List<String> charges = Lists.newArrayList();
    	for (RecurringCharge rc: recurringCharges) {
    		charges.add(rc.toString());
    	}
    	String rcs = String.join("|", charges);
    	List<String> tagList = Lists.newArrayList();
    	for (Tag t: tags) {
    		tagList.add(t.getKey() + ":" + t.getValue());
    	}
    	String tagsStr = String.join("|", tagList);
    	
    	// Now build the line
    	String[] fields = new String[] {
    			accountId,
    	        product,
    	        region,
    	        reservationId,
    	        reservationOfferingId,
    	        instanceType,
    	        scope,
    	        availabilityZone,
    	        multiAZ.toString(),
    	        LineItem.amazonBillingDateFormat.print(new DateTime(start.getTime())),
    	        LineItem.amazonBillingDateFormat.print(new DateTime(end.getTime())),
    	        duration.toString(),
    	        usagePrice.toString(),
    	        fixedPrice.toString(),
    	        instanceCount.toString(),
    	        productDescription,
    	        state,
    	        currencyCode,
    	        offeringType,
    	        rcs,
    	        parentReservationId,
    	        offeringClass,
    	        tagsStr,
        };
    	return String.join(",", fields);
    }
	
	public boolean isProduct(String product) {
		return this.product.equals(product);
	}
		
	public double getRecurringHourlyCharges() {
		double charge = 0.0;
    	for (RecurringCharge rc: recurringCharges) {
    		if (rc.frequency.equals(hourly)) {
    			charge += rc.cost;
    		}
    	}
    	return charge;
	}
	
	public String getParentReservationId() {
		return parentReservationId;
	}

	public void setParentReservationId(String parentReservationId) {
		this.parentReservationId = parentReservationId;
	}

	public String getOfferingClass() {
		return offeringClass;
	}

	public void setOfferingClass(String offeringClass) {
		this.offeringClass = offeringClass;
	}

	public List<Tag> getTags() {
		return tags;
	}

	public void setTags(List<Tag> tags) {
		this.tags = tags;
	}

}
    