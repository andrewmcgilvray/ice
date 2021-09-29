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
package com.netflix.ice.tag;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

public class Product extends Tag {
	private static final long serialVersionUID = 2L;
	
	/*
	 * Product is a Tag representing AWS products. The name field
	 * stored in the inherited Tag class is used for both the
	 * map key in the ProductService and the product names returned
	 * by the DataManager which are ultimately displayed in the
	 * browser UI.
	 * 
	 * By default the name is the canonical name for the product which
	 * is the Amazon/AWS name pulled from the billing
	 * reports with the AWS or Amazon prefix removed. For some products
	 * we break down the usage into sub-categories as defined below.
	 *
	 * The product's names can also be overridden to use the
	 * common acronym such as EC2 for Elastic Compute Cloud and
	 * S3 for Simple Storage Service.
	 */
	
	/*
	 * serviceCode is used for naming product-specific files
	 */
	private String serviceName; // The service name as it appears in reports and pricing services
	private String iceName; // The product name displayed in dashboards
	private final Source source;
	private final Code code;
	
	public enum Source {
		pricing,
		dbr,
		cur,
		code;
	}
	
	/*
	 * Standard product codes needed to test identity in the "is" methods.
	 */
	public enum Code {
		ApiGateway("Amazon API Gateway", "AmazonApiGateway", true),
		AppSync("AWS AppSync", "AWSAppSync", false),
		CloudFront("Amazon CloudFront", "AmazonCloudFront", true),
		Cloudhsm("AWS CloudHSM", "CloudHSM", false),
	    CloudWatch("AmazonCloudWatch", "AmazonCloudWatch", false),
		DataTransfer("AWS Data Transfer", "AWSDataTransfer", false),
		DirectConnect("AWS Direct Connect", "AWSDirectConnect", false),
		DynamoDB("Amazon DynamoDB", "AmazonDynamoDB", true),
		Ec2("Amazon Elastic Compute Cloud", "AmazonEC2", true),
		ElastiCache("Amazon ElastiCache", "AmazonElastiCache", true),
		Elasticsearch("Amazon Elasticsearch Service", "AmazonES", true),
		Emr("Amazon Elastic MapReduce", "ElasticMapReduce", true),
		Rds("Amazon RDS Service", "AmazonRDS", true),
		RdsFull("Amazon Relational Database Service", "AmazonRDS", true),
		Redshift("Amazon Redshift", "AmazonRedshift", true),
		Registrar("Amazon Registrar", "AmazonRegistrar", false),
		S3("Amazon Simple Storage Service", "AmazonS3", true),
	    Ebs("Amazon Elastic Block Store", "EBS", true),
	    Ec2Instance("EC2 Instance", "EC2Instance", true),
	    Eip("Elastic IP", "EIP", false),
	    RdsInstance("RDS Instance", "RDSInstance", true),
		Lambda("AWS Lambda", "AWSLambda", true),
		Ecs("Amazon EC2 Container Service", "AmazonECS", true),
		ComputeSavingsPlans("Savings Plans for AWS Compute usage", "ComputeSavingsPlans", false);
		
		final public String serviceName;
		final public String serviceCode;
		final public boolean enableTagCoverage;
		
		private Code(String serviceName, String serviceCode, boolean enableTagCoverage) {
			this.serviceName = serviceName;
			this.serviceCode = serviceCode;
			this.enableTagCoverage = enableTagCoverage;
		}
		
		public String toString() {
			return serviceName + "," + serviceCode + "," + (enableTagCoverage ? "true" : "false");
		}
	}
	
	/*
	 * Maps for holding overridden product names. One map for each lookup direction.
	 */
    private static ConcurrentMap<String, String> overrideNames = Maps.newConcurrentMap();
    private static ConcurrentMap<String, String> canonicalNames = Maps.newConcurrentMap();
   
    static {    	
    	addOverride(canonicalName(Code.Ec2.serviceName), "EC2");
    	addOverride(canonicalName(Code.RdsFull.serviceName), "RDS");
    	addOverride(canonicalName(Code.S3.serviceName), "S3");
    }

    /*
     * Product constructor should only be called by the ProductService.
     * All references to products needs to be through the product service maps.
     * 
     * Product can be constructed using either the AWS name or the alternate name.
     * The inherited tag name value will use the alternate name if present, otherwise
     * it uses the canonical AWS name (AWS and Amazon stripped off the head).
     * 
     * The Canonical name is always used by the TagGroup serializer so that changing
     * the override name won't corrupt the data files.
     */
    public Product(String serviceName, String serviceCode, Source source) {
    	// ServiceCode which is immutable goes into the parent.
    	super(serviceCode);
    	this.iceName = getOverride(canonicalName(serviceName));
    	this.serviceName = serviceName;
    	this.source = source;
    	
    	Code code = null;
    	for (Code c: Code.values()) {
    		if (c.serviceCode.equals(serviceCode) || c.serviceName.equals(serviceName)) {
    			code = c;
    			break;
    		}
    	}
    	this.code = code;
    }
    
    public Product(Code code) {
    	super(code.serviceCode);
    	this.iceName = getOverride(canonicalName(code.serviceName));
    	this.serviceName = code.serviceName;
    	this.source = Source.code;
    	this.code = code;
    }
    
    public void update(String serviceName, String iceName) {
    	this.serviceName = serviceName;
    	this.iceName = iceName;
    }

    @Override
    public String getName() {
        return this.iceName;
    }

    protected static String canonicalName(String name) {
    	String s = name;
    	// Strip off "Amazon" or "AWS"
    	if (s.startsWith("Amazon"))
    		s = s.substring("Amazon".length()).trim();
    	else if (s.startsWith("AWS"))
    		s = s.substring("AWS".length()).trim();
    	// Strip off any parenthetical portions unless it's a support product
    	//   e.g. "EC2 Container Registry (ECR)" or "Contact Center Telecommunications (service sold by AMCS, LLC)"
    	if (s.indexOf("(") > 0 && !s.startsWith("Support")) {
    		s = s.substring(0, s.indexOf("(")).trim();
    	}
    	// Make sure there are no commas
    	if (s.contains(","))
    		s = s.replace(",", "-");
    	return s;
    }
    
    public static void addOverride(String canonical, String override) {
    	overrideNames.put(canonical,  override);
    	canonicalNames.put(override,  canonical);
    }

    /*
     * getOverride() will return the override name corresponding to the supplied
     * canonical name if the canonical name has been overridden. If not
     * overridden, then it just returns the canonical name supplied.
     */
    protected static String getOverride(String canonicalName) {
    	String n;
    	return (n = overrideNames.get(canonicalName)) != null ? n : canonicalName;
    }
    
    /*
     * getCanonicalName() will return the canonical name for the provided name.
     * If there is no override for the supplied name it just returns the
     * supplied name.
     */
    protected static String getCanonicalName(String name) {
    	String n;
    	return (n = canonicalNames.get(name)) != null ? n : name;
    }
    
    public String getCanonicalName() {
    	String n;
    	return (n = canonicalNames.get(name)) != null ? n : name;
    }

    public String getServiceCode() {
    	return super.name;
    }
    
    public String getServiceName() {
    	return serviceName;
    }
    
    public String getIceName() {
    	return iceName;
    }
    
    public Source getSource() {
    	return source;
    }
    
    public Code getCode() {
    	return code;
    }
    
    public boolean isSupport() {
    	return getCanonicalName().contains("Support");
    }
    
    public boolean isApiGateway() {
    	return code == Code.ApiGateway;
    }
    
    public boolean isCloudFront() {
    	return code == Code.CloudFront;
    }
    
    public boolean isCloudHsm() {
    	return code == Code.Cloudhsm;
    }
    
    public boolean isDataTransfer() {
    	return code == Code.DataTransfer;
    }
    
    public boolean isEc2() {
    	return code == Code.Ec2;
    }
    
    public boolean isEmr() {
    	return code == Code.Emr;
    }
    
    public boolean isRds() {
    	return code == Code.Rds || code == Code.RdsFull;
    }
    
    public boolean isRedshift() {
    	return code == Code.Redshift;
    }
    
    public boolean isRegistrar() {
    	return code == Code.Registrar;
    }
    
    public boolean isS3() {
    	return code == Code.S3;
    }
    
    public boolean isEbs() {
    	return code == Code.Ebs;
    }
    
    public boolean isCloudWatch() {
    	return code == Code.CloudWatch;
    }
    
    public boolean isEc2Instance() {
    	return code == Code.Ec2Instance;
    }
    
    public boolean isEip() {
    	return code == Code.Eip;
    }
    
    public boolean isRdsInstance() {
    	return code == Code.RdsInstance;
    }
    
    public boolean isDynamoDB() {
    	return code == Code.DynamoDB;
    }
    
    public boolean isElastiCache() {
    	return code == Code.ElastiCache;
    }
    
    public boolean isElasticsearch() {
    	return code == Code.Elasticsearch;
    }
    
    public boolean isEcs() {
    	return code == Code.Ecs;
    }
    
    public boolean isLambda() {
    	return code == Code.Lambda;
    }
    
    public boolean isComputeSavingsPlans() {
    	return code == Code.ComputeSavingsPlans;
    }
    
    public boolean enableTagCoverage() {
    	return code != null && code.enableTagCoverage;
    }
    
    public boolean hasReservations() {
    	return isEc2Instance() || isRdsInstance() || isRedshift() || isElastiCache() || isElasticsearch();
    }
    
    public boolean hasSavingsPlans() {
    	return isEc2Instance() || isEcs() || isLambda() || isComputeSavingsPlans();
    }
}
