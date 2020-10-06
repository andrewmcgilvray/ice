package com.netflix.ice.processor.postproc;

import org.apache.commons.lang.StringUtils;

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReadWriteData;

public class TagGroupSpec {
	public enum DataType {
		cost,
		usage;
	}
	DataType dataType;
	String account;
	String region;
	String zone;
	String productServiceCode;
	String operation;
	String usageType;
	String[] resourceGroup;
	Double value;
	
	public TagGroupSpec(DataType dataType, String account, String region, String zone, String product, String operation, String usageType, String[] resourceGroup, Double value) {
		this.dataType = dataType;
		this.account = account;
		this.region = region;
		this.zone = zone;
		this.productServiceCode = product;
		this.operation = operation;
		this.usageType = usageType;
		this.value = value;
		this.resourceGroup = resourceGroup;
	}

	public TagGroupSpec(DataType dataType, String account, String region, String product, String operation, String usageType, String[] resourceGroup, Double value) {
		this.dataType = dataType;
		this.account = account;
		this.region = region;
		this.productServiceCode = product;
		this.operation = operation;
		this.usageType = usageType;
		this.value = value;
		this.resourceGroup = resourceGroup;
	}

	public TagGroupSpec(DataType dataType, String account, String region, String product, String operation, String usageType, Double value) {
		this.dataType = dataType;
		this.account = account;
		this.region = region;
		this.productServiceCode = product;
		this.operation = operation;
		this.usageType = usageType;
		this.value = value;
		this.resourceGroup = null;
	}

	public TagGroup getTagGroup(AccountService as, ProductService ps) throws Exception {
		return TagGroup.getTagGroup(account, region, zone, productServiceCode, operation, usageType, "", resourceGroup, as, ps);
	}
	
	public TagGroup getTagGroup(String account, AccountService as, ProductService ps) throws Exception {
		return TagGroup.getTagGroup(account, region, zone, productServiceCode, operation, usageType, "", resourceGroup, as, ps);
	}
	
	public String toString() {
		return "[" + 
				dataType.toString() + "," +
				account + "," +
				region + "," +
				zone + "," +
				productServiceCode + "," +
				operation + "," +
				usageType + "," +
				"[" + StringUtils.join(resourceGroup) + "]" +
				"]";
	}

	static public void loadData(TagGroupSpec[] dataSpecs, ReadWriteData usageData, ReadWriteData costData, int hour, AccountService as, ProductService ps) throws Exception {
        for (TagGroupSpec spec: dataSpecs) {
        	if (spec.dataType == DataType.cost)
        		costData.put(hour, spec.getTagGroup(as, ps), spec.value);
        	else
        		usageData.put(hour, spec.getTagGroup(as, ps), spec.value);
        }
    }

}
