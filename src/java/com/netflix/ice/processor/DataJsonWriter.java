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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ProcessorConfig.JsonFileType;
import com.netflix.ice.processor.ReadWriteDataSerializer.TagGroupFilter;
import com.netflix.ice.processor.pricelist.InstancePrices;
import com.netflix.ice.processor.pricelist.InstancePrices.OfferingClass;
import com.netflix.ice.processor.pricelist.InstancePrices.RateKey;
import com.netflix.ice.processor.pricelist.PriceListService;
import com.netflix.ice.processor.pricelist.InstancePrices.LeaseContractLength;
import com.netflix.ice.processor.pricelist.InstancePrices.ServiceCode;
import com.netflix.ice.reader.InstanceMetrics;
import com.netflix.ice.tag.FamilyTag;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.UserTagKey;

public class DataJsonWriter extends DataFile {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
	private final DateTime monthDateTime;
	protected OutputStreamWriter writer;
	private List<UserTagKey> tagKeys;
	private JsonFileType fileType;
    private final Map<Product, DataSerializer> dataByProduct;
    protected boolean addNormalizedRates;
    protected InstanceMetrics instanceMetrics;
    protected InstancePrices ec2Prices;
    protected InstancePrices rdsPrices;
    
	public DataJsonWriter(String name, DateTime monthDateTime, List<UserTagKey> tagKeys, JsonFileType fileType,
			Map<Product, DataSerializer> dataByProduct,
			InstanceMetrics instanceMetrics, PriceListService priceListService, WorkBucketConfig workBucketConfig)
			throws Exception {
		super(name, workBucketConfig);
		this.monthDateTime = monthDateTime;
		this.tagKeys = tagKeys;
		this.fileType = fileType;
		this.dataByProduct = dataByProduct;
	    this.instanceMetrics = instanceMetrics;
	    if (fileType == JsonFileType.hourlyRI) {
		    this.ec2Prices = priceListService.getPrices(monthDateTime, ServiceCode.AmazonEC2);
		    this.rdsPrices = priceListService.getPrices(monthDateTime, ServiceCode.AmazonRDS);
	    }
	}
	
	// For unit testing
	protected DataJsonWriter(DateTime monthDateTime, List<UserTagKey> tagKeys,
			Map<Product, DataSerializer> dataByProduct) {
		super();
		this.monthDateTime = monthDateTime;
		this.tagKeys = tagKeys;
		this.dataByProduct = dataByProduct;
	}
	
	@Override
    public void open() throws IOException {
		super.open();
    	writer = new OutputStreamWriter(os);
    }

	@Override
    public void close() throws IOException {
		writer.close();
		super.close();
    }


	@Override
	protected void write(TagGroupFilter filter) throws IOException {
        for (Product product: dataByProduct.keySet()) {
        	// Skip the "null" product map that doesn't have resource tags
        	if (product == null)
        		continue;
        	
        	if (fileType == JsonFileType.daily)
            	writeDaily(dataByProduct.get(product));
        	else
        		write(dataByProduct.get(product));
        }
	}
	
	private void write(DataSerializer data) throws IOException {
		Gson gson = new GsonBuilder().registerTypeAdapter(ResourceGroup.class, new ResourceGroupSerializer()).create();
		DateTimeFormatter dtf = ISODateTimeFormat.dateTimeNoMillis();
        for (int i = 0; i < data.getNum(); i++) {
            Map<TagGroup, DataSerializer.CostAndUsage> costAndUsageMap = data.getData(i);
            if (costAndUsageMap.isEmpty())
            	continue;
            
            for (Entry<TagGroup, DataSerializer.CostAndUsage> cauEntry: costAndUsageMap.entrySet()) {
            	TagGroup tg = cauEntry.getKey();
            	boolean rates = false;
            	if (fileType == JsonFileType.hourlyRI) {
            		rates = tg.product.isEc2Instance() || tg.product.isRdsInstance();
            		if (!rates)
            			continue;
            	}

            	DataSerializer.CostAndUsage cau = cauEntry.getValue();
            	Item item = new Item(dtf.print(monthDateTime.plusHours(i)), cauEntry.getKey(), cau.cost, cau.usage, rates);
            	String json = gson.toJson(item);
            	writer.write(json + "\n");
            }
        }
	}
	
	private void writeDaily(DataSerializer data) throws IOException {
		Gson gson = new GsonBuilder().registerTypeAdapter(ResourceGroup.class, new ResourceGroupSerializer()).create();
		DateTimeFormatter dtf = ISODateTimeFormat.dateTimeNoMillis();
		
        List<Map<TagGroup, DataSerializer.CostAndUsage>> daily = Lists.newArrayList();
        
        Collection<TagGroup> tagGroups = data.getTagGroups();

        // Aggregate
        for (int hour = 0; hour < data.getNum(); hour++) {
            Map<TagGroup, DataSerializer.CostAndUsage> cauMap = data.getData(hour);

            for (TagGroup tagGroup: tagGroups) {
            	DataSerializer.CostAndUsage cau = cauMap.get(tagGroup);
            	if (cau != null && !cau.isZero())            	
            		addValue(daily, hour/24, tagGroup, cau);
            }
        }
        
        // Write it out
        for (int day = 0; day < daily.size(); day++) {
            Map<TagGroup, DataSerializer.CostAndUsage> cauMap = daily.get(day);
            if (cauMap.isEmpty())
            	continue;
        	
            for (Entry<TagGroup, DataSerializer.CostAndUsage> cauEntry: cauMap.entrySet()) {
            	DataSerializer.CostAndUsage cau = cauEntry.getValue();
            	Item item = new Item(dtf.print(monthDateTime.plusDays(day)), cauEntry.getKey(), cau.cost, cau.usage, false);
            	String json = gson.toJson(item);
            	writer.write(json + "\n");
            }
        }
	}
	
    private void addValue(List<Map<TagGroup, DataSerializer.CostAndUsage>> list, int index, TagGroup tagGroup, DataSerializer.CostAndUsage v) {
        Map<TagGroup, DataSerializer.CostAndUsage> map = DataSerializer.getCreateData(list, index);
        DataSerializer.CostAndUsage existedV = map.get(tagGroup);
        map.put(tagGroup, existedV == null ? v : existedV.add(v));
    }

	
	public class ResourceGroupSerializer implements JsonSerializer<ResourceGroup> {
		public JsonElement serialize(ResourceGroup rg, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject tags = new JsonObject();
			
			UserTag[] userTags = rg.getUserTags();
			for (int i = 0; i < userTags.length; i++) {
				if (userTags[i] == null || userTags[i].name.isEmpty())
					continue;
				tags.addProperty(tagKeys.get(i).name, userTags[i].name);
			}
			
			return tags;
		}
	}
	
	public class NormalizedRate {
		Double noUpfrontHourly;
		Double partialUpfrontFixed;
		Double partialUpfrontHourly;
		Double allUpfrontFixed;
		
		public NormalizedRate(InstancePrices.Product product, LeaseContractLength lcl, OfferingClass oc) {
			double nsf = product.normalizationSizeFactor;
			InstancePrices.Rate rate = product.getReservationRate(new RateKey(lcl, PurchaseOption.NoUpfront, oc));
			this.noUpfrontHourly = rate != null && rate.hourly > 0 ? rate.hourly / nsf : null;
			
			rate = product.getReservationRate(new RateKey(lcl, PurchaseOption.PartialUpfront, oc));
			if (rate != null) {
				this.partialUpfrontFixed = rate.fixed > 0 ? rate.fixed / nsf : null;
				this.partialUpfrontHourly = rate.hourly > 0 ? rate.hourly / nsf : null;
			}
			
			rate = product.getReservationRate(new RateKey(lcl, PurchaseOption.AllUpfront, oc));
			this.allUpfrontFixed = rate != null && rate.fixed > 0 ? rate.fixed / nsf : null;
		}
		
		boolean isNull() {
			return noUpfrontHourly == null &&
					partialUpfrontFixed == null &&
					partialUpfrontHourly == null &&
					allUpfrontFixed == null;
		}
	}
	
	public class NormalizedRates {
		Double onDemand;
		NormalizedRate oneYearStd;
		NormalizedRate oneYearConv;
		NormalizedRate threeYearStd;
		NormalizedRate threeYearConv;
		
		public NormalizedRates(TagGroup tg) {
			InstancePrices prices = tg.product.isEc2Instance() ? ec2Prices : tg.product.isRdsInstance() ? rdsPrices : null;
			if (prices == null)
				return;
			
			InstancePrices.Product product = prices.getProduct(tg.region, tg.usageType);
			if (product == null) {
				logger.info("no product for " + prices.getServiceCode() + ", " + tg);
				return;
			}
			
			double nsf = product.normalizationSizeFactor;
			onDemand = product.getOnDemandRate() / nsf;
			oneYearStd = new NormalizedRate(product, LeaseContractLength.oneyear, OfferingClass.standard);
			oneYearStd = oneYearStd.isNull() ? null : oneYearStd;
			oneYearConv = new NormalizedRate(product, LeaseContractLength.oneyear, OfferingClass.convertible);
			oneYearConv = oneYearConv.isNull() ? null : oneYearConv;
			threeYearStd = new NormalizedRate(product, LeaseContractLength.threeyear, OfferingClass.standard);
			threeYearStd = threeYearStd.isNull() ? null : threeYearStd;
			threeYearConv = new NormalizedRate(product, LeaseContractLength.threeyear, OfferingClass.convertible);
			threeYearConv = threeYearConv.isNull() ? null : threeYearConv;
		}
	}
	
	public class Item {
		String hour;
		String org;
		String costType;
		String accountId;
		String account;
		String region;
		String zone;
		String product;
		String operation;
		String usageType;
		ResourceGroup tags;
		Double cost;
		Double usage;
		String instanceFamily;
		Double normalizedUsage;
		NormalizedRates normalizedRates;
		
		public Item(String hour, TagGroup tg, Double cost, Double usage, boolean rates) {
			this.hour = hour;
			this.cost = cost;
			this.usage = usage;
			
			org = String.join("/", tg.account.getParents());
			costType = tg.costType.name;
			accountId = tg.account.getId();
			account = tg.account.getIceName();
			region = tg.region.name;
			zone = tg.zone == null ? null : tg.zone.name;
			product = tg.product.getIceName();
			operation = tg.operation.name;
			usageType = tg.usageType.name;
			tags = tg.resourceGroup;
			
			// EC2 & RDS instances
			if (rates) {
				instanceFamily = FamilyTag.getFamilyName(tg.usageType.name);
				normalizedUsage = usage == null ? null : usage * instanceMetrics.getNormalizationFactor(tg.usageType);
				
				if (tg.operation.isOnDemand() || tg.operation.isUsed()) {
					normalizedRates = new NormalizedRates(tg);
				}
			}
		}
	}

}
