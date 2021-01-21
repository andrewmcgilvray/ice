package com.netflix.ice.processor.postproc;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.DataSerializer;
import com.netflix.ice.tag.Product;

public abstract class RuleProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
	protected Rule rule;
	protected AccountService accountService;
	protected ProductService productService;
    
	public RuleProcessor(Rule rule, AccountService accountService, ProductService productService) {
		this.rule = rule;
		this.accountService = accountService;
		this.productService = productService;
	}
	
	public Rule getRule() {
		return rule;
	}
	
	public RuleConfig getConfig() {
		return rule.config;
	}
	
	/**
	 * Aggregate the data using the regex groups contained in the input filters
	 * @throws Exception 
	 */
	public Map<AggregationTagGroup, Double[]> runQuery(Query query, CostAndUsageData data,
			boolean isNonResource, int maxHours, String ruleName) throws Exception {
		StopWatch sw = new StopWatch();
		sw.start();
		
		Map<AggregationTagGroup, Double[]> valuesMap = Maps.newHashMap();
		Collection<Product> products = isNonResource ? Lists.newArrayList(new Product[]{null}) : query.getProducts(productService);
		
		boolean isCost = query.getType() == RuleConfig.DataType.cost;

		if (query.isSingleTagGroup()) {
			// Handle a single tagGroup lookup - Doing this explicitly avoids a scan of the tag group map.
			TagGroup tg = query.getSingleTagGroup(accountService, productService, isNonResource);
			Product product = isNonResource ? null : tg.product;
			DataSerializer inData = data.get(product);
			Double[] values = new Double[query.isMonthly() ? 1 : maxHours];
			for (int i = 0; i < values.length; i++)
				values[i] = 0.0;
			getData(inData, isCost, tg, values, query.isMonthly());
			AggregationTagGroup aggregatedTagGroup = query.aggregateTagGroup(tg, accountService, productService);
			valuesMap.put(aggregatedTagGroup, values);
		}
		else {
			for (Product product: products) {
				DataSerializer inData = data.get(product);
				if (inData == null)
					continue;
				
				for (TagGroup tg: inData.getTagGroups()) {
					AggregationTagGroup aggregatedTagGroup = query.aggregateTagGroup(tg, accountService, productService);
					if (aggregatedTagGroup == null)
						continue;
					
					Double[] values = valuesMap.get(aggregatedTagGroup);
					if (values == null) {
						values = new Double[query.isMonthly() ? 1 : maxHours];
						for (int i = 0; i < values.length; i++)
							values[i] = 0.0;
						valuesMap.put(aggregatedTagGroup, values);
					}
					getData(inData, isCost, tg, values, query.isMonthly());
				}
			}
		}
		if (valuesMap.isEmpty())
			logger.warn("No query results for rule " + ruleName + ". Query: " + query.toString());			
		else
			logger.info("  -- runQuery elapsed time: " + sw + ", size: " + valuesMap.size());
		return valuesMap;
	}

	private void getData(DataSerializer data, boolean isCost, TagGroup tg, Double[] values, boolean isMonthly) {
		for (int hour = 0; hour < data.getNum(); hour++) {
			DataSerializer.CostAndUsage cau = data.get(hour, tg);
			if (cau != null) {
				values[isMonthly ? 0 : hour] += isCost ? cau.cost : cau.usage;
			}
		}
	}
	
	public abstract boolean process(CostAndUsageData data) throws Exception;
}
