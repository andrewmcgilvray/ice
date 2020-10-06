package com.netflix.ice.processor.postproc;

import java.util.List;
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
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.postproc.OperandConfig.OperandType;
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
	public Map<AggregationTagGroup, Double[]> getInData(InputOperand in, CostAndUsageData data,
			boolean isNonResource, int maxNum, String ruleName) throws Exception {
		StopWatch sw = new StopWatch();
		sw.start();
		
		int maxHours = in.isMonthly() ? 1 : maxNum;
		Map<AggregationTagGroup, Double[]> inValues = Maps.newHashMap();
		List<Product> inProducts = isNonResource ? Lists.newArrayList(new Product[]{null}) : in.getProducts(productService);			

		for (Product inProduct: inProducts) {
			ReadWriteData inData = in.getType() == OperandType.cost ? data.getCost(inProduct) : data.getUsage(inProduct);
			if (inData == null)
				continue;
			
			for (TagGroup tg: inData.getTagGroups()) {
				AggregationTagGroup aggregatedTagGroup = in.aggregateTagGroup(tg, accountService, productService);
				if (aggregatedTagGroup == null)
					continue;
				
				Double[] values = inValues.get(aggregatedTagGroup);
				if (values == null) {
					values = new Double[maxHours];
					for (int i = 0; i < values.length; i++)
						values[i] = 0.0;
					inValues.put(aggregatedTagGroup, values);
				}
				for (int hour = 0; hour < inData.getNum(); hour++) {
					Double v = inData.get(hour, tg);
					if (v != null)
						values[in.isMonthly() ? 0 : hour] += v;
				}
			}
		}
		if (inValues.isEmpty())
			logger.warn("No input data for rule " + ruleName + ". In operand: " + in.toString());			
		else
			logger.info("  -- getInData elapsed time: " + sw + ", size: " + inValues.size());
		return inValues;
	}

	
	public abstract void process(CostAndUsageData data) throws Exception;
}
