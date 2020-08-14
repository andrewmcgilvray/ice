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
package com.netflix.ice.processor.postproc;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.StopWatch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.postproc.OperandConfig.OperandType;
import com.netflix.ice.tag.Product;

public class PostProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected boolean debug = false;

	private List<RuleConfig> rules;
	private AccountService accountService;
	private ProductService productService;
	private ResourceService resourceService;
	
	private int cacheMisses;
	private int cacheHits;
	
	public PostProcessor(List<RuleConfig> rules, AccountService accountService, ProductService productService, ResourceService resourceService) {
		this.rules = rules;
		this.accountService = accountService;
		this.productService = productService;
		this.resourceService = resourceService;
		this.cacheMisses = 0;
		this.cacheHits = 0;
	}
	
	public void process(CostAndUsageData data) {
		logger.info("Post-process " + rules.size() + " rules");
		for (RuleConfig rc: rules) {
			try {
				process(rc, data);
			} catch (Exception e) {
				logger.error("Error post-processing cost and usage data for rule " + rc.getName() + ": " + e);
				e.printStackTrace();
			}
		}
	}
	
	private boolean isActive(RuleConfig rc, long startMilli) {
		long ruleStart = new DateTime(rc.getStart(), DateTimeZone.UTC).getMillis();
		long ruleEnd = new DateTime(rc.getEnd(), DateTimeZone.UTC).getMillis();
		return startMilli >= ruleStart && startMilli < ruleEnd;
	}
	
	protected void process(RuleConfig rc, CostAndUsageData data) throws Exception {
		// Make sure the rule is in effect for the start date
		if (!isActive(rc, data.getStartMilli())) {
			logger.info("Post-process rule " + rc.getName() + " is not active for this month, start=" + rc.getStart() + ", end=" + rc.getEnd());
			return;
		}
		
		if (rc.getIn().getProduct() == null) {
			logger.error("Post processing rule " + rc.getName() + " has no product specified for the 'in' operand");
			return;
		}
		
		// Cache the single values across the resource and non-resource based passes
		// in case we can reuse them. This save a lot of time on operands that
		// aggregate a large amount of data into a single value and are not grouping
		// by any user tags.
		Map<String, Double[]> operandSingleValueCache = Maps.newHashMap();

		logger.info("Post-process with rule " + rc.getName() + " on non-resource data");
		
		Rule rule = new Rule(rc, accountService, productService, resourceService);
		processReadWriteData(rule, data, true, operandSingleValueCache);
		
		logger.info("Post-process with rule " + rc.getName() + " on resource data");
		processReadWriteData(rule, data, false, operandSingleValueCache);
	}
		
	protected void processReadWriteData(Rule rule, CostAndUsageData data, boolean isNonResource, Map<String, Double[]> operandSingleValueCache) throws Exception {
		// Get data maps for operands
		int opDataSize = 0;
		Map<String, Map<Product, ReadWriteData>> dataByOperand = Maps.newHashMap();
		
		for (String name: rule.getOperands().keySet()) {			
			List<Product> products = isNonResource ? Lists.newArrayList(new Product[]{null}) : rule.getOperand(name).getProducts(productService);			
			for (Product p: products) {
				ReadWriteData rwd = rule.getOperand(name).getType() == OperandType.cost ? data.getCost(p) : data.getUsage(p);
				if (rwd == null)
					continue;
				
				Map<Product, ReadWriteData> dataMap = dataByOperand.get(name);
				if (dataMap == null) {
					dataMap = Maps.newHashMap();
					dataByOperand.put(name, dataMap);
				}
				dataMap.put(p, rwd);
				opDataSize++;
			}
		}
				
		// Get data maps for results. Handle case where we're creating a new product
		List<ReadWriteData> resultData = Lists.newArrayList();
		for (Operand result: rule.getResults()) {
			Product p = isNonResource ? null : result.getProduct(productService);
			ReadWriteData rwd = result.getType() == OperandType.usage ? data.getUsage(p) : data.getCost(p);
			if (rwd == null) {
				rwd = new ReadWriteData(data.getNumUserTags());
				if (result.getType() == OperandType.usage)
					data.putUsage(p, rwd);
				else
					data.putCost(p, rwd);
			}
			resultData.add(rwd);
		}
		
		logger.info("  -- opData size: " + opDataSize + ", resultData size: " + resultData.size());
			
		int maxNum = data.getMaxNum();
		
		// Get the aggregated value for the input operand
		Map<AggregationTagGroup, Double[]> inData = getInData(rule.getIn(), data, isNonResource, maxNum);
		logger.info("  -- in data size = " + inData.size());
		
		Map<String, Double[]> opSingleValues = getOperandSingleValues(rule, dataByOperand, isNonResource, maxNum, operandSingleValueCache);
		
		Map<AggregationTagGroup, Map<String, Double[]>> opValues = getOperandValues(rule, inData, dataByOperand, isNonResource, maxNum);
		int results = applyRule(rule, inData, opValues, opSingleValues, resultData, isNonResource, maxNum);
		
		logger.info("  -- data for rule " + rule.config.getName() + " -- in data size = " + inData.size() + ", --- results size = " + results);
	}
	
	/**
	 * Aggregate the data using the regex groups contained in the input filters
	 * @throws Exception 
	 */
	public Map<AggregationTagGroup, Double[]> getInData(InputOperand in, CostAndUsageData data,
			boolean isNonResource, int maxNum) throws Exception {
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
		logger.info("  -- getInData elapsed time: " + sw);
		return inValues;
	}
	
	/*
	 * Returns a map containing the operand values needed to compute the results for each input aggregation tag group.
	 * Each aggregation tag group entry hold a map of Double arrays keyed by the operand name.
	 */
	protected Map<AggregationTagGroup, Map<String, Double[]>> getOperandValues(Rule rule, Map<AggregationTagGroup, Double[]> in, 
			Map<String, Map<Product, ReadWriteData>> dataByOperand, boolean isNonResource, int maxHours) throws Exception {
		StopWatch sw = new StopWatch();
		sw.start();
		
		Map<AggregationTagGroup, Map<String, Double[]>> operandValueMap = Maps.newHashMap();
		Map<String, Double[]> operandValueCache = Maps.newHashMap();
		cacheMisses = 0;
		cacheHits = 0;
		
		// Determine which operands are aggregations that require scanning all the data
		Map<String, InputOperand> aggregationOperands = Maps.newHashMap();
		for (String opName: rule.getOperands().keySet()) {
			InputOperand op = rule.getOperand(opName);
			if (op.isSingle())
				continue;
			
			if (op.hasAggregation()) {
				// Queue the operand for tagGroup scanning
				aggregationOperands.put(opName, op);
			}
			else {
				// Get the values directly
				getValuesForOperand(opName, op, in, dataByOperand.get(opName), isNonResource, maxHours, operandValueMap, operandValueCache);
			}
		}
		if (!aggregationOperands.isEmpty()) {
			getAggregatedOperandValues(rule, aggregationOperands, in, dataByOperand, maxHours, operandValueMap, operandValueCache);
		}
		logger.info("  -- getOperandValues elapsed time: " + sw + ", cache hits = " + cacheHits + ", cache misses = " + cacheMisses);
		return operandValueMap;
	}
	
	/*
	 * Returns a map containing the single operand values needed to compute the results.
	 */
	protected Map<String, Double[]> getOperandSingleValues(Rule rule, Map<String, Map<Product, ReadWriteData>> dataByOperand,
			boolean isNonResource, int maxHours,
			Map<String, Double[]> operandSingleValueCache) throws Exception {
				
		Map<String, Double[]> operandSingleValues = Maps.newHashMap();
		for (String opName: rule.getOperands().keySet()) {
			
			Map<Product, ReadWriteData> dataMap = dataByOperand.get(opName);
			if (dataMap == null || dataMap.size() == 0)
				continue;
			
			InputOperand op = rule.getOperand(opName);
			if (!op.isSingle())
				continue;
			
			String cacheKey = null;
			if (!op.hasGroupByTags()) {
				// See if the values are in the cache
				cacheKey = op.cacheKey(null);
				if (operandSingleValueCache.containsKey(cacheKey)) {
					operandSingleValues.put(opName, operandSingleValueCache.get(cacheKey));
					logger.info("  -- getOperandSingleValues found values in cache for operand \"" + opName + "\", value[0] = " + operandSingleValueCache.get(cacheKey)[0]);
					continue;
				}
			}
			
			Double[] values = new Double[op.isMonthly() ? 1 : maxHours];
			for (int i = 0; i < values.length; i++)
				values[i] = 0.0;
			
			operandSingleValues.put(opName, values);
			if (!op.hasGroupByTags()) {
				operandSingleValueCache.put(cacheKey, values);
			}

			if (op.hasAggregation()) {
				for (ReadWriteData rwd: dataMap.values()) {					
					for (TagGroup tg: rwd.getTagGroups()) {															
						if (op.matches(null, tg)) {
							getData(rwd, tg, values, maxHours, op.isMonthly());
						}					
					}					
				}
			}
			else {
				if (dataMap.size() > 1)
					throw new Exception("operand \"" + opName + "\" has more than one data map, but is non-aggregating");
				
				ReadWriteData data = dataMap.values().iterator().next();
				TagGroup tg = op.tagGroup(null, accountService, productService, isNonResource);
				getData(data, tg, values, maxHours, op.isMonthly());
			}
			if (op.isMonthly())
				logger.info("  -- single monthly operand " + opName + " has value " + values[0]);
		}
		return operandSingleValues;
	}
	
	private void getData(ReadWriteData data, TagGroup tg, Double[] values, int maxHours, boolean isMonthly) {
		for (int hour = 0; hour < data.getNum(); hour++) {
			Double v = data.get(hour, tg);
			if (v != null)
				values[isMonthly ? 0 : hour] += v;
		}
	}
	
	protected void getValuesForOperand(
			String opName,
			InputOperand op,
			Map<AggregationTagGroup, Double[]> in, 
			Map<Product, ReadWriteData> dataMap,
			boolean isNonResource,
			int maxHours,
			Map<AggregationTagGroup, Map<String, Double[]>> operandValueMap,
			Map<String, Double[]> operandValueCache) throws Exception {
		
		logger.info("  -- getValuesForOperand... ");
		StopWatch sw = new StopWatch();
		sw.start();
		
		for (AggregationTagGroup atg: in.keySet()) {
			TagGroup tg = op.tagGroup(atg, accountService, productService, isNonResource);
			
			
			for (Product product: dataMap.keySet()) {
				// Skip resource-based data sets with non-matching product
				if (product != null && product != tg.product)
					continue;
				
				ReadWriteData data = dataMap.get(product);
				
				Map<String, Double[]> opValuesMap = operandValueMap.get(atg);
				if (opValuesMap == null) {
					opValuesMap = Maps.newHashMap();
					operandValueMap.put(atg, opValuesMap);
				}
				
				// See if the values are in the cache
				String cacheKey = op.cacheKey(atg);
				if (operandValueCache.containsKey(cacheKey)) {
					opValuesMap.put(opName, operandValueCache.get(cacheKey));
					cacheHits++;
					continue;
				}
	
				Double[] values = new Double[op.isMonthly() ? 1 : maxHours];
				for (int i = 0; i < values.length; i++)
					values[i] = 0.0;
				opValuesMap.put(opName, values);
				getData(data, tg, values, maxHours, op.isMonthly());
				operandValueCache.put(cacheKey, values);
				cacheMisses++;
			}
		}
		logger.info("  -- getValuesForOperand elapsed time: " + sw);
	}
	
	protected void getAggregatedOperandValues(
			Rule rule,
			Map<String, InputOperand> aggregationOperands, 
			Map<AggregationTagGroup, Double[]> in, 
			Map<String, Map<Product, ReadWriteData>> dataByOperand, 
			int maxHours,
			Map<AggregationTagGroup, Map<String, Double[]>> operandValueMap,
			Map<String, Double[]> operandValueCache) {
		
		logger.info("  -- getAggregatedOperandValues... ");
		StopWatch sw = new StopWatch();
		sw.start();
				
		for (AggregationTagGroup atg: in.keySet()) {
			for (String opName: aggregationOperands.keySet()) {
				
				Map<String, Double[]> opValuesMap = operandValueMap.get(atg);
				if (opValuesMap == null) {
					opValuesMap = Maps.newHashMap();
					operandValueMap.put(atg, opValuesMap);
				}
				
				InputOperand op = rule.getOperand(opName);
				
				// See if the values are in the cache
				String cacheKey = op.cacheKey(atg);
				if (operandValueCache.containsKey(cacheKey)) {
					opValuesMap.put(opName, operandValueCache.get(cacheKey));
					cacheHits++;
					continue;
				}

				Double[] values = new Double[op.isMonthly() ? 1 : maxHours];
				opValuesMap.put(opName, values);
				for (int i = 0; i < values.length; i++)
					values[i] = 0.0;
				
				for (ReadWriteData rwd: dataByOperand.get(opName).values()) {					
					for (TagGroup tg: rwd.getTagGroups()) {															
						if (op.matches(atg, tg)) {
							getData(rwd, tg, values, maxHours, op.isMonthly());
						}					
					}					
				}
				operandValueCache.put(cacheKey, values);
				if (debug)
					logger.info("  -- operand " + opName + " value for hour 0: " + values[0]);
				cacheMisses++;				
			}
		}
				
		logger.info("  -- getAggregatedOperandValues elapsed time: " + sw);
	}
	
	protected int applyRule(
			Rule rule,
			Map<AggregationTagGroup, Double[]> in,
			Map<AggregationTagGroup, Map<String, Double[]>> opValues,
			Map<String, Double[]> opSingleValues,
			List<ReadWriteData> resultData,
			boolean isNonResource,
			int maxNum) throws Exception {
		
		int numResults = 0;
		
		// For each result operand...
		for (int i = 0; i < rule.getResults().size(); i++) {
			//logger.info("result " + i + " for atg: " + atg);
			Operand result = rule.getResult(i);
			
			if (result.isSingle()) {
				TagGroup outTagGroup = result.tagGroup(null, accountService, productService, isNonResource);
				
				String expr = rule.getResultValue(i);
				if (expr != null && !expr.isEmpty()) {
					//logger.info("process hour data");
					eval(i, rule, expr, null, null, opSingleValues, resultData.get(i), outTagGroup, result.isMonthly() ? 1 : maxNum);
					numResults++;
				}
			}
			else {
				for (AggregationTagGroup atg: in.keySet()) {
				
					TagGroup outTagGroup = result.tagGroup(atg, accountService, productService, isNonResource);
					
					String expr = rule.getResultValue(i);
					if (expr != null && !expr.isEmpty()) {
						//logger.info("process hour data");
						eval(i, rule, expr, in.get(atg), opValues.get(atg), opSingleValues, resultData.get(i), outTagGroup, maxNum);
						numResults++;
					}
					
					debug = false;
				}
			}
		}
		
		return numResults;
	}
	
	private void eval(
			int index,
			Rule rule, 
			String outExpr, 
			Double[] inValues, 
			Map<String, Double[]> opValuesMap,
			Map<String, Double[]> opSingleValuesMap,
			ReadWriteData resultData,
			TagGroup outTagGroup,
			int maxNum) throws Exception {

		int maxHours = inValues == null ? maxNum : inValues.length;
		
		// Process each hour of data - we'll only have one if 'in' is a monthly operand
		for (int hour = 0; hour < maxHours; hour++) {
			// Replace variables
			String expr = inValues == null ? outExpr : outExpr.replace("${in}", inValues[hour].toString());
			
			for (String opName: rule.getOperands().keySet()) {			
				// Get the operand values from the proper data source
				InputOperand op = rule.getOperand(opName);
				Double[] opValues = op.isSingle() ? opSingleValuesMap.get(opName) : opValuesMap.get(opName);
				Double opValue = opValues == null ? 0.0 : opValues[op.isMonthly() ? 0 : hour];
				expr = expr.replace("${" + opName + "}", opValue.toString());
			}
			try {
				Double value = new Evaluator().eval(expr);
				if (debug && hour == 0)
					logger.info("eval(" + index + "): " + outExpr + " = " + expr + " = " + value + ", " + outTagGroup);
				resultData.put(hour, outTagGroup, value);
			}
			catch (Exception e) {
				logger.error("Error processing expression \"" + expr + "\", " + e.getMessage());
				throw e;
			}
			
			//if (hour == 0)
			//	logger.info("eval: " + outExpr + " = "  + expr + " = " + value + ", outTagGroup: " + outTagGroup);
		}
	}
	
	public int getCacheMisses() {
		return cacheMisses;
	}
	public int getCacheHits() {
		return cacheHits;
	}
}
