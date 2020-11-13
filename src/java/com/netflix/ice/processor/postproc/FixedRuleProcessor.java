package com.netflix.ice.processor.postproc;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.StopWatch;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.CostAndUsageData.PostProcessorStats;
import com.netflix.ice.processor.CostAndUsageData.RuleType;
import com.netflix.ice.tag.Product;

public class FixedRuleProcessor extends RuleProcessor {
    protected boolean debug = false;
    
	private int cacheMisses;
	private int cacheHits;

	public FixedRuleProcessor(Rule rule, AccountService accountService, ProductService productService) {
		super(rule, accountService, productService);
		this.cacheMisses = 0;
		this.cacheHits = 0;
	}
	
	@Override
	public void process(CostAndUsageData data) throws Exception {
		// Cache the single values across the resource and non-resource based passes
		// in case we can reuse them. This saves a lot of time on operands that
		// aggregate a large amount of data into a single value and are not grouping
		// by any user tags.
		Map<Query, Double[]> operandSingleValueCache = Maps.newHashMap();
				
		logger.info("Post-process with rule " + getConfig().getName() + " on non-resource data");
		processReadWriteData(data, true, operandSingleValueCache);
		
		logger.info("Post-process with rule " + getConfig().getName() + " on resource data");
		processReadWriteData(data, false, operandSingleValueCache);
	}
	
	protected void processReadWriteData(CostAndUsageData data, boolean isNonResource, Map<Query, Double[]> operandSingleValueCache) throws Exception {		
		StopWatch sw = new StopWatch();
		sw.start();
		
		// Get data maps for operands
		int opDataSize = 0;
						
		// Get data maps for results. Handle case where we're creating a new product
		List<ReadWriteData> resultData = Lists.newArrayList();
		for (Rule.Result result: rule.getResults()) {
			Product p = isNonResource ? null : productService.getProductByServiceCode(result.getProduct());
			ReadWriteData rwd = result.getType() == RuleConfig.DataType.usage ? data.getUsage(p) : data.getCost(p);
			if (rwd == null) {
				rwd = new ReadWriteData(data.getNumUserTags());
				if (result.getType() == RuleConfig.DataType.usage)
					data.putUsage(p, rwd);
				else
					data.putCost(p, rwd);
			}
			resultData.add(rwd);
		}
		
		logger.info("  -- opData size: " + opDataSize + ", resultData size: " + resultData.size());
			
		int maxNum = data.getMaxNum();
		
		// Get the aggregated value for the input operand
		Map<AggregationTagGroup, Double[]> inData = runQuery(rule.getIn(), data, isNonResource, maxNum, rule.config.getName());
		
		Map<String, Double[]> opSingleValues = getOperandSingleValues(rule, data, isNonResource, maxNum, operandSingleValueCache);
		
		int results = applyRule(rule, inData, opSingleValues, resultData, isNonResource, maxNum);
		
		sw.stop();
		String info = "Elapsed time: " + sw.toString();
		
		data.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Fixed, isNonResource, inData.size(), results, info));
		logger.info("  -- data for rule " + rule.config.getName() + " -- in data size = " + inData.size() + ", --- results size = " + results);
	}
		
	
	/*
	 * Returns a map containing the single operand values needed to compute the results.
	 */
	protected Map<String, Double[]> getOperandSingleValues(Rule rule, CostAndUsageData data,
			boolean isNonResource, int maxHours,
			Map<Query, Double[]> operandSingleValueCache) throws Exception {
				
		Map<String, Double[]> operandSingleValues = Maps.newHashMap();
		for (String opName: rule.getOperands().keySet()) {			
			Query op = rule.getOperand(opName);
			if (!op.isSingleAggregation()) {
				throw new Exception("Unsupported configuration: operand \"" + opName + "\" has more than a single aggregated value per hour.");
			}
						
			// See if the values are in the cache
			if (operandSingleValueCache.containsKey(op)) {
				operandSingleValues.put(opName, operandSingleValueCache.get(op));
				logger.info("  -- getOperandSingleValues found values in cache for operand \"" + opName + "\", value[0] = " + operandSingleValueCache.get(op)[0]);
				continue;
			}
			
			Map<AggregationTagGroup, Double[]> opAggTagGroups = runQuery(op, data, isNonResource, maxHours, rule.config.getName());
			if (opAggTagGroups.size() > 1)
				throw new Exception("Single value operand \"" + opName + "\" has more than one tag group.");
			
			Double[] values = opAggTagGroups.values().iterator().next();
			
			operandSingleValues.put(opName, values);
			operandSingleValueCache.put(op, values);

			if (op.isMonthly())
				logger.info("  -- single monthly operand " + opName + " has value " + values[0]);
		}
		return operandSingleValues;
	}
	
	protected int applyRule(
			Rule rule,
			Map<AggregationTagGroup, Double[]> in,
			Map<String, Double[]> opSingleValues,
			List<ReadWriteData> resultData,
			boolean isNonResource,
			int maxNum) throws Exception {
		
		int numResults = 0;
		
		// For each result operand...
		for (int i = 0; i < rule.getResults().size(); i++) {
			//logger.info("result " + i + " for atg: " + atg);
			Rule.Result result = rule.getResult(i);
			
			if (result.isSingle()) {
				TagGroup outTagGroup = result.tagGroup(null, accountService, productService, isNonResource);
				ReadWriteData rwd = resultData.get(i);
				// Remove any existing value from the result data
				for (int hour = 0; hour < rwd.getNum(); hour++)
					rwd.remove(hour, outTagGroup);
						
				String expr = rule.getResultValue(i);
				if (expr != null && !expr.isEmpty()) {
					//logger.info("process hour data");
					eval(i, rule, expr, null, opSingleValues, rwd, outTagGroup, maxNum);
					numResults++;
				}
			}
			else {
				for (AggregationTagGroup atg: in.keySet()) {
				
					TagGroup outTagGroup = result.tagGroup(atg, accountService, productService, isNonResource);
					
					String expr = rule.getResultValue(i);
					if (expr != null && !expr.isEmpty()) {
						//logger.info("process hour data");
						eval(i, rule, expr, in.get(atg), opSingleValues, resultData.get(i), outTagGroup, maxNum);
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
				Query op = rule.getOperand(opName);
				Double[] opValues = op.isSingleAggregation() ? opSingleValuesMap.get(opName) : null;
				Double opValue = opValues == null ? 0.0 : opValues[op.isMonthly() ? 0 : hour];
				expr = expr.replace("${" + opName + "}", opValue.toString());
			}
			try {
				Double value = new Evaluator().eval(expr);
				if (debug && hour == 0)
					logger.info("eval(" + index + "): " + outExpr + " = " + expr + " = " + value + ", " + outTagGroup);
				resultData.add(hour, outTagGroup, value);
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
