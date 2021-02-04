package com.netflix.ice.processor.postproc;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AggregationTagGroup;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.DataSerializer;
import com.netflix.ice.processor.CostAndUsageData.PostProcessorStats;
import com.netflix.ice.processor.CostAndUsageData.RuleType;
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
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
	public boolean process(CostAndUsageData data) throws Exception {
		// Cache the single values across the resource and non-resource based passes
		// in case we can reuse them. This saves a lot of time on operands that
		// aggregate a large amount of data into a single value and are not grouping
		// by any user tags.
		Map<Query, CostAndUsage[]> operandSingleValueCache = Maps.newHashMap();
				
		logger.info("Post-process with rule " + getConfig().getName() + " on non-resource data");
		processData(data, true, operandSingleValueCache);
		
		logger.info("Post-process with rule " + getConfig().getName() + " on resource data");
		processData(data, false, operandSingleValueCache);
		
		return true;
	}
	
	protected void processData(CostAndUsageData data, boolean isNonResource, Map<Query, CostAndUsage[]> operandSingleValueCache) throws Exception {		
		StopWatch sw = new StopWatch();
		sw.start();
		
		// Get data maps for results. Handle case where we're creating a new product
		List<DataSerializer> resultData = Lists.newArrayList();
		for (Rule.Result result: rule.getResults()) {
			Product p = isNonResource ? null : productService.getProductByServiceCode(result.getProduct());
			DataSerializer ds = data.get(p);
			if (ds == null) {
				ds = new DataSerializer(data.getNumUserTags());
				data.put(p, ds);
			}
			resultData.add(ds);
		}
		
		logger.info("  -- resultData size: " + resultData.size());
			
		int maxNum = data.getMaxNum();
		
		// Get the aggregated value for the input operand
		Map<AggregationTagGroup, CostAndUsage[]> inData = runQuery(rule.getIn(), data, isNonResource, maxNum, rule.config.getName());
		
		Map<String, CostAndUsage[]> opSingleValues = getOperandSingleValues(rule, data, isNonResource, maxNum, operandSingleValueCache);
		
		int results = applyRule(rule, inData, opSingleValues, resultData, isNonResource, maxNum);
		
		sw.stop();
		String info = "Elapsed time: " + sw.toString();
		
		data.addPostProcessorStats(new PostProcessorStats(rule.config.getName(), RuleType.Fixed, isNonResource, inData.size(), results, info));
		logger.info("  -- data for rule " + rule.config.getName() + " -- in data size = " + inData.size() + ", --- results size = " + results);
	}
		
	
	/*
	 * Returns a map containing the single operand values needed to compute the results.
	 */
	protected Map<String, CostAndUsage[]> getOperandSingleValues(Rule rule, CostAndUsageData data,
			boolean isNonResource, int maxHours,
			Map<Query, CostAndUsage[]> operandSingleValueCache) throws Exception {
				
		Map<String, CostAndUsage[]> operandSingleValues = Maps.newHashMap();
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
			
			Map<AggregationTagGroup, CostAndUsage[]> opAggTagGroups = runQuery(op, data, isNonResource, maxHours, rule.config.getName());
			if (opAggTagGroups.size() > 1)
				throw new Exception("Single value operand \"" + opName + "\" has more than one tag group.");
			
			CostAndUsage[] values = opAggTagGroups.values().iterator().next();
			
			operandSingleValues.put(opName, values);
			operandSingleValueCache.put(op, values);

			if (op.isMonthly())
				logger.info("  -- single monthly operand " + opName + " has value " + values[0]);
		}
		return operandSingleValues;
	}
	
	protected int applyRule(
			Rule rule,
			Map<AggregationTagGroup, CostAndUsage[]> in,
			Map<String, CostAndUsage[]> opSingleValues,
			List<DataSerializer> resultData,
			boolean isNonResource,
			int maxNum) throws Exception {
		
		int numResults = 0;
		
		// For each result operand...
		for (int i = 0; i < rule.getResults().size(); i++) {
			//logger.info("result " + i + " for atg: " + atg);
			Rule.Result result = rule.getResult(i);
			
			if (result.isSingle()) {
				TagGroup outTagGroup = result.tagGroup(null, accountService, productService, isNonResource);
				DataSerializer ds = resultData.get(i);
				// Remove any existing value from the result data
				for (int hour = 0; hour < ds.getNum(); hour++)
					ds.remove(hour, outTagGroup);
						
				if (eval(i, rule, result, null, opSingleValues, ds, outTagGroup, maxNum))
					numResults++;
			}
			else {
				for (AggregationTagGroup atg: in.keySet()) {
				
					TagGroup outTagGroup = result.tagGroup(atg, accountService, productService, isNonResource);
					
					if (eval(i, rule, result, in.get(atg), opSingleValues, resultData.get(i), outTagGroup, maxNum))
						numResults++;
					
					debug = false;
				}
			}
		}
		
		return numResults;
	}
	
	static class Expression {
		String original;
		List<String> splits;
		List<Ref> refs;
		
		class Ref {
			int index;
			String opName;
			boolean isCost;
			boolean isMonthly;
			
			Ref(int index, String opName, boolean isCost, boolean isMonthly) {
				this.index = index;
				this.opName = opName;
				this.isCost = isCost;
				this.isMonthly = isMonthly;
			}
		}
		
		Expression(String expr, Map<String, Query> ops) {
			original = expr;
			splits = Lists.newArrayList();
			refs = Lists.newArrayList();
			
			if (original == null || original.isEmpty()) {
				original = null;
				return;
			}
			
			String[] initialSplit = original.split("\\$\\{", -1);
			for (int i = 0; i < initialSplit.length; i++) {
				if (initialSplit[i].contains("}")) {
					// We have one or two pieces with the first being a ref
					String[] secondarySplit = initialSplit[i].split("}");
					
					// Split into opName and cost/usage
					String[] op = secondarySplit[0].split("\\.");
					boolean isMonthly = op[0].equals("in") ? false : ops.get(op[0]).isMonthly();
					refs.add(new Ref(splits.size(), op[0], op[1].equals("cost"), isMonthly));
					splits.add("${" + secondarySplit[0] + "}");
					
					if (secondarySplit.length > 1)
						splits.add(secondarySplit[1]);
				}
				else {
					splits.add(initialSplit[i]);
				}
			}
		}
		
		String expand(CostAndUsage in, Map<String, CostAndUsage[]> opSingleValuesMap, int hour) {
			if (original == null)
				return null;
			
			List<String> ret = Lists.newArrayListWithCapacity(splits.size());
			for (Ref ref: refs) {
				for (int i = ret.size(); i < ref.index; i++)
					ret.add(splits.get(i));
				if (ref.opName.equals("in")) {
					ret.add(Double.toString(ref.isCost ? in.cost : in.usage));
				}
				else {
					CostAndUsage[] opValues = opSingleValuesMap.get(ref.opName);
					CostAndUsage opValue = opValues == null ? new CostAndUsage() : opValues[ref.isMonthly ? 0 : hour];
					ret.add(Double.toString(ref.isCost ? opValue.cost : opValue.usage));
				}
			}
			for (int i = ret.size(); i < splits.size(); i++)
				ret.add(splits.get(i));
					
			return String.join("", ret);
		}
	}
	
	private boolean eval(
			int index,
			Rule rule,
			Rule.Result result,
			CostAndUsage[] inValues, 
			Map<String, CostAndUsage[]> opSingleValuesMap,
			DataSerializer resultData,
			TagGroup outTagGroup,
			int maxNum) throws Exception {

		int maxHours = inValues == null ? maxNum : inValues.length;
		boolean hasCost = !StringUtils.isEmpty(result.getCost());
		boolean hasUsage = !StringUtils.isEmpty(result.getUsage());
		
		if (!hasCost && !hasUsage)
			return false;
		
		Expression costExp = new Expression(result.getCost(), rule.getOperands());
		Expression usageExp = new Expression(result.getUsage(), rule.getOperands());
		
		// Process each hour of data - we'll only have one if 'in' is a monthly operand
		for (int hour = 0; hour < maxHours; hour++) {
			// Replace variables
			String cost = inValues == null ? result.getCost() : costExp.expand(inValues[hour], opSingleValuesMap, hour);
			String usage = inValues == null ? result.getUsage() : usageExp.expand(inValues[hour], opSingleValuesMap, hour);
			Double costResult = 0.0;
			Double usageResult = 0.0;
			
			if (hasCost) {
				try {
					costResult = new Evaluator().eval(cost);
				}
				catch (Exception e) {
					logger.error("Error processing expressions \"" + cost + "\", " + e.getMessage());
					throw e;
				}
				if (debug && hour == 0)
					logger.info("eval(" + index + ") cost: " + result.getCost() + " = " + cost + " = " + costResult + ", " + outTagGroup);
			}
			
			if (hasUsage) {
				try {
					usageResult = hasUsage ? new Evaluator().eval(usage) : 0.0;
				}
				catch (Exception e) {
					logger.error("Error processing expressions \"" + usage + "\", " + e.getMessage());
					throw e;
				}
				if (debug && hour == 0)
					logger.info("eval(" + index + ") usage: " + result.getUsage() + " = " + usage + " = " + usageResult + ", " + outTagGroup);
			}
			resultData.add(hour, outTagGroup, new CostAndUsage(costResult, usageResult));
		}
		return true;
	}
	
	public int getCacheMisses() {
		return cacheMisses;
	}
	public int getCacheHits() {
		return cacheHits;
	}
	
}
