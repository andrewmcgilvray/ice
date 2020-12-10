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

/**
 * Post Processor Rule Configuration
 * 
 * The post processor provides a way to generate arbitrary cost and usage records based on existing cost and usage data processed from the reports.
 * Each rule can be given a name which is used only by the processor for logging purposes and report naming. Start and end dates inform the processor when in time
 * the rule is active. The rule will not be applied to data outside of the active window.
 * 
 * The 'reports' property specifies that the destination for the cost and usage records will be one to three CSV report files. When the rule specifies a
 * report, the monthly cost and usage data will remain unchanged. The report property is used to specify one, two, or three aggregation reports. Possible
 * values are 'hourly', 'daily', and 'monthly'.
 * reports are written to the work bucket named report-[name]-[aggregation]-yyyy-mm.csv.gz.
 * 
 * Inputs to the rule are specified using Queries. The 'in' query drives the process while additional query operands can be used to get values for
 * the result value expressions.
 * 
 * The 'results' list holds the expression and tag group information for the values to be computed and written to the cost and usage data sets.
 */
public class RuleConfig {
	public enum DataType {
		cost,
		usage;
	}
	
	private String name;
	private String start;
	private String end;
	private ReportConfig report;
	private Map<String, QueryConfig> operands;
	private QueryConfig in;
	private Map<String, String> patterns;
	private List<ResultConfig> results;
	private AllocationConfig allocation;
	
	public enum Aggregation {
		hourly,
		daily,
		monthly;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
	public String getEnd() {
		return end;
	}
	public void setEnd(String end) {
		this.end = end;
	}
	public boolean isReport() {
		return report != null;
	}
	public ReportConfig getReport() {
		return report;
	}
	public void setReport(ReportConfig report) {
		this.report = report;
	}
	public Map<String, QueryConfig> getOperands() {
		return operands;
	}
	public void setOperands(Map<String, QueryConfig> operands) {
		this.operands = operands;
	}
	public QueryConfig getOperand(String name) {
		return this.operands.get(name);
	}
	public QueryConfig getIn() {
		return in;
	}
	public void setIn(QueryConfig in) {
		this.in = in;
	}
	
	public Map<String, String> getPatterns() {
		return patterns;
	}
	public void setPatterns(Map<String, String> patterns) {
		this.patterns = patterns;
	}
	public List<ResultConfig> getResults() {
		return results;
	}
	public void setResults(List<ResultConfig> results) {
		this.results = results;
	}
	
	public AllocationConfig getAllocation() {
		return allocation;
	}
	
	public void setAllocation(AllocationConfig allocation) {
		this.allocation = allocation;
	}
}
