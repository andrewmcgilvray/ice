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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.Config.TagCoverage;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;

public class CostAndUsageDataTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    static private ProductService ps;
	static private AccountService as;
	static private List<String> userTags;
	static private TagGroup tg;
	static private TagGroup staleDataTagGroup;
	
	@BeforeClass
	static public void init() {
		ps = new BasicProductService();
		as = new BasicAccountService();
		userTags = Lists.newArrayList(new String[]{ "Email, Environment" });
        tg = TagGroup.getTagGroup(as.getAccountById("123", ""), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.ondemandInstances, UsageType.getUsageType("c1.medium", "hours"), null);
        staleDataTagGroup = TagGroup.getTagGroup(as.getAccountById("123", ""), Region.US_WEST_1, null, ps.getProduct(Product.Code.S3), Operation.ondemandInstances, UsageType.getUsageType("c1.medium", "hours"), null);
	}
	

	@Test
	public void testAddTagCoverage() {
		CostAndUsageData cau = new CostAndUsageData(0, null, userTags, TagCoverage.withUserTags, as, ps);
		
		cau.addTagCoverage(null, 0, tg, new boolean[]{true, false});
		
		ReadWriteTagCoverageData data = cau.getTagCoverage(null);
		
		TagCoverageMetrics tcm = data.getData(0).get(tg);
		
		assertEquals("wrong metrics total", 1, tcm.total);
		assertEquals("wrong count on Email tag", 1, tcm.counts[0]);
		assertEquals("wrong count on Environment tag", 0, tcm.counts[1]);		
	}
	
	@Test
	public void testAggregateSummaryData() {
		CostAndUsageData cau = new CostAndUsageData(0, null, userTags, TagCoverage.withUserTags, as, ps);
		ReadWriteData cost = new ReadWriteData();
		cau.putCost(null, cost);
		
		class TestCase {
			int daysFromLastMonth;
			int daysInMonth;
			int numberOfWeeks;
			
			TestCase(int daysFromLastMonth, int daysInMonth, int numberOfWeeks) {
				this.daysFromLastMonth = daysFromLastMonth;
				this.daysInMonth = daysInMonth;
				this.numberOfWeeks = numberOfWeeks;
			}
			
			public String toString() {
				return "[" + Integer.toString(daysFromLastMonth) + ", " + Integer.toString(daysInMonth) + ", " + Integer.toString(numberOfWeeks) + "]";
			}
		}
		
		TestCase[] testCases = new TestCase[]{
				new TestCase(0, 31, 5),
				new TestCase(2, 31, 5),
				new TestCase(4, 31, 5),
				new TestCase(0, 28, 4),
				new TestCase(1, 28, 5),
		};
		
		for (TestCase tc: testCases) {
			cost.cutData(0);
			for (int hour = 0; hour < 24 * tc.daysInMonth; hour++) {
				Map<TagGroup, Double> hourlyCost = cost.getData(hour);
				hourlyCost.put(tg, 1.0);
			}
			
	        // init daily, weekly and monthly
	        List<Map<TagGroup, Double>> daily = Lists.newArrayList();
	        List<Map<TagGroup, Double>> weekly = Lists.newArrayList();
	        List<Map<TagGroup, Double>> monthly = Lists.newArrayList();
	
	        Collection<TagGroup> tagGroups = cost.getTagGroups();
	        
	        cau.addValue(weekly, 0, tg, 24.0 * tc.daysFromLastMonth);
	
	        cau.aggregateSummaryData(cost, tagGroups, tc.daysFromLastMonth, daily, weekly, monthly);
		
	        assertEquals("wrong number of days", tc.daysInMonth, daily.size());
	        assertEquals("wrong number of weeks", tc.numberOfWeeks, weekly.size());
	        assertEquals("wrong number of months", 1, monthly.size());
	        
	        for (int day = 0; day < daily.size(); day++)
	        	assertEquals("wrong value for day " + day + " for test case " + tc, 24.0, daily.get(day).get(tg), 0.001);
	        
	        int daysInLastWeek = (tc.daysInMonth + tc.daysFromLastMonth) % 7;
	        daysInLastWeek = daysInLastWeek == 0 ? 7 : daysInLastWeek;
	        for (int week = 0; week < weekly.size(); week++) {
	        	double expected = 24 * (week < weekly.size() - 1 ? 7 : daysInLastWeek);
	        	assertEquals("wrong value for week " + week + " for test case " + tc, expected, weekly.get(week).get(tg), 0.001);
	        }
	        assertEquals("wrong value for month for test case " + tc, 24 * tc.daysInMonth, monthly.get(0).get(tg), 0.001);
		}
	}
	
	@Test
	public void testGetPartialWeekFromLastMonth() throws Exception {
		CostAndUsageData cau = new CostAndUsageData(0, null, userTags, TagCoverage.withUserTags, as, ps);
		ReadWriteData cost = new ReadWriteData();
		cau.putCost(null, cost);

        DateTime monthDateTime = new DateTime("2020-01", DateTimeZone.UTC);
		for (int day = 0; day < 365; day++) {
			Map<TagGroup, Double> dailyCost = cost.getData(day);
			dailyCost.put(tg, 1.0);
		}
		
		int daysFromLastMonth = 4;
		Collection<TagGroup> tagGroups = cost.getTagGroups();
        List<Map<TagGroup, Double>> weekly = Lists.newArrayList();

        
        int lastMonthNumDays = monthDateTime.minusMonths(1).dayOfMonth().getMaximumValue();
        int lastMonthDayOfYear = monthDateTime.minusMonths(1).getDayOfYear();
        int startDay = lastMonthDayOfYear + lastMonthNumDays - daysFromLastMonth - 1;

        cau.getPartialWeek(cost, startDay, daysFromLastMonth, 0, tagGroups, weekly);
        
        assertEquals("wrong number of weeks", 1, weekly.size());
        assertEquals("wrong value for days from last month", 1.0 * daysFromLastMonth, weekly.get(0).get(tg), 0.001);
	}
	
	class TestDataWriter extends DataWriter {
		private ReadWriteData archive;
		
		TestDataWriter(String dbName, ReadWriteDataSerializer data, boolean compress, ReadWriteData archive) throws Exception {
			super(dbName, data, compress, false, null, null, null);
			this.archive = archive;
		}

		@Override
	    void archive() throws IOException {
			if (archive != null)
				archive.putAll((ReadWriteData)data);
	    }
	    
		@Override
	    void delete() {
	    }
	}
	
	class TestCostAndUsageData extends CostAndUsageData {
		public ReadWriteData dailyCost = new ReadWriteData();
		public ReadWriteData weeklyCost = new ReadWriteData();
		public ReadWriteData monthlyCost = new ReadWriteData();
		public DateTime startMonth;
		public DateTime currentMonth;
		public DateTime endMonth;

		public TestCostAndUsageData(DateTime startMonth, DateTime currentMonth, DateTime endMonth) {
			super(startMonth.getMillis(), null, userTags, TagCoverage.withUserTags, as, ps);
			this.startMonth = startMonth;
			this.currentMonth = currentMonth;
			this.endMonth = endMonth;
		}
		
		@Override
	    protected DataWriter getDataWriter(String name, ReadWriteDataSerializer data, boolean compress, boolean load) throws Exception {
			DateTime start = new DateTime(getStartMilli(), DateTimeZone.UTC);
			ReadWriteData archive = null;
			// Prepare "loaded" data and provide destination for archive to capture output
			if (name.contains("daily_")) {
				int startYear = startMonth.getYear();
				int currentYear = currentMonth.getYear();
				int endYear = endMonth.getYear();
				int endDay = endMonth.getDayOfYear() - 1;
				archive = dailyCost;
				
				if (startYear != currentYear || currentYear != endYear) {
					int year = Integer.parseInt(name.substring(name.length() - 4));
					if (year < currentYear)
						endDay = currentMonth.minusYears(1).dayOfYear().getMaximumValue();
					else if (year > currentYear)
						archive = null;
					else if (year == currentYear && currentYear != endYear)
						endDay = currentMonth.dayOfYear().getMaximumValue();
				}
				for (int day = 0; day < endDay; day++) {
					Map<TagGroup, Double> cost = ((ReadWriteData)data).getData(day);
					cost.put(tg, 1.0 * 24);
					cost.put(staleDataTagGroup, 1.0 * 24);
				}
			}
			else if (name.contains("weekly_")) {				
		        int weeks = Weeks.weeksBetween(startMonth, endMonth).getWeeks() + (startMonth.dayOfWeek() == currentMonth.withDayOfWeek(1).dayOfWeek() ? 0 : 1);
				for (int week = 0; week < weeks; week++) {
					Map<TagGroup, Double> cost = ((ReadWriteData)data).getData(week);
					cost.put(tg, 1.0 * 24 * 7);
					cost.put(staleDataTagGroup, 1.0 * 24 * 7);
				}
				archive = weeklyCost;
			}
			else if (name.contains("monthly_")) {
				for (int month = 0; month < Months.monthsBetween(startMonth, endMonth).getMonths(); month++) {
					Map<TagGroup, Double> cost = ((ReadWriteData)data).getData(month);
					int daysInMonth = start.plusMonths(month).dayOfMonth().getMaximumValue();
					cost.put(tg, 1.0 * 24 * daysInMonth);
					cost.put(staleDataTagGroup, 1.0 * 24 * daysInMonth);
				}
				archive = monthlyCost;
			}

			DataWriter writer = new TestDataWriter(name, data, compress, archive);
	        return writer;
	    }
	}

	@Test
	public void testArchiveSummaryProduct() throws Exception {
		
		
		// Test case where startDate and monthDate are both Jan 1. 2020
        testArchive("2020-01", "2020-01", "2020-01", 31, 5, 1);
        
        // Test case where startDate and monthDate are both Jan 1., but we have already processed Jan previously.
        testArchive("2020-01", "2020-01", "2020-02", 31, 5, 1);
        
        // Test case where startDate is before Jan 1. 2020
        testArchive("2019-12", "2020-01", "2020-02", 31, 10, 2);
        
        // Test case where startDate is before Jan 1. 2020 and we already have data processed for Feb, 2020
        testArchive("2019-12", "2020-01", "2020-03", 31 + 29, 14, 3);
        
        // Test case where startDate and monthDate are in earlier year than next month with remainder of week.
        testArchive("2019-12", "2019-12", "2020-02", 365, 9, 2);
	}
	
	private void testArchive(String start, String month, String end, int expectedDays, int expectedWeeks, int expectedMonths) throws Exception {
		DateTime startDate = new DateTime(start, DateTimeZone.UTC);
        DateTime monthDate = new DateTime(month, DateTimeZone.UTC);
        DateTime existingDataEndDay = new DateTime(end, DateTimeZone.UTC);
        
        // Load existing data for month we're about to process to make sure it's overwritten and not added to.
		TestCostAndUsageData cau = new TestCostAndUsageData(startDate, monthDate, existingDataEndDay);
		ReadWriteData cost = new ReadWriteData();
		cau.putCost(null, cost);
		// Load data for Jan 2020
		for (int hour = 0; hour < 24 * 31; hour++) {
			Map<TagGroup, Double> hourlyCost = cost.getData(hour);
			hourlyCost.put(tg, 1.0);
		}

	    cau.archiveSummaryProduct(monthDate, startDate, null, cost, "", true);

        assertEquals("wrong number of days", expectedDays, cau.dailyCost.getNum());
        assertEquals("wrong number of weeks", expectedWeeks, cau.weeklyCost.getNum());
        assertEquals("wrong number of months", expectedMonths, cau.monthlyCost.getNum());
	    
        // For daily data, make sure we've only updated the days in the month
        int firstDay = monthDate.getDayOfYear() - 1;
        int daysInMonth = monthDate.dayOfMonth().getMaximumValue();
        for (int day = 0; day < firstDay; day++)
        	assertNotNull("did not find stale data for day " + day, cau.dailyCost.getData(day).get(staleDataTagGroup));
        
        for (int day = firstDay; day < firstDay + daysInMonth; day++) {
        	assertEquals("wrong value for day " + day, 24.0, cau.dailyCost.getData(day).get(tg), 0.001);
        	assertNull("found stale data for day " + day, cau.dailyCost.getData(day).get(staleDataTagGroup));
        }
        for (int day = firstDay + daysInMonth; day < cau.dailyCost.getNum(); day++)
        	assertNotNull("did not find stale data for day " + day, cau.dailyCost.getData(day).get(staleDataTagGroup));
        
        // For weekly data, make sure we've only updated the proper weeks
        int dayOfWeek = monthDate.getDayOfWeek(); // Monday is first day of week == 1
        int daysFromLastMonth = dayOfWeek - 1;
        int daysInLastWeek = (monthDate.dayOfMonth().getMaximumValue() + daysFromLastMonth) % 7;
        daysInLastWeek = daysInLastWeek == 0 ? 7 : daysInLastWeek;
        
        DateTime weekStart = monthDate.withDayOfWeek(1);
        int index;
        if (!weekStart.isAfter(startDate))
            index = 0;
        else
            index = Weeks.weeksBetween(startDate, weekStart).getWeeks() + (startDate.dayOfWeek() == weekStart.dayOfWeek() ? 0 : 1);
        
        for (int week = 0; week < index; week++)
        	assertNotNull("did not find stale data for week " + week, cau.weeklyCost.getData(week).get(staleDataTagGroup));
        
        int daysInNextMonth = 7 - (monthDate.plusMonths(1).getDayOfWeek() - 1);
        if (daysInNextMonth == 7)
        	daysInNextMonth = 0;
        int totalDays = monthDate.getDayOfWeek() - 1 + monthDate.dayOfMonth().getMaximumValue() + daysInNextMonth;
        int weeks = Math.min((int) Math.ceil(totalDays / 7.0), cau.weeklyCost.getNum() - index);

        for (int week = index; week < index + weeks; week++) {
        	double daysInFirstWeek = monthDate.isAfter(startDate) ? 7 : 7 - daysFromLastMonth;
        	double expected = 24 * (week == index ? daysInFirstWeek : week < cau.weeklyCost.getNum() - 1 ? 7 : daysInLastWeek);
        	assertEquals("wrong value for week " + week, expected, cau.weeklyCost.getData(week).get(tg), 0.001);
        	assertNull("found stale data for week " + week, cau.weeklyCost.getData(week).get(staleDataTagGroup));
        }
        for (int week = index + weeks; week < cau.weeklyCost.getNum(); week++)
        	assertNotNull("did not find stale data for week " + week, cau.weeklyCost.getData(week).get(staleDataTagGroup));
        
        // For monthly data, make sure we've only updated the one month
        int monthIndex = Months.monthsBetween(startDate, monthDate).getMonths();
        for (int m = 0; m < monthIndex; m++)
        	assertNotNull("did not find stale data for month", cau.monthlyCost.getData(m).get(staleDataTagGroup));
        	
        assertEquals("wrong value for month", 24 * monthDate.dayOfMonth().getMaximumValue(), cau.monthlyCost.getData(monthIndex).get(tg), 0.001);
    	assertNull("found stale data for month", cau.monthlyCost.getData(monthIndex).get(staleDataTagGroup));
        
        for (int m = monthIndex + 1; m < cau.monthlyCost.getNum(); m++)
        	assertNotNull("did not find stale data for month", cau.monthlyCost.getData(m).get(staleDataTagGroup));
	}
}
