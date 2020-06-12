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

import java.text.DecimalFormat;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.netflix.ice.common.PurchaseOption;
import com.netflix.ice.common.TagGroupSP;

public class SavingsPlan {
	final public TagGroupSP tagGroup;
	final public PurchaseOption paymentOption;
	final public String term; // 1yr, 3yr
	final public String offeringType; // ComputeSavingsPlan, EC2InstanceSavingsPlan
	final public long start;
	final public long end;
	final public double hourlyRecurringFee;
	final public double hourlyAmortization;
	final public double normalizedRecurring;
	final public double normalizedAmortization;

	private static DecimalFormat df = new DecimalFormat("#.###");

	public SavingsPlan(TagGroupSP tagGroup, PurchaseOption paymentOption, String term, String offeringType, long start, long end, double hourlyRecurringFee, double hourlyAmortization) {
		this.tagGroup = tagGroup;
		this.paymentOption = paymentOption;
		this.term = term;
		this.offeringType = offeringType;
		this.start = start;
		this.end = end;
		this.hourlyRecurringFee = hourlyRecurringFee;
		this.hourlyAmortization = hourlyAmortization;
		this.normalizedRecurring = hourlyRecurringFee / (hourlyRecurringFee + hourlyAmortization);
		this.normalizedAmortization = hourlyAmortization / (hourlyRecurringFee + hourlyAmortization);
	}
	
	public double getRecurring(double effectiveCost) {
		return effectiveCost * normalizedRecurring;
	}
	
	public double getAmortization(double effectiveCost) {
		return effectiveCost * normalizedAmortization;
	}
	
	public String toString() {
		return tagGroup.arn.name + "," + paymentOption.name() + "," + ((Double)hourlyRecurringFee).toString() + "," + ((Double)hourlyAmortization).toString();
	}
	
    public static String[] header() {
		return new String[] {"SavingsPlanARN", "Account", "AccountID", "Region", "Zone", "ServiceCode", "UsageType", "Start", "End", "PurchaseOption", "Term", "OfferingType", "HourlyAmortization", "HourlyRecurring"};
    }
    
    public String[] values() {
		return new String[]{
				tagGroup.arn.toString(),
				tagGroup.account.getIceName(),
				tagGroup.account.getId(),
				tagGroup.region.name,
				tagGroup.zone == null ? "" : tagGroup.zone.name,
				tagGroup.product.getServiceCode(),
				tagGroup.usageType.name,
				new DateTime(start, DateTimeZone.UTC).toString(),
				new DateTime(end, DateTimeZone.UTC).toString(),
				paymentOption.name,
				term,
				offeringType,
				df.format(hourlyAmortization),
				df.format(hourlyRecurringFee),
			};
    }
    
}
