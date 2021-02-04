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

import java.util.List;

import org.joda.time.DateTime;
import org.junit.Test;

import com.amazonaws.services.ec2.model.Tag;
import com.netflix.ice.processor.CanonicalReservedInstances.RecurringCharge;

public class CanonicalReservedInstancesTest {
	// test values
	private static final String account = "111111111111";
	private static final String product = "EC2";
	private static final String region = "us-east-1";
	private static final String reservationId = "aaaaaaaa-bbbb-cccc-ddddddddddddddddd";
	private static final String offeringId = "eeee-fffffff-gggggg-hhhhhhhh";
	private static final String instanceType = "m1.large";
	private static final String scope = "Availability Zone";
	private static final String zone = "us-east-1a";
	private static final String multiAZ = "false";
	private static final String start = "2017-06-09 21:21:37";
	private static final String end = "2018-06-09 21:21:36";
	private static final String duration = "31536000";
	private static final String usagePrice = "10.00";
	private static final String fixedPrice = "20.00";
	private static final String instanceCount = "5";
	private static final String description = "Linux/UNIX (Amazon VPC)";
	private static final String state = "active";
	private static final String currency = "USD";
	private static final String offeringType = "All Upfront";
	private static final String recurringCharges = "hourly:1.0|monthly:30.0";
	private static final String parentReservationId = "bbbbbbbb-cccc-cccc-ddddddddddddddddd";
	private static final String offeringClass = "standard";
	private static final String tags = "foo:bar|Tag:Value";
	
	
	private void compareValues(CanonicalReservedInstances cri, int numRecurring) {
		assertEquals("Account doesn't match", account, cri.getAccountId());
		assertEquals("Region doesn't match", region, cri.getRegion());
		assertEquals("Product doesn't match", product, cri.getProduct());
		assertEquals("ReservationID doesn't match", reservationId, cri.getReservationId());
		assertEquals("OfferingId doesn't match", offeringId, cri.getReservationOfferingId());
		assertEquals("InstanceType doesn't match", instanceType, cri.getInstanceType());
		assertEquals("Scope doesn't match", scope, cri.getScope());
		assertEquals("Zone doesn't match", zone, cri.getAvailabilityZone());
		assertEquals("MultiAZ doesn't match", multiAZ, cri.getMultiAZ().toString());
		String s = LineItem.amazonBillingDateFormat.print(new DateTime(cri.getStart().getTime()));
		assertEquals("Start doesn't match", start, s);
		String e = LineItem.amazonBillingDateFormat.print(new DateTime(cri.getEnd().getTime()));
		assertEquals("End doesn't match", end, e);
		assertEquals("Duration doesn't match", duration, cri.getDuration().toString());
		assertEquals("UsagePrice doesn't match", Double.parseDouble(usagePrice), cri.getUsagePrice(), 0.001);
		assertEquals("FixedPrice doesn't match", Double.parseDouble(fixedPrice), cri.getFixedPrice(), 0.001);
		assertEquals("InstanceCount doesn't match", instanceCount, cri.getInstanceCount().toString());
		assertEquals("Description doesn't match", description, cri.getProductDescription());
		assertEquals("State doesn't match", state, cri.getState());
		assertEquals("Currency doesn't match", currency, cri.getCurrencyCode());
		assertEquals("OfferingType doesn't match", offeringType, cri.getOfferingType());
		List<RecurringCharge> rcsA = cri.getRecurringCharges();
		String[] rcsB = recurringCharges.split("\\|");
		assertEquals("Number of recurring charges is wrong", rcsA.size(), numRecurring);
		for (int i = 0; i < numRecurring; i++) {
			String[] rcB = rcsB[i].split(":");
			assertEquals("Recurrence frequency for index " + i + " doesn't match", rcB[0], rcsA.get(i).frequency);
			assertEquals("Recurrence cost for index " + i + " doesn't match", Double.parseDouble(rcB[1]), rcsA.get(i).cost, 0.001);
		}
		assertEquals("ParentReservationId doesn't match", parentReservationId, cri.getParentReservationId());
		assertEquals("OfferingClass doesn't match", offeringClass, cri.getOfferingClass());
		int foundCount = 0;
		for (String expectedTag: tags.split("\\|")) {
			String[] tagParts = expectedTag.split(":");
			for (Tag t: cri.getTags()) {
				if (t.getKey().equals(tagParts[0])) {
					foundCount++;
					assertEquals("Wrong tag value for " + tagParts[0], tagParts[1], t.getValue());
				}
			}
		}
		assertEquals("Not all tags were found", tags.split("\\|").length, foundCount);
	}

	@Test
	public void testCSVConstructor() {
		String[] testRes = new String[]{
				account, product, region, reservationId, offeringId, instanceType, scope,
				zone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount,
				description, state, currency, offeringType, recurringCharges, parentReservationId,
				offeringClass, tags
		};
		CanonicalReservedInstances cri = new CanonicalReservedInstances(String.join(",", testRes));
		compareValues(cri, 2);
	}
	
	@Test
	public void testCSVConstructorNoRecurring() {
		String[] testRes = new String[]{
				account, product, region, reservationId, offeringId, instanceType, scope,
				zone, multiAZ, start, end, duration, usagePrice, fixedPrice, instanceCount,
				description, state, currency, offeringType, "", parentReservationId,
				offeringClass, tags
		};
		CanonicalReservedInstances cri = new CanonicalReservedInstances(String.join(",", testRes));
		compareValues(cri, 0);
	}
}
