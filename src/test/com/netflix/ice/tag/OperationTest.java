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
package com.netflix.ice.tag;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

public class OperationTest {

	@Test
	public void testIsSavingsPlanBonus() {
		assertTrue(Operation.savingsPlanBonusNoUpfront.isBonus());
	}

	@Test
	public void testGetSavingsPlanOperations() {
		assertEquals("wrong number of savings plan operations", 24, Operation.getSavingsPlanOperations(false).size());
	}
	
	@Test
	public void testGetOperations() {
		String op = "SavingsPlan Used - All Upfront";
		List<String> ops = Lists.newArrayList(op);
		assertEquals("missing operation", op, Operation.getOperations(ops).get(0).name);
	}
	
	@Test
	public void testIdentity() {
		// Test Borrowed
		List<Operation> operations = Operation.getReservationOperations(false);
		List<Operation> excluded = Lists.newArrayList();
		List<Operation.Identity.Value> exclude = Lists.newArrayList(Operation.Identity.Value.Borrowed);
		int bits = Operation.Identity.getIdentitySet(exclude);
		for (Operation op: operations) {
			if (op.isOneOf(bits))
				excluded.add(op);
		}
		assertEquals("wrong number of borrowed reservation items", 11, excluded.size());

		operations = Operation.getSavingsPlanOperations(false);
		excluded = Lists.newArrayList();
		exclude = Lists.newArrayList(Operation.Identity.Value.Borrowed);
		bits = Operation.Identity.getIdentitySet(exclude);
		for (Operation op: operations) {
			if (op.isOneOf(bits))
				excluded.add(op);
		}
		assertEquals("wrong number of borrowed savings plan items", 5, excluded.size());		

		// Test lent
		operations = Operation.getReservationOperations(true);
		excluded = Lists.newArrayList();
		exclude = Lists.newArrayList(Operation.Identity.Value.Lent);
		bits = Operation.Identity.getIdentitySet(exclude);
		for (Operation op: operations) {
			if (op.isOneOf(bits))
				excluded.add(op);
		}
		assertEquals("wrong number of lent items", 11, excluded.size());	
		
		operations = Operation.getSavingsPlanOperations(true);
		excluded = Lists.newArrayList();
		exclude = Lists.newArrayList(Operation.Identity.Value.Lent);
		bits = Operation.Identity.getIdentitySet(exclude);
		for (Operation op: operations) {
			if (op.isOneOf(bits))
				excluded.add(op);
		}
		assertEquals("wrong number of lent savings plan items", 5, excluded.size());		

	}
			
    public static final Operation AATag = Operation.getOperation("AA");
    public static final Operation BBTag = Operation.getOperation("BB");
    public static final Operation aaTag = Operation.getOperation("aa");
    public static final Operation bbTag = Operation.getOperation("bb");
    
	@Test
	public void testCompareTo() {		
		assertTrue("aa tag not less than bb", aaTag.compareTo(bbTag) < 0);
		assertTrue("bb tag not greater than aa", bbTag.compareTo(aaTag) > 0);
		assertTrue("AA tag not less than aa", AATag.compareTo(aaTag) < 0);
		assertTrue("aa tag not greater than AA", aaTag.compareTo(AATag) > 0);
		assertTrue("aa tag not less than BB", aaTag.compareTo(BBTag) < 0);
		assertTrue("BB tag not greater than aa", BBTag.compareTo(aaTag) > 0);
	}

}
