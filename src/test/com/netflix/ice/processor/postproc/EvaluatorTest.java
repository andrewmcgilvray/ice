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

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class EvaluatorTest {

	@Test
	public void testBasic() throws Exception {
		String expr = "5.0 * ((4 / 2 + 4.3E-2) + 12.2 * (3 + 1) + 1)";
		Double expect = 5.0 * ((4 / 2 + 4.3E-2) + 12.2 * (3 + 1) + 1);
		
		assertEquals("Wrong evaluator result", expect, new Evaluator().eval(expr), 0.001);
	}
	
	@Test
	public void testMinMax() throws Exception {
		String expr = "MIN(0.0, 3.0) + MAX(2.0, 4.0) * 3.0";
		Double expect = Math.min(0.0, 3.0) + Math.max(2.0, 4.0) * 3.0;
		assertEquals("Wrong evaluator result", expect, new Evaluator().eval(expr), 0.001);		
	}

	@Test
	public void testDivideByZero() throws Exception {
		String expr = "1.0 / 0.0";
		assertEquals("Divide by zero should return 0.0", 0.0, new Evaluator().eval(expr), 0.001);
	}
	
	@Test
	public void testZeroValue() throws Exception {
		String expr = "0.0 * -9.123242853155332E-7 / 6660.573763753991";
		assertEquals("Value should be zero", 0.0, new Evaluator().eval(expr), 0.001);
	}
	
	@Test
	public void testParse() throws Exception {
		String expr = "0.0 * -9.123242853155332E-7 / 6660.573763753991";
		Double expect = 0.0 * -9.123242853155332E-7 / 6660.573763753991;
		Evaluator e = new Evaluator();
		List<String> tokens = e.parse(expr);
		assertEquals("wrong number of tokens", 5, tokens.size());
		assertEquals("wrong value", expect, e.eval(expr), 0.001);
	}
	
	/* Tests for unuary minus operator when we support it.
	@Test
	public void testLeadingUnaryMinus() throws Exception {
		String expr = "-1.0 * 0.0059919";
		Double expect = -1.0 * 0.0059919;
		Evaluator e = new Evaluator();
		List<String> tokens = e.parse(expr);
		assertEquals("wrong number of tokens", 3, tokens.size());
		assertEquals("wrong value", expect, e.eval(expr), 0.001);
		
		// Test with parenthetical expression
		expr = "-(1.0 * 0.0059919)";
		expect = -(1.0 * 0.0059919);
		e = new Evaluator();
		tokens = e.parse(expr);
		assertEquals("wrong number of tokens", 6, tokens.size());
		assertEquals("wrong value", expect, e.eval(expr), 0.001);

		// Test negative zero
		expr = "-0.0 * 0.0059919";
		expect = -0.0 * 0.0059919;
		e = new Evaluator();
		tokens = e.parse(expr);
		assertEquals("wrong number of tokens", 3, tokens.size());
		assertEquals("wrong value", expect, e.eval(expr), 0.001);
	}
	*/
}
