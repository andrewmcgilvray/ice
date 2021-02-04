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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netflix.ice.processor.CostAndUsageReport.Manifest;

public class CostAndUsageReportTest {
    private static final String resourcesDir = "src/test/resources";

	@Test
	public void testReadManifest() {
		File file = new File(resourcesDir + "/manifestTest.json");
        Reader reader;
        
        Manifest manifest = null;
        
        try {
			reader = new BufferedReader(new FileReader(file));
	        Gson gson = new GsonBuilder().create();
		    manifest = gson.fromJson(reader, Manifest.class);
		} catch (FileNotFoundException e) {
			fail("Failed to parse manifest file" + e);
		}
        
        assertEquals("assembly ID is wrong", "abcdefgh-e98d-48d6-86b4-29967358fe6c", manifest.assemblyId);
	}
	
	@Test
	public void testGetCategoryHeader() {
		File file = new File(resourcesDir + "/manifestTest.json");
        Reader reader;
        Manifest manifest;
        try {
			reader = new BufferedReader(new FileReader(file));
	        Gson gson = new GsonBuilder().create();
		    manifest = gson.fromJson(reader, Manifest.class);
		} catch (FileNotFoundException e) {
			fail("Failed to parse manifest file" + e);
			return;
		}
        
        String[] expect = new String[]{
                "resourceTags/aws:createdBy",
                "resourceTags/user:Environment",
                "resourceTags/user:Email",
                "resourceTags/user:BusinessUnit",
                "resourceTags/user:Name",
                "resourceTags/user:Project",
                "resourceTags/user:Product",
        };
        String[] got = manifest.getCategoryHeader("resourceTags");
        assertEquals("Incorrect header length", expect.length, got.length);
        for (int i = 0; i < expect.length; i++)
        	assertTrue("Incorrect header at position " + i, expect[i].equals(got[i]));
        
	}
	
	@Test
	public void testISOTimeFormatter() {
		Long millis = LineItem.amazonBillingDateFormatISO.parseMillis("2017-07-01T00:00:00Z");
		Long expect = 1498867200000L;
		assertEquals("Error parsing time in ISO format", millis, expect);
	}
}
