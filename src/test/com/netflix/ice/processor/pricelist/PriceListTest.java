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
package com.netflix.ice.processor.pricelist;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.netflix.ice.processor.pricelist.PriceList.Product;
import com.netflix.ice.processor.pricelist.PriceList.Rate;
import com.netflix.ice.processor.pricelist.PriceList.Term;
import com.netflix.ice.processor.pricelist.PriceList.Terms;
import com.netflix.ice.processor.pricelist.PriceList.Product.Attributes;
import com.netflix.ice.tag.Region;

public class PriceListTest {
	private static final String resourceDir = "src/test/resources/";

	@Test
	public void testEc2PriceList() throws MalformedURLException, IOException {
		File testFile = new File(resourceDir + "PriceListTestData.json");
		InputStream stream = new FileInputStream(testFile);
        PriceList priceList = new PriceList(stream);
        stream.close();
        
        // Look up prices for t2.small instance in us-east-1 region
        Map<String, Product> products = priceList.getProducts();
        for (String sku: products.keySet()) {
        	Product p = products.get(sku);
        	if (!p.productFamily.equals("Compute Instance") ||
        			!StringUtils.equals(p.getAttribute(Attributes.location), Region.US_EAST_1.priceListName) ||
        			!StringUtils.equals(p.getAttribute(Attributes.instanceType), "t2.small") ||
        			!StringUtils.equals(p.getAttribute(Attributes.operatingSystem), "Linux") ||
        			!StringUtils.equals(p.getAttribute(Attributes.tenancy), "Shared")) {
        		continue;
        	}
    		Terms terms = priceList.getTerms();
    		
    		// First check the OnDemand price
    		Map<String, Term> offerTerms = terms.OnDemand.get(p.sku);
    		// Get what should be the only term
    		Term term = offerTerms.entrySet().iterator().next().getValue();
    		// Get what should be the only rate
    		Rate rate = term.priceDimensions.entrySet().iterator().next().getValue();
			Double pricePerUnitUSD = Double.parseDouble(rate.pricePerUnit.get("USD"));
			double expected = 0.023;
			assertEquals("OnDemand price wrong, got " + pricePerUnitUSD + ", expected " + expected, pricePerUnitUSD, expected, 0.001);
			
			// Now check reserved instance prices
			offerTerms = terms.Reserved.get(p.sku);
			// We should have terms for permutations of:
			//	LeaseContractLength(1yr, 3yr)
			//	PurchaseOption(All Upfront,Partial Upfront, No Upfront)
			//	OfferingClass(standard, convertible)
			verifyReservedPrices(offerTerms, "1yr", "No Upfront", 		"standard", 0, 0.0168);
			verifyReservedPrices(offerTerms, "1yr", "Partial Upfront", 	"standard", 70, 0.008);
			verifyReservedPrices(offerTerms, "1yr", "All Upfront", 		"standard", 137, 0);
			verifyReservedPrices(offerTerms, "3yr", "No Upfront", 		"standard", 0, 0.012);
			verifyReservedPrices(offerTerms, "3yr", "Partial Upfront",	"standard", 145, 0.006);
			verifyReservedPrices(offerTerms, "3yr", "All Upfront",		"standard", 272, 0);
			verifyReservedPrices(offerTerms, "3yr", "No Upfront",		"convertible", 0, 0.0139);
			verifyReservedPrices(offerTerms, "3yr", "Partial Upfront",	"convertible", 169, 0.0064);
			verifyReservedPrices(offerTerms, "3yr", "All Upfront",		"convertible", 332, 0);
			
			break;
        }
	}
	
	@Test
	public void testRdsPriceList() throws MalformedURLException, IOException {
		File testFile = new File(resourceDir + "RdsPriceListTestData.json");
		InputStream stream = new FileInputStream(testFile);
        PriceList priceList = new PriceList(stream);
        stream.close();
        
        // Look up prices for m4.4xlarge instance in us-east-1 region
        Map<String, Product> products = priceList.getProducts();
        for (String sku: products.keySet()) {
        	Product p = products.get(sku);
        	if (!p.productFamily.equals("Compute Instance") ||
        			!StringUtils.equals(p.getAttribute(Attributes.location), Region.US_EAST_1.priceListName) ||
        			!StringUtils.equals(p.getAttribute(Attributes.instanceType), "db.m4.4xlarge") ||
        			!StringUtils.equals(p.getAttribute(Attributes.databaseEngine), "MySQL")) {
        		continue;
        	}
    		Terms terms = priceList.getTerms();
    		
    		// First check the OnDemand price
    		Map<String, Term> offerTerms = terms.OnDemand.get(p.sku);
    		// Get what should be the only term
    		Term term = offerTerms.entrySet().iterator().next().getValue();
    		// Get what should be the only rate
    		Rate rate = term.priceDimensions.entrySet().iterator().next().getValue();
			Double pricePerUnitUSD = Double.parseDouble(rate.pricePerUnit.get("USD"));
			boolean singleAz = StringUtils.equals(p.getAttribute(Attributes.deploymentOption), "Single-AZ");
			double expected = singleAz ? 1.401 : 2.802;
			assertEquals("OnDemand price wrong, got " + pricePerUnitUSD + ", expected " + expected, pricePerUnitUSD, expected, 0.001);
			
			// Now check reserved instance prices
			offerTerms = terms.Reserved.get(p.sku);
			// We should have terms for permutations of:
			//	LeaseContractLength(1yr, 3yr)
			//	PurchaseOption(All Upfront,Partial Upfront, No Upfront)
			//	OfferingClass(standard)
			verifyReservedPrices(offerTerms, "1yr", "No Upfront", 		"standard", 0, singleAz ? 0.963 : 1.926);
			verifyReservedPrices(offerTerms, "1yr", "Partial Upfront", 	"standard", singleAz ? 2593 : 70, singleAz ? 0.527 : 1.054);
			verifyReservedPrices(offerTerms, "1yr", "All Upfront", 		"standard", singleAz ? 7064 : 14128, 0);
			verifyReservedPrices(offerTerms, "3yr", "Partial Upfront",	"standard", singleAz ? 5256 : 10512, singleAz ? 0.356 : 0.712);
			verifyReservedPrices(offerTerms, "3yr", "All Upfront",		"standard", singleAz ? 13735 : 27470, 0);
			
			break;
        }
	}
	
	private void verifyReservedPrices(Map<String, Term> offerTerms, String leaseContractLength, String purchaseOption, String offeringClass, double fixed, double hourly) {
		String permutation = leaseContractLength + ", " + purchaseOption + ", " + offeringClass;
		// Scan the terms looking for a match
		for (String skuOfferCode: offerTerms.keySet()) {
			Term term = offerTerms.get(skuOfferCode);
			if (!StringUtils.equals(term.termAttributes.LeaseContractLength, leaseContractLength) ||
					!StringUtils.equals(term.termAttributes.PurchaseOption, purchaseOption) ||
					!StringUtils.equals(term.termAttributes.OfferingClass, offeringClass)) {
				continue;
			}
			Double quantity = null;
			Double hrs = null;
			for (String skuOfferCodeRateCode: term.priceDimensions.keySet()) {
				Rate rate = term.priceDimensions.get(skuOfferCodeRateCode);
				if (rate.unit.equals("Quantity"))
					quantity = Double.parseDouble(rate.pricePerUnit.get("USD"));
				if (rate.unit.equals("Hrs"))
					hrs = Double.parseDouble(rate.pricePerUnit.get("USD"));
			}
			if (!purchaseOption.equals("No Upfront")) {
				assertNotEquals("No fixed price dimension for " + permutation, quantity, null);
				assertEquals("Wrong fixed price for " + permutation + ": expected " + fixed + ", got " + quantity, quantity, fixed, 0.0001);
			}
			assertNotEquals("No hourly price dimension " + permutation, hrs, null);
			assertEquals("Wrong hourly price for " + permutation + ": expected " + hourly + ", got " + hrs, hrs, hourly, 0.0001);
			
			return;
		}
		fail("Didn't find term for " + permutation);
	}
}
