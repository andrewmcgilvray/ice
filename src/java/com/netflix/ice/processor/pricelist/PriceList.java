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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * PriceList holds the data imported from an AWS price list:
 *   http://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/price-changes.html
 */
public class PriceList {
	Root priceList;

	public PriceList(InputStream in) throws IOException {
        Reader reader = null;
		reader = new BufferedReader(new InputStreamReader(in));
        Gson gson = new GsonBuilder().create();
	    this.priceList = gson.fromJson(reader, Root.class);
		reader.close();
	}
	
	public Map<String, Product> getProducts() {
		return priceList.products;
	}
	
	public Terms getTerms() {
		return priceList.terms;
	}
	
	public class Root {
		Index.Header header;
		String version;
		Map<String, Product> products;
		Terms terms;
	}
	
	static public class Product {
		String sku;
		String productFamily;
		private Map<String, String> attributes;
		
		public enum Attributes {
	        servicecode,
	        location,
	        locationType,
	        instanceType,
	        currentGeneration,
	        instanceFamily,
	        instanceTypeFamily,
	        vcpu,
	        physicalProcessor,
	        clockSpeed,
	        memory,
	        memoryGib,
	        storage,
	        io,
	        networkPerformance,
	        processorArchitecture,
	        tenancy,
	        engineCode,
	        operatingSystem,
	        databaseEngine,
	        databaseEdition,
	        licenseModel,
	        usagetype,
	        operation,
	        capacitystatus,
	        dedicatedEbsThroughput,
	        ecu,
	        usageFamily,
	        enhancedNetworkingSupported,
	        instancesku,
	        intelAvxAvailable,
	        intelAvx2Available,
	        intelTurboAvailable,
	        normalizationSizeFactor,
	        preInstalledSw,
	        processorFeatures,
	        deploymentOption,
	        servicename;
		}

		public String getAttribute(Attributes a) {
			String v = attributes.get(a.name());
			return v == null ? "" : v;
		}
		
		public boolean hasAttribute(Attributes a) {
			return attributes.containsKey(a.name());
		}

		@Override
		public String toString() {
			List<String> attrs = Lists.newArrayList();
        	for (String a: attributes.keySet()) {
        		attrs.add(a + ":" + attributes.get(a));
        	}
            return "{" + String.join(",", attrs) + "}";
		}
	}
	
	public class Terms {
		Map<String, Map<String, Term>> OnDemand; // Key is SKU, second Key is SKU.OfferTermCode
		Map<String, Map<String, Term>> Reserved; // Key is SKU, second Key is SKU.OfferTermCode
	}
	
	public class Term {
		String offerTermCode;
		String sku;
		String effectiveDate;
		Map<String, Rate> priceDimensions;
		TermAttributes termAttributes;
	}
		
	public class Rate {
		String rateCode;
		String description;
		String beginRange;
		String endRange;
		String unit;
		Map<String, String> pricePerUnit; // Key is currency e.g. "USD"
		String[] appliesTo;
	}
	
	public class TermAttributes {
		String LeaseContractLength; // e.g. "1yr" or "3yr"
		String OfferingClass; // e.g. standard, convertible
		String PurchaseOption; // e.g. "All Upfront", "No Upfront", "Partial Upfront"
	}

}
