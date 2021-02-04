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


/**
 * Interface to process each line item in billing file.
 */
public interface LineItemProcessor {
    Result process(
    		String fileName,
    		long reportMilli,
    		boolean processAll,
    		String root,
    		LineItem lineItem, 
    		CostAndUsageData costAndUsageData,
    		Instances instances,
    		double edpDiscount);

    public static enum Result {
        delay,
        ignore,
        hourly,
        hourlyTruncate, // delay and throw away items after the data end time.
        monthly,
        daily
    }
}

