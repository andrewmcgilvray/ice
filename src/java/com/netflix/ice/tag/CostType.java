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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.processor.LineItem.BillType;
import com.netflix.ice.processor.LineItemType;

public class CostType extends Tag {
    private static final long serialVersionUID = 1L;
    private int seq;
    private static Map<String, CostType> costTypeByName = Maps.newHashMap();

    private static int sequence = 0;
    public static final CostType savings = new CostType("Savings");
    public static final CostType recurring = new CostType("Recurring");
    public static final CostType amortization = new CostType("Amortization");
    public static final CostType subscription = new CostType("Subscription");
    public static final CostType subscriptionTax = new CostType("SubscriptionTax");
    public static final CostType credit = new CostType("Credit");
    public static final CostType refund = new CostType("Refund");
    public static final CostType refundTax = new CostType("RefundTax");
    public static final CostType tax = new CostType("Tax");
    public static final CostType other = new CostType("Other");

    private static List<CostType> defaultCostTypes = Lists
            .newArrayList(new CostType[] { recurring, amortization, credit, tax });

    private CostType(String name) {
        super(name);
        seq = sequence++;
        costTypeByName.put(name, this);
    }

    public static CostType get(String costType) {
        return costTypeByName.get(costType);
    }

    public static CostType get(BillType billType, LineItemType lineItemType) {
        switch (billType) {
        case Anniversary:
            switch (lineItemType) {
            case Fee: // Anniversary/Fee is how Registrar Domain renewals appear. Treat as recurring
            case Usage:
            case DiscountedUsage:
            case SavingsPlanCoveredUsage:
                return recurring;

            case RIFee:
            case SavingsPlanRecurringFee:
                return amortization;

            case Credit:
                return credit;

            case Tax:
                return tax;

            default:
                return null;
            }

        case Purchase:
            switch (lineItemType) {
            case Fee:
                return subscription;
            case Tax:
                return subscriptionTax;
            default:
                return null;
            }

        case Refund:
            switch (lineItemType) {
            case Refund:
                return refund;
            case Tax:
                return refundTax;
            default:
                return null;
            }

        default:
            return null;
        }
    }

    public static List<CostType> getCostTypes(List<String> names) {
        List<CostType> result = Lists.newArrayList();
        for (String name : names) {
            CostType ct = costTypeByName.get(name);
            if (ct == null)
                continue;

            result.add(ct);
        }
        return result;
    }

    public static Collection<CostType> getDefaults() {
        return Collections.unmodifiableCollection(defaultCostTypes);
    }

    @Override
    public int compareTo(Tag t) {
        if (t instanceof CostType) {
            CostType o = (CostType) t;
            return this.seq - o.seq;
        } else {
            return super.compareTo(t);
        }
    }

}
