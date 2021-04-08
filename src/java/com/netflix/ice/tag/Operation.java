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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.PurchaseOption;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class Operation extends Tag {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2L;
	
	protected final int seq;
	protected final Category category;
	protected final Identity identity;
	
	public static class Identity {
		public enum Value {
			Borrowed(1 << 0),
			Lent(1 << 1);
			
			private final int bit;
			
			Value(int bit) {
				this.bit = bit;
			}
			
			public int getBit() {
				return bit;
			}
		}
		
		// Bitfield for fast comparison of multiple values at once
		private final int bitfield;
		
		public final boolean isBorrowed;
		public final boolean isLent;
		
		Identity(Category category) {
			isBorrowed = category == Category.Borrowed || category == Category.BorrowedAmortized;
			isLent = category == Category.Lent || category == Category.LentAmortized;
			
			int bf = isBorrowed ? Value.Borrowed.getBit() : 0;
			bf |= isLent ? Value.Lent.getBit() : 0;
			
			bitfield = bf;			
		}
		
		public static int getIdentitySet(Collection<Value> values) {
			int bitfield = 0;
			
			for (Value v: values) {
				bitfield |= v.getBit();
			}
			return bitfield;
		}
		
		public boolean isOneOf(int bits) {
			return (bits & bitfield) != 0;
		}
	}
	
    private Operation(String name) {
        super(name);
        this.seq = Integer.MAX_VALUE;
        this.category = Category.None;
        this.identity = new Identity(this.category);
    }
    
    private Operation(String name, int seq, Category category) {
        super(name);
        this.seq = seq;
        this.category = category;
        this.identity = new Identity(this.category);
    }
    
    private static ConcurrentMap<String, Operation> operations = Maps.newConcurrentMap();
    private static List<Operation> reservationOperations = Lists.newArrayList();
    private static List<Operation> savingsPlanOperations = Lists.newArrayList();
    private static List<Operation> reservationOperationsWithBorrowed = null;
    private static List<Operation> reservationOperationsWithLent = null;
    private static List<Operation> savingsPlanOperationsWithBorrowed = null;
    private static List<Operation> savingsPlanOperationsWithLent = null;
    
    private enum Category {
    	None,
    	Savings,
    	Used,
    	Bonus,
    	Borrowed,
    	Lent,
    	Amortized,
    	BorrowedAmortized,
    	LentAmortized,
    	Unused,
    	UnusedAmortized,
    	Credit,
    	Refund,
    	Tax;
    }
    
    private static int sequence = 0;
    public static final ReservationOperation spotInstanceSavings = new ReservationOperation("Savings - Spot", Category.Savings);
    public static final ReservationOperation spotInstances = new ReservationOperation("Spot Instances", Category.None);
    public static final ReservationOperation ondemandInstances = new ReservationOperation("On-Demand Instances", Category.None);

    public static final SavingsPlanOperation lambaInvoke = new SavingsPlanOperation("Invoke", Category.None);
    public static final SavingsPlanOperation fargateTask = new SavingsPlanOperation("FargateTask", Category.None);
    public static final SavingsPlanOperation fargatePod = new SavingsPlanOperation("FargatePod", Category.None);

    public static final ReservationOperation savingsNoUpfront = new ReservationOperation(Category.Savings, PurchaseOption.NoUpfront);
    public static final ReservationOperation reservedInstancesNoUpfront = new ReservationOperation(Category.Used, PurchaseOption.NoUpfront);
    public static final ReservationOperation bonusReservedInstancesNoUpfront = new ReservationOperation(Category.Bonus, PurchaseOption.NoUpfront);
    public static final ReservationOperation borrowedInstancesNoUpfront = new ReservationOperation(Category.Borrowed, PurchaseOption.NoUpfront);
    public static final ReservationOperation lentInstancesNoUpfront = new ReservationOperation(Category.Lent, PurchaseOption.NoUpfront);
    public static final ReservationOperation unusedInstancesNoUpfront = new ReservationOperation(Category.Unused, PurchaseOption.NoUpfront);

    public static final ReservationOperation savingsPartialUpfront = new ReservationOperation(Category.Savings, PurchaseOption.PartialUpfront);
    public static final ReservationOperation reservedInstancesPartialUpfront = new ReservationOperation(Category.Used, PurchaseOption.PartialUpfront);
    public static final ReservationOperation bonusReservedInstancesPartialUpfront = new ReservationOperation(Category.Bonus, PurchaseOption.PartialUpfront);
    public static final ReservationOperation borrowedInstancesPartialUpfront = new ReservationOperation(Category.Borrowed, PurchaseOption.PartialUpfront);
    public static final ReservationOperation lentInstancesPartialUpfront = new ReservationOperation(Category.Lent, PurchaseOption.PartialUpfront);
    public static final ReservationOperation amortizedPartialUpfront = new ReservationOperation(Category.Amortized, PurchaseOption.PartialUpfront);    
    public static final ReservationOperation borrowedAmortizedPartialUpfront = new ReservationOperation(Category.BorrowedAmortized, PurchaseOption.PartialUpfront);    
    public static final ReservationOperation lentAmortizedPartialUpfront = new ReservationOperation(Category.LentAmortized, PurchaseOption.PartialUpfront);    
    public static final ReservationOperation unusedInstancesPartialUpfront = new ReservationOperation(Category.Unused, PurchaseOption.PartialUpfront);
    public static final ReservationOperation unusedAmortizedPartialUpfront = new ReservationOperation(Category.UnusedAmortized, PurchaseOption.PartialUpfront);

    public static final ReservationOperation savingsAllUpfront = new ReservationOperation(Category.Savings, PurchaseOption.AllUpfront);
    public static final ReservationOperation reservedInstancesAllUpfront = new ReservationOperation(Category.Used, PurchaseOption.AllUpfront);
    public static final ReservationOperation bonusReservedInstancesAllUpfront = new ReservationOperation(Category.Bonus, PurchaseOption.AllUpfront);
    public static final ReservationOperation borrowedInstancesAllUpfront = new ReservationOperation(Category.Borrowed, PurchaseOption.AllUpfront);
    public static final ReservationOperation lentInstancesAllUpfront = new ReservationOperation(Category.Lent, PurchaseOption.AllUpfront);
    public static final ReservationOperation amortizedAllUpfront = new ReservationOperation(Category.Amortized, PurchaseOption.AllUpfront);
    public static final ReservationOperation borrowedAmortizedAllUpfront = new ReservationOperation(Category.BorrowedAmortized, PurchaseOption.AllUpfront);
    public static final ReservationOperation lentAmortizedAllUpfront = new ReservationOperation(Category.LentAmortized, PurchaseOption.AllUpfront);
    public static final ReservationOperation unusedInstancesAllUpfront = new ReservationOperation(Category.Unused, PurchaseOption.AllUpfront);
    public static final ReservationOperation unusedAmortizedAllUpfront = new ReservationOperation(Category.UnusedAmortized, PurchaseOption.AllUpfront);

    // Legacy Heavy/Medium/Light Utilization types used only by ElastiCache. There is account borrowing at least for ElastiCache.
    public static final ReservationOperation savingsHeavy = new ReservationOperation(Category.Savings, PurchaseOption.Heavy);
    public static final ReservationOperation reservedInstancesHeavy = new ReservationOperation(Category.Used, PurchaseOption.Heavy);
    public static final ReservationOperation bonusReservedInstancesHeavy = new ReservationOperation(Category.Bonus, PurchaseOption.Heavy);
    public static final ReservationOperation borrowedInstancesHeavy = new ReservationOperation(Category.Borrowed, PurchaseOption.Heavy);
    public static final ReservationOperation lentInstancesHeavy = new ReservationOperation(Category.Lent, PurchaseOption.Heavy);
    public static final ReservationOperation amortizedHeavy = new ReservationOperation(Category.Amortized, PurchaseOption.Heavy);
    public static final ReservationOperation borrowedAmortizedHeavy = new ReservationOperation(Category.BorrowedAmortized, PurchaseOption.Heavy);
    public static final ReservationOperation lentAmortizedHeavy = new ReservationOperation(Category.LentAmortized, PurchaseOption.Heavy);
    public static final ReservationOperation unusedInstancesHeavy = new ReservationOperation(Category.Unused, PurchaseOption.Heavy);
    public static final ReservationOperation unusedAmortizedHeavy = new ReservationOperation(Category.UnusedAmortized, PurchaseOption.Heavy);

    public static final ReservationOperation savingsMedium = new ReservationOperation(Category.Savings, PurchaseOption.Medium);
    public static final ReservationOperation reservedInstancesMedium = new ReservationOperation(Category.Used, PurchaseOption.Medium);
    public static final ReservationOperation bonusReservedInstancesMedium = new ReservationOperation(Category.Bonus, PurchaseOption.Medium);
    public static final ReservationOperation borrowedInstancesMedium = new ReservationOperation(Category.Borrowed, PurchaseOption.Medium);
    public static final ReservationOperation lentInstancesMedium = new ReservationOperation(Category.Lent, PurchaseOption.Medium);
    public static final ReservationOperation amortizedMedium = new ReservationOperation(Category.Amortized, PurchaseOption.Medium);
    public static final ReservationOperation borrowedAmortizedMedium = new ReservationOperation(Category.BorrowedAmortized, PurchaseOption.Medium);
    public static final ReservationOperation lentAmortizedMedium = new ReservationOperation(Category.LentAmortized, PurchaseOption.Medium);
    public static final ReservationOperation unusedInstancesMedium = new ReservationOperation(Category.Unused, PurchaseOption.Medium);
    public static final ReservationOperation unusedAmortizedMedium = new ReservationOperation(Category.UnusedAmortized, PurchaseOption.Medium);

    public static final ReservationOperation savingsLight = new ReservationOperation(Category.Savings, PurchaseOption.Light);
    public static final ReservationOperation reservedInstancesLight = new ReservationOperation(Category.Used, PurchaseOption.Light);
    public static final ReservationOperation bonusReservedInstancesLight = new ReservationOperation(Category.Bonus, PurchaseOption.Light);
    public static final ReservationOperation borrowedInstancesLight = new ReservationOperation(Category.Borrowed, PurchaseOption.Light);
    public static final ReservationOperation lentInstancesLight = new ReservationOperation(Category.Lent, PurchaseOption.Light);
    public static final ReservationOperation amortizedLight = new ReservationOperation(Category.Amortized, PurchaseOption.Light);
    public static final ReservationOperation borrowedAmortizedLight = new ReservationOperation(Category.BorrowedAmortized, PurchaseOption.Light);
    public static final ReservationOperation lentAmortizedLight = new ReservationOperation(Category.LentAmortized, PurchaseOption.Light);
    public static final ReservationOperation unusedInstancesLight = new ReservationOperation(Category.Unused, PurchaseOption.Light);
    public static final ReservationOperation unusedAmortizedLight = new ReservationOperation(Category.UnusedAmortized, PurchaseOption.Light);

    public static final ReservationOperation reservedInstancesCredits = new ReservationOperation("Credit - RI", Category.Credit);
    public static final ReservationOperation reservedInstancesRefunds = new ReservationOperation("Refund - RI", Category.Refund);
    
    public static final SavingsPlanOperation savingsPlanSavingsNoUpfront = new SavingsPlanOperation(Category.Savings, PurchaseOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanUsedNoUpfront = new SavingsPlanOperation(Category.Used, PurchaseOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanBonusNoUpfront = new SavingsPlanOperation(Category.Bonus, PurchaseOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanBorrowedNoUpfront = new SavingsPlanOperation(Category.Borrowed, PurchaseOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanLentNoUpfront = new SavingsPlanOperation(Category.Lent, PurchaseOption.NoUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedNoUpfront = new SavingsPlanOperation(Category.Unused, PurchaseOption.NoUpfront);

    public static final SavingsPlanOperation savingsPlanSavingsPartialUpfront = new SavingsPlanOperation(Category.Savings, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanUsedPartialUpfront = new SavingsPlanOperation(Category.Used, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanBonusPartialUpfront = new SavingsPlanOperation(Category.Bonus, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanBorrowedPartialUpfront = new SavingsPlanOperation(Category.Borrowed, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanLentPartialUpfront = new SavingsPlanOperation(Category.Lent, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanAmortizedPartialUpfront = new SavingsPlanOperation(Category.Amortized, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanBorrowedAmortizedPartialUpfront = new SavingsPlanOperation(Category.BorrowedAmortized, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanLentAmortizedPartialUpfront = new SavingsPlanOperation(Category.LentAmortized, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedPartialUpfront = new SavingsPlanOperation(Category.Unused, PurchaseOption.PartialUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedAmortizedPartialUpfront = new SavingsPlanOperation(Category.UnusedAmortized, PurchaseOption.PartialUpfront);

    public static final SavingsPlanOperation savingsPlanSavingsAllUpfront = new SavingsPlanOperation(Category.Savings, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanUsedAllUpfront = new SavingsPlanOperation(Category.Used, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanBonusAllUpfront = new SavingsPlanOperation(Category.Bonus, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanBorrowedAllUpfront = new SavingsPlanOperation(Category.Borrowed, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanLentAllUpfront = new SavingsPlanOperation(Category.Lent, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanAmortizedAllUpfront = new SavingsPlanOperation(Category.Amortized, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanBorrowedAmortizedAllUpfront = new SavingsPlanOperation(Category.BorrowedAmortized, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanLentAmortizedAllUpfront = new SavingsPlanOperation(Category.LentAmortized, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedAllUpfront = new SavingsPlanOperation(Category.Unused, PurchaseOption.AllUpfront);
    public static final SavingsPlanOperation savingsPlanUnusedAmortizedAllUpfront = new SavingsPlanOperation(Category.UnusedAmortized, PurchaseOption.AllUpfront);
    
    public static final Operation[] lentOperations = new Operation[] {
    	lentInstancesNoUpfront,
    	lentInstancesPartialUpfront,
    	lentInstancesAllUpfront,
    	lentInstancesHeavy,
    	lentInstancesMedium,
    	lentInstancesLight,
    	lentAmortizedPartialUpfront,
    	lentAmortizedAllUpfront,
    	lentAmortizedHeavy,
    	lentAmortizedMedium,
    	lentAmortizedLight,
    	savingsPlanLentNoUpfront,
    	savingsPlanLentPartialUpfront,
    	savingsPlanLentAllUpfront,
    	savingsPlanLentAmortizedPartialUpfront,
    	savingsPlanLentAmortizedAllUpfront,
    };
    
    public static final Operation[] borrowedOperations = new Operation[] {
    	borrowedInstancesNoUpfront,
    	borrowedInstancesPartialUpfront,
    	borrowedInstancesAllUpfront,
    	borrowedInstancesHeavy,
    	borrowedInstancesMedium,
    	borrowedInstancesLight,
    	borrowedAmortizedPartialUpfront,
    	borrowedAmortizedAllUpfront,
    	borrowedAmortizedHeavy,
    	borrowedAmortizedMedium,
    	borrowedAmortizedLight,
    	savingsPlanBorrowedNoUpfront,
    	savingsPlanBorrowedPartialUpfront,
    	savingsPlanBorrowedAllUpfront,
    	savingsPlanBorrowedAmortizedPartialUpfront,
    	savingsPlanBorrowedAmortizedAllUpfront,
    };
    
    public static final Operation[] savingsOperations = new Operation[] {
    	spotInstanceSavings,
    	savingsAllUpfront,
    	savingsNoUpfront,
    	savingsPartialUpfront,
    	savingsHeavy,
    	savingsMedium,
    	savingsLight,
    	savingsPlanSavingsNoUpfront,
    	savingsPlanSavingsPartialUpfront,
    	savingsPlanSavingsAllUpfront,
    };
    
    public static final Operation[] amortizationOperations = new Operation[] {
    	amortizedPartialUpfront,
    	amortizedAllUpfront,
    	amortizedHeavy,
    	amortizedMedium,
    	amortizedLight,
    	borrowedAmortizedPartialUpfront,
    	borrowedAmortizedAllUpfront,
    	borrowedAmortizedHeavy,
    	borrowedAmortizedMedium,
    	borrowedAmortizedLight,
    	lentAmortizedPartialUpfront,
    	lentAmortizedAllUpfront,
    	lentAmortizedHeavy,
    	lentAmortizedMedium,
    	lentAmortizedLight,
    	unusedAmortizedPartialUpfront,
    	unusedAmortizedAllUpfront,
    	unusedAmortizedHeavy,
    	unusedAmortizedMedium,
    	unusedAmortizedLight,
    	savingsPlanAmortizedPartialUpfront,
    	savingsPlanAmortizedAllUpfront,
    	savingsPlanBorrowedAmortizedPartialUpfront,
    	savingsPlanBorrowedAmortizedAllUpfront,
    	savingsPlanLentAmortizedPartialUpfront,
    	savingsPlanLentAmortizedAllUpfront,
    	savingsPlanUnusedAmortizedPartialUpfront,
    	savingsPlanUnusedAmortizedAllUpfront,
    };

    public boolean isBonus() {
    	return category == Category.Bonus;
    }
    public boolean isLent() {
    	return identity.isLent;
    }
    public boolean isBorrowed() {
    	return identity.isBorrowed;
    }
    public boolean isUsed() {
    	return category == Category.Used;
    }
    public boolean isUnused() {
    	return category == Category.Unused || category == Category.UnusedAmortized;
    }
    public boolean isOnDemand() {
    	return this == ondemandInstances;
    }
    public boolean isSpot() {
    	return this == spotInstances;
    }
    
    public boolean isOneOf(int bitSet) {
    	return identity.isOneOf(bitSet);
    }
    
    public static ReservationOperation getReservedInstances(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return reservedInstancesAllUpfront;
            case NoUpfront: return reservedInstancesNoUpfront;
            case PartialUpfront: return reservedInstancesPartialUpfront;
            case Heavy: return reservedInstancesHeavy;
            case Medium: return reservedInstancesMedium;
            case Light: return reservedInstancesLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getBonusReservedInstances(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return bonusReservedInstancesAllUpfront;
            case NoUpfront: return bonusReservedInstancesNoUpfront;
            case PartialUpfront: return bonusReservedInstancesPartialUpfront;
            case Heavy: return bonusReservedInstancesHeavy;
            case Medium: return bonusReservedInstancesMedium;
            case Light: return bonusReservedInstancesLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getBorrowedInstances(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return borrowedInstancesAllUpfront;
            case NoUpfront: return borrowedInstancesNoUpfront;
            case PartialUpfront: return borrowedInstancesPartialUpfront;
            case Heavy: return borrowedInstancesHeavy;
            case Medium: return borrowedInstancesMedium;
            case Light: return borrowedInstancesLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getLentInstances(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return lentInstancesAllUpfront;
            case NoUpfront: return lentInstancesNoUpfront;
            case PartialUpfront: return lentInstancesPartialUpfront;
            case Heavy: return lentInstancesHeavy;
            case Medium: return lentInstancesMedium;
            case Light: return lentInstancesLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getUnusedInstances(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return unusedInstancesAllUpfront;
            case NoUpfront: return unusedInstancesNoUpfront;
            case PartialUpfront: return unusedInstancesPartialUpfront;
            case Heavy: return unusedInstancesHeavy;
            case Medium: return unusedInstancesMedium;
            case Light: return unusedInstancesLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getAmortized(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return amortizedAllUpfront;
            case PartialUpfront: return amortizedPartialUpfront;
            case Heavy: return amortizedHeavy;
            case Medium: return amortizedMedium;
            case Light: return amortizedLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getBorrowedAmortized(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return borrowedAmortizedAllUpfront;
            case PartialUpfront: return borrowedAmortizedPartialUpfront;
            case Heavy: return borrowedAmortizedHeavy;
            case Medium: return borrowedAmortizedMedium;
            case Light: return borrowedAmortizedLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getLentAmortized(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return lentAmortizedAllUpfront;
            case PartialUpfront: return lentAmortizedPartialUpfront;
            case Heavy: return lentAmortizedHeavy;
            case Medium: return lentAmortizedMedium;
            case Light: return lentAmortizedLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getUnusedAmortized(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return unusedAmortizedAllUpfront;
            case PartialUpfront: return unusedAmortizedPartialUpfront;
            case Heavy: return unusedAmortizedHeavy;
            case Medium: return unusedAmortizedMedium;
            case Light: return unusedAmortizedLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }

    public static ReservationOperation getSavings(PurchaseOption purchaseOption) {
        switch (purchaseOption) {
            case AllUpfront: return savingsAllUpfront;
            case NoUpfront: return savingsNoUpfront;
            case PartialUpfront: return savingsPartialUpfront;
            case Heavy: return savingsHeavy;
            case Medium: return savingsMedium;
            case Light: return savingsLight;
            default: throw new RuntimeException("Unknown PurchaseOption " + purchaseOption);
        }
    }
    
    public static Operation getOperation(String name) {
    	if (name.isEmpty()) {
    		// Support entries don't have an operation field
    		name = "None";
    	}
        Operation operation = operations.get(name);
        if (operation == null) {
            operations.putIfAbsent(name, new Operation(name));
            operation = operations.get(name);
        }

        return operation;
    }
    
    // Used by the reader when deserializing operation strings.
    public static Operation deserializeOperation(String name) {
    	return getOperation(name);
    }

    public static List<Operation> getOperations(List<String> names) {
        List<Operation> result = Lists.newArrayList();
        for (String name: names) {
        	Operation op = operations.get(name);
        	if (op == null)
        		continue;
        	
            result.add(op);
        }
        return result;
    }
    
    public static List<Operation> exclude(List<Operation> operations, List<Identity.Value> exclude) {
    	if (exclude == null || exclude.isEmpty())
    		return operations;
    	
        int excludeBitSet = Identity.getIdentitySet(exclude);
    	List<Operation> result = Lists.newArrayList();
    	for (Operation op: operations) {
    		if (op.isOneOf(excludeBitSet))
    			continue;
    		result.add(op);
    	}
    	return result;
    }

	public static List<Operation.Identity.Value> exclude(boolean showLent) {
		// Figure out what operations we should exclude
		List<Operation.Identity.Value> result = Lists.newArrayList();
		result.add(showLent ? Operation.Identity.Value.Borrowed : Operation.Identity.Value.Lent);
		return result;
	}

    public static List<Operation> getReservationOperations(boolean showLent) {
    	if (showLent) {
    		if (reservationOperationsWithLent == null) {
    			List<Operation> ops = Lists.newArrayList(reservationOperations);
    			List<Operation> borrowed = Lists.newArrayList(borrowedOperations);
    			ops.removeAll(borrowed);
    			reservationOperationsWithLent = ops;
    		}
    		return reservationOperationsWithLent;
    	}
    	else {
	    	if (reservationOperationsWithBorrowed == null) {
	    		List<Operation> ops = Lists.newArrayList(reservationOperations);
				List<Operation> lent = Lists.newArrayList(lentOperations);
				ops.removeAll(lent);
				reservationOperationsWithBorrowed = ops;
			}
	    	return reservationOperationsWithBorrowed;
    	}
    }

    public static class ReservationOperation extends Operation {
		private static final long serialVersionUID = 1L;
		private PurchaseOption purchaseOption = null;

		private ReservationOperation(String name, PurchaseOption purchaseOption) {
            super(name, sequence++, Category.None);
            this.purchaseOption = purchaseOption;
            operations.put(name, this);
            reservationOperations.add(this);
        }
		
		private ReservationOperation(String name, Category category) {
			super(name, sequence++, category);
			this.purchaseOption = null;
			operations.put(name,  this);
			reservationOperations.add(this);
		}
		
		private ReservationOperation(Category category, PurchaseOption purchaseOption) {
            super(category.name() + " RIs - " + purchaseOption.name, sequence++, category);
            this.purchaseOption = purchaseOption;
            operations.put(name, this);
            reservationOperations.add(this);
        }
		
		public PurchaseOption getPurchaseOption() {
			return purchaseOption;
		}
    }

    @Override
    public int compareTo(Tag t) {
        if (t instanceof Operation) {
            Operation o = (Operation)t;
            int result = this.seq - o.seq;
            result = result != 0 ? result : this.getName().compareToIgnoreCase(t.getName());
            return result != 0 ? result : this.getName().compareTo(t.getName());
        }
        else
            return super.compareTo(t);
    }

    public static List<Operation> getSavingsPlanOperations(boolean showLent) {
    	if (showLent) {
    		if (savingsPlanOperationsWithLent == null) {
    			List<Operation> ops = Lists.newArrayList(savingsPlanOperations);
    			List<Operation> borrowed = Lists.newArrayList(borrowedOperations);
    			ops.removeAll(borrowed);
    			savingsPlanOperationsWithLent = ops;
    		}
    		return savingsPlanOperationsWithLent;
    	}
    	else {
    		if (savingsPlanOperationsWithBorrowed == null) {
    			List<Operation> ops = Lists.newArrayList(savingsPlanOperations);
    			List<Operation> lent = Lists.newArrayList(lentOperations);
    			ops.removeAll(lent);
    			savingsPlanOperationsWithBorrowed = ops;
    		}
    		return savingsPlanOperationsWithBorrowed;    		
    	}
    }

    public static SavingsPlanOperation getSavingsPlanAmortized(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case PartialUpfront: return savingsPlanAmortizedPartialUpfront;
    	case AllUpfront: return savingsPlanAmortizedAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanBorrowedAmortized(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case PartialUpfront: return savingsPlanBorrowedAmortizedPartialUpfront;
    	case AllUpfront: return savingsPlanBorrowedAmortizedAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanLentAmortized(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case PartialUpfront: return savingsPlanLentAmortizedPartialUpfront;
    	case AllUpfront: return savingsPlanLentAmortizedAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanUnusedAmortized(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case PartialUpfront: return savingsPlanUnusedAmortizedPartialUpfront;
    	case AllUpfront: return savingsPlanUnusedAmortizedAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanUnused(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanUnusedNoUpfront;
    	case PartialUpfront: return savingsPlanUnusedPartialUpfront;
    	case AllUpfront: return savingsPlanUnusedAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanUsed(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanUsedNoUpfront;
    	case PartialUpfront: return savingsPlanUsedPartialUpfront;
    	case AllUpfront: return savingsPlanUsedAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanBorrowed(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanBorrowedNoUpfront;
    	case PartialUpfront: return savingsPlanBorrowedPartialUpfront;
    	case AllUpfront: return savingsPlanBorrowedAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanLent(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanLentNoUpfront;
    	case PartialUpfront: return savingsPlanLentPartialUpfront;
    	case AllUpfront: return savingsPlanLentAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanBonus(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanBonusNoUpfront;
    	case PartialUpfront: return savingsPlanBonusPartialUpfront;
    	case AllUpfront: return savingsPlanBonusAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
    
    public static SavingsPlanOperation getSavingsPlanSavings(PurchaseOption paymentOption) {
    	switch(paymentOption) {
    	case NoUpfront: return savingsPlanSavingsNoUpfront;
    	case PartialUpfront: return savingsPlanSavingsPartialUpfront;
    	case AllUpfront: return savingsPlanSavingsAllUpfront;
    	default: throw new RuntimeException("Unknown PurchaseOption " + paymentOption);
    	}
    }
     
    public boolean isSavingsPlan() {
    	return this instanceof SavingsPlanOperation;
    }
        
    public static class SavingsPlanOperation extends Operation {
		private static final long serialVersionUID = 1L;
		private final PurchaseOption paymentOption;

        private SavingsPlanOperation(String name, Category category) {
            super(name, sequence++, category);
            this.paymentOption = null;
            operations.put(name,  this);
            savingsPlanOperations.add(this);
        }

		private SavingsPlanOperation(Category category, PurchaseOption paymentOption) {
			super("SavingsPlan " + category.name() + " - " + paymentOption.name, sequence++, category);
			this.paymentOption = paymentOption;
            operations.put(name, this);
            savingsPlanOperations.add(this);
		}
    	
		public PurchaseOption getPaymentOption() {
			return paymentOption;
		}
    }
    
}
