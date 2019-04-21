package Indexes;

import Utils.utils;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class TreeSearch {
	public String operation;
	public PrimitiveValue leftValue;
	public PrimaryIndex.RangePolicy leftPolicy;
	public PrimitiveValue rightValue;
	public PrimaryIndex.RangePolicy rightPolicy;
	
	public void addEqualsToOperator(PrimitiveValue leftValue) {
		this.operation = "EQUALS";
		this.leftValue = leftValue;
	}
	
	public boolean addRangeOperator(PrimitiveValue leftValue, PrimaryIndex.RangePolicy leftPolicy, PrimitiveValue rightValue, PrimaryIndex.RangePolicy rightPolicy) {
		this.operation = "RANGE";
		
		if (this.leftValue == null && this.rightValue == null) {
			this.leftValue = leftValue;
			this.rightValue = rightValue;
			this.leftPolicy = leftPolicy;
			this.rightPolicy = rightPolicy;
			
			return true;
		}
		
		if (leftValue != null) {
			// Inserting left Value
			
			
			if (this.rightValue != null && utils.primitiveValueComparator(leftValue, this.rightValue) > 0) {
				return false;
			}
			
			if (this.leftValue != null) {
				// Already has a left Value
				
				if (utils.primitiveValueComparator(leftValue, this.leftValue) < 0) {
					this.leftValue = leftValue;
					this.leftPolicy = leftPolicy;
				}
			} else {
				this.leftValue = leftValue;
			}
			
			return true;
		} else {
			// Insert right Value
			
			if (this.leftValue != null && utils.primitiveValueComparator(rightValue, this.leftValue) < 0) {
				return false;
			}
			
			if (this.rightValue != null) {
				// Already has a right Value
				
				if (utils.primitiveValueComparator(rightValue, this.rightValue) > 0) {
					this.rightValue = rightValue;
					this.rightPolicy = rightPolicy;
				}
			} else {
				this.rightValue = rightValue;
			}
			
			return true;
		}
	}
}
