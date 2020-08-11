package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;

    private int max;

    private int buckets;

    private int width;

    private int[] histogram;

    private int ntups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (int) Math.ceil((max-min+1.0)/buckets);
        this.histogram = new int[buckets];
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = valueIndexOf(v);
        if (index >100 || index < 0){
            System.out.println(11);
        }
        histogram[index]++;
        ntups++;
    }

    private int valueIndexOf(int v){
        if(v == max){
            return buckets - 1;
        }else{
            return (v-min)/ width;
        }
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int bucketIdx = valueIndexOf(v);        // 值所在的索引
        // 当前柱的左右值
        int left = bucketIdx*width+min;
        int right = left+width-1;

        switch (op){
            case EQUALS:{
                if(v < min || v>max){
                    return  0.0;
                }
                int height = histogram[bucketIdx];
                return (height*1.0 / width) / ntups;
            }
            case LESS_THAN:{
                if(v < min){
                    return 0;
                }
                if(v > max){
                    return 1;
                }
                int totalLeft = 0;
                int height = histogram[bucketIdx];
                for (int i = 0; i < bucketIdx; i++) {
                    totalLeft += histogram[i];
                }
                double p1 = totalLeft*1.0/ntups;
                double p2 = 1.0*height/ntups*((v-left)*1.0/width);
                return p1+p2;
            }
            case GREATER_THAN:{
                if (v < min){
                    return 1.0;
                }
                if (v > max){
                    return 0;
                }
                int totalRight = 0;
                int height = histogram[bucketIdx];
                for (int i = bucketIdx+1; i < buckets; i++) {
                    totalRight += histogram[i];
                }
                double p1 = 1.0*totalRight/ntups;
                double p2 = 1.0*height/ntups*((right-v)*1.0/width);
                return p1+p2;
            }
            case LESS_THAN_OR_EQ:{
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            case GREATER_THAN_OR_EQ:
            {
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            case NOT_EQUALS:{
                return 1- estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            case LIKE:{
                //int应该不支持like才对，但是StringHistogram间接调用这个方法，那里应该支持。。。
                return avgSelectivity();
            }
            default: throw new RuntimeException("Should not reach hear");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return "[to be implemented]";
    }

    public void setBound(int min, int max){
        this.min = min;
        this.max = max;
    }
}
