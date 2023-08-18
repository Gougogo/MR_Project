package com.goumi.flow;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @version 1.0
 * @auther GouMi
 */
public class SameKeyData implements WritableComparable<SameKeyData> {
    private static final SameKeyData sameKeyData = new SameKeyData();

    public static SameKeyData getSameKeyData(){
        return sameKeyData;
    }

    private SameKeyData() {
        super();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public int compareTo(SameKeyData o) {
        return 1;
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj){
            return true;
        }

        return false;
    }

    /*static {
        WritableComparator.define(SameKeyData.class, new SameKeyData.Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(SameKeyData.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            assert 0 == l1;

            assert 0 == l2;

            return 0;
        }
    }*/
}
