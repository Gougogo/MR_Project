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
public class NullWritableMine implements WritableComparable<NullWritableMine> {
    private static final NullWritableMine THIS = new NullWritableMine();

    private NullWritableMine() {
    }

    public static NullWritableMine get() {
        return THIS;
    }

    public String toString() {
        return "";
    }

    public int hashCode() {
        return 0;
    }

    public int compareTo(NullWritableMine other) {
        return 1;
    }

    public boolean equals(Object other) {
        return other instanceof NullWritableMine;
    }

    public void readFields(DataInput in) throws IOException {
    }

    public void write(DataOutput out) throws IOException {
    }

    static {
        WritableComparator.define(NullWritableMine.class, new NullWritableMine.Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(NullWritableMine.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            assert 0 == l1;

            assert 0 == l2;

            return 0;
        }
    }
}
