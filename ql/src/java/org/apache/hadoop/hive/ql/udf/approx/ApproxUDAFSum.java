/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.approx;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFSum.
 *
 */
@Description(name = "approx_sum", value = "_FUNC_(x) - Returns the approximate sum of a set of numbers with error bars")
public class ApproxUDAFSum extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(ApproxUDAFSum.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 3) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:

      return new ApproxUDAFSumLong();
    case FLOAT:
    case DOUBLE:
      return new ApproxUDAFSumDouble();
    case DATE:
    case TIMESTAMP:
    case STRING:
    case BOOLEAN:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * ApproxUDAFSumDouble.
   *
   */
  public static class ApproxUDAFSumDouble extends GenericUDAFEvaluator {
    // private PrimitiveObjectInspector inputOI;
    // private DoubleWritable result;

    // For PARTIAL1 and COMPLETE
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector totalRowsOI;
    private PrimitiveObjectInspector samplingRatioOI;

    // For PARTIAL2 and FINAL
    private StructObjectInspector soi;
    private StructField countField;
    private StructField sumField;
    private StructField varianceField;
    private StructField totalRowsField;
    private StructField samplingRatioField;
    private LongObjectInspector countFieldOI;
    private DoubleObjectInspector sumFieldOI;
    private DoubleObjectInspector varianceFieldOI;
    private LongObjectInspector totalRowsFieldOI;
    private DoubleObjectInspector samplingRatioFieldOI;

    // For PARTIAL1 and PARTIAL2
    private Object[] partialResult;

    // For FINAL and COMPLETE
    ArrayList<DoubleWritable> result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 3);
      super.init(m, parameters);

      if (parameters.length == 3) {
        totalRowsOI = (PrimitiveObjectInspector) parameters[1];
        samplingRatioOI = (PrimitiveObjectInspector) parameters[2];
      }

      // result = new DoubleWritable(0);
      // inputOI = (PrimitiveObjectInspector) parameters[0];
      // return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

      // init input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
      } else {
        soi = (StructObjectInspector) parameters[0];

        countField = soi.getStructFieldRef("count");
        sumField = soi.getStructFieldRef("sum");
        varianceField = soi.getStructFieldRef("variance");
        totalRowsField = soi.getStructFieldRef("totalRows");
        samplingRatioField = soi.getStructFieldRef("samplingRatio");

        countFieldOI = (LongObjectInspector) countField
            .getFieldObjectInspector();
        sumFieldOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
        varianceFieldOI = (DoubleObjectInspector) varianceField
            .getFieldObjectInspector();
        totalRowsFieldOI = (LongObjectInspector) totalRowsField
            .getFieldObjectInspector();
        samplingRatioFieldOI = (DoubleObjectInspector) samplingRatioField
            .getFieldObjectInspector();
      }

      // init output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        // The output of a partial aggregation is a struct containing
        // a long count and doubles sum and variance.

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

        foi.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

        ArrayList<String> fname = new ArrayList<String>();
        fname.add("empty");
        fname.add("count");
        fname.add("sum");
        fname.add("variance");
        fname.add("totalRows");
        fname.add("samplingRatio");

        partialResult = new Object[6];
        partialResult[0] = new BooleanWritable();
        partialResult[1] = new LongWritable(0);
        partialResult[2] = new DoubleWritable(0);
        partialResult[3] = new DoubleWritable(0);
        partialResult[4] = new LongWritable(0);
        partialResult[5] = new DoubleWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);

      } else {
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("approx_sum");
        fname.add("error");
        fname.add("confidence");
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        result = new ArrayList<DoubleWritable>(3);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
      }

    }

    /** class for storing double sum value. */
    static class SumDoubleAgg implements AggregationBuffer {
      boolean empty;
      long count; // number of elements
      double sum;
      double variance; // sum[x-avg^2] (this is actually n times the variance)
      long totalRows;
      double samplingRatio;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumDoubleAgg result = new SumDoubleAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      myagg.empty = true;
      myagg.count = 0;
      myagg.sum = 0;
      myagg.variance = 0;
      myagg.totalRows = 0;
      myagg.samplingRatio = 0;
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 3);

      if (parameters.length == 3) {
        ((SumDoubleAgg) agg).totalRows = PrimitiveObjectInspectorUtils.getLong(parameters[1],
            totalRowsOI);
        ((SumDoubleAgg) agg).samplingRatio = PrimitiveObjectInspectorUtils.getDouble(parameters[2],
            samplingRatioOI);
      }

      Object p = parameters[0];
      if (p != null) {
        SumDoubleAgg myagg = (SumDoubleAgg) agg;
        try {
          double v = PrimitiveObjectInspectorUtils.getDouble(p, inputOI);
          myagg.count++;
          myagg.sum += v;
          if (myagg.count > 1) {
            double t = myagg.count * v - myagg.sum;
            myagg.variance += (t * t) / ((double) myagg.count * (myagg.count - 1));
          }
        } catch (NumberFormatException e) {
          if (!warned) {
            warned = true;
            LOG.warn(getClass().getSimpleName() + " "
                + StringUtils.stringifyException(e));
            LOG
            .warn(getClass().getSimpleName()
                + " ignoring similar exceptions.");
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      // return terminate(agg);
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      ((BooleanWritable) partialResult[0]).set(myagg.empty);
      ((LongWritable) partialResult[1]).set(myagg.count);
      ((DoubleWritable) partialResult[2]).set(myagg.sum);
      ((DoubleWritable) partialResult[3]).set(myagg.variance);
      ((LongWritable) partialResult[4]).set(myagg.totalRows);
      ((DoubleWritable) partialResult[5]).set(myagg.samplingRatio);
      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {

      if (partial != null) {

        SumDoubleAgg myagg = (SumDoubleAgg) agg;
        myagg.empty = false;

        Object partialCount = soi.getStructFieldData(partial, countField);
        Object partialSum = soi.getStructFieldData(partial, sumField);
        Object partialVariance = soi.getStructFieldData(partial, varianceField);
        Object partialTotalRows = soi.getStructFieldData(partial, totalRowsField);
        Object partialSamplingRatio = soi.getStructFieldData(partial, samplingRatioField);


        long n = myagg.count;
        long m = countFieldOI.get(partialCount);
        long q = totalRowsFieldOI.get(partialTotalRows);
        double r = samplingRatioFieldOI.get(partialSamplingRatio);
        myagg.totalRows = q;
        myagg.samplingRatio = r;


        if (n == 0) {
          // Just copy the information since there is nothing so far
          myagg.variance = varianceFieldOI.get(partialVariance);
          myagg.count = countFieldOI.get(partialCount);
          myagg.sum = sumFieldOI.get(partialSum);
        }

        if (m != 0 && n != 0) {
          // Merge the two partials

          double a = myagg.sum;
          double b = sumFieldOI.get(partialSum);

          myagg.empty = false;
          myagg.count += m;
          myagg.sum += b;
          double t = (m / (double) n) * a - b;
          myagg.variance += varianceFieldOI.get(partialVariance)
              + ((n / (double) m) / ((double) n + m)) * t * t;

        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      if (myagg.empty) {
        return null;
      }

      double approx_sum = (double) (myagg.sum / myagg.samplingRatio);
      double probability = ((double) myagg.count) / ((double) myagg.totalRows);
      double mean = myagg.sum / myagg.count;

      ArrayList<DoubleWritable> bin = new ArrayList<DoubleWritable>(3);
      bin.add(new DoubleWritable(approx_sum));
      bin.add(new DoubleWritable((2.575 * (1 / myagg.samplingRatio) * Math
          .sqrt((myagg.variance / myagg.count) + ((1 - probability) * mean * mean)))));
      bin.add(new DoubleWritable(99.0));

      result = bin;
      return result;

    }

  }

  /**
   * GenericUDAFSumLong.
   *
   */
  public static class ApproxUDAFSumLong extends GenericUDAFEvaluator {
    // private PrimitiveObjectInspector inputOI;
    // private LongWritable result;

    // For PARTIAL1 and COMPLETE
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector totalRowsOI;
    private PrimitiveObjectInspector samplingRatioOI;

    // For PARTIAL2 and FINAL
    private StructObjectInspector soi;
    private StructField countField;
    private StructField sumField;
    private StructField varianceField;
    private StructField totalRowsField;
    private StructField samplingRatioField;
    private LongObjectInspector countFieldOI;
    private LongObjectInspector sumFieldOI;
    private DoubleObjectInspector varianceFieldOI;
    private LongObjectInspector totalRowsFieldOI;
    private DoubleObjectInspector samplingRatioFieldOI;

    // For PARTIAL1 and PARTIAL2
    private Object[] partialResult;

    // For FINAL and COMPLETE
    ArrayList<LongWritable> result;


    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 3);
      super.init(m, parameters);

      if (parameters.length == 3) {
        totalRowsOI = (PrimitiveObjectInspector) parameters[1];
        samplingRatioOI = (PrimitiveObjectInspector) parameters[2];
      }

      // init input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
      } else {
        soi = (StructObjectInspector) parameters[0];

        countField = soi.getStructFieldRef("count");
        sumField = soi.getStructFieldRef("sum");
        varianceField = soi.getStructFieldRef("variance");
        totalRowsField = soi.getStructFieldRef("totalRows");
        samplingRatioField = soi.getStructFieldRef("samplingRatio");

        countFieldOI = (LongObjectInspector) countField
            .getFieldObjectInspector();
        sumFieldOI = (LongObjectInspector) sumField.getFieldObjectInspector();
        varianceFieldOI = (DoubleObjectInspector) varianceField
            .getFieldObjectInspector();
        totalRowsFieldOI = (LongObjectInspector) totalRowsField
            .getFieldObjectInspector();
        samplingRatioFieldOI = (DoubleObjectInspector) samplingRatioField
            .getFieldObjectInspector();
      }

      // init output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        // The output of a partial aggregation is a struct containing
        // a long count and doubles sum and variance.

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

        foi.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

        ArrayList<String> fname = new ArrayList<String>();
        fname.add("empty");
        fname.add("count");
        fname.add("sum");
        fname.add("variance");
        fname.add("totalRows");
        fname.add("samplingRatio");

        partialResult = new Object[6];
        partialResult[0] = new BooleanWritable();
        partialResult[1] = new LongWritable(0);
        partialResult[2] = new LongWritable(0);
        partialResult[3] = new DoubleWritable(0);
        partialResult[4] = new LongWritable(0);
        partialResult[5] = new DoubleWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);

      } else {
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("approx_sum");
        fname.add("error");
        fname.add("confidence");
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        result = new ArrayList<LongWritable>(3);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
      }

    }

    /** class for storing double sum value. */
    static class SumLongAgg implements AggregationBuffer {
      boolean empty;
      long count; // number of elements
      long sum;
      double variance; // sum[x-avg^2] (this is actually n times the variance)
      long totalRows;
      double samplingRatio;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumLongAgg result = new SumLongAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumLongAgg myagg = (SumLongAgg) agg;
      myagg.empty = true;
      myagg.count = 0;
      myagg.sum = 0;
      myagg.variance = 0;
      myagg.totalRows = 0;
      myagg.samplingRatio = 0;
    }

    private boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 3);

      Object p = parameters[0];
      if (p != null) {
        SumLongAgg myagg = (SumLongAgg) agg;

        if (parameters.length == 3) {
          myagg.totalRows = PrimitiveObjectInspectorUtils.getLong(parameters[1],
              totalRowsOI);
          myagg.samplingRatio = PrimitiveObjectInspectorUtils.getDouble(parameters[2],
              samplingRatioOI);
        }

        try {
          long v = PrimitiveObjectInspectorUtils.getLong(p, inputOI);
          myagg.count++;
          myagg.sum += v;
          if (myagg.count > 1) {
            double t = myagg.count * v - myagg.sum;
            myagg.variance += (t * t) / ((double) myagg.count * (myagg.count - 1));
          }
        } catch (NumberFormatException e) {
          if (!warned) {
            warned = true;
            LOG.warn(getClass().getSimpleName() + " "
                + StringUtils.stringifyException(e));
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {

      SumLongAgg myagg = (SumLongAgg) agg;
      ((BooleanWritable) partialResult[0]).set(myagg.empty);
      ((LongWritable) partialResult[1]).set(myagg.count);
      ((LongWritable) partialResult[2]).set(myagg.sum);
      ((DoubleWritable) partialResult[3]).set(myagg.variance);
      ((LongWritable) partialResult[4]).set(myagg.totalRows);
      ((DoubleWritable) partialResult[5]).set(myagg.samplingRatio);

      return partialResult;

    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {

      if (partial != null) {
        SumLongAgg myagg = (SumLongAgg) agg;
        myagg.empty = false;

        Object partialCount = soi.getStructFieldData(partial, countField);
        Object partialSum = soi.getStructFieldData(partial, sumField);
        Object partialVariance = soi.getStructFieldData(partial, varianceField);
        Object partialTotalRows = soi.getStructFieldData(partial, totalRowsField);
        Object partialSamplingRatio = soi.getStructFieldData(partial, samplingRatioField);

        long n = myagg.count;
        long m = countFieldOI.get(partialCount);
        long q = totalRowsFieldOI.get(partialTotalRows);
        double r = samplingRatioFieldOI.get(partialSamplingRatio);

        myagg.totalRows = q;
        myagg.samplingRatio = r;

        if (n == 0) {
          // Just copy the information since there is nothing so far
          myagg.variance = varianceFieldOI.get(partialVariance);
          myagg.count = countFieldOI.get(partialCount);
          myagg.sum = sumFieldOI.get(partialSum);
        }

        if (m != 0 && n != 0) {
          // Merge the two partials

          long a = myagg.sum;
          long b = sumFieldOI.get(partialSum);

          myagg.empty = false;
          myagg.count += m;
          myagg.sum += b;
          double t = (m / (double) n) * a - b;
          myagg.variance += varianceFieldOI.get(partialVariance)
              + ((n / (double) m) / ((double) n + m)) * t * t;
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {

      SumLongAgg myagg = (SumLongAgg) agg;

      if (myagg.empty) {
        return null;
      }

      long approx_sum = (long) (myagg.sum / myagg.samplingRatio);
      double probability = ((double) myagg.count) / ((double) myagg.totalRows);
      double mean = myagg.sum / myagg.count;

      LOG.info("Sum: " + approx_sum);
      LOG.info("TotalRows: " + myagg.totalRows);
      LOG.info("Probability: " + probability);
      LOG.info("Sampling Ratio: " + myagg.samplingRatio);

      ArrayList<LongWritable> bin = new ArrayList<LongWritable>(3);
      bin.add(new LongWritable(approx_sum));
      bin.add(new LongWritable((long) Math.ceil((2.575 * (1 / myagg.samplingRatio) * Math
          .sqrt((myagg.variance / myagg.count) + ((1 - probability) * mean * mean))))));
      bin.add(new LongWritable(99));

      result = bin;
      return result;

    }

  }

}
