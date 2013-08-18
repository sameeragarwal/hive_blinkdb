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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount.GenericUDAFCountEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * This class implements the Approximate COUNT aggregation function as in SQL.
 * Implemented using Closed Forms: N(np, n(1-p)p)
 * We keep track of total elements to calculate p
 *
 * Note: Doesn't work with DISTINCT
 */
@Description(name = "approx_count",
value = "_FUNC_() - Returns the total number of retrieved rows, including "
    + "rows containing NULL values.\n"

        + "_FUNC_(expr) - Returns the number of rows for which the supplied "
        + "expression is non-NULL.\n")
public class ApproxUDAFCount implements GenericUDAFResolver2 {

  private static final Log LOG = LogFactory.getLog(GenericUDAFCount.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    // This method implementation is preserved for backward compatibility.
    return new GenericUDAFCountEvaluator();
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo)
      throws SemanticException {

    // Adding 2 new parameters:
    // (1) Table Size for error correction (totalRows)
    // (2) Sample Size for scaling (sampleRows)
    TypeInfo[] parameters = paramInfo.getParameters();

    assert !paramInfo.isDistinct() : "DISTINCT not supported with APPROX COUNT";

    return new ApproxUDAFCountEvaluator().setCountAllColumns(
        paramInfo.isAllColumns());

  }

  /**
   * ApproxUDAFCountEvaluator.
   *
   */
  public static class ApproxUDAFCountEvaluator extends GenericUDAFEvaluator {
    private boolean countAllColumns = false;

    private PrimitiveObjectInspector totalRowsOI;
    private PrimitiveObjectInspector sampleRowsOI;

    // For PARTIAL1 and COMPLETE
    // private PrimitiveObjectInspector inputOI;

    // For PARTIAL2 and FINAL
    private StructObjectInspector soi;
    private StructField countField;
    private StructField totalRowsField;
    private StructField sampleRowsField;
    private LongObjectInspector countFieldOI;
    private LongObjectInspector totalRowsFieldOI;
    private LongObjectInspector sampleRowsFieldOI;

    // For PARTIAL1 and PARTIAL2
    private Object[] partialResult;

    // For FINAL and COMPLETE
    Text result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);

      if (parameters.length == 2) {
        totalRowsOI = (PrimitiveObjectInspector) parameters[1];
        sampleRowsOI = (PrimitiveObjectInspector) parameters[0];
      } else if (parameters.length == 3) {
        totalRowsOI = (PrimitiveObjectInspector) parameters[2];
        sampleRowsOI = (PrimitiveObjectInspector) parameters[1];
      }

      // init input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        // sameerag: approx_count doesn't need to read input
        // inputOI = (PrimitiveObjectInspector) parameters[0];
      } else {

        soi = (StructObjectInspector) parameters[0];

        countField = soi.getStructFieldRef("count");
        totalRowsField = soi.getStructFieldRef("totalRows");
        sampleRowsField = soi.getStructFieldRef("sampleRows");

        countFieldOI = (LongObjectInspector) countField
            .getFieldObjectInspector();
        totalRowsFieldOI = (LongObjectInspector) totalRowsField
            .getFieldObjectInspector();
        sampleRowsFieldOI = (LongObjectInspector) sampleRowsField
            .getFieldObjectInspector();
      }

      // init output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        ArrayList<String> fname = new ArrayList<String>();
        fname.add("count");
        fname.add("totalRows");
        fname.add("sampleRows");

        partialResult = new Object[3];
        partialResult[0] = new LongWritable(0);
        partialResult[1] = new LongWritable(0);
        partialResult[2] = new LongWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);

      } else {
        // ArrayList<String> fname = new ArrayList<String>();
        // fname.add("approx_count");
        // fname.add("error");
        // fname.add("confidence");
        // ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        // foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        // foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        // foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        // result = new ArrayList<LongWritable>(3);
        // return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);

        result = new Text();
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      }

    }

    private ApproxUDAFCountEvaluator setCountAllColumns(boolean countAllCols) {
      countAllColumns = countAllCols;
      return this;
    }

    /** class for storing count value. */
    static class CountAgg implements AggregationBuffer {
      long value;
      long totalRows;
      long sampleRows;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      CountAgg buffer = new CountAgg();
      reset(buffer);
      return buffer;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((CountAgg) agg).value = 0;
      ((CountAgg) agg).totalRows = 0;
      ((CountAgg) agg).sampleRows = 0;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      // parameters == null means the input table/split is empty
      if (parameters == null) {
        return;
      }

      if (parameters.length == 2) {
        ((CountAgg) agg).totalRows = PrimitiveObjectInspectorUtils.getLong(parameters[1],
            totalRowsOI);
        ((CountAgg) agg).sampleRows = PrimitiveObjectInspectorUtils.getLong(parameters[0],
            sampleRowsOI);
      } else {
        if (parameters.length == 3) {
          ((CountAgg) agg).totalRows = PrimitiveObjectInspectorUtils.getLong(parameters[2],
              totalRowsOI);
          ((CountAgg) agg).sampleRows = PrimitiveObjectInspectorUtils.getLong(parameters[1],
              sampleRowsOI);
        }
      }

      if (countAllColumns) {
        // assert parameters.length == 0;
        ((CountAgg) agg).value++;

      } else {
        assert parameters.length > 0;
        boolean countThisRow = true;
        for (Object nextParam : parameters) {
          if (nextParam == null) {
            countThisRow = false;
            break;
          }
        }
        if (countThisRow) {
          ((CountAgg) agg).value++;
        }
      }

    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {

        Object partialCount = soi.getStructFieldData(partial, countField);
        Object partialTotalRows = soi.getStructFieldData(partial, totalRowsField);
        Object partialSampleRows = soi.getStructFieldData(partial, sampleRowsField);

        long p = countFieldOI.get(partialCount);
        long q = totalRowsFieldOI.get(partialTotalRows);
        long r = sampleRowsFieldOI.get(partialSampleRows);

        LOG.info("Merge Value: " + p);
        LOG.info("Merge Total Rows: " + q);
        LOG.info("Merge Sampling Rows: " + r);

        ((CountAgg) agg).value += p;
        ((CountAgg) agg).totalRows = q;
        ((CountAgg) agg).sampleRows = r;

      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {

      CountAgg myagg = (CountAgg) agg;

      long approx_count = (long) ((double) (myagg.value * myagg.totalRows) / myagg.sampleRows);
      double probability = ((double) myagg.value) / ((double) myagg.sampleRows);

      LOG.info("Value: " + myagg.value);
      LOG.info("TotalRows: " + myagg.totalRows);
      LOG.info("Probability: " + probability);
      LOG.info("Sampling Ratio: " + (((double) myagg.sampleRows) / myagg.totalRows));

      StringBuilder sb = new StringBuilder();
      sb.append(approx_count);
      sb.append(" +/- ");
      sb.append(Math.ceil(2.575 * (((double) myagg.totalRows) / myagg.sampleRows)
          * Math.sqrt(myagg.value * (1 - probability))));
      sb.append(" (99% Confidence) ");

      result.set(sb.toString());
      return result;

    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {

      CountAgg myagg = (CountAgg) agg;
      ((LongWritable) partialResult[0]).set(myagg.value);
      ((LongWritable) partialResult[1]).set(myagg.totalRows);
      ((LongWritable) partialResult[2]).set(myagg.sampleRows);
      return partialResult;

    }
  }
}
