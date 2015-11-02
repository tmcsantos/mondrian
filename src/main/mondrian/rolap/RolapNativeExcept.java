/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2015 Pentaho
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RolapNativeExcept extends RolapNativeSet {

    public RolapNativeExcept() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeExcept.get());
    }

    static class ExceptConstraint extends SetConstraint {
        Exp except;

        public ExceptConstraint(
            CrossJoinArg[] cjArgs,
            RolapEvaluator evaluator,
            Exp except)
        {
            super(cjArgs, evaluator, true);
            this.except = except;
        }

        protected boolean isJoinRequired() {
            final AtomicBoolean mustJoin = new AtomicBoolean(false);
            for (int i = 0; i < args.length; i++) {
                if (args[i] instanceof NonEmptyCrossJoinArg) {
                    mustJoin.set(true);
                    break;
                }
            }
            return mustJoin.get()
                || (getEvaluator().isNonEmpty() && super.isJoinRequired());
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            // Use aggregate table to generate filter condition
            RolapNativeSql sql =
                new RolapNativeSql(
                    sqlQuery, aggStar, getEvaluator(), args[0].getLevel());

            for (CrossJoinArg arg : args) {
                arg.addConstraint(sqlQuery, baseCube, aggStar);
            }

            String filterSql = sql.generateFilterCondition(except);
            if (filterSql != null) {
                sqlQuery.addWhere(filterSql);
            }

            if (getEvaluator().isNonEmpty() || isJoinRequired()) {
                // only apply context constraint if non empty, or
                // if a join is required to fulfill the filter condition
                super.addConstraint(sqlQuery, baseCube, aggStar);
            }
        }

        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            key.add(super.getCacheKey());

            if (except != null) {
                key.add(except.toString());
            }
            key.add(getEvaluator().isNonEmpty());

            return key;
        }
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        if (!isEnabled()) {
            return null;
        }
        if (!ExceptConstraint.isValidContext(
            evaluator, restrictMemberTypes()))
        {
            return null;
        }
        String funName = fun.getName();
        if (!"Except".equalsIgnoreCase(funName)) {
            return null;
        }

        if (args.length != 2) {
            return null;
        }

        // extract the set expression
        List<CrossJoinArg[]> allArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

        if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
            return null;
        }

        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the WHERE condition
        // Need to generate where condition here to determine whether
        // or not the filter condition can be created. The filter
        // condition could change to use an aggregate table later in evaluation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeExcept");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, cjArgs[0].getLevel());
        final Exp exceptArgs = args[1];
        String filterExprStr = sql.generateFilterCondition(exceptArgs);
        if (filterExprStr == null) {
            return null;
        }

        // Check to see if evaluator contains a calculated member that can't be
        // expanded.  This is necessary due to the SqlConstraintsUtils.
        // addContextConstraint()
        // method which gets called when generating the native SQL.
        if (SqlConstraintUtils.containsCalculatedMember(
            evaluator.getNonAllMembers(), true))
        {
            return null;
        }

        LOGGER.debug("using native except");

        final int savepoint = evaluator.savepoint();
        try {
            overrideContext(evaluator, cjArgs, sql.getStoredMeasure());

            // no need to have any context if there is no measure, we are doing
            // a filter only on the current dimension. This prevents
            // SqlContextConstraint from expanding unnecessary calculated
            // members on the
            // slicer calling expandSupportedCalculatedMembers
            if (!evaluator.isNonEmpty() && sql.getStoredMeasure() == null) {
                // No need to have anything on the context
                for (Member m : evaluator.getMembers()) {
                    evaluator.setContext(
                        m.getLevel().getHierarchy().getDefaultMember());
                }
            }
            // Now construct the TupleConstraint that contains both the CJ
            // dimensions and the additional filter on them.
            CrossJoinArg[] combinedArgs = cjArgs;
            if (allArgs.size() == 2) {
                CrossJoinArg[] predicateArgs = allArgs.get(1);
                if (predicateArgs != null) {
                    // Combined the CJ and the additional predicate args.
                    combinedArgs =
                        Util.appendArrays(cjArgs, predicateArgs);
                }
            }

            TupleConstraint constraint =
                new ExceptConstraint(combinedArgs, evaluator, exceptArgs);
            return new SetEvaluator(cjArgs, schemaReader, constraint);
        } finally {
            evaluator.restore(savepoint);
        }
    }
}
