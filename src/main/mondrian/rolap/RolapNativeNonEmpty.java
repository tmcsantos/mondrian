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
import mondrian.rolap.sql.*;

import javax.sql.DataSource;
import java.util.List;

public class RolapNativeNonEmpty  extends RolapNativeSet {

    public RolapNativeNonEmpty() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeNonEmptyFun.get());
    }

    static class NonEmptyConstraint extends SetConstraint {

        public NonEmptyConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator)
        {
            super(args, evaluator, true);
        }

        protected boolean isJoinRequired() {
            return true;
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
        if (!NonEmptyConstraint.isValidContext(
            evaluator, restrictMemberTypes()))
        {
            return null;
        }
        String funName = fun.getName();
        if (!"NonEmpty".equalsIgnoreCase(funName)) {
            return null;
        }

        if (args.length > 1) {
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
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeNonEmpty");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, cjArgs[0].getLevel());

        // Check to see if evaluator contains a calculated member that can't be
        // expanded.  This is necessary due to the SqlConstraintsUtils.
        // addContextConstraint()
        // method which gets called when generating the native SQL.
        if (SqlConstraintUtils.containsCalculatedMember(
            evaluator.getNonAllMembers(), true))
        {
            return null;
        }

        LOGGER.debug("using native NonEmpty()");

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
                new NonEmptyConstraint(combinedArgs, evaluator);
            return new SetEvaluator(cjArgs, schemaReader, constraint);
        } finally {
            evaluator.restore(savepoint);
        }
    }
}
