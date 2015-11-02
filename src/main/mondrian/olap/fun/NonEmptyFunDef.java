/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2015 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.RolapEvaluator;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Definition of the <code>NonEmpty</code> MDX function.
 *
 * @author tsantos
 * @since Out 19, 2015
 */
public class NonEmptyFunDef extends FunDefBase {
    static final ReflectiveMultiResolver resolver =
        new ReflectiveMultiResolver(
            "NonEmpty",
            "NonEmpty(<Set1>,[<measure>])",
            "Returns the set of tuples that are not empty from a specified set.",
            new String[] {"fxx","fxxm"},
            NonEmptyFunDef.class);

    public NonEmptyFunDef(FunDef dummyFunDef)
    {
        super(dummyFunDef);
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final MemberCalc memberCalc;
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        if (call.getArgs().length == 2) {
            memberCalc = compiler.compileMember(call.getArg(1));
        } else {
            memberCalc = null;
        }

        return new AbstractListCalc(call, new Calc[]{listCalc, memberCalc}) {
            public TupleList evaluateList(Evaluator evaluator) {
                SchemaReader schemaReader = evaluator.getSchemaReader();

                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(true);
                    if (evaluator instanceof RolapEvaluator) {
                        for (Member member
                            : ((RolapEvaluator) evaluator).getSlicerMembers()) {
                            if (getType().getElementType().usesHierarchy(
                                member.getHierarchy(), true)) {
                                evaluator.setContext(
                                    member.getHierarchy().getAllMember());
                            }
                        }
                    }

                    NativeEvaluator nativeEvaluator =
                        schemaReader.getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), evaluator, this);
                    if (nativeEvaluator != null) {
                        evaluator.restore(savepoint);
                        return
                            (TupleList) nativeEvaluator.execute(
                                ResultStyle.LIST);
                    }

                    Set<Member> measureSet = new HashSet<>();
                    if (memberCalc != null) {
                        measureSet.add(memberCalc.evaluateMember(evaluator));
                    }

                    final TupleList list1 = listCalc.evaluateList(evaluator);

                    // remove any remaining empty crossings from the result
                    return nonEmptyList(evaluator, list1, measureSet);
                } finally {
                    evaluator.restore(savepoint);
                }
            }
        };
    }

    protected TupleList nonEmptyList(
        Evaluator evaluator,
        TupleList list,
        Set<Member> measureSet)
    {
        if (list.isEmpty()) {
            return list;
        }

        TupleList result =
            TupleCollections.createList(
                list.getArity(), (list.size() + 2) >> 1);

        final int savepoint = evaluator.savepoint();
        try {
            final TupleCursor cursor = list.tupleCursor();
            while (cursor.forward()) {
                cursor.setContext(evaluator);
                if (checkData(measureSet, evaluator)) {
                    result.addCurrent(cursor);
                }
            }
            return result;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    private static boolean checkData(
        Set<Member> measureSet,
        Evaluator evaluator)
    {
        // no measures found, use standard algorithm
        if (measureSet.isEmpty()) {
            Object value = evaluator.evaluateCurrent();
            if (value != null
                && !(value instanceof Throwable))
            {
                return true;
            }
        } else {
            // Here we evaluate across all measures just to
            // make sure that the data is all loaded
            boolean found = false;
            for (Member measure : measureSet) {
                evaluator.setContext(measure);
                Object value = evaluator.evaluateCurrent();
                if (value != null
                    && !(value instanceof Throwable))
                {
                    found = true;
                }
            }
            return found;
        }
        return false;
    }
}
