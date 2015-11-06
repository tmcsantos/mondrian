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
            "NonEmpty(<Set1>,[<Set2>])",
            "Returns the set of tuples that are not empty from a specified set.",
            new String[] {"fxx","fxxm", "fxxI"},
            NonEmptyFunDef.class);

    public NonEmptyFunDef(FunDef dummyFunDef)
    {
        super(dummyFunDef);
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final Calc arg1;
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        if (call.getArgs().length == 2) {
            arg1 = compiler.compileScalar(call.getArg(1), true);
        } else {
            arg1 = null;
        }

        return new AbstractListCalc(call, new Calc[]{listCalc, arg1}) {
            public TupleList evaluateList(Evaluator evaluator) {
                SchemaReader schemaReader = evaluator.getSchemaReader();

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
                    return
                        (TupleList) nativeEvaluator.execute(
                            ResultStyle.LIST);
                }

                final TupleList list1 = listCalc.evaluateList(evaluator);

                // remove any remaining empty crossings from the result
                return nonEmptyList(evaluator, list1, arg1);
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                return butDepends(getCalcs(), hierarchy);
            }
        };
    }

    protected TupleList nonEmptyList(
        Evaluator evaluator,
        TupleList list,
        Calc calc)
    {
        if (list.isEmpty()) {
            return list;
        }

        TupleList result =
            TupleCollections.createList(
                list.getArity(), list.size() >> 1);

        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setNonEmpty(false);
            final TupleCursor cursor = list.tupleCursor();
            while (cursor.forward()) {
                cursor.setContext(evaluator);
                if (checkData(calc, evaluator)) {
                    result.addCurrent(cursor);
                }
            }
            return result;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    private static boolean checkData(
        Calc calc,
        Evaluator evaluator)
    {
        // no measures found, use standard algorithm
        if (calc == null) {
            Object value = evaluator.evaluateCurrent();
            if (!Util.isNull(value)
                && !(value instanceof Throwable))
            {
                return true;
            }
        } else {
            // Here we evaluate across all measures just to
            // make sure that the data is all loaded
            boolean found = false;
            Object value = calc.evaluate(evaluator);
            if (!Util.isNull(value)
                && !(value instanceof Throwable))
            {
                found = true;
            }
            return found;
        }
        return false;
    }
}
