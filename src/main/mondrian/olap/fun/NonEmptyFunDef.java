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

                Member member = null;
                if (memberCalc != null) {
                    member = memberCalc.evaluateMember(evaluator);
                }

                final TupleList list1 = listCalc.evaluateList(evaluator);

                // remove any remaining empty crossings from the result
                return nonEmptyList(evaluator, list1, member);
            }
        };
    }

    protected TupleList nonEmptyList(
        Evaluator evaluator,
        TupleList list,
        Member member)
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
                if (checkData(member, evaluator)) {
                    result.addCurrent(cursor);
                }
            }
            return result;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    private static boolean checkData(
        Member member,
        Evaluator evaluator)
    {
        // no measures found, use standard algorithm
        if (member == null) {
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
            evaluator.setContext(member);
            Object value = evaluator.evaluateCurrent();
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
