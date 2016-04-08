/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractIntegerCalc;
import mondrian.mdx.*;
import mondrian.olap.*;

/**
 * Definition of the <code>Count</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class CountFunDef extends AbstractAggregateFunDef {
    static final String[] ReservedWords =
        new String[] {"INCLUDEEMPTY", "EXCLUDEEMPTY"};

    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Count",
            "Count(<Set>[, EXCLUDEEMPTY | INCLUDEEMPTY])",
            "Returns the number of tuples in a set, empty cells included unless the optional EXCLUDEEMPTY flag is used.",
            new String[]{"fnx", "fnxy"},
            CountFunDef.class,
            ReservedWords);

    public CountFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    @Override
    protected Exp validateArg(Validator validator, Exp[] args, int i, int category) {
        Exp exp = super.validateArg(validator, args, i, category);

        if(i == 0){
            // Wrap set to be counted with NonEmpty function if EXCLUDEEMPTY was passed
            // Count(<set>, EXCLUDEEMPTY) -> Count(NonEmpty(<set>))
            // Count(<set>, INCLUDEEMPTY) -> Count(<set>)
            // By doing this, the behaviour from here will always be the "INCLUDEEMPTY" one
            if(args.length == 2 && ((Literal)args[1]).getValue().equals("EXCLUDEEMPTY")){
                UnresolvedFunCall newExp = new UnresolvedFunCall(
                    NonEmptyFunDef.NAME,
                    Syntax.Function,
                    new Exp[]{exp});
                exp = validator.validate(newExp, false);
            }
        }

        return exp;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Calc calc =
            compiler.compileAs(
                call.getArg(0), null, ResultStyle.ITERABLE_ANY);
        return new AbstractIntegerCalc(
            call,
            new Calc[] {calc})
        {
            public int evaluateInteger(Evaluator evaluator) {
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    final int count;
                    if (calc instanceof IterCalc) {
                        IterCalc iterCalc = (IterCalc) calc;
                        TupleIterable iterable = evaluateCurrentIterable(iterCalc, evaluator);
                        count = count(evaluator, iterable, true);
                    } else {
                        // must be ListCalc
                        ListCalc listCalc = (ListCalc) calc;
                        TupleList list = evaluateCurrentList(listCalc, evaluator);
                        count = count(evaluator, list, true);
                    }
                    return count;
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                return super.dependsOn(hierarchy);
            }
        };
    }
}

// End CountFunDef.java
