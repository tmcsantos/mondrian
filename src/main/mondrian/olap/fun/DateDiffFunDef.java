/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2015 Pentaho and others
*/package mondrian.olap.fun;

import groovy.time.*;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractIntegerCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import org.apache.commons.lang.time.DateUtils;

import java.util.*;

/**
 * Calculates the time between two dates.
 *
 * <p>Syntax: 
 * <blockquote><code>DateDiff(&lt;datepart&gt;, &lt;startdate&gt;, &lt;enddate&gt;)</code></blockquote>
 *
 * where startdate and enddate are valid date member and datepart is one of the following:
 *
 * | datepart | Abbreveation |
 * |----------+--------------|
 * | year     | y            |
 * | quarter  | q            |
 * | month    | m            |
 * | day      | d            |
 *
 * @author tsantos
 * @since January 15, 2015
 */
class DateDiffFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "DateDiff",
            "DateDiff(interval, date1, date2)",
            "Returns a Variant (Long) specifying the number of time intervals "
                + "between two specified dates.",
            new String[] {"fnSmm", "fnSDD", "fnSDm", "fnSmD"},
            DateDiffFunDef.class);

    public DateDiffFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final StringCalc intervalCalc = compiler.compileString(call.getArg(0));
        final Calc leftDateCalc = compiler.compile(call.getArg(1));
        final Calc rightDateCalc = compiler.compile(call.getArg(2));
        return new DateDiffIntegerCalcImpl(
            call, intervalCalc, leftDateCalc, rightDateCalc);
    }

    private static class DateDiffIntegerCalcImpl extends AbstractIntegerCalc {
        private enum Interval {
            Year("y"),
            Quarter("q"),
            Month("m"),
            Day("d");

            private final String name;

            Interval(String name) {
                this.name = name;
            }

            String getName() {
                return name;
            }

            public static Interval find(String name) {
                for (Interval i : Interval.values()) {
                    if (i.name.equals(name)
                        || i.name().toLowerCase().equals(name.toLowerCase())) {
                        return i;
                    }
                }
                return null;
            }
        }

        private enum CalcType {
            DateTimeType{
                Calendar getDate(Evaluator evaluator, Calc calc) {
                    Date date =
                        ((DateTimeCalc) calc).evaluateDateTime(evaluator);
                    return DateUtils.toCalendar(
                        DateUtils.truncate(date, Calendar.DAY_OF_MONTH));
                }
            },
            MemberType{
                Calendar getDate(Evaluator evaluator, Calc calc) {
                    Member member =
                        ((MemberCalc) calc).evaluateMember(evaluator);

                    Calendar date =
                        DateUtils.truncate(
                            Calendar.getInstance(),
                            Calendar.DAY_OF_MONTH);
                    // set to first day
                    date.set(Calendar.DAY_OF_MONTH, 1);
                    date.set(Calendar.MONTH, Calendar.JANUARY);

                    final SchemaReader schemaReader =
                        member.getDimension().getSchema().getSchemaReader();
                    final ArrayList<Member> ancestorList =
                        new ArrayList<Member>();
                    schemaReader.getMemberAncestors(member, ancestorList);
                    ancestorList.add(member);

                    for(Member parent : ancestorList) {
                        if (parent.isAll()) {
                            continue;
                        }
                        switch (parent.getLevel().getLevelType()){
                            case TimeYears:
                                date.set(
                                    Calendar.YEAR,
                                    Integer.parseInt(parent.getName(), 10));
                                break;
                            case TimeWeeks:
                                date.set(Calendar.WEEK_OF_YEAR,
                                    Integer.parseInt(parent.getName(), 10));
                                break;
                            case TimeQuarters:
                                int quarter;
                                try {
                                    quarter = Integer.parseInt(parent.getName());
                                } catch (NumberFormatException ignore) {
                                    // not a numeric format
                                    // try CN where C = char, N = number
                                    quarter = Integer.parseInt(
                                        parent.getName().substring(1), 10);
                                }
                                date.set(
                                    Calendar.MONTH,
                                    3 * (quarter - 1));
                                break;
                            case TimeMonths:
                                date.set(
                                    Calendar.MONTH,
                                    Integer.parseInt(parent.getName(), 10) - 1);
                                break;
                            case TimeDays:
                                date.set(
                                    Calendar.DAY_OF_MONTH,
                                    Integer.parseInt(parent.getName(), 10));
                                break;
                        }
                    }

                    return date;
                }
            };

            abstract Calendar getDate(Evaluator evaluator, Calc calc);
        }

        private final StringCalc intervalCalc;
        private final Calc leftCalc;
        private final Calc rightCalc;

        public DateDiffIntegerCalcImpl(
            final ResolvedFunCall call,
            StringCalc intervalCalc,
            Calc leftCalc,
            Calc rightCalc)
        {
            super(call, new Calc[] {intervalCalc, leftCalc, rightCalc});
            this.intervalCalc = intervalCalc;
            this.leftCalc = leftCalc;
            this.rightCalc = rightCalc;
        }

        public int evaluateInteger(Evaluator evaluator) {
            Interval interval = Interval.find(
                intervalCalc.evaluateString(evaluator));

            Calendar leftDate = CalcType.valueOf(
                leftCalc.getType().getClass().getSimpleName())
                .getDate(evaluator, leftCalc);

            Calendar rightDate = CalcType.valueOf(
                rightCalc.getType().getClass().getSimpleName())
                .getDate(evaluator, rightCalc);

            return getDiff(interval, rightDate, leftDate);
        }

        private int getDiff(Interval interval, Calendar left, Calendar right) {
            int quarterBetween;
            int yearBetween;
            int monthBetween;
            TimeDuration duration;
            switch (interval) {
                case Year:
                    return left.get(Calendar.YEAR) - right.get(Calendar.YEAR);
                case Quarter:
                    quarterBetween =
                        ((left.get(Calendar.MONTH) / 3))
                            - ((right.get(Calendar.MONTH) / 3));
                    yearBetween =
                        left.get(Calendar.YEAR)
                            - right.get(Calendar.YEAR);
                    return quarterBetween + yearBetween * 4;
                case Month:
                    monthBetween = left.get(Calendar.MONTH)
                        - right.get(Calendar.MONTH);
                    yearBetween = left.get(Calendar.YEAR)
                        - right.get(Calendar.YEAR);
                    return monthBetween + yearBetween * 12;
                case Day:
                    duration = TimeCategory.minus(
                        left.getTime(),
                        right.getTime());
                    return duration.getDays();
            }
            return 0;
        }
    }
}