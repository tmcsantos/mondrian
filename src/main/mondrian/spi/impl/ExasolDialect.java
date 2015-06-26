package mondrian.spi.impl;

import mondrian.rolap.SqlStatement;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class ExasolDialect extends JdbcDialectImpl {

    private final String escapeRegexp = "(\\\\Q([^\\\\Q]+)\\\\E)";
    private final Pattern escapePattern = Pattern.compile(escapeRegexp);

    public ExasolDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public boolean allowsJoinOn() {
        return true;
    }

    @Override
    public boolean allowsRegularExpressionInWhereClause() {
        return true;
    }

    @Override
    public boolean allowsOrderByAlias() {
        return true;
    }

    @Override
    public boolean allowsSelectNotInGroupBy() {
        return false;
    }

    @Override
    public String generateInline(List<String> columnNames, List<String> columnTypes, List<String[]> valueList) {
        return generateInlineGeneric(columnNames, columnTypes, valueList, " from dual", false);
    }

    @Override
    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.EXASOL;
    }

    @Override
    public boolean requiresOrderByAlias() {
        return false;
    }

    @Override
    public String generateRegularExpression(
            String source,
            String javaRegex)
    {
        try {
            Pattern.compile(javaRegex);
        } catch (PatternSyntaxException e) {
            // Not a valid Java regex. Too risky to continue.
            return null;
        }
        final Matcher escapeMatcher = escapePattern.matcher(javaRegex);
        while (escapeMatcher.find()) {
            javaRegex =
                    javaRegex.replace(
                            escapeMatcher.group(1),
                            escapeMatcher.group(2));
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(source);
        sb.append(" REGEXP_LIKE ");
        quoteStringLiteral(sb, javaRegex);
        return sb.toString();
    }

    @Override
    public SqlStatement.Type getType(
            ResultSetMetaData metaData, int columnIndex)
            throws SQLException
    {

        final int columnType = metaData.getColumnType(columnIndex + 1);
        final int precision = metaData.getPrecision(columnIndex + 1);
        final int scale = metaData.getScale(columnIndex + 1);
        final String columnName = metaData.getColumnName(columnIndex + 1);
        SqlStatement.Type type;

        if (columnType == Types.NUMERIC || columnType == Types.DECIMAL) {
            if (scale == -127 && precision != 0) {
                // non zero precision w/ -127 scale means float in Oracle.
                type = SqlStatement.Type.DOUBLE;
            } else if (columnType == Types.NUMERIC
                    && (scale == 0 || scale == -127)
                    && precision == 0 && columnName.startsWith("m"))
            {
                // In GROUPING SETS queries, Oracle
                // loosens the type of columns compared to mere GROUP BY
                // queries. We need integer GROUP BY columns to remain integers,
                // otherwise the segments won't be found; but if we convert
                // measure (whose column names are like "m0", "m1") to integers,
                // data loss will occur.
                type = SqlStatement.Type.OBJECT;
            } else if (scale == -127 && precision ==0) {
                type = SqlStatement.Type.INT;
            } else if (scale == 0 && (precision == 38 || precision == 0)) {
                // NUMBER(38, 0) is conventionally used in
                // Oracle for integers of unspecified precision, so let's be
                // bold and assume that they can fit into an int.
                type = SqlStatement.Type.INT;
            } else if (scale == 0 && precision <= 9) {
                // An int (up to 2^31 = 2.1B) can hold any NUMBER(10, 0) value
                // (up to 10^9 = 1B).
                type = SqlStatement.Type.INT;
            } else {
                type = SqlStatement.Type.DOUBLE;
            }

        } else {
            type = super.getType(metaData, columnIndex);
        }
        return type;
    }



    @Override
    public boolean supportsGroupingSets() {
        return true;
    }

    @Override
    public boolean supportsMultiValueInExpr() {
        return false;
    }

    @Override
    public boolean supportsUnlimitedValueList() {
        return true;
    }
}
