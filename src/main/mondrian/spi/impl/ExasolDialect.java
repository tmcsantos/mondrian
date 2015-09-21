package mondrian.spi.impl;

import mondrian.rolap.SqlStatement;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class ExasolDialect extends JdbcDialectImpl {

    private final String escapeRegexp = "(\\\\Q([^\\\\Q]+)\\\\E)";
    private final Pattern escapePattern = Pattern.compile(escapeRegexp);

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            ExasolDialect.class,
            DatabaseProduct.EXASOL)
        {
            protected boolean acceptsConnection(Connection connection) {
                return isDatabase(DatabaseProduct.EXASOL, connection);
            }
        };

    public ExasolDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.EXASOL;
    }

    public boolean allowsRegularExpressionInWhereClause() {
        return true;
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
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        return
            generateOrderByNullsAnsi(
                expr,
                ascending,
                collateNullsLast);
    }

    public boolean requiresAliasForFromQuery() {
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
    public boolean supportsGroupingSets() {
        return true;
    }

    @Override
    public boolean supportsMultiValueInExpr() {
        return true;
    }

    @Override
    public boolean supportsUnlimitedValueList() {
        return true;
    }

        @Override
    public SqlStatement.Type getType(
            ResultSetMetaData metaData, int columnIndex)
            throws SQLException
    {
        final int columnType = metaData.getColumnType(columnIndex + 1);

        SqlStatement.Type internalType = null;
        if (columnType == Types.NUMERIC || columnType == Types.DECIMAL) {
            final int precision = metaData.getPrecision(columnIndex + 1);
            final int scale = metaData.getScale(columnIndex + 1);
            if (scale == 0 && precision <= 18) {
                internalType = SqlStatement.Type.INT;
            } else if (scale == 0 && precision <= 36) {
                internalType = SqlStatement.Type.LONG;
            } else {
                internalType = SqlStatement.Type.DOUBLE;
            }
        } else {
            internalType = super.getType(metaData, columnIndex);
        }
        return internalType;
    }

}
