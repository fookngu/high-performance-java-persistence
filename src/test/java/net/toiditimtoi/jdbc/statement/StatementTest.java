package net.toiditimtoi.jdbc.statement;

import net.toiditimtoi.jdbc.BasePostgresSqlTest;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * java.sql.Statement interface is used to execute queries against Relational Database
 * Statement is obtained from a java.sql.Connection
 * Statement needs to be closed when we're done with it
 *
 * A similarity of Statement can be PreparedStatement
 * A PreparedStatement can have some parameters inserted into the SQL, so that it can be reused many times with
 * the actual value changed
 * The Statement, on the other hand, requires a complete SQL statement.
 */
public class StatementTest extends BasePostgresSqlTest {
    @Test
    public void creating_using_and_closing_statement() throws SQLException {
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {

            // using try-with-resource is always the recommended way to safely close the statement
            try (var statement = connection.createStatement()) {
                var selectSql = "SELECT * FROM post";
                try (var result = statement.executeQuery(selectSql)) {
                    var resultCount = 0;
                    while (result.next()) {
                        ++resultCount;
                    }
                    assertEquals(3, resultCount);
                }
            }
        }
    }
}
