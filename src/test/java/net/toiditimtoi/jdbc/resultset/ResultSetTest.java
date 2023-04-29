package net.toiditimtoi.jdbc.resultset;

import net.toiditimtoi.jdbc.BasePostgresSqlTest;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Diving deeper into ResultSet, we will see that there are more than the usual-simple use of ResultSet.
 *
 */
public class ResultSetTest extends BasePostgresSqlTest {

    @Test
    public void test_result_set_scrollability() throws SQLException {
        try(var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            // can navigate forward, backward, jump to first, last, absolute position, relative position from the current position
            try(var statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
                var query = "SELECT * FROM post";
                try (var resultSet = statement.executeQuery(query)) {
                    System.out.println("Jumping to the last row.");
                    var lastRow = resultSet.last();
                    if (lastRow) {
                        printCurrentRow(resultSet);
                    }

                    System.out.println("Jumping to the first row.");
                    var firstRow = resultSet.first();
                    if (firstRow) {
                        printCurrentRow(resultSet);
                    }

                    System.out.println("Jumping to 1 row after current row.");
                    resultSet.relative(1);
                    printCurrentRow(resultSet);

                    System.out.println("Jumping to row at index 1.");
                    resultSet.absolute(1);
                    printCurrentRow(resultSet);

                    System.out.println("Jumping before the first row.");
                    resultSet.beforeFirst();
                    assertThrows(SQLException.class, () -> printCurrentRow(resultSet));

                    System.out.println("Jumping after the last row.");
                    resultSet.afterLast();
                    assertThrows(SQLException.class, () -> printCurrentRow(resultSet));
                }
            }
        }
    }

    /**
     * Here we see what the SENSITIVE & CONCUR_UPDATABLE mean to the result set
     * SENSITIVE means the result is sensitive to the changes in the database
     * To be able for the result set to get the changes from the database, we also have to use ResultSet.CONCUR_UPDATABLE
     * Otherwise the result set is immutable
     * This demonstrates the result I got
     * > Task :test
     * Current row (1): ID= 1, Title= Hypersistence, Version= 0
     * Current row (2): ID= 2, Title= Advanced Java Performance, Version= 0
     * Current row (3): ID= 3, Title= Getting Oracle Certified Programmer, Version= 0
     * Sleep for 10 seconds
     * Now jump back to before the first row, go through each row and reload
     * Current row (1): ID= 1, Title= Hypersistence 2nd, Version= 1
     * Current row (2): ID= 2, Title= Advanced Java Performance 2nd, Version= 1
     * Current row (3): ID= 3, Title= Getting Oracle Certified Programmer 3rd, Version= 1
     * BUILD SUCCESSFUL in 10s
     * As we can see, the result set is now updated with the latest change from the database.
     * The resultSet.refreshRow() refreshes the current row only
     * To be able to refresh row, JDBC needs to determine the primary of the table.
     * So your table must have an identity column/primary key
     */
    @Test
    public void result_set_sensitivity_to_changes() throws SQLException, InterruptedException {
        try(var conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            try(var statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
                var query = "SELECT * from %s".formatted(POST_TABLE_NAME);
                try(var resultSet = statement.executeQuery(query)) {
                    while (resultSet.next()) {
                        printCurrentRow(resultSet);
                    }
                    // wait for 20 seconds and then refresh the result set
                    // in the meantime, try changing the data in the database and see if it really gets the change updated here
                    System.out.println("Sleep for 10 seconds");
                    Thread.sleep(10_000);

                    System.out.println("Now jump back to before the first row, go through each row and reload");
                    resultSet.beforeFirst();
                    while(resultSet.next()) {
                        resultSet.refreshRow();
                        printCurrentRow(resultSet);
                    }
                }
            }
        }
    }

    // a dummy function just to reset the DB
    @Test
    public void reset_db() {

    }

    private static void printCurrentRow(ResultSet resultSet) throws SQLException {
        if (resultSet.isBeforeFirst() || resultSet.isAfterLast()) {
            return;
        }
        var currentPos = resultSet.getRow();
        var id = resultSet.getInt("id");
        var title = resultSet.getString("title");
        var version = resultSet.getInt("version");
        System.out.printf("Current row (%s): ID= %s, Title= %s, Version= %s%n", currentPos, id, title, version);
    }
}
