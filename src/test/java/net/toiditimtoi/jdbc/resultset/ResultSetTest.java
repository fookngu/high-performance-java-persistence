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
                    assertThrows(SQLException.class, () -> printCurrentRow(resultSet));
                }
            }
        }
    }

    private static void printCurrentRow(ResultSet resultSet) throws SQLException {
        var currentPos = resultSet.getRow();
        var id = resultSet.getInt("id");
        var title = resultSet.getString("title");
        var version = resultSet.getInt("version");
        System.out.printf("Current row (%s): ID= %s, Title= %s, Version= %s%n", currentPos, id, title, version);
    }
}
