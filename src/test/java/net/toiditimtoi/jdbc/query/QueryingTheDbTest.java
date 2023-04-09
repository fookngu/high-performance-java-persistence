package net.toiditimtoi.jdbc.query;

import net.toiditimtoi.jdbc.BasePostgresSqlTest;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryingTheDbTest extends BasePostgresSqlTest {

    @Test
    public void execute_normal_query_and_get_back_result_set() throws SQLException {
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            var query = "SELECT * FROM %s".formatted(POST_TABLE_NAME);
            var resultSet = statement.executeQuery(query);
            // the ResultSet contains the result of the query
            // the result is returned in rows with columns of data
            // we traverse the result set and obtain the data like this
            var resultCount = 0;
            while (true) {
                // before calling ResultSet.next(), the cursor is before the first row
                // calling resultSet.next() will move the cursor to the next row (if any).
                // if the cursor moved, the method returns true, otherwise return false
                var availableRow = resultSet.next();
                if (!availableRow) break;
                var id = resultSet.getInt("id");
                var title = resultSet.getString("title");
                var version = resultSet.getInt("version");
                System.out.printf("Id: %s, title: %s, version: %d%n", id, title, version);
                ++resultCount;
            }

            assertTrue(resultCount > 0);

            // another way to access the value of a column is to use the index of the column instead. (0-based)
            // but first, let's move the cursor to the first row
            resultSet.first();
            resultCount = 0;
            do {
                var id = resultSet.getInt(1);
                var title = resultSet.getString(2);
                var version = resultSet.getInt(3);
                System.out.printf("Id: %d, title: %s, version: %d%n", id, title, version);
                ++resultCount;
            } while (resultSet.next());
            assertTrue(resultCount > 0);

            // When iterating a large result set, it is recommended to obtain column value by using index
            // first, let's once again jump back to the first row
            resultSet.first();
            resultCount = 0;

            // The index of the column can be found by
            var indexOfId = resultSet.findColumn("id");
            var indexOfTitle = resultSet.findColumn("title");
            var indexOfVersion = resultSet.findColumn("version");
            do {
                var id = resultSet.getInt(indexOfId);
                var title = resultSet.getString(indexOfTitle);
                var version = resultSet.getInt(indexOfVersion);
                System.out.printf("Id: %d, title: %s, version: %d%n", id, title, version);
                ++resultCount;
            } while (resultSet.next());
            assertTrue(resultCount > 0);

            // it is important to close ResultSet when we're done with it
            resultSet.close();
        }
    }
}
