package net.toiditimtoi.jdbc.update;

import net.toiditimtoi.jdbc.BasePostgresSqlTest;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Updating the DB consists of two types of query: updating the data and deleting some rows
 * Instead of using Statement.executeQuery(), we use Statement.executeUpdate()
 * The result returned indicate how many rows was updated/deleted by the query
 */
public class UpdateTheDatabaseTest extends BasePostgresSqlTest {
    @Test
    public void update_existing_record_value() throws SQLException {
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var statement = connection.createStatement();
            var updateSql = "UPDATE post set version = %d WHERE title = '%s'".formatted(1, "Advanced Java Performance");
            var rowsAffected = statement.executeUpdate(updateSql);
            assertEquals(1, rowsAffected);
        }
    }

    @Test
    public void delete_records() throws SQLException {
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var statement = connection.createStatement();
            var updateSql = "DELETE FROM post WHERE title = '%s' AND version = %d".formatted("Advanced Java Performance", 0);
            var rowsAffected = statement.executeUpdate(updateSql);
            assertEquals(1, rowsAffected);
        }
    }

    @Test
    public void insert_some_new_records() throws SQLException {
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var statement = connection.createStatement();
            var insertSql = "INSERT INTO post(title, version) values ('Wine Collection', 0)";
            var rowInserted = statement.executeUpdate(insertSql);
            assertEquals(1, rowInserted);
        }
    }
}
