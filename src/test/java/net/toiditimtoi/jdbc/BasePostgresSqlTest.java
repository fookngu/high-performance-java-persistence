package net.toiditimtoi.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class BasePostgresSqlTest {
    protected static final String USER = "postgres";
    protected static final String PASSWORD = "mysecretpassword";
    protected static final String URL = "jdbc:postgresql://localhost:5433/dummy";

    protected static final String POST_TABLE_NAME = "post";

    @AfterEach
    public void restoreSnapshot() throws SQLException {
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var truncateSql = "TRUNCATE %s RESTART IDENTITY".formatted(POST_TABLE_NAME);
            var createTableSqlString = "CREATE TABLE IF NOT EXISTS %s(id serial not null , title varchar(255), version int)".formatted(POST_TABLE_NAME);
            try (var stm = connection.createStatement()) {
                // create the table if not exists
                stm.executeUpdate(createTableSqlString);

                // truncate the table, reset the sequence
                stm.executeUpdate(truncateSql);

                // insert some records into the DB
                var insertQuery = """
                        INSERT INTO post(title, version) values
                        ('Hypersistence', 0),
                        ('Advanced Java Performance', 0),
                        ('Getting Oracle Certified Programmer', 0);
                        """;
                stm.executeUpdate(insertQuery);
            }
        }
    }
}
