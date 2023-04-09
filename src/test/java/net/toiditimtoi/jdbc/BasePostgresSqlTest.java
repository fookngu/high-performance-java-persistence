package net.toiditimtoi.jdbc;

import org.junit.jupiter.api.AfterEach;

import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class BasePostgresSqlTest {
    protected final String USER = "postgres";
    protected final String PASSWORD = "mysecretpassword";
    protected final String URL = "jdbc:postgresql://localhost:5433/dummy";

    protected final String POST_TABLE_NAME = "post";

    @AfterEach
    public void restoreSnapshot() throws SQLException {
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var truncateSql = "TRUNCATE %s".formatted(POST_TABLE_NAME);
            var stm = connection.createStatement();
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
