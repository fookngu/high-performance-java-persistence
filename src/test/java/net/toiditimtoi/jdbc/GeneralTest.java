package net.toiditimtoi.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.*;

public class GeneralTest {
    private static final String url = "jdbc:postgresql://localhost:5433/dummy";
    private static final String userName = "postgres";
    private static final String password = "mysecretpassword";

    @Test
    public void summaryExample() throws SQLException {
        // before doing any interaction with the database, we need to establish a connection to the database
        try(Connection connection = DriverManager.getConnection(url, userName, password)) {
            try(Statement statement = connection.createStatement()) {
                String insertStm = """
                        insert into post(title, version) values
                        ('Hypersistence', 0),
                        ('Advanced Java Performance', 0);
                        """;
                int rowAffected = statement.executeUpdate(insertStm);
                System.out.println(rowAffected);
                String sql = "select * from post";
                try(ResultSet result = statement.executeQuery(sql)) {
                    while(result.next()) {
                        int id = result.getInt("id");
                        String title = result.getString("title");
                        int version = result.getInt("version");
                        String output = String.format("Id = %d, title = %s, version = %d", id, title, version);
                        System.out.println(output);
                    }
                }
            }
        }
    }
}
