package net.toiditimtoi.jdbc.connection;

import net.toiditimtoi.jdbc.BasePostgresSqlTest;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class AcquiringConnectionTest extends BasePostgresSqlTest {

    /**
     * Before Java 6, to start using JDBC, we need to tell which specific JDBC driver we want to use using:
     * Class.forName("fully qualified JDBC driver class name");
     * This is simply to bring relevant classes into current Class Loader context so that the DriverManager can find
     * the JDBC driver later
     * <p>
     * Since Java 6, with the introduction of ServiceLoader, the DriverManager can help resolve the driver
     * and we don't have to load the driver manually
     */
    @Test
    @SuppressWarnings("unused")
    public void loading_the_jdbc_driver() throws Exception {
        var myName = "Kevin";
        var driver = Class.forName("org.postgresql.Driver");
    }

    @Test
    public void opening_a_connection_with_url_only() {
        var invalidCredentialsException = assertThrows(SQLException.class, () -> {
                    try (var myFirstConnection = DriverManager.getConnection(URL)) {
                        System.out.println(myFirstConnection.getAutoCommit());
                    }
                }
        );
        assertFalse(invalidCredentialsException.getSQLState().isBlank());
    }

    /**
     * This is one of the most common ways to open a connection to the DB
     * A URL, a username and a password
     *
     * @throws SQLException indicating any problem during connection acquisition
     */
    @Test
    public void opening_another_connection_successfully_using_url_user_and_password() throws SQLException {
        try (var mySecondConnection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            // check if the connection is still opened and valid to use
            assertTrue(mySecondConnection.isValid(1000));
        }
    }

    /**
     * The third option is to pass a Properties object along with the URL
     * In this particular test, we simply use user and password
     * However depending on the database, it may require additional parameter
     * And depending on our need, we may want to pass additional parameter to customize
     * the connection
     *
     * @throws SQLException sql exception
     */
    @Test
    public void opening_connection_successfully_using_url_and_properties() throws SQLException {
        var userAndPasswordProperties = new Properties();
        userAndPasswordProperties.put("user", USER);
        userAndPasswordProperties.put("password", PASSWORD);
        try (var myThirdConnection = DriverManager.getConnection(URL, userAndPasswordProperties)) {
            assertTrue(myThirdConnection.isValid(1000));
        }
    }

    /**
     * After we are done with the connection. It is essential to close the connection to release the resource on both
     * side: the client (our Java application) and the Database Management System we are connecting to
     * <p>
     * A recommended approach is to use the try-with-resource syntax so that the connection will be automatically closed
     * even when there is a problem with the connection
     *
     * @throws SQLException sql exception
     */
    @Test
    public void closing_the_connection_after_we_are_done() throws SQLException {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            assertTrue(connection.isValid(1000));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            assert connection != null;
            connection.close();
        }
        assertTrue(connection.isClosed());
    }

    /**
     * in this example, we will turn off the auto commit mode on a connection, insert into the DB a new record
     * and close the connection without committing
     * we will see that the record will be discarded
     */
    @Test
    public void configuring_auto_commit_mode() throws SQLException {
        var postName = "Disappear Book";
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            // autocommit is enabled by default
            assertTrue(connection.getAutoCommit());

            // toggle autocommit off to play around with transaction
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            var insertSql = createInsertQuery(postName);
            var rowsAffected = statement.executeUpdate(insertSql);
            assertEquals(1, rowsAffected);
        }
        // after the block above, the connection was closed without calling COMMIT
        // now let's see if the record is still there
        try (var anotherConnection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var statement = anotherConnection.createStatement();
            var query = createSelectQuery(postName);
            try (var resultSet = statement.executeQuery(query)) {
                String firstTitle = null;
                while (resultSet.next()) {
                    firstTitle = resultSet.getString("title");
                }
                assertNull(firstTitle);
            }
        }
    }

    /**
     * The setup is pretty much the same as the previous example. Except that, this time we will commit before closing
     * the connection
     * We will see that the record is available when we use another connection to query it
     */
    @Test
    public void manually_committing_the_query() throws SQLException {
        var postName = "Disappear Book";
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            // autocommit is enabled by default
            assertTrue(connection.getAutoCommit());

            // toggle autocommit off to play around with transaction
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            var insertSql = createInsertQuery(postName);
            var rowsAffected = statement.executeUpdate(insertSql);
            assertEquals(1, rowsAffected);
            connection.commit();
        }
        // after the block above, the connection was closed without calling COMMIT
        // now let's see if the record is still there
        try (var anotherConnection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var statement = anotherConnection.createStatement();
            var query = createSelectQuery(postName);
            try (var resultSet = statement.executeQuery(query)) {
                String firstTitle = null;
                while (resultSet.next()) {
                    firstTitle = resultSet.getString("title");
                }
                assertEquals("Disappear Book", firstTitle);
            }
        }
    }

    @Test
    @SuppressWarnings("always true")
    public void roll_back_a_transaction() throws SQLException {
        Connection connection = null;
        var postName = "Rolling in the deep";
        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            connection.setAutoCommit(false);
            var statement = connection.createStatement();
            var sql = createInsertQuery(postName);
            statement.executeUpdate(sql);
            if (true) {
                throw new RuntimeException("I need another discussion with my wife before doing this");
            }
            // if everything goes well, we should commit the transaction
            connection.commit();
        } catch (Exception anyException) {
            // in case any exception happened, roll back
            assert connection != null;
            connection.rollback();
        } finally {
            assert connection != null;
            String title = null;
            var selectSql = createSelectQuery(postName);
            var stm = connection.createStatement();
            try (var resultSet = stm.executeQuery(selectSql)) {
                while (resultSet.next()) {
                    title = resultSet.getString("title");
                }
                assertNull(title);
                connection.close();
            }
        }
    }

    @Test
    public void prepared_statement() {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var preparedStm = connection.prepareStatement("INSERT INTO post(title, version) values (?, 0)");
            preparedStm.setString(1, "Prepared Statement");
            var rowAffected = preparedStm.executeUpdate();
            assertEquals(1, rowAffected);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void get_metadata() throws SQLException {
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            var metadata = connection.getMetaData();
            assertNotNull(metadata);
        }
    }

    private String createInsertQuery(String postName) {
        return "INSERT INTO post(title, version) values ('%s', 0)".formatted(postName);
    }

    private String createSelectQuery(String postName) {
        return "SELECT * FROM post WHERE title = '%s'".formatted(postName);
    }
}
