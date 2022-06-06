import java.sql.*;
import java.util.ArrayList;


public class Database {

    private String DB_USERNAME;
    private String DB_PASSWORD;
    private String DB_URL;
    private String DB_NAME;
    private Connection connection;
    private Statement statement;

    public Database(String USERNAME, String PASSWORD, String URL, String NAME) {
        DB_USERNAME = USERNAME;
        DB_PASSWORD = PASSWORD;
        DB_URL = URL;
        DB_NAME = NAME;

        try {
            Class.forName("org.postgresql.Driver"); // connects postgresql driver to java
            Connection connection = DriverManager.getConnection(DB_URL + DB_NAME, DB_USERNAME, DB_PASSWORD);
            statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected void finalize() {
        try {
            statement.close();
//            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String name, String fields) {
        String query = "CREATE TABLE \"" + name + "\"(" + fields + ");";
        System.out.println(query);
        try {
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertRow(String tableName, String values) {
        String query = "INSERT INTO " + tableName + " VALUES (" + values + ");";
        try {
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String findMaxTransaction(int customerID, String tableName) {
        String maxTransaction = null;

        String query = "SELECT MAX(amount) FROM " + tableName + " WHERE customer_id=" + customerID + ";";
// SELECT Max(amount) FROM transactions WHERE customer_id = 39026145;
        System.out.println(query);
        ResultSet result = null;
        try {
            result = statement.executeQuery(query);
            if (result.next()) {
                maxTransaction = result.getString("max");
                System.out.println(maxTransaction);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return maxTransaction;
    }

    public ArrayList<String> findTransactions(int customerID, String tableName) {
        ArrayList<String> transactions = new ArrayList<>();

        String query = "SELECT * FROM " + tableName + " WHERE customer_id=" + customerID + " ORDER BY amount DESC;";
// SELECT * FROM transactions WHERE customer_id = 39026145 ORDER BY amount DESC;
        System.out.println(query);
        ResultSet result = null;
        try {
            result = statement.executeQuery(query);
            ResultSetMetaData metaData = result.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i < columnCount; i++) {
                transactions.add(metaData.getColumnName(i) + ", ");
                System.out.print(metaData.getColumnName(i) + ", ");
            }
            if (columnCount != 0) {
                transactions.add(metaData.getColumnName(columnCount) + "\n");
                System.out.println(metaData.getColumnName(columnCount));
            }
            while (result.next()) {
                for (int i = 1; i < columnCount; i++) {
                    transactions.add(result.getString(i) + ", ");
                    System.out.print(result.getString(i) + ", ");
                }
                if (columnCount != 0) {
                    transactions.add(result.getString(columnCount) + "\n");
                    System.out.println(result.getString(columnCount));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return transactions;
    }

    public String findMostFrequentAbsTrans(int customerID, String tableName) {
        String frequentTrans = null;
        int amount = 0;

        String query = "SELECT * FROM (SELECT abs, COUNT(abs) FROM (SELECT ABS(amount) FROM " + tableName + " WHERE customer_id = " + customerID + ") AS absamount\n" +
                "GROUP BY abs) as x WHERE count =\n" +
                "(SELECT MAX(count) FROM (SELECT abs, COUNT(abs) FROM (SELECT ABS(amount) FROM " + tableName+ " WHERE customer_id = " + customerID + ") AS absamount GROUP BY abs) AS y);";
// SELECT abs, COUNT(abs) FROM (SELECT ABS(amount) FROM transactions WHERE customer_id = 39026145) AS absamount GROUP BY abs ORDER BY count DESC LIMIT 1;
/*
SELECT * FROM (SELECT abs, COUNT(abs) FROM (SELECT ABS(amount) FROM transactions WHERE customer_id = 39026145) AS absamount
GROUP BY abs) as x WHERE count =
(SELECT MAX(count) FROM (SELECT abs, COUNT(abs) FROM (SELECT ABS(amount) FROM transactions WHERE customer_id = 39026145) AS absamount GROUP BY abs) AS y);
*/
        System.out.println(query);
        ResultSet result = null;
        try {
            result = statement.executeQuery(query);
            if (result.next()) {
                frequentTrans = result.getString("abs");
                amount = result.getInt("count");
                System.out.println(frequentTrans + ", amount: " + amount);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return frequentTrans;
    }

}
