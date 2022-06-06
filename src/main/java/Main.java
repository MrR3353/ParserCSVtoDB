import java.util.ArrayList;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) {
        final String DB_USERNAME = "postgres";
        final String DB_PASSWORD = "admin";
        final String DB_URL = "jdbc:postgresql://localhost:5432/";
        final String DB_NAME = "ex2";
        final int LIMIT = 10000;

        HashMap<String, String> separatorInFile = new HashMap<>();
        {    // static initializer
            separatorInFile.put("gender_train.csv", ",");
            separatorInFile.put("gender_train_cut.csv", ",");
            separatorInFile.put("transactions.csv", ",");
            separatorInFile.put("transactions_cut.csv", ",");
            separatorInFile.put("tr_mcc_codes.csv", ";");
            separatorInFile.put("tr_types.csv", ";");
        };

        HashMap<String, String> fieldNameType = new HashMap<>();  // types for DB
        {    // static initializer
            fieldNameType.put("customer_id", "integer");
            fieldNameType.put("gender", "smallint");
            fieldNameType.put("tr_type", "integer");
            fieldNameType.put("tr_description", "varchar(150)");
            fieldNameType.put("mcc_code", "smallint");
            fieldNameType.put("mcc_description", "varchar(200)");
            fieldNameType.put("tr_datetime", "varchar(20)");
            fieldNameType.put("amount", "numeric(15,2)");
            fieldNameType.put("term_id", "varchar(15)");
        };

        String folderPath = "C:\\Users\\mishi\\OneDrive\\Рабочий стол\\Work\\Files\\Новая папка\\";
        ArrayList<String> fileNames = null;
        try {
            fileNames = CSV.getCSVFileNames(folderPath);
            if (fileNames.size() == 0) {
                throw new Exception("No csv files in that directory");
            }
            for (String fileName: fileNames) {
                System.out.println(fileName);
            }
            System.out.println("Found: " + fileNames.size() + " .csv files");
        } catch (Exception e) { // invalid folderPath
            e.printStackTrace();
        }

        Database db = new Database(DB_USERNAME, DB_PASSWORD, DB_URL, DB_NAME);

        for (String fileName: fileNames) {
            CSV.readCSVAndWriteDB(db, folderPath + fileName, separatorInFile.get(fileName), fieldNameType, LIMIT);
        }
        String maxTransaction = db.findMaxTransaction(39026145, "transactions");
        CSV.write("max.csv", maxTransaction);

        String frequentTransaction = db.findMostFrequentAbsTrans(39026145, "transactions");
        CSV.write("frequent.csv", frequentTransaction);

        ArrayList<String> transactions = db.findTransactions(39026145, "transactions");
        CSV.write("transactionsDESC.csv", transactions);

        db.finalize();
    }
}
