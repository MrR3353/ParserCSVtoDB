import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class CSV {

    public static void readCSVAndWriteDB(Database db, String path, String splitBy, HashMap<String, String> fieldNameType, int limit) {
        String line = "";
        String tableName = path.substring(path.lastIndexOf('\\') + 1, path.lastIndexOf('.'));

        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            String[] fieldNames = null;
            if ((line = br.readLine()) != null) {   // work with 1st line of csv, it's contains table names
                fieldNames = line.split(splitBy);
                StringBuilder fieldsWithTypes = new StringBuilder(); // string for query which creating table
                for (String field: fieldNames) {    // add types after field names
                    fieldsWithTypes.append(field).append(" ").append(fieldNameType.get(field)).append(", ");
                }
                fieldsWithTypes.delete(fieldsWithTypes.length() - 2, fieldsWithTypes.length()); // delete last comma from string
                db.createTable(tableName, fieldsWithTypes.toString());
                System.out.println("Table " + tableName + " created!");
            }

            while ((line = br.readLine()) != null && limit > 0) {    // work with other lines
                limit--;
                String[] data = line.split(splitBy);
                StringBuilder values = new StringBuilder(); // values for insert query in DB
                for (int i = 0; i < fieldNames.length; i++) {   // loop creates valid values for update query
                    if (data.length > i) { // if in csv file skipped last column
                        if (fieldNameType.get(fieldNames[i]).startsWith("varchar")) { // need add quotes for string types
                            values.append("'").append(data[i]).append("', ");
                        } else {
                            values.append(data[i]).append(", "); // just leave as is
                        }
                    } else {
                        break;
                    }
                }
                values.delete(values.length() - 2, values.length());
                db.insertRow(tableName, values.toString());
            }
            br.close();
        } catch (FileNotFoundException e) {
            System.out.println("Invalid file path!");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Error in csv file");
            e.printStackTrace();
        }
    }

    public static ArrayList<String> getCSVFileNames(String folderPath) throws Exception {
        ArrayList<String> fileNames = new ArrayList<>();
        File folder = new File(folderPath);
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file: files) {
                if (file.getName().contains(".csv")) {
                    fileNames.add(file.getName());
                }
            }
            return fileNames;
        } else {
            throw new Exception("No such directory");
        }
    }

    public static void write(String fileName, String data) {
        try {
            PrintWriter writer = new PrintWriter(new File(fileName));
            writer.write(data);
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void write(String fileName, ArrayList<String> data) {
        try {
            PrintWriter writer = new PrintWriter(new File(fileName));
            for (String s: data) {
                writer.write(s);
            }
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
