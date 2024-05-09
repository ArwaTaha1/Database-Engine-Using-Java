package db;
import java.io.BufferedReader;
import java.io.*;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
public class Metadata 

 {
    private static final String METADATA_FILE_PATH = "metadata.csv";
    private static final String TRUE = "True";
    private static final String NULL = "null";
    
    
    
    public Metadata( ) {
    	createFile();
    }
    
    public void createFile() {
		try {
			FileOutputStream fileOut = new FileOutputStream("metadata.csv", false);
			
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	
    }

    
   
    public static void writeMetadata(String tableName,String strClusteringKeyColumn, Hashtable<String, String> metadata) throws IOException {
            try (FileWriter writer = new FileWriter("metadata.csv", true)) {
                // Write metadata to file
            	int count = 0;
                for (String columnName : metadata.keySet()) {
                	count++;
                    String columnType = metadata.get(columnName);
                    String isClusteringKey;
                    if(columnName.equals(strClusteringKeyColumn)) {
                    	isClusteringKey = "True";
                    }
                    else {
                    	isClusteringKey = "False";
                    }
                    String indexName = metadata.containsKey(columnName + "_index") ? metadata.get(columnName + "_index") : "null";
                    String indexType = metadata.containsKey(columnName + "_indexType") ? metadata.get(columnName + "_indexType") : "null";
                    
                    writer.append(tableName).append(",")
                          .append(columnName).append(",")
                          .append(columnType).append(",")
                          .append(isClusteringKey).append(",")
                          .append(indexName).append(",")
                          .append(indexType).append("\n");
                }
                
            }
        }
    
   // public void addMetaData()
    public Table loadTable(String tableName) throws DBAppException {
        Vector<Hashtable<String, String>> metadata = loadMetadata();

        Vector<Hashtable<String, String>> tableColumns = new Vector<>();

        for (Hashtable<String, String> row : metadata) {
            if (row.get("TableName").equals(tableName)) {
                tableColumns.add(row);
            }
        }

        if (tableColumns.isEmpty()) {
            throw new DBAppException("Table " + tableName + " not found");
        }

        Hashtable<String, String> columnTypes = new Hashtable<>();
        Hashtable<String, String> columnIndexName = new Hashtable<>();
        Hashtable<String, String> columnIndexType = new Hashtable<>();

        String clusteringKey = "";

        for (Hashtable<String, String> tableColumn : tableColumns) {
            String columnName = tableColumn.get("ColumnName");
            String columnType = tableColumn.get("ColumnType");
            String indexName = tableColumn.get("IndexName");
            String indexType = tableColumn.get("IndexType");
            String isClusteringKey = tableColumn.get("ClusteringKey");

            columnTypes.put(columnName, columnType);
            if (!indexName.equals(NULL)) {
                columnIndexName.put(columnName, indexName);
                columnIndexType.put(columnName, indexType);
            }
            if (isClusteringKey.equals(TRUE)) {
                clusteringKey = columnName;
            }
        }

        return new Table(
            tableName,
            clusteringKey,
            columnTypes
        );
    }

    public static Vector<Hashtable<String, String>> loadMetadata() throws DBAppException {
        Vector<Hashtable<String, String>> metadata = new Vector<>();
        try (BufferedReader br = new BufferedReader(new FileReader(METADATA_FILE_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length > 6) {
                    throw new DBAppException("Invalid metadata format in line: " + line);
                }
                Hashtable<String, String> row = new Hashtable<>();
                row.put("TableName", parts[0]);
                row.put("ColumnName", parts[1]);
                row.put("ColumnType", parts[2]);
                row.put("ClusteringKey", parts[3]);
               row.put("IndexName", parts[4]);
               row.put("IndexType", parts[5]);
                metadata.add(row);
            }
        } catch (IOException e) {
            throw new DBAppException("Error loading metadata: " + e.getMessage());
        }
        return metadata;
    }
}

