package db;
import java.io.*;
import java.util.*;
public class Table <T extends Comparable<T>>implements  Serializable {

//   private static final long serialVersionUID = 1L;
   private Vector<Page> pages = new Vector<>();
   
   private final String tableName;
   private  Hashtable<String, String> columns;
   //first string = esm el column eg ID -> the KEY
   //second string = type of data stored in column eg java.lang.integer -> the VALUE
  
   String filename;
   private final String clusteringKeyColumn;
//   private  Vector<String> indexNames = new Vector(); //[gpa_index,id_index]
   //private  Hashtable<T, String> indexTypes = new Hashtable<T, String>(); //{ 1 = id_index, 1 = gpa_index} 
 //  private Hashtable<T, String> indexFiles = new Hashtable<T,String>(); //{[1, Ahmed Noor, 0.5] = student1.class}
 //  private Vector <T> indexKeys= new Vector<T>();
   private Vector<String> pageNames = new Vector<String>();
   private int pageCount = 0;
   
   public void removePageNames(String pageName) {
	   pageNames.remove(pageName);
   }
   
//   public void addToIndexFiles( T key ,Page page) {
//	   indexFiles.put(key,page.getFileName());
//   }
   
//   public void removeFromIndexFiles(Tuples t) {
//	   indexFiles.remove(t);
//   }
   
//   public Hashtable <T,String> getIndexFiles() {
//	   return indexFiles;
//   }

//   public Vector<String> getIndexNames() {
//	return indexNames;
//}
//   public void addToIndexTypes(T index, String indexName) {
//	   indexTypes.put(index,indexName);
//   }
//   public void addToIndexNames(String indexName) {
//	   indexNames.add(indexName);
//	}

public  int getPageCount() {
	return pageCount;
}

public  void setPageCount(int pageCount) {
	this.pageCount = pageCount;
}


   public Table(String tableName,String clusteringKeyColumn,Hashtable<String, String> column) {
	   this.tableName = tableName;
	   this.clusteringKeyColumn = clusteringKeyColumn;
	   this.columns = column;
	   createFile(tableName);

   }
   
   public String getClusteringKeyColumn() {
	return clusteringKeyColumn;
}

public Hashtable<String, String> getColumns() {
	return columns;
}

  public Vector<String> getPageNames() {
	return pageNames;
}
  
  public void addPageNames(String s) {
		pageNames.add(s);
	}

public String getFilename() {
	return filename;
}
public void createFile(String tableName) {
		this.filename = tableName+".class"  ;

		try {
			FileOutputStream fileOut = new FileOutputStream(filename, false);

			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

public String getTableName() {
	return this.tableName;
}
public Page lastElement() {
   	return pages.lastElement();
}
public Vector<Page> getPages() {
	return pages;
}
public void addPages(Page p ) {
	pages.add(p);
}
public void removePage(Page p) {
	pages.remove(p);
}

//public Vector <T> getIndexKeys() {
//	return indexKeys;
//}

//public void addToIndexKeys(Vector <T> indexKeys) {
//	this.indexKeys = indexKeys;
//}

//public boolean equals(Page page) {
//	
//}


//public Hashtable<T, String> getIndexTypes() {
//	return indexTypes;
//}

//public static Table loadTableFromCSV(String tableName) throws IOException, DBAppException {
////   String filePath = tableName + ".csv"; // Assuming the CSV file name is the same as the table name
// Vector<Hashtable<String, String>> metadata = new Vector<>();
// try (BufferedReader br = new BufferedReader(new FileReader("metadata.csv"))) {
//     // Read CSV file line by line
//     String line;
//     while ((line = br.readLine()) != null) {
//     	String[] parts = line.split(",");
//         if (parts.length > 6) {
//             throw new DBAppException("Invalid metadata format in line: " + line);
//         }
//         Hashtable<String, String> row = new Hashtable<>();
//         row.put("TableName", parts[0]);
//         row.put("ColumnName", parts[1]);
//         row.put("ColumnType", parts[2]);
//         row.put("ClusteringKey", parts[3]);
//         row.put("IndexName", parts[4]);
//         row.put("IndexType", parts[5]);
//         metadata.add(row);
//
//     }
//     Vector<Hashtable<String, String>> tableColumns = new Vector<>();
//
//     for (Hashtable<String, String> row : metadata) {
//         if (row.get("TableName").equals(tableName)) {
//             tableColumns.add(row);
//         }
//     }
//
//     if (tableColumns.isEmpty()) {
//         throw new DBAppException("Table " + tableName + " not found");
//     }
//
//     Hashtable<String, String> columnTypes = new Hashtable<>();
//     Hashtable<String, String> columnIndexName = new Hashtable<>();
//     Hashtable<String, String> columnIndexType = new Hashtable<>();
//
//     String clusteringKey = "";
//
//     for (Hashtable<String, String> tableColumn : tableColumns) {
//         String columnName = tableColumn.get("ColumnName");
//         String columnType = tableColumn.get("ColumnType");
//         String indexName = tableColumn.get("IndexName");
//         String indexType = tableColumn.get("IndexType");
//         String isClusteringKey = tableColumn.get("ClusteringKey");
//
//         columnTypes.put(columnName, columnType);
//        
////         if (isClusteringKey.equals(TRUE)) {
////             clusteringKey = columnName;
////         }
//     }
//
//     return new Table(
//         tableName,
//         clusteringKey,
//         columnTypes
//     );
// }
//    
//}

//public static void main(String args []) throws DBAppException, IOException {
//Hashtable<String, String> columns = new Hashtable();
//columns.put("id", "java.lang.Integer");
//columns.put("gpa", "java.lang.Double");
//columns.put("name", "java.lang.String");
////Table t = new Table("Student","id",columns);
//DBApp db = new DBApp();
//db.createTable("Student", "id", columns);
//Vector <Table> tables = db.getTables();
//Table table = tables.get(0);
//
////System.out.println(table.pages.size());
//Hashtable  columnsData = new Hashtable();
//
//columnsData.put("id", 1);
//columnsData.put("name", "arwa");
//columnsData.put("gpa", 2.5);
//
//db.insertIntoTable("Student", columnsData);
//db.insertIntoTable("Student", columnsData);
//db.insertIntoTable("Student", columnsData);
////System.out.println();
////System.out.println(table.pages.size());
////table.setPageNames(); //3AYZA A7OT DEEH FE METHOD INSERT!!!!!
//for (int i = 0; i < tables.size(); i++) {
//	Vector<Page> p = table.getPages();
//	System.out.println();
//	System.out.println("number of pages = " +p.size());
//	for (int j = 0; j < p.size(); j++) {
//	    Page page = p.get(j);
//		System.out.println(page.getFileName());
//	    System.out.println(page.toString());
//	}
//}
//}

}
