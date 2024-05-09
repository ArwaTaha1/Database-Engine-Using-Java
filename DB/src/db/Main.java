package db;

import java.util.Hashtable;

public class Main {

	public static <T extends Comparable<T>>void main( String[] args ) {

	try{
		
		
		DBApp dbApp = new DBApp( );
//		dbApp.init();
//
//		
//		//Student table
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
//		dbApp.createTable( strTableName, "id", htblColNameType);
//		dbApp.createIndex( strTableName, "id","ID_index"); // ->tree
//      dbApp.createIndex( strTableName, "gpa","gpa_index");

		
//		
//		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new Integer (1 ));
//		htblColNameValue.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 11));
//		htblColNameValue.put("name", new String("noor taha" ) );
//		htblColNameValue.put("gpa", new Double( 1.4 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 2 ));
//		htblColNameValue.put("name", new String("reem huss" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 4 ));
//		htblColNameValue.put("name", new String("John walid" ) );
//		htblColNameValue.put("gpa", new Double( 1.5 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue);
//		htblColNameValue.clear();
//		htblColNameValue.put("name", new String("Dalia khalid" ) );
//		htblColNameValue.put("gpa", new Double( 1.5 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 7));
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		htblColNameValue.put("gpa", new Double(2.9 ) );
//		htblColNameValue.put("name", new String("hamed" ) );
//    	dbApp.updateTable(strTableName, "0", htblColNameValue );        
//	    htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 23499 ));
//		htblColNameValue.put("name", new String("mo" ) );
//		htblColNameValue.put("gpa", new Double( 1.7 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue);
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 20000 ));
//		htblColNameValue.put("name", new String("menna" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue);
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 20001 ));
//		htblColNameValue.put("name", new String("yellow" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue);
//		htblColNameValue.clear();
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 0));
//		htblColNameValue.put("name", new String("zeina" ) );
//		htblColNameValue.put("gpa", new Double( 2.5 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 15));
//		htblColNameValue.put("name", new String("haya" ) );
//		htblColNameValue.put("gpa", new Double( 1.46 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 25));
//		htblColNameValue.put("name", new String("mody" ) );
//		htblColNameValue.put("gpa", new Double( 1.9 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 76));
//		htblColNameValue.put("name", new String("hola" ) );
//		htblColNameValue.put("gpa", new Double( 1.9 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 89));
//		htblColNameValue.put("name", new String("polo" ) );
//		htblColNameValue.put("gpa", new Double( 2.8 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(78));
//		htblColNameValue.put("name", new String("marco" ) );
//		htblColNameValue.put("gpa", new Double( 1.433 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 29));
//		htblColNameValue.put("name", new String("jill" ) );
//		htblColNameValue.put("gpa", new Double( 2.7) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 87));
//		htblColNameValue.put("name", new String("moro" ) );
//		htblColNameValue.put("gpa", new Double( 1.4 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 55));
//		htblColNameValue.put("name", new String("voila" ) );
//		htblColNameValue.put("gpa", new Double( 4.3 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 1101111));
//		htblColNameValue.put("name", new String("reem taha" ) );
//		htblColNameValue.put("gpa", new Double( 1.4 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("gpa", new Double(1.3 ) );
//		htblColNameValue.put("name", new String("marwa" ) );
//    	dbApp.updateTable(strTableName, "1111", htblColNameValue );
//    	htblColNameValue.clear();
//	    htblColNameValue.put("id", new Integer(5674567  ));
//		htblColNameValue.put("name", new String("Dalia khalid" ) );
//		dbApp.deleteFromTable(strTableName, htblColNameValue);    
//		htblColNameValue.clear();
//		htblColNameValue.put("name", new String("John walid" ) );
//		htblColNameValue.put("gpa", new Double( 1.5 ) );
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//		htblColNameValue.put("gpa", new Double( 2.9 ) );
//		dbApp.deleteFromTable(strTableName, htblColNameValue);


//		htblColNameValue.clear();	
//	    htblColNameValue.put("name", new String( "marwa"  ));
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();	
//	    htblColNameValue.put("name", new String("reem taha"  ));
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//	    htblColNameValue.clear();
//	    htblColNameValue.put("id", new Integer(2343432  ));
//		dbApp.deleteFromTable(strTableName, htblColNameValue); 
		
//		
//		htblColNameValue.clear();
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 3));
//		htblColNameValue.put("name", new String("shahd" ));
//		htblColNameValue.put("gpa", new Double( 1.3 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 2));
//		htblColNameValue.put("name", new String("habiba" ) );
//		htblColNameValue.put("gpa", new Double( 1.9 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 1));
//		htblColNameValue.put("name", new String("reem" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 6));
//		htblColNameValue.put("name", new String("arwa" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.clear();
//		htblColNameValue.put("name", new String( "marco"  ));
//		dbApp.deleteFromTable(strTableName, htblColNameValue); 
//		htblColNameValue.clear();
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 8));
//		htblColNameValue.put("name", new String("meme" ) );
//		htblColNameValue.put("gpa", new Double( 12 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 10));
//		htblColNameValue.put("name", new String("momo" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 9));
//		htblColNameValue.put("name", new String("yo" ) );
//		htblColNameValue.put("gpa", new Double( 12 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
//		htblColNameValue.clear();
//		htblColNameValue.clear();
//		htblColNameValue.clear();
//		htblColNameValue.put("gpa", new Double( 1.25 ));
//		dbApp.deleteFromTable(strTableName, htblColNameValue); 
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer( 14));
//		htblColNameValue.put("name", new String("mariamm" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
////		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		BTree <T, String> tree = dbApp.deserializeBPTree("ID_index");
//		tree.print();
//		System.out.println(tree.getKeys());
//		BTree <T, String> gpaTree = dbApp.deserializeBPTree("gpa_index");
//		gpaTree.print();
//		System.out.println(gpaTree.getKeys());
//		System.out.println(gpaTree.getDuplicateKeys());
//		System.out.println(gpaTree.getDuplicateKeysPages());
		Table t = dbApp.deserializeTable(strTableName+".class");
	    for (int i =0;i<t.getPageCount();i++) {
	    	String pagen = (String) t.getPageNames().get(i);
	    	Page page = dbApp.deserializePage((String)pagen);
	    	System.out.println(page.getFileName());
	        System.out.println( page  );
	    }
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[1];
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[0]._strTableName = "Student";
//		arrSQLTerms[0]._strColumnName= "gpa";
//		arrSQLTerms[0]._strOperator = ">";
//		arrSQLTerms[0]._objValue = new Double( 2.9 );
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[1]._strTableName = "Student";
//		arrSQLTerms[1]._strColumnName= "id";
//		arrSQLTerms[1]._strOperator = ">";
//		arrSQLTerms[1]._objValue = new Integer( 2 );
//		
//
//		String[]strarrOperators = new String[0];
////		strarrOperators[0] = "AND";
//	//	 select * from Student where name = “John Noor” or gpa = 1.5;
//		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);	
//		while (resultSet.hasNext()) {
//            Tuples element = (Tuples) resultSet.next();
//            System.out.println(element.getColumns());
//            System.out.println(element);
//            System.out.println("hello");
//        }
	}	
		catch(Exception exp){
			exp.printStackTrace( );
		}
	
	}

}
