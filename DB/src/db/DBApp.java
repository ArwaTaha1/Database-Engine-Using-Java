package db;

/** * @author Wael Abouelsaadat */ 

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.*;
import db.Page;
import db.Tuples;
import btree.*;

import java.io.*;
public class DBApp <T extends Comparable<T>>{

	public DBApp( ){
		

//		
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		
		Metadata metadata = new Metadata();
	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	
	//5ELSET
	public void createTable(String strTableName, 
		String strClusteringKeyColumn,  
		Hashtable<String,String> htblColNameType) throws DBAppException, IOException{//CHECK DATA TYPES ENTERED
			
		if (strClusteringKeyColumn==null) 
			throw new DBAppException("Table must have a primary key");
		
		boolean flag = false;
        for (String key : htblColNameType.keySet()) {
            if (!htblColNameType.get(key).equals("java.lang.Integer") &&!htblColNameType.get(key).equals("java.lang.Double") && !htblColNameType.get(key).equals("java.lang.String"))
            	throw new DBAppException("Only insert double,integers or strings");
            if (key.equals(strClusteringKeyColumn)) 
            	flag=true;
            
        }
        if (flag ==false) 
        	throw new DBAppException("Primary Key is not found in Column Name Types");
	    
        flag = false;
	    Table t = new Table(strTableName,strClusteringKeyColumn,htblColNameType);

	    serializeTable(t.getFilename(), t);
 		String filename = "metadata.csv";
	    try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		String line;
	    while ((line = br.readLine()) != null) {
    		String[] fields = line.split(",");
    		if (fields[0].equals(strTableName)) {
   				flag = true;
   			}
    }

 	}   
	    catch (IOException e) {
 			System.err.println("Error occurred while reading CSV file: " + e.getMessage());
    }

    	try {
    		if (flag==false) {
	    		Metadata.writeMetadata(strTableName,strClusteringKeyColumn,htblColNameType);	
	    	}
    	}
    	catch (Exception e) {
   			e.printStackTrace(); 
    	}     	
        
        
    
	}
		    
		
	// following method creates a B+tree index 
	
	//5ELSET 
	public <T extends Comparable<T>> void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException {
	    Table t = null;
		try {
			t = deserializeTable(strTableName+".class");
		} catch (FileNotFoundException e) {
			throw new DBAppException ("Table not Found");
		}
		

	//    System.out.println(t.getColumns());
	    Hashtable <String, String> tableColumnsType = t.getColumns();
	    String columnName =null;
	    String type=null;
	    for (String key : tableColumnsType.keySet()) {
            if (key.equals(strColName)) {
            	columnName = strColName; //ha3mel ID el index
            	type = tableColumnsType.get(columnName); //typo integer
            	break;
            }
        }
	    if (columnName ==null || type ==null ) {
	    	throw new DBAppException("Column does not exist");
	    }
	//    System.out.println(columnName);
	//    System.out.println(type);
	    BTree <T,String>tree ;
	    switch (type) {
        case "java.lang.Integer" : {
        	tree = new BTree(); break;
        }
        case "java.lang.String" :
        	tree = new BTree(); break;
        case "java.lang.Double" :
        	tree = new BTree(); break;
        default : {
            throw new DBAppException("Cannot create B+ tree index on data type: " + type);
        	}    
	    }

	    T value = null;
	   // System.out.println();
        Vector<Tuples> tuples =null;
        Vector <Page> ps= t.getPages();
      //  System.out.println(ps);
        for (int i =0; i<ps.size();i++) {
        	Page page = ps.get(i);
        //	System.out.println(page);
        	page = deserializePage(page.getFileName());
        //	System.out.println("I am Here! " + page.toString());
        	tuples = page.getTuples();
        	for (int j = 0; j < tuples.size(); j++) {
                Tuples tuple = tuples.get(j); 
                Vector <String> columns = tuple.getColumns(); //[name,age,id] [arwa,20,1]
                int index =columns.indexOf(strColName);
                value = (T) tuple.getTuples().get(index);
              //  System.out.println(value);
                if (value!=null) {
            	  //  System.out.println("hi");
                	tree.insert(value,page.getFileName());
                //	t.addToIndexFiles(tuple,page); // { [1,arwa,20] = Student1.class }
                	tree.addKeys(value);
                	//tuple  = tree.search(value);
                }  // System.out.println(tuple+"heloooooo");
            }
        	serializePage(page.getFileName(),page) ;
        }
        BufferedReader br = null;
		String csvBody = "";
		try {
			br = new BufferedReader(new FileReader("metadata.csv"));
			String line = br.readLine();
           // System.out.println(line);
			while (line != null) {
				String[] info = line.split(",");
				if (info[0].equals(strTableName) && info[1].equals(strColName) ) {
					info[4] = strIndexName;
			    	info[5] = "B+Tree";
					line = info[0] + "," + info[1] + "," + info[2] + "," + info[3] + "," + info[4] +","+ info[5] ;
				}	
//				else {
//					line = info[0] + "," + info[1] + "," + info[2] + "," + info[3] + "," + null +","+ null ;
//				}
				csvBody = csvBody + line + "\n";
				line = br.readLine();
			}
			br.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter("metadata.csv");
			fileWriter.append(csvBody);
		} catch (Exception e) {
			//System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter !!!");
				e.printStackTrace();
			}

		}
		serializeTable(t.getFilename(), t);
		//tree.print();
		serializeBPTree(strIndexName,tree);

	}
	
	
	
	
	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	//insert name = Ahmed, ID = 2, Age = 20
	
	//ME7TAGEEN NEZAABT EL MAXROWS LE KOL PAGE MN .CONFIG FILE
	public  void insertIntoTable(String strTableName, 
			Hashtable<String,Object>  htblColNameValue) throws DBAppException, IOException{
		Table t = null;
		try {
			t = deserializeTable(strTableName+".class");
		} catch (FileNotFoundException e) {
			throw new DBAppException("table not found");
		}

		String primaryKey = getPrimaryKeyFromCSV("metadata.csv",strTableName);
		for (String key : htblColNameValue.keySet() ) {
			if (key.equals(primaryKey) && htblColNameValue.get(key)==null) 
				throw new DBAppException("Primary Key field must contain a value");	
		}
		checkObjectClassFromCSV("metadata.csv",strTableName,htblColNameValue);
		Hashtable <String,String> indicies =  checkForIndexFromCSV("metadata.csv",strTableName); //{id = id_index, gpa = gpa_index}
		Hashtable <String, String> tableColumns = t.getColumns();
		Set<String> keysSetOfTableColumns = tableColumns.keySet();
		String[] keysArrayOfTableColumns = (String[]) keysSetOfTableColumns.toArray(new String[0]);
		Set<String> keysSetOfhtblColNameValue = htblColNameValue.keySet();
		String[] keysArrayOfhtblColNameValue = (String[]) keysSetOfhtblColNameValue.toArray(new String[0]); 
		Set<String> mainSet = new HashSet<>();
		for (String str : keysArrayOfTableColumns) {
			mainSet.add(str);
		}

		if (keysArrayOfhtblColNameValue.length == 0) {
			throw new DBAppException("htblColNameValue is empty");
		}
		boolean isSubset = true;
		for (String str : keysArrayOfhtblColNameValue) {
			if (!mainSet.contains(str)) {
				isSubset = false;
				break;
			}
		}
		if (!isSubset) 
			throw new DBAppException("Column does not exist");	
		boolean flag = false ;
		if (indicies.containsKey(primaryKey)) {
			flag = true;
		}
		BTree <T,String> tree = null;
		Tuples tuple = new Tuples(htblColNameValue,strTableName);
		boolean empty;
//		Vector <Page> pages = t.getPageNames().size()
		int numberOfPages = t.getPageNames().size();
		if (numberOfPages==0)
			empty = true;
		else
			empty = false;
		
		if (empty) {
			Page newPage = new Page(strTableName); //nezabat el maxRows
			t.setPageCount(t.getPageCount()+1);
			newPage.addTuple(tuple);
			t.addPages(newPage);
			t.addPageNames(newPage.getFileName());
			if (!indicies.isEmpty()) { //							
				String indexName ="";
				for (String key : indicies.keySet()) {         
					indexName = indicies.get(key); 
					T treeValue = (T) htblColNameValue.get(key); 
					tree = deserializeBPTree(indexName);
					tree.insert(treeValue, newPage.getFileName());
					tree.addKeys(treeValue);
					serializeBPTree(indexName,tree);

				}

			}
			serializePage(newPage.getFileName(), newPage);
			serializeTable(t.getFilename(),t);
			return;
		}

		Vector<String> pageNames = t.getPageNames(); 
		int primaryIndex = tuple.getColumns().indexOf(primaryKey);
		T pk = (T) tuple.getTuples().get(primaryIndex);
		String pageName = binarySearchPage(pageNames,tuple,primaryKey);
		Page page = deserializePage(pageName);
		Vector<Tuples> tuples = page.getTuples();
		int index = pageNames.indexOf(pageName);
		if (indicies.isEmpty() || flag==false ) {
			int pos = binarySearch(page,tuple,primaryKey);
			if(pos>=page.getMaxRows()&& page.getFileName().equals(pageNames.get(pageNames.size()-1))) { //=> case en a5er page malyana
				Page newPage = new Page(strTableName);
				newPage.addTuple(tuple);
				t.setPageCount(t.getPageCount() + 1);
				t.addPages(newPage);
				t.addPageNames(newPage.getFileName());
				if (!indicies.isEmpty()) { //							
					String indexName ="";
					for (String key : indicies.keySet()) {         
						indexName = indicies.get(key); 
						T treeValue = (T) htblColNameValue.get(key);
						if (treeValue!=null) {
							tree = deserializeBPTree(indexName);
							tree.insert(treeValue, newPage.getFileName());
							tree.addKeys(treeValue);
							serializeBPTree(indexName,tree);
						}
					}
				}
				serializePage(newPage.getFileName(), newPage);
				serializeTable(t.getFilename(), t);
				return;
			}
			else {
				
				
				
				if(pos>=page.getMaxRows()) {
					pos = pos-page.getMaxRows();
					page = deserializePage(pageNames.get(index+1));
					tuples = page.getTuples();
					//System.out.println(page.getFileName());
				}
				
				
				if(!page.isFull()) {
					tuples.add(null);
					for (int j = tuples.size() - 2; j >= pos ; j--) {
						tuples.set(j + 1, tuples.get(j));
					}
					tuples.set(pos, tuple);
				}
				else {  // => en el main page malyana
					int i;
					for ( i = index+1; i < pageNames.size(); i++) { //go through pages starting from page after the main page
						Page p = deserializePage(pageNames.get(i));
						Vector<Tuples> tuples1 = p.getTuples();	     
//						System.out.println(tuples1);
						if(!p.isFull()) {
							tuples1.add(null); 
							for (int j = tuples1.size() - 2; j >= 0; j--) {//shift in first non full page
								tuples1.set(j + 1, tuples1.get(j));
							}
							Page lastPage = deserializePage(pageNames.get(i-1)); //=> has2al aleha
							tuples1.set(0, lastPage.getTuples().lastElement());	
							if (!indicies.isEmpty()) { 		
								String indexName ="";
								for (String fkey : indicies.keySet()) {         
									indexName = indicies.get(fkey); 
									int colIndex = lastPage.getTuples().lastElement().getColumns().indexOf(fkey);
									T treeValue = (T) p.getTuples().get(0).getTuples().get(colIndex);
									if (treeValue!=null) {
										tree = deserializeBPTree(indexName);
										tree.delete(treeValue);
										tree.insert(treeValue, pageNames.get(i));
										serializeBPTree(indexName,tree);
									}
								}
							}
							serializePage(lastPage.getFileName(), lastPage);
							serializePage(p.getFileName(), p);
							serializeTable(t.getFilename(), t);
							break;
						}
					}
					if(i==pageNames.size()) { //if all pages are full
						Page newPage = new Page(strTableName);
						t.setPageCount(t.getPageCount() + 1);
						t.addPages(newPage);
						t.addPageNames(newPage.getFileName());
						Page p1 = deserializePage(pageNames.get(i-1));
						newPage.addTuple(p1.getTuples().lastElement());
						if (!indicies.isEmpty()) { 							
							String indexName ="";
							for (String fkey : indicies.keySet()) {         
								indexName = indicies.get(fkey); 
								int colIndex = p1.getTuples().lastElement().getColumns().indexOf(fkey);
								T treeValue = (T) newPage.getTuples().get(0).getTuples().get(colIndex);
								if (treeValue!=null) {
									tree = deserializeBPTree(indexName);
									tree.delete(treeValue);
									tree.insert(treeValue, newPage.getFileName());
								}
								serializeBPTree(indexName,tree);
							}
						}
						serializePage(p1.getFileName(),p1);
						serializePage(newPage.getFileName(), newPage);
						serializeTable(t.getFilename(), t);
					}
	
					for(int k = i-1 ; k > pageNames.indexOf(page.getFileName()) ; k-- ) { //start from last page before the non filled one and go back till i reach the main page
						Page p = deserializePage(pageNames.get(k));
						
						Vector<Tuples> tuples1 = p.getTuples();
						for ( int l = tuples1.size() - 2; l >= 0; l--) {
							tuples1.set(l + 1, tuples1.get(l));
						}
						Page lastPage = deserializePage(pageNames.get(k-1));
						tuples1.set(0, lastPage.getTuples().lastElement());
						tuples.set(tuples.size()-1,null);
						if (!indicies.isEmpty()) { 							
							String indexName ="";
							for (String fkey : indicies.keySet()) {         
								indexName = indicies.get(fkey); 
								int colIndex = lastPage.getTuples().lastElement().getColumns().indexOf(fkey);
								T treeValue = (T) tuples1.get(0).getTuples().get(colIndex);
								if (treeValue!=null) {
									tree = deserializeBPTree(indexName);
									tree.delete(treeValue);
									tree.insert(treeValue, p.getFileName());
									serializeBPTree(indexName,tree);
								}
							}
						}
						serializePage(p.getFileName(), p);
						serializePage(lastPage.getFileName(), lastPage);
						serializeTable(t.getFilename(), t);
					}	
	
					for (int m = tuples.size() - 2; m >= pos ; m--) {
						tuples.set(m + 1, tuples.get(m));
					}
					if(pos>=page.getMaxRows()) {
						return;
					}
					else {
						tuples.set(pos, tuple);
					}
	
				}
			}
			if (!indicies.isEmpty()) { //							indicies = {id = id_index, gpa = gpa_index}
				String indexName ="";
				for (String fkey : indicies.keySet()) {
					indexName = indicies.get(fkey); 
					T treeValue = (T) htblColNameValue.get(fkey); 
					tree = deserializeBPTree(indexName);
					tree.insert(treeValue, pageName);
					tree.addKeys(treeValue);
					serializeBPTree(indexName,tree);

				}
			}
			serializePage(page.getFileName(), page);
			serializeTable(t.getFilename(), t);
			return;
		}
		 {
			boolean flag1 = false;
			String treeIndex = indicies.get(primaryKey); //id_index
			tree = deserializeBPTree(treeIndex); 
			Vector <T> keys = tree.getKeys();
			T key = null;
			for (int i =0; i<keys.size()-1;i++) {
				if (pk.compareTo(keys.get(i))>0 && pk.compareTo(keys.get(i+1))<0 ) {
					flag1 = true;
					key = keys.get(i+1);
					break;
				}
			}
			int pos = binarySearch(page,tuple,primaryKey);
			//int pos1 = pos-1;
			
			if (flag1 == true) { 			
			//	System.out.println("hi " + tuple );// => hwa mawgood within el range
				pageName = tree.search(key);
				page = deserializePage(pageName);
				pos = binarySearch(page,tuple,primaryKey);

				tuples = page.getTuples();
				Tuples temp = null;
				

				if(pos>=page.getMaxRows()) {
					pos = pos-page.getMaxRows();
					page = deserializePage(pageNames.get(index+1));
					tuples = page.getTuples();
				}
				if(!page.isFull()) {
				//	System.out.println("hi " + tuple );
					tuples.add(null);
					for (int j = tuples.size() - 2; j >= pos ; j--) {
						tuples.set(j + 1, tuples.get(j));
					}
					tuples.set(pos, tuple);
				}
				else {  	
					//System.out.println("hi " + tuple );// => en el main page malyana
					int i;
					for ( i = index+1; i < pageNames.size(); i++) { //go through pages starting from page after the main page
						Page p = deserializePage(pageNames.get(i));
						Vector<Tuples> tuples1 = p.getTuples();	     
//						System.out.println(tuples1);
						if(!p.isFull()) {
							tuples1.add(null); 
							for (int j = tuples1.size() - 2; j >= 0; j--) {//shift in first non full page
								tuples1.set(j + 1, tuples1.get(j));
							}
							Page lastPage = deserializePage(pageNames.get(i-1)); //=> has2al aleha
							tuples1.set(0, lastPage.getTuples().lastElement());	
							if (!indicies.isEmpty()) { 		
								String indexName ="";
								for (String fkey : indicies.keySet()) {         
									indexName = indicies.get(fkey); 
									int colIndex = lastPage.getTuples().lastElement().getColumns().indexOf(fkey);
									T treeValue = (T) p.getTuples().get(0).getTuples().get(colIndex);
									if (treeValue!=null) {
										tree = deserializeBPTree(indexName);
										tree.delete(treeValue);
										tree.insert(treeValue, pageNames.get(i));
										serializeBPTree(indexName,tree);
									}
								}
							}
							serializePage(lastPage.getFileName(), lastPage);
							serializePage(p.getFileName(), p);
							serializeTable(t.getFilename(), t);
							break;
						}
					}
					if(i==pageNames.size()) { //if all pages are full
					//	System.out.println("hello "+tuple);
						Page newPage = new Page(strTableName);
						t.setPageCount(t.getPageCount() + 1);
						t.addPages(newPage);
						t.addPageNames(newPage.getFileName());
						Page p1 = deserializePage(pageNames.get(i-1));
						newPage.addTuple(p1.getTuples().lastElement());
						if (!indicies.isEmpty()) { 							
							String indexName ="";
							for (String fkey : indicies.keySet()) {         
								indexName = indicies.get(fkey); 
								int colIndex = p1.getTuples().lastElement().getColumns().indexOf(fkey);
								T treeValue = (T) newPage.getTuples().get(0).getTuples().get(colIndex);
								if (treeValue!=null) {
									tree = deserializeBPTree(indexName);
									tree.delete(treeValue);
									tree.insert(treeValue, newPage.getFileName());
								}
								serializeBPTree(indexName,tree);
							}
						}
						serializePage(p1.getFileName(),p1);
						serializePage(newPage.getFileName(), newPage);
						serializeTable(t.getFilename(), t);
					}
	
					for(int k = i-1 ; k > pageNames.indexOf(page.getFileName()) ; k-- ) { //start from last page before the non filled one and go back till i reach the main page
						//System.out.println("hello " + tuple );
						Page p = deserializePage(pageNames.get(k));
						Vector<Tuples> tuples1 = p.getTuples();
						for ( int l = tuples1.size() - 2; l >= 0; l--) {
							tuples1.set(l + 1, tuples1.get(l));
						}
						Page lastPage = deserializePage(pageNames.get(k-1));
						tuples1.set(0, lastPage.getTuples().lastElement());
						if (!indicies.isEmpty()) { 							
							String indexName ="";
							for (String fkey : indicies.keySet()) {         
								indexName = indicies.get(fkey); 
								int colIndex = lastPage.getTuples().lastElement().getColumns().indexOf(fkey);
								T treeValue = (T) tuples1.get(0).getTuples().get(colIndex);
								if (treeValue!=null) {
									tree = deserializeBPTree(indexName);
									tree.delete(treeValue);
									tree.insert(treeValue, p.getFileName());
									serializeBPTree(indexName,tree);
								}
							}
						}
						serializePage(p.getFileName(), p);
						serializePage(lastPage.getFileName(), lastPage);
						serializeTable(t.getFilename(), t);
					}	
	
					for (int m = tuples.size() - 2; m >= pos ; m--) {
						tuples.set(m + 1, tuples.get(m));
					}
					if(pos>=page.getMaxRows()) {
						return;
					}
					else {
						tuples.set(pos, tuple);
					}
	
				}

			}
			else if (flag1 == false && pk.compareTo(keys.get(keys.size()-1))>0 ) {
//				System.out.println("hi");
				T key1 = keys.get(keys.size()-1);
				String pname = tree.search(key1);
				Page p = deserializePage(pname);
				if (p.isFull()) {
					System.out.println("hi");
					Page newPage = new Page(strTableName);
					newPage.addTuple(tuple);
					t.setPageCount(t.getPageCount() + 1);
					t.addPages(newPage);
					t.addPageNames(newPage.getFileName());
					if (!indicies.isEmpty()) { //							
						String indexName ="";
						for (String fkey : indicies.keySet()) {         
							indexName = indicies.get(fkey); 
							T treeValue = (T) htblColNameValue.get(fkey);
							if (treeValue!=null) {
								tree = deserializeBPTree(indexName);
								tree.insert(treeValue, newPage.getFileName());
								tree.addKeys(treeValue);
								serializeBPTree(indexName,tree);
							}
						}
					}
					serializePage(p.getFileName(),p);
					serializePage(newPage.getFileName(), newPage);
					serializeTable(t.getFilename(), t);	
				}
				else {
					p.addTuple(tuple);
					if (!indicies.isEmpty()) { //							
						String indexName ="";
						for (String fkey : indicies.keySet()) {         
							indexName = indicies.get(fkey); 
							T treeValue = (T) htblColNameValue.get(fkey);
							if (treeValue!=null) {
								tree = deserializeBPTree(indexName);
								tree.insert(treeValue, p.getFileName());
								tree.addKeys(treeValue);
								serializeBPTree(indexName,tree);
							}
						}
					}
					serializePage(p.getFileName(), p);
					serializeTable(t.getFilename(), t);
					return;
				}
				
			}
			else {
				key = keys.get(0);
				pageName = tree.search(key);
				page = deserializePage(pageName);
				tuples = page.getTuples();
				pos = binarySearch(page,tuple,primaryKey);
				if(!page.isFull()) {
					tuples.add(null);
					for (int j = tuples.size() - 2; j >= pos ; j--) {
						tuples.set(j + 1, tuples.get(j));
					}
					tuples.set(pos, tuple);
				}
				else {  											// => en el main page malyana
					int i;
					for ( i = index+1; i < pageNames.size(); i++) { //go through pages starting from page after the main page
						Page p = deserializePage(pageNames.get(i));
						Vector<Tuples> tuples1 = p.getTuples();	     
//						System.out.println(tuples1);
						if(!p.isFull()) {
							tuples1.add(null); 
							for (int j = tuples1.size() - 2; j >= 0; j--) {//shift in first non full page
								tuples1.set(j + 1, tuples1.get(j));
							}
							Page lastPage = deserializePage(pageNames.get(i-1)); //=> has2al aleha
							tuples1.set(0, lastPage.getTuples().lastElement());	
							if (!indicies.isEmpty()) { 		
								String indexName ="";
								for (String fkey : indicies.keySet()) {         
									indexName = indicies.get(fkey); 
									int colIndex = lastPage.getTuples().lastElement().getColumns().indexOf(fkey);
									T treeValue = (T) p.getTuples().get(0).getTuples().get(colIndex);
									if (treeValue!=null) {
										tree = deserializeBPTree(indexName);
										tree.delete(treeValue);
										tree.insert(treeValue, p.getFileName());
										serializeBPTree(indexName,tree);
									}
								}
							}
							serializePage(lastPage.getFileName(), lastPage);
							serializePage(p.getFileName(), p);
							serializeTable(t.getFilename(), t);
							break;
						}
					}
					if(i==pageNames.size()) { //if all pages are full
						Page newPage = new Page(strTableName);
						t.setPageCount(t.getPageCount() + 1);
						t.addPages(newPage);
						t.addPageNames(newPage.getFileName());
						Page p1 = deserializePage(pageNames.get(i-1));
						newPage.addTuple(p1.getTuples().lastElement());
						if (!indicies.isEmpty()) { 							
							String indexName ="";
							for (String fkey : indicies.keySet()) {         
								indexName = indicies.get(fkey); 
								int colIndex = p1.getTuples().lastElement().getColumns().indexOf(fkey);
								T treeValue = (T) newPage.getTuples().get(0).getTuples().get(colIndex);
								if (treeValue!=null) {
									tree = deserializeBPTree(indexName);
									tree.delete(treeValue);
									tree.insert(treeValue, newPage.getFileName());
								}
								serializeBPTree(indexName,tree);
							}
						}
						serializePage(p1.getFileName(),p1);
						serializePage(newPage.getFileName(), newPage);
						serializeTable(t.getFilename(), t);
					}
	
					for(int k = i-1 ; k > pageNames.indexOf(page.getFileName()) ; k-- ) { //start from last page before the non filled one and go back till i reach the main page
						Page p = deserializePage(pageNames.get(k));
						Vector<Tuples> tuples1 = p.getTuples();
						for ( int l = tuples1.size() - 2; l >= 0; l--) {
							tuples1.set(l + 1, tuples1.get(l));
						}
						Page lastPage = deserializePage(pageNames.get(k-1));
						tuples1.set(0, lastPage.getTuples().lastElement());
						if (!indicies.isEmpty()) { 							
							String indexName ="";
							for (String fkey : indicies.keySet()) {         
								indexName = indicies.get(fkey); 
								int colIndex = lastPage.getTuples().lastElement().getColumns().indexOf(fkey);
								T treeValue = (T) tuples1.get(0).getTuples().get(colIndex);
								if (treeValue!=null) {
									tree = deserializeBPTree(indexName);
									tree.delete(treeValue);
									tree.insert(treeValue, p.getFileName());
									serializeBPTree(indexName,tree);
								}
							}
						}
						serializePage(p.getFileName(), p);
						serializePage(lastPage.getFileName(), lastPage);
						serializeTable(t.getFilename(), t);
					}	
	
					for (int m = tuples.size() - 2; m >= pos ; m--) {
						tuples.set(m + 1, tuples.get(m));
					}
					if(pos>=page.getMaxRows()) {
						return;
					}
					else {
						tuples.set(pos, tuple);
					}
	
				}
			}
			
			String indexName ="";
			for (String fkey : indicies.keySet()) {
				indexName = indicies.get(fkey); 
				T treeValue = (T) htblColNameValue.get(fkey); 
				tree = deserializeBPTree(indexName);
				tree.insert(treeValue, pageName);
				tree.addKeys(treeValue);
				serializeBPTree(indexName,tree);

			}
			
		}
			serializePage(page.getFileName(), page);
			serializeTable(t.getFilename(), t);
	}



	public String binarySearchPage( Vector<String> pageNames, Tuples target, String primaryKey) {
		//System.out.println("tuple= "+ target);
		int low = 0;
		int high = pageNames.size() - 1;
		// Vector<Tuples> tuples = page.getTuples();
		String pageName = null;
		while (low <= high) {
			int mid = low + (high - low) / 2;
			pageName = pageNames.get(mid);
			Page page = deserializePage(pageName);
			int primaryIndex = target.getColumns().indexOf(primaryKey);
			T minPK = (T)page.getTuples().get(0).getTuples().get(primaryIndex);
			T maxPK =  (T)page.getTuples().lastElement().getTuples().get(primaryIndex);
			T targetPK =(T) target.getTuples().get(primaryIndex);
			if(targetPK.compareTo(minPK)>=0 && targetPK.compareTo(maxPK)<=0) {
				return pageName;
			}
			if(targetPK.compareTo(maxPK)>0) {
				low = mid + 1;

			}
			else {
				high = mid - 1;
			}

		}
		// If the target value doesn't exist in the array, return the insertion point
		return pageName;
	}
	
	
	public static int getMaxRows() {
		int maxRows = 0;
    	try (InputStream input = new FileInputStream("DBApp.config")) {

    	Properties prop = new Properties();

    	// load a properties file
    	prop.load(input);

    	// get the property value and print it out
    	maxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));

    	} catch (IOException ex) {
    	ex.printStackTrace();
    	}

    	return maxRows;
    	}



// Binary search to find the insertion point for the target value
	public int binarySearch(Page page, Tuples target, String primaryKey) throws DBAppException {
		int low = 0;
		int high = page.getTuples().size() - 1;
		Vector<Tuples> tuples = page.getTuples();

		while (low <= high) {
			int mid = (int) low + (high - low) / 2;
			Tuples tuple = tuples.get(mid);
			int primaryIndex = tuple.getColumns().indexOf(primaryKey);
			T value = (T)tuple.getTuples().get(primaryIndex);
			// System.out.println(value);
			T newValue = (T)target.getTuples().get(primaryIndex);
			if (value.equals(newValue)) {
				throw new DBAppException("Duplicate key");
			} else if (value.compareTo(newValue)<0) {
				low = mid + 1;
			} else {
				high = mid - 1;
			}
		}
		// If the target value doesn't exist in the array, return the insertion point
		return low;
	}
	
	public Hashtable <String,String> checkForIndexFromCSV(String csvFilePath, String strTableName) throws DBAppException {
		Hashtable <String,String> columnIndicies = new Hashtable<String,String>(); //Hashtable of columns that have indices
		//String primaryKey = "";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("metadata.csv"));
			String line = br.readLine();
			while (line != null) {
				String[] info = line.split(",");
				if (info[0].equals(strTableName) && !info[5].equals("null"))  {
					columnIndicies.put(info[1], info[4]); //1)column ely ma3molo index
				}										 //2) esm el index dah (which supposedly i have an already created btree for it thats saved somewhere)
				
				line = br.readLine();
		}
			br.close();

		} catch (FileNotFoundException e ) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
		return columnIndicies;
	}


	public String getPrimaryKeyFromCSV(String csvFilePath, String strTableName) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line into fields using the CSV delimiter (e.g., comma)
            	
                String[] fields = line.split(",");
            //    System.out.println(fields[0]);
                if (fields[0].equals(strTableName) && fields[3].equals("True")) 
                	return fields[1];  
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
	
	public boolean checkObjectClassFromCSV(String csvFilePath,String strTableName,Hashtable<String,Object>  htblColNameValue) throws DBAppException {
		Hashtable <String,String> fromCSV = new Hashtable();
		try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line into fields using the CSV delimiter (e.g., comma)
                String[] fields = line.split(",");
                if (fields[0].equals(strTableName))
                	fromCSV.put(fields[1], fields[2]); 
   
            }
            if (fromCSV.isEmpty())
            	throw new DBAppException("table does not exist");
        }
        catch (IOException e) {
            System.err.print("file not found");
        }
		Set<String> keysFromCSV = (Set<String>) fromCSV.keySet(); 
		String[] keysFromCSVString = (String[]) keysFromCSV.toArray(new String[0]);
		Set<String> keysFromHtbl = (Set<String>) htblColNameValue.keySet();
		String[] keysFromHtblString = (String[]) keysFromHtbl.toArray(new String[0]);
		Set<String> mainSet = new HashSet<>();
        for (String str : keysFromCSVString) {
            mainSet.add(str);
        }
        if (keysFromHtblString.length == 0) {
        //    throw new DBAppException("htblColNameValue is empty");
        }
        boolean isSubset = true;
        for (String str : keysFromHtblString) {
            if (!mainSet.contains(str)) {
                isSubset = false;
                break;
            }
        }
        if (!isSubset) 
        	throw new DBAppException("Column does not exist");
        
		for (String key : htblColNameValue.keySet()) {  //[id = java.lang.integer, gpa = java.lang.Double, name = java.lang.Double]
			String value1 = fromCSV.get(key);
			Object value2 = htblColNameValue.get(key);
			if (value2==null)
				throw new DBAppException("Primary key field must be filled ");
			Class<?> objClass = value2.getClass();
			String typeOfObject = objClass.getName();
			//System.out.println(typeOfObject);
		//	System.out.println(value1);
			if (!value1.equals(typeOfObject))
				throw new DBAppException("Column types are not aligned");
		}
		
        return true;
	}

		
	   	   
	

	// following method updates one row only
	// htblColNameValue holds the key and new value  => {gpa = 1.2, name = noor}
	// htblColNameValue will not include clustering key as column name
	// WE DO NOU UPDATE THE PRIMARY KEY FIELD
	// strClusteringKeyValue is the value to look for to find the row to update.

	
	public String binarySearchPageName( Vector<String> pageNames, String primaryKeyValue , String primaryKey) {
		
		int low = 0;
		int high = pageNames.size() - 1;
		String pageName = null;
		while (low <= high) {
			int mid = low + (high - low) / 2;
			pageName = pageNames.get(mid);
			Page page = deserializePage(pageName);
			Tuples t = page.getTuples().get(0);
			int primaryIndex = t.getColumns().indexOf(primaryKey);
			String minPK = ""+ page.getTuples().get(0).getTuples().get(primaryIndex);
			String maxPK = ""+ page.getTuples().lastElement().getTuples().get(primaryIndex);
			
			try {
				int p = Integer.parseInt(primaryKeyValue);
				int min = Integer.parseInt(minPK);
				int max = Integer.parseInt(maxPK);
				if(p>=min && p<=max) {
					serializePage(page.getFileName(),page);
					return pageName;
				}
				if(p>max) {
					serializePage(page.getFileName(),page);
					low = mid + 1;
				}
				else {
					serializePage(page.getFileName(),page);
					high = mid - 1;
				}
	        } catch (NumberFormatException e) {
	            try {
	                double p = Double.parseDouble(primaryKeyValue);
	                double min = Double.parseDouble(minPK);
					double max = Double.parseDouble(maxPK);
					if(p>=min && p<=max) {
						serializePage(page.getFileName(),page);
						return pageName;
					}
					if(p>max) {
						serializePage(page.getFileName(),page);
						low = mid + 1;
					}
					else {
						serializePage(page.getFileName(),page);
						high = mid - 1;
					}
	            } catch (NumberFormatException e2) {
	            	if(primaryKeyValue.compareTo(minPK)>=0 && primaryKeyValue.compareTo(maxPK)<=0) {
						serializePage(page.getFileName(),page);
						return pageName;
					}
					if(primaryKeyValue.compareTo(maxPK)>0) {
						serializePage(page.getFileName(),page);
						low = mid + 1;
					}
					else {
						serializePage(page.getFileName(),page);
						high = mid - 1;
					}
	            }
	        }
			
		}
		
		return pageName;
	}
	
	
	public void updateTable(String strTableName, 
							String strClusteringKeyValue, // => 1 [id = 1 ; name = arwa; gpa = 0.2]
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException, IOException{
		Table t = null;
		try {
			t = deserializeTable(strTableName+".class");
		} catch (FileNotFoundException e) {
			throw new DBAppException("table not found");
		}

	    String primaryKey = getPrimaryKeyFromCSV("metadata.csv",strTableName); //ID
	    for (String key : htblColNameValue.keySet() ) { 
	    	if (key.equals(primaryKey)) 
	    		throw new DBAppException("Column name shouldn't contain a value for the primary key");		
	    }
	    
	    checkObjectClassFromCSV("metadata.csv",strTableName,htblColNameValue);
	    BTree<T,String> tree =null;
	    Hashtable <String,String> indicies =  checkForIndexFromCSV("metadata.csv",strTableName); //{id = id_index, gpa = gpp_index}
		if (indicies.isEmpty() || !indicies.containsKey(primaryKey)) { //binary search
			boolean flag = false;
			Vector<String> pages = t.getPageNames();
			String pageName = binarySearchPageName( pages, strClusteringKeyValue , primaryKey);
			if (pageName==null) {
				throw new DBAppException("page not found");
			}
			Page page = deserializePage(pageName);
        	Vector <Tuples> tuples = page.getTuples();
        	for (int j =0; j<tuples.size();j++) {
        		Tuples tuple = tuples.get(j);
        		Vector <T> tupleData = tuple.getTuples();  //[0.95,Ahmed Noor,2343432] 
        		Vector <String> tupleColumn = tuple.getColumns();
        		int clusteringKeyIndex = tupleColumn.indexOf(primaryKey);
        		String clusteringKey = "" + tupleData.get(clusteringKeyIndex);
        		if (clusteringKey.equals(strClusteringKeyValue)) {
        			Tuples tmp = tuples.get(j);
        			if (!indicies.isEmpty()) {
        				for (String fKey : indicies.keySet()) { //[id = id_index, gpa = gpa_index]
        					tree = deserializeBPTree(indicies.get(fKey));
        					int column = tmp.getColumns().indexOf(fKey);
        					T oldValue = (T) tmp.getTuples().get(column);
        					tree.delete(oldValue);
        					tree.removeKeys(oldValue);
        					serializeBPTree(indicies.get(fKey),tree);
        				}
        				    tuple.updateTuple(htblColNameValue);
        					tree = null;
        					for (String fKey : indicies.keySet()) { 
        						tree = deserializeBPTree(indicies.get(fKey));
        						int column = tuples.get(j).getColumns().indexOf(fKey);
        						T newValue = (T)tuple.getTuples().get(column);
        						tree.insert(newValue, page.getFileName());
        						tree.addKeys(newValue);
        						serializeBPTree(indicies.get(fKey),tree);
        					}
        				
        			}
        			else {
        				tuple.updateTuple(htblColNameValue);
        			}
        			serializePage(page.getFileName(), page);
        			serializeTable(t.getFilename(),t);
        			return;
        		}
        	}
        	serializePage(page.getFileName(), page);	
			if (flag==false) {
				serializeTable(t.getFilename(),t);
				throw new DBAppException("tuple not found");
			}
		}
		
		String primaryKeyIndex = indicies.get(primaryKey); //Second case => give priority to primary key index
		T key =null;
		tree = deserializeBPTree(primaryKeyIndex);
		Vector <T> keys = tree.getKeys();
		for (T candidateKey : keys) {
			String sKey = "" + candidateKey;
			if (sKey.equals(strClusteringKeyValue)) {
				key = candidateKey;
				break;
			}
		}
		if (!tree.getKeys().contains(key))
			throw new DBAppException("id does not exist");
		String pageName = tree.search(key);
		if (pageName==null) {
			throw new DBAppException("id does not exist");
		}
		Page page = deserializePage(pageName);
		Vector <Tuples> tuples = page.getTuples();
		for (int i =0;i<tuples.size();i++) {
			Tuples currentTuple = tuples.get(i);
			int primaryIndex = currentTuple.getColumns().indexOf(primaryKey);
			T primaryIndexValue = (T) currentTuple.getTuples().get(primaryIndex);
			String primaryIndexValueString = "" + primaryIndexValue;
			if (primaryIndexValueString.equals(strClusteringKeyValue)) {
				Tuples tmp = currentTuple;
				for (String fKey : indicies.keySet()) { //[id = id_index, gpa = gpa_index]
					tree = deserializeBPTree(indicies.get(fKey));
					int column = tmp.getColumns().indexOf(fKey);
					T oldValue = (T) tmp.getTuples().get(column);
					tree.delete(oldValue);
					tree.removeKeys(oldValue);
					serializeBPTree(indicies.get(fKey),tree);
				}
                currentTuple.updateTuple(htblColNameValue);
				serializeBPTree(primaryKeyIndex,tree);
				tree = null;
				for (String fKey : indicies.keySet()) { //[id = id_index, gpa = gpa_index]
					tree = deserializeBPTree(indicies.get(fKey));
					int column = tuples.get(i).getColumns().indexOf(fKey);
					T newValue = (T)currentTuple.getTuples().get(column);
					tree.insert(newValue, page.getFileName());
					tree.addKeys(newValue);
					serializeBPTree(indicies.get(fKey),tree);
				}
				serializePage(page.getFileName(), page);
       			serializeTable(t.getFilename(),t);
       			return;
			}
				
		}

	       
}
	
	
	public String binarySearchPageDelete( Vector<String> pageNames, Tuples target, String primaryKey) {
		int low = 0;
		int high = pageNames.size() - 1;
		String pageName = null;
		while (low <= high) {
			int mid = low + (high - low) / 2;
			pageName = pageNames.get(mid);
			Page page = deserializePage(pageName);
			int primaryIndex = page.getTuples().get(0).getColumns().indexOf(primaryKey);
			int primaryIndex2 = target.getColumns().indexOf(primaryKey);
			T minPK = (T)page.getTuples().get(0).getTuples().get(primaryIndex);
			T maxPK =  (T)page.getTuples().lastElement().getTuples().get(primaryIndex);
			T targetPK = (T) target.getTuples().get(primaryIndex2);
			if(targetPK.compareTo(minPK)>=0 && targetPK.compareTo(maxPK)<=0) {
				boolean flag = false;
				Vector <Tuples> tuples = page.getTuples();
				for (int i =0;i<tuples.size();i++) {
					Tuples tuple = tuples.get(i);
					if (tuple.equals(tuple)) {
						return pageName;
					}
				}
				if (flag==false)
					return null;
			}
			if(targetPK.compareTo(maxPK)>0) {
				low = mid + 1;

			}
			else {
				high = mid - 1;
			}

		}
		// If the target value doesn't exist in the array, return the insertion point
		return pageName;
	}



	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	//Find the value to delete using the index / ranges - they are more efficient, next delete from the page, next delete from the index/range.
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException{
		Table t = null;         //delete records where : name = arwa and id = 1111
		try {
			t = deserializeTable(strTableName+".class");
			
		} catch (FileNotFoundException e) {
			throw new DBAppException("table not found");
		}
		BTree <T,String> tree = null;
		checkObjectClassFromCSV("metadata.csv",strTableName,htblColNameValue);
		Hashtable <String,String> columnIndicies = new Hashtable<String,String>(); //[id = id_index, gpa = gpa_index]
		String primaryKey = getPrimaryKeyFromCSV("metadata.csv",strTableName);
		Hashtable <String,String> columns = t.getColumns();
		if (htblColNameValue.isEmpty()) {
			Vector <String> dpageNames = t.getPageNames();
			int size = dpageNames.size();
			int count =0;
			Vector <Page> dpages = t.getPages();
			for (int i=0;i<size;i++) {
				Page page =null;
				try {
				page = deserializePage(dpageNames.get(i));
				}
				catch (Exception e){
					page = deserializePage(dpageNames.get(i-count));
				}
				Vector <Tuples> dtuples = page.getTuples();
				for (int j =0;j<dtuples.size();j++) {
					Tuples tuple = dtuples.get(j);
					for (String key : columns.keySet()) {
						int colIndex = tuple.getColumns().indexOf(key);
						T colValue = (T) tuple.getTuples().get(colIndex);
						if (columnIndicies.containsKey(key) && !columnIndicies.isEmpty()) {
							String treeName = columnIndicies.get(key);
							tree = deserializeBPTree(treeName);
							tree.delete(colValue);
							tree.removeKeys(colValue);
							if (tree.getDuplicateKeys().size()!=0 ) {
								tree.getDuplicateKeys().removeAllElements();
								tree.getDuplicateKeysPages().removeAllElements();
							}
							serializeBPTree(treeName,tree);
							
						}
					}
					
				}
				File file = new File(page.getFileName());
    	        if (file.exists()) {
    	            if (file.delete()) {
    	            //    System.out.println("File deleted successfully.");
    	            } else 
    	                System.out.println("Failed to delete the file.");
    	        } else 
    	            System.out.println("File does not exist.");
        		t.removePage(page);	
        		t.setPageCount(t.getPageCount()-1);
        		t.removePageNames(page.getFileName());
        		count++;
		}
		serializeTable(t.getFilename(),t);
		return;
	}
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("metadata.csv"));
			String line = br.readLine();
			while (line != null) {
				String[] info = line.split(",");
				if (info[0].equals(strTableName) && !info[5].equals("null"))  {
					for (String key : htblColNameValue.keySet() ) {
						Object value = htblColNameValue.get(key);
						if (!(value instanceof java.lang.Double) && !(value instanceof java.lang.String)&& !(value instanceof java.lang.Integer)) {
							throw new DBAppException("Object must be integer, string or double");
						} //ne3mel keda fe kol el methods
					}
					columnIndicies.put(info[1], info[4]); //1)column ely ma3molo index
				}										 //2) esm el index dah (which supposedly i have an already created btree for it thats saved somewhere)
				
				line = br.readLine();
		}
			br.close();

		} catch (FileNotFoundException e ) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		
		Tuples rowToDelete = new Tuples(htblColNameValue,strTableName); //[0.95,Ahmed Noor]
		Vector <String> columnNames = rowToDelete.getColumns(); //[gpa,name]
		Vector <T> tupleValues = rowToDelete.getTuples(); //[1,Ahmed Noor]
		String tupleValuesString ="";
		String columnNamesString ="";
		int deleteColumn =0;
		T deleteValue = null;
		for (T o : tupleValues) {
			tupleValuesString = tupleValuesString + o;  //"1AhmedNoor"
		}
		for (String s : columnNames) {
			columnNamesString = columnNamesString + s;  //"gpaname"
		}
		String columnIndex = "";
		String treeIndex = "";
		boolean flag = false;
		for (String index : columnIndicies.keySet()) {
			for (int i =0; i<columnNames.size();i++) {
				String columnName = columnNames.get(i);
				if (index.equals(columnName)) {
					flag = true;
					break;
				}
				
			}
			if (flag == true ) {
				treeIndex = columnIndicies.get(index);
				tree = deserializeBPTree(treeIndex); 
				columnIndex = index;
				deleteColumn = rowToDelete.getColumns().indexOf(columnIndex);
				deleteValue = (T) rowToDelete.getTuples().get(deleteColumn);	
				break;
			}
		}
		boolean flagPrimary = false;
		if (rowToDelete.getColumns().contains(primaryKey)) {
			flagPrimary = true;
		}
		boolean flag2=false;
		if (flagPrimary==true && !columnIndicies.containsKey(primaryKey)) {   //=> BINARY SEARCH
			String pageName = binarySearchPageDelete( t.getPageNames(), rowToDelete , primaryKey);
			if (pageName==null) {
				throw new DBAppException("primary key value not found");
			}
			int primaryIndex = columnNames.indexOf(primaryKey);
			T primaryValue = tupleValues.get(primaryIndex);
			Page page = deserializePage(pageName);
			Vector <Tuples> tuples = page.getTuples();
			outerloop:
			for (int i =0;i<tuples.size();i++) {
				Tuples tuple = tuples.get(i);
				int index = tuple.getColumns().indexOf(primaryKey);
				T value = (T) tuple.getTuples().get(index);
				if (value.compareTo(primaryValue)==0) {
					flag2 = true;
					if (tuples.size() == 1 ) { 								//	indicies = {id = id_index, age = age_index}
		        		String indexName = "";								//tuple [id = 1, name = ahmed noor, age = 20, gpa = 0.1]
		        		for (String fkey : columnIndicies.keySet()) { 	// htblColNameValue =  {name =pola, gpa = 0.1}
		                	indexName = columnIndicies.get(fkey);
		                	tree = deserializeBPTree(indexName);
		                	int colIndex = tuple.getColumns().indexOf(fkey);
		                	T sth = (T) tuple.getTuples().get(colIndex);
		                	tree.removeKeys(sth);
		                	tree.delete(sth);
		                	serializeBPTree(indexName,tree);
		                }					
		        		File file = new File(page.getFileName());
	        	        if (file.exists()) {
	        	            if (file.delete()) {
	        	            //    System.out.println("File deleted successfully.");
	        	            } else 
	        	                System.out.println("Failed to delete the file.");
	        	        } else 
	        	            System.out.println("File does not exist.");
		        		page.removeFromPage(tuple);
		        		t.removePage(page);	
		        		t.setPageCount(t.getPageCount()-1);
		        		t.removePageNames(page.getFileName());
		        		break outerloop;
	        		}
	        		else {
	        			page.removeFromPage(tuple); 
	        			String indexName = "";							//indicies = {id = id_index, age = age_index}
	        			for (String fkey : columnIndicies.keySet()) { // htblColNameValue =  {name =pola, gpa = 0.1}		
	                		indexName = columnIndicies.get(fkey);
	                		tree = deserializeBPTree(indexName);
	                		int colIndex = tuple.getColumns().indexOf(fkey);
	                		T sth = (T) tuple.getTuples().get(colIndex);
	                		tree.removeKeys(sth);
	                		tree.delete(sth);	
	                		serializeBPTree(indexName,tree);	
	                	}		
	        			serializePage(page.getFileName(), page);
	        			break outerloop;
	        		}
				}
			}
			serializeTable(t.getFilename(),t);
			if(flag2==false) {
				throw new DBAppException("column not found");
			}
			return;
		}
		
		else if (columnNames.contains(primaryKey) && columnIndicies.containsKey(primaryKey)) { // => THIRD CASE -> DELETES ONLY ONE ROW
			tree = deserializeBPTree(columnIndicies.get(primaryKey));
			int primaryIndex = columnNames.indexOf(primaryKey);
			
			T primaryValue = tupleValues.get(primaryIndex);
			String pageName = tree.search(primaryValue); 
			if (pageName==null) 
				throw new DBAppException("record not found");
			if (pageName!=null) {
				Page page = deserializePage(pageName);
				Vector <Tuples> tuples = page.getTuples();
				outerloop:
				for (int i =0;i<tuples.size();i++) {
					Tuples tuple = tuples.get(i);
					int index = tuple.getColumns().indexOf(primaryKey);
					T value = (T) tuple.getTuples().get(index);
					if (value.compareTo(primaryValue)==0) {
						flag2 = true;
						if (tuples.size() == 1 ) { 								//	indicies = {id = id_index, age = age_index}
			        		String indexName = "";								//tuple [id = 1, name = ahmed noor, age = 20, gpa = 0.1]
			        		for (String fkey : columnIndicies.keySet()) { 	// htblColNameValue =  {name =pola, gpa = 0.1}
			                	indexName = columnIndicies.get(fkey);
			                	tree = deserializeBPTree(indexName);
			                	int colIndex = tuple.getColumns().indexOf(fkey);
			                	T sth = (T) tuple.getTuples().get(colIndex);
			                	tree.removeKeys(sth);
			                	tree.delete(sth);
			                	serializeBPTree(indexName,tree);
			                }					
			        		 File file = new File(page.getFileName());
			        	        if (file.exists()) {
			        	            if (file.delete()) {
			        	      //          System.out.println("File deleted successfully.");
			        	            } else 
			        	                System.out.println("Failed to delete the file.");
			        	        } else 
			        	            System.out.println("File does not exist.");
			        		page.removeFromPage(tuple);
			        		t.removePage(page);	
			        		t.setPageCount(t.getPageCount()-1);
			        		t.removePageNames(page.getFileName());
			        		break outerloop;
		        		}
		        		else {
		        			page.removeFromPage(tuple); 
		        			String indexName = "";							//indicies = {id = id_index, age = age_index}
		        			for (String fkey : columnIndicies.keySet()) { // htblColNameValue =  {name =pola, gpa = 0.1}		
		                		indexName = columnIndicies.get(fkey);
		                		tree = deserializeBPTree(indexName);
		                		int colIndex = tuple.getColumns().indexOf(fkey);
		                		T sth = (T) tuple.getTuples().get(colIndex);
		                		tree.removeKeys(sth);
		                		tree.delete(sth);	
		                		serializeBPTree(indexName,tree);	
		                	}		
		        			serializePage(page.getFileName(), page);
		        			break outerloop;
		        		}
					}
				}
				serializeTable(t.getFilename(),t);
				if(flag2==false) {
					throw new DBAppException("column not found");
				}
				return;
			}
		}

		else if (flag == true && !tree.getDuplicateKeys().contains(deleteValue) ) {
//			if (tree.getDuplicateKeys().contains(deleteValue)) {       //	=> FOURTH CASE : CHOOSE ONE TREE INDEX TO DELETE WITH AND HANDLING DUPLICATES -> DELETE MULTIPLE ROWS 
//				int size = tree.getDuplicateKeys().size();
//				for (Object duplicateKey : tree.getDuplicateKeys()) {
//					
//					if ((T) duplicateKey.getClass().getName()==deleteValue.getClass().getName()) {
//					if (((T) duplicateKey).compareTo(deleteValue)==0 ) {
////						System.out.println(tree.getDuplicateKeys());
//						int inde = tree.getDuplicateKeys().indexOf(duplicateKey);
//						String pageName =(String) tree.getDuplicateKeysPages().get(inde);
//						if (pageName==null)
//							continue;
//						Page page = deserializePage(pageName);
//						System.out.println(page);
//						Vector <Tuples> tuples = page.getTuples();
//						for (int j =0; j<tuples.size();j++) {
//							Tuples tuple = tuples.get(j);
//							//System.out.println(tuple);
//							String tupleString ="";
//		            		String columnString ="";
//		        			for (String k : columnNames ) { //[gpa, name]
//			        			int colIndex = tuple.getColumns().indexOf(k); 
//			        			T data = (T) tuple.getTuples().get(colIndex); 
//			        			columnString = columnString+k; 
//			        			tupleString = tupleString+data; 
//		        			}
//		        			if (columnString.equals(columnNamesString) && tupleString.equals(tupleValuesString) ) {	
//			        			if (tuples.size() == 1 ) { 							//indicies = {id = id_index, age = age_index}
//				        			String indexName = "";							//tuple [id = 1, name = ahmed noor, age = 20, gpa = 0.1]
//				        			for (String key : columnIndicies.keySet()) { 	// htblColNameValue =  {name =pola, gpa = 0.1}
//			                			indexName = columnIndicies.get(key);
//			                			tree = deserializeBPTree(indexName);
//			                			int colIndex = tuple.getColumns().indexOf(key);
//			                			T sth = (T) tuple.getTuples().get(colIndex);
//			                			tree.removeKeys(sth);
//			                			if (tree.getDuplicateKeys().size()!=0) {
//			                				tree.getDuplicateKeys().remove(duplicateKey);
//			                				int ind = tree.getDuplicateKeys().indexOf(duplicateKey);
//			                				String pn = (String) tree.getDuplicateKeysPages().get(ind);
//			                				tree.getDuplicateKeysPages().remove(pn);
//			                			}
//			                			tree.delete(sth);
//			                		    serializeBPTree(indexName,tree);	
//			                		}												
//				        			page.removeFromPage(tuple);
//				        			t.removePageNames(page.getFileName());
//				        			t.removePage(page);
//				        			t.setPageCount(t.getPageCount()-1);
//				        			flag = true;
//				        			File file = new File(page.getFileName());
//				        	        if (file.exists()) {
//				        	            if (file.delete()) {
//				        	            //    System.out.println("File deleted successfully.");
//				        	            } else 
//				        	                System.out.println("Failed to delete the file.");
//				        	        } else 
//				        	            System.out.println("File does not exist.");
//			        			}
//			        			else {
//			        				page.removeFromPage(tuple); 
//			        				flag = true;
//			        				String indexName = "";
//				                		for (String key : columnIndicies.keySet()) { // htblColNameValue =  {name =pola, gpa = 0.1}
//				                			indexName = columnIndicies.get(key);	// {gpa = gpa_index, 
//				                			tree = deserializeBPTree(indexName);
//				                			int colIndex = tuple.getColumns().indexOf(key);
//				                			T sth = (T) tuple.getTuples().get(colIndex);
//				                			tree.removeKeys(sth);
//				                			if (tree.getDuplicateKeys().size()!=0) {
//				                				System.out.println(tree.getDuplicateKeys().size());
//				                				tree.getDuplicateKeys().set(i, null);
//				                				tree.getDuplicateKeysPages().set(i, null);
//				                				System.out.println(tree.getDuplicateKeys().size());
//				                			}
//				                			tree.delete(sth);
//				                		    serializeBPTree(indexName,tree);
//				                		}
//			        				serializePage(page.getFileName(), page);
//			        			}
//			        		}
//
//						}
//					}
//					}
//				//	tree = deserializeBPTree(treeIndex);
//				}
//				serializeTable(t.getFilename(),t);
//				if (flag == false) 
//					throw new DBAppException("column not found");
//				return;
//	       }
			flag = false;
			String pageName = tree.search(deleteValue);  // => FOURTH CASE -> KEY HAS NO DUPLICATES
			if (pageName ==null) {
				throw new DBAppException("record does not exist");
			}
			Page page = deserializePage(pageName);
			Vector <Tuples> tuples = page.getTuples();
			for (int j =0; j<tuples.size();j++) {
				Tuples tuple = tuples.get(j);
				String tupleString ="";
	    		String columnString ="";
				for (String k : columnNames ) { 
	    			int colIndex = tuple.getColumns().indexOf(k); 
	    			T data = (T) tuple.getTuples().get(colIndex); 
	    			columnString = columnString+k; 
	    			tupleString = tupleString+data; 
				}
				if (columnString.equals(columnNamesString) && tupleString.equals(tupleValuesString) ) {
	    			if (tuples.size() == 1 ) { 
	        			String indexName = "";										
	        			for (String key : columnIndicies.keySet()) { 
	            			indexName = columnIndicies.get(key);
	            			tree = deserializeBPTree(indexName);
	            			int colIndex = tuple.getColumns().indexOf(key);
	            			T sth = (T) tuple.getTuples().get(colIndex);
	            			tree.removeKeys(sth);
	            			tree.delete(sth);
	            		    serializeBPTree(indexName,tree);	
	            		}	
	        			flag = true;
	        			page.removeFromPage(tuple);
	        			t.removePageNames(page.getFileName());
	        			t.removePage(page);
	        			t.setPageCount(t.getPageCount()-1);
	        			File file = new File(page.getFileName());
	        	        if (file.exists()) {
	        	            if (file.delete()) {
	        	             //   System.out.println("File deleted successfully.");
	        	            } else 
	        	                System.out.println("Failed to delete the file.");
	        	        } else 
	        	            System.out.println("File does not exist.");
	    			}
	    			else {
	    				flag = true;
	    				page.removeFromPage(tuple); 
	    				String indexName = "";
	    				for (String key : columnIndicies.keySet()) { 
	            			indexName = columnIndicies.get(key);
	            			tree = deserializeBPTree(indexName);
	            			int colIndex = tuple.getColumns().indexOf(key);
	            			T sth = (T) tuple.getTuples().get(colIndex);
	            			tree.removeKeys(sth);
	            			tree.delete(sth);
	            		    serializeBPTree(indexName,tree);
	            		}	
	    				serializePage(page.getFileName(), page);	
	    			}
	    		}

			}
			serializeTable(t.getFilename(),t);
			if (flag == false )
				throw new DBAppException("column not found");
			return;
		
		}
		else {
			Vector<Page> pages = t.getPages();
			int size = t.getPageNames().size();
			Vector <String> name = t.getPageNames();
			int count =0;
			for (int i =0; i< size;i++) {
				Page page =null;
				try {
	        	page = deserializePage((String) t.getPageNames().get(i)); 
				}
				catch (Exception e) {
					page = deserializePage((String) t.getPageNames().get(i-count));
				}
				
	        	Vector <Tuples> tuples = page.getTuples();
	        	Iterator<Tuples> iterator = tuples.iterator();
	        	outerloop:
	        	while (iterator.hasNext()) {
        			Tuples tuple = iterator.next();
        			String tupleString ="";
            		String columnString ="";
        			for (String k : columnNames ) { //[gpa, name]
	        			int colIndex = tuple.getColumns().indexOf(k); //gpa, name
	        			T data = (T) tuple.getTuples().get(colIndex); //2, ahmed noor
	        			columnString = columnString+k; //"gpaname
	        			tupleString = tupleString+data; //"2ahmednoor"
        			}
	        		if (columnString.equals(columnNamesString) && tupleString.equals(tupleValuesString) ) {
	        			if (tuples.size() == 1 ) { 
		        			String indexName = "";									
		        			if (!columnIndicies.isEmpty()) { 					
		                		for (String key : columnIndicies.keySet()) { 
		                			indexName = columnIndicies.get(key);
		                			tree = deserializeBPTree(indexName);
		                			int colIndex = tuple.getColumns().indexOf(key);
		                			T sth = (T) tuple.getTuples().get(colIndex);
		                			tree.delete(sth);
		                			tree.removeKeys(sth);
		                		    serializeBPTree(indexName,tree);	
		                		}
		                	}
		        			flag2=true;
		        			iterator.remove();
		        			t.removePageNames(page.getFileName());
		        			t.removePage(pages.get(i));
		        			t.setPageCount(t.getPageCount()-1);
		        			count++;
		        			i = i-1;
		        			File file = new File(page.getFileName());
		        	        if (file.exists()) {
		        	            if (file.delete()) {
		        	             //   System.out.println("File deleted successfully.");
		        	            } else 
		        	                System.out.println("Failed to delete the file.");
		        	        } else 
		        	            System.out.println("File does not exist.");
		        			break outerloop;
	        			}
	        			else {
	        				flag2=true;
	        				iterator.remove();
	        				String indexName = "";
	        				if (!columnIndicies.isEmpty()) { 
		                		for (String key : columnIndicies.keySet()) { 
		                			indexName = columnIndicies.get(key);
		                			tree = deserializeBPTree(indexName);
		                			int colIndex = tuple.getColumns().indexOf(key);
		                			T sth = (T) tuple.getTuples().get(colIndex);
		                			tree.delete(sth);
		                			tree.removeKeys(sth);
		                		    serializeBPTree(indexName,tree);
		                		}
		        	        	
		                	}
	        				serializePage(page.getFileName(), page);
	        			}
	        		}
        		}
        	}	
			serializeTable(t.getFilename(),t);
			if(flag2==false) 
				throw new DBAppException("column not found");
			return;    // => END OF LINEAR SEARCH
		}
		
}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException, IOException{
		String strTableName = arrSQLTerms[0]._strTableName;
		Table t = null;         //delete records where : name = arwa and id = 1111
		try {
			t = deserializeTable(strTableName+".class");
			
		} catch (FileNotFoundException e) {
			throw new DBAppException("table not found");
		}
		Vector <String> columnType = new Vector<>();
		Vector <T> columnValues = new Vector<>();
		Vector <String> columnTypeCSV = new Vector<>();
		Vector <String> columnValuesCSV = new Vector<>();
		for (int i =0; i<arrSQLTerms.length;i++) {
			columnType.add(arrSQLTerms[i]._strColumnName);
			columnValues.add((T) arrSQLTerms[i]._objValue );
		}
		for (int i =0; i<columnType.size();i++) {
			try (BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"))) {
	            String line;
	            while ((line = reader.readLine()) != null) {
	                String[] fields = line.split(",");
	                if (fields[0].equals(strTableName)) {
		                if (columnType.get(i).equals(fields[1])) {
		                	columnTypeCSV.add(fields[2]);
		                	break;
		                }
	                }
	                
	            }
	
	        }
			
	        catch (IOException e) {
	            System.err.print("file not found");
	        }

		}
		
		for (int i =0;i<columnValues.size();i++) {
			columnValuesCSV.add(columnValues.get(i).getClass().getName());
		}
		for (int i =0;i<columnValuesCSV.size();i++) {
			if (!columnValuesCSV.get(i).equals(columnTypeCSV.get(i))) {
				throw new DBAppException("colum types are not aligned");
			}
		}
		String primaryKey = getPrimaryKeyFromCSV("metadata.csv",strTableName);
		Hashtable treeIndicies = checkForIndexFromCSV("metadata.csv",strTableName);
	    Vector<String> pagesNames = t.getPageNames();
		Vector<Tuples> selectedTuples = new Vector<>();
		BTree <T,String> tree =null;
        Set<String> treeSetKeys = treeIndicies.keySet();
		String treeIndex = "";
		for (int i =0; i< columnType.size();i++) {
			T data = columnValues.get(i);
			if (!treeIndicies.isEmpty()&&treeIndicies.containsKey(columnType.get(i))) {
				//System.out.println(data);
				treeIndex = (String) treeIndicies.get(columnType.get(i));
				tree = deserializeBPTree(treeIndex);
				Vector <T> duplicateKeys = tree.getDuplicateKeys();
				Vector <String> duplicateKeysPages = tree.getDuplicateKeysPages();
				if (duplicateKeys.contains(data)) {
					for (String pageName : pagesNames ) {
						Page page = deserializePage(pageName);
						for (Tuples tuple : page.getTuples()) {			
							int colIndex = tuple.getColumns().indexOf(columnType.get(i));
							T colIndexValue = (T) tuple.getTuples().get(colIndex);
							if (colIndexValue.compareTo(data)==0) {
								if (checkConditions(tuple, arrSQLTerms, strarrOperators)) {
									if((!selectedTuples.isEmpty() &&  !selectedTuples.lastElement().equals(tuple)) || selectedTuples.isEmpty() ) {
	      			                     selectedTuples.add(tuple);

	                            	   }
				                }
							}
			            }
						serializePage(pageName,page);
					}
				}
				if (!duplicateKeys.contains(data)) {
					String pageName = tree.search(data);
					if (pageName==null) {
						throw new DBAppException("value does not exist");
					}
					Page page = deserializePage(pageName);
					Vector <Tuples> tuples = page.getTuples();
					for (int j =0;j<tuples.size();j++) {
						Tuples tuple = tuples.get(j);
						int colIndex = tuple.getColumns().indexOf(columnType.get(i));
						T colIndexValue = (T) tuple.getTuples().get(colIndex);
						if (colIndexValue.compareTo(data)==0) {
							if (checkConditions(tuple, arrSQLTerms, strarrOperators)) {
			                    selectedTuples.add(tuple);
			                }
						}
					}
					serializePage(pageName,page);
				}
			}
			else if (columnType.get(i).equals(primaryKey) && !treeIndicies.containsKey(primaryKey) ) {
				String primaryValue = ""+columnValues.get(i);
				System.out.println("hello");
				String pageName =binarySearchPageName( t.getPageNames(), primaryValue ,primaryKey);
				if (pageName==null) {
					throw new DBAppException("page not found");
				}
				Page page = deserializePage(pageName);
				Vector <Tuples> tuples = page.getTuples();
				for (int j =0;j<tuples.size();j++) {
					Tuples tuple = tuples.get(j);
					int colIndex = tuple.getColumns().indexOf(primaryKey);
					String colValue = ""+ tuple.getTuples().get(colIndex);
					if (colValue.equals(primaryValue)) {
						if (checkConditions(tuple, arrSQLTerms, strarrOperators)) {
							if((!selectedTuples.isEmpty() &&  !selectedTuples.lastElement().equals(tuple)) || selectedTuples.isEmpty() ) {
 			                     selectedTuples.add(tuple);

                       	   }
		                }
					}
				}
				serializePage(page.getFileName(),page);
				
			}
			
			else { //	=> LINEAR SEARCH
				for (int k = 0 ; k<t.getPageNames().size();k++) {
				    System.out.println("Hello");
				    String pageName = (String) t.getPageNames().get(k);
				    Page page = deserializePage(pageName);
					for (Tuples tuple : page.getTuples()) {			
						int colIndex = tuple.getColumns().indexOf(columnType.get(i));
						T colIndexValue = (T) tuple.getTuples().get(colIndex);
						if (colIndexValue.compareTo(data)==0) {
							if (checkConditions(tuple, arrSQLTerms, strarrOperators)) {
							    System.out.println("tuple: "+tuple);
                               if(!selectedTuples.contains(tuple)) {
                            	   if((!selectedTuples.isEmpty() &&  !selectedTuples.lastElement().equals(tuple)) || selectedTuples.isEmpty() ) {
      			                     selectedTuples.add(tuple);

                            	   }
                               }
			                }
						}
		            }
					serializePage(pageName,page);
				}
				
				
			}
			
			
			
		}
//
//		else if (treeIndicies.isEmpty()) {
//        	for (String pageName : pagesNames) {
//	        	Page page = deserializePage(pageName);
//	        	//System.out.println(page);
//	            for (Tuples tuple : page.getTuples()) {
//	                if (checkConditions(tuple, arrSQLTerms, strarrOperators)) {
//	                    selectedTuples.add(tuple);
//	                }
//	            }
//	            serializePage(pageName,page);
//	        }  
//		}
        serializeTable(t.getFilename(),t);
        Iterator<Tuples> iterator = selectedTuples.iterator();
        return iterator;
   
	}
	
	private boolean checkConditions(Tuples tuple, SQLTerm[] arrSQLTerms, String[] strarrOperators) {
        boolean result = true;
        for (int i = 0; i < arrSQLTerms.length; i++) { 
            SQLTerm term = arrSQLTerms[i];
            String columnName = term._strColumnName;
            int columnValue = tuple.getColumns().indexOf(columnName);
            T object = (T) tuple.getTuples().get(columnValue);
            String operator = term._strOperator;
            Object value = term._objValue;
            boolean condition = evaluateCondition(object, operator, value);
            if (i > 0) {
                String logicalOperator = strarrOperators[i - 1];
                if (logicalOperator.equalsIgnoreCase("AND")) {
                    result = result && condition;
                } else if (logicalOperator.equalsIgnoreCase("OR")) {
                    result = result || condition;
                }
                else if (logicalOperator.equalsIgnoreCase("XOR")) {
                    result = result ^ condition;
                }
            } else {
                result = condition;
            }
        }
        return result;
    }
    
	private boolean evaluateCondition(T columnValue, String operator, Object value) {
	    // Convert columnValue and value to appropriate types for comparison
	    T valueComparable = (T) value;
//	    System.out.println(valueComparable + " hiiii");
//	    System.out.println(columnValue.equals(value));
	    switch (operator) {
	        case ">":
	            return columnValue.compareTo(valueComparable) > 0;
	        case ">=":
	            return columnValue.compareTo(valueComparable) >= 0;
	        case "<":
	            return columnValue.compareTo(valueComparable) < 0;
	        case "<=":
	            return columnValue.compareTo(valueComparable) <= 0;
	        case "!=":
	            return !columnValue.equals(value);
	        case "=":
	            return columnValue.equals(value);
	        default:
	            throw new IllegalArgumentException("Invalid operator: " + operator);
	    }
	}
//Serilaization
	
public void serializePage(String filename, Page page) {
    	
        try (FileOutputStream fileOut = new FileOutputStream(filename);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
             out.writeObject(page);
          //  System.out.printf("Serialized data is saved in %s%n", filename + ".ser");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	public Page deserializePage(String filename) {
    	Page deserializedPage ;
    	try {
            FileInputStream fileInputStream = new FileInputStream(filename);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            deserializedPage = (Page) objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();
         //	System.out.println("Page deserialized");
            return deserializedPage;
            
       }catch (IOException | ClassNotFoundException e) {
    	   return null;
   }
   
    	
    }

	    private void serializeTable(String filename, Table table) {
	        try (FileOutputStream fileOut = new FileOutputStream(filename);
	             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
	             out.writeObject(table);
	            //System.out.printf("Serialized data is saved in %s%n", filename);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }

	    Table deserializeTable(String filename) throws FileNotFoundException{
	        Table table = null;
	        try (FileInputStream fileIn = new FileInputStream(filename);
	             ObjectInputStream in = new ObjectInputStream(fileIn)) {
	            table = (Table) in.readObject();
	        } catch (IOException | ClassNotFoundException e) {
	        	System.out.println("no such table exists");
	        }
	        
	        return table;
	    }

	    private void serializeBPTree(String filename, BTree tree) {
	        try (FileOutputStream fileOut = new FileOutputStream(filename);
	             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
	            out.writeObject(tree);
	         //   System.out.printf("Serialized data is saved in %s%n", filename);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }

	    public BTree deserializeBPTree(String filename) {
	        BTree tree = null;
	        try (FileInputStream fileIn = new FileInputStream(filename);
	             ObjectInputStream in = new ObjectInputStream(fileIn)) {
	            tree = (BTree) in.readObject();
	        } catch (IOException | ClassNotFoundException e) {
	        	System.out.println("no such tree exists");
	        }
	        return tree;
	    }
}