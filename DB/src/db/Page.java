package db;

import java.io.*;
import java.util.*;

public class Page implements Serializable {
	private static final long serialVersionUID = 9129782331623932199L;
    private Vector<Tuples> tuples = new Vector<>() ;
    private static int pageID;
	private String clusteringKey; //primaryKey
    private  final int maxRows;
    private  Hashtable<String, String> column;
    //awel String = esm el cloumn
    //tany string = type of data cloumn accepts
 //   private currentIndex;
    private String tableName;
    private  String fileName ;
    
    private Table deserializeTable(String filename) throws FileNotFoundException{
        Table table = null;
        try (FileInputStream fileIn = new FileInputStream(filename);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            table = (Table) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
        	System.out.println("no such table exists");
        }
        
        return table;
    }
    
   
    public Page(String tableName) throws FileNotFoundException {
    	this.maxRows = DBApp.getMaxRows();
		this.tableName = tableName;
		Table t = deserializeTable(tableName+".class");
		//System.out.println(tableName);
		int size = t.getPageCount()+1;
		createFile(size, this.tableName);
    }
    
    public void addTuple(Tuples tuple, String tupleFileName) {
        tuples.add(tuple);
    }


   
    public Tuples getFirstTupleInPage() {
    	return tuples.get(0);
    }
    
//    public Page(int maxRows, Hashtable<String, String> columnTypes, String clusteringKey, String tablename)
//    {
//    	this.tableName = tablename;
//       this.maxRows = maxRows;
//       this.column = columnTypes;
//       this.clusteringKey = clusteringKey;
//       createFile(count++, this.tableName);
//	   this.tuples = new Vector();
//   }
    
    public void createFile(int pageIndex, String tableName) {
		this.fileName = this.tableName + pageIndex + ".class";
		//System.out.println(fileName);
		

		try {
			FileOutputStream fileOut = new FileOutputStream(fileName, false);
			
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}
    
    public boolean isFull() {
    	if (tuples.size()>=maxRows) 
    		return true;
    	
    	return false;
    }
    
    public String getFileName() {
		return fileName;
	}
    public void setFileName(String string) {
		this.fileName = string +".class";
	}

    
    public Page getPageName(Tuples tuple) {
    	return this;
    }
   
	public  Vector<Tuples> getTuples() {
		return tuples;
	}
	public void addTuple(Tuples tuple) {
		tuples.add(tuple);
	}
	public void removeFromPage(Tuples tuple) {
		   tuples.remove(tuple);
	   }
	public int getPageId() {
		return pageID;
	}
	public String getClusteringKey() {
		return clusteringKey;
	}
	public int getMaxRows() {
		return maxRows;
	}
	public Hashtable<String, String> getColumn() {
		return column;
	}

	public String toString() {
    	String s = "";
    	for(int i = 0 ; i< tuples.size() ; i++) {
    		s = s+ tuples.get(i) +"\n";
    	}
    	if (s.length()>0) {
    		s = s.substring(0,s.length() - 1);
            s = s+"\n";
    		}

    	return s;
    }
//    public static void main(String[]args) {
//		Hashtable<String,String> h = new Hashtable<>();
//		Page p = new Page(3,h,"Age","Student");
//		p.serializePage(p.getFileName(), p);
//		Page p1 = new Page(4,h,"height","St");
//		p1.serializePage(p1.getFileName(), p1);
//		Page p2 = new Page(4,h,"height","bath");
//		h.put("Name", "Ahmed");
//		h.put("Age", "20");
//		h.put("Add", "Zamalek");
//		Tuples t = new Tuples(h);
//		p.tuples.add(t);
//		p1.tuples.add(t);
//		Hashtable<String,String> h1 = new Hashtable<>();
//		h1.put("Name", "Mariam");
//		h1.put("Age", "20");
//		h1.put("Add", "Cairo");
//		Tuples t1 = new Tuples(h1);
//		p.tuples.add(t1);
//		p.deserializePage(p.getFileName());
//		System.out.println(p.toString());
//		p.setFileName("page1.class");
	//	System.out.println(p.getFileName());
//		System.out.println(p1.getFileName());
//		System.out.println(p2.getFileName());


		
		
		

	}

	
    
    
