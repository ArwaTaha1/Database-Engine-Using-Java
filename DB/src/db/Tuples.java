package db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Tuples<T extends Comparable<T>>  implements Serializable,Comparable{
  //  private final Hashtable<String, T> columns;
    private Vector<T> tuples = new Vector<T>() ;
    private Vector<String> columns = new Vector<>() ;
    
    
    
    public Tuples( Hashtable<String, T> columnValue, String strTableName) {
    	
    	for (String s : columnValue.keySet()) {
    		columns.add(s);
    		tuples.add(columnValue.get(s));
    	}
    }
    
    public void updateTuple(Hashtable <String, T> newValues) { //[name = arwa, age = 21]
    	for (String key : newValues.keySet()) {						//[id,gpa,name] => columnName
//    														        //[1,1.3,ahmed] =>tuples
    		String columnName = key;								
    		int colIndex = columns.indexOf(columnName);
    		T object = tuples.get(colIndex);
    		tuples.set(colIndex, newValues.get(columnName)); //[1,1.3,arwa]
    		

    	}
    }
   
	public String toString() {
		String s = "";
		for (int i = 0; i<tuples.size();i++) {
	           s = s + tuples.get(i) +",";
	        }
		if (s.length()>0) {
			s = s.substring(0,s.length() - 1);
	        s = s+"\n";
		}
    	return s;
	}
	public Vector<String> getColumns() {
		return columns;
	}
	public Vector<T> getTuples() {
		return tuples;
	}
	
	public boolean equals(Tuples tuples) {
		if (this.tuples.equals(tuples.tuples) && this.columns.equals(tuples.columns)) {
			return true;
		}
		return false;
	}

	@Override
	public int compareTo(Object o) {
		Tuples t = (Tuples) o;
		for(int i = 0;i <this.getColumns().size(); i++) {
			if(this.getTuples().get(i).compareTo((T) t.getTuples().get(i))==0) {
				
			}
			else {
				break;
			}
		}
		
		
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}
