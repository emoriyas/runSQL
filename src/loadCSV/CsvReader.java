package loadCSV;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.opencsv.CSVReader;

public class CsvReader {

	public CsvReader(){
	}
	
	//p
	public ArrayList<String[]> readCSV(String file){
		
	    String [] nextLine;
		ArrayList<String[]> csv = new ArrayList<String[]>();
		
		try {
			CSVReader reader = new CSVReader(new FileReader(file), ',', '\0');
			
		     while ((nextLine = reader.readNext()) != null) {
		    	 csv.add(nextLine);
			 }
		     
		     reader.close();
			
		} catch(FileNotFoundException e) {
			System.out.println("CSV file not found");
			csv = null;
		} catch(IOException e) {
			System.out.println("error while reading csv");
			csv = null;
		}
		
		
		return(csv);
	}
	
	public void printCSV(ArrayList<String[]> csv){
	     for(int i = 0; i < csv.size();i++) {
	    	 for(int j = 0; j < csv.get(i).length; j++) {
	    		 if (j > 0) {
	    			 System.out.print(", ");
	    		 }
	    		 System.out.print(csv.get(i)[j]);
	    	 }
	    	 System.out.println();
	     }
	}
}
