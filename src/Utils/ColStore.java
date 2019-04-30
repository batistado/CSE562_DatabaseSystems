package Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import Iterators.RAIterator;
import Models.Schema;
import dubstep.Main;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class ColStore {
	public static void makeColStore(CreateTable createTable) {
		try {
			String dir = RAIterator.TEMP_DIR + "ColStore/"+ createTable.getTable().getName() + "/";
			File directory = new File(dir);
	 	    if (!directory.exists()){
	 	        directory.mkdir();
	 	    }
	 	    
			Map<String, Schema> schemaByColumnName = Main.tableSchemas.get(createTable.getTable().getName())
					.schemaByName();
			int size = getSize(schemaByColumnName);

			BufferedWriter[] writers = new BufferedWriter[size];

			for (String columnName : schemaByColumnName.keySet()) {

				BufferedWriter bw = new BufferedWriter(new FileWriter(
						dir + columnName));
				writers[schemaByColumnName.get(columnName).getColumnIndex()] = bw;

			}

			BufferedReader br = new BufferedReader(
					new FileReader(RAIterator.DIR + createTable.getTable().getName() + ".csv"));
			
			String line = "";
			
			while ((line = br.readLine())!= null) {
				String[] values = line.split("\\|");
				
				for (int i=0; i<values.length; i++) {
					writeValue(values[i], writers, i);
				}
			}
			
			close(writers);
			
			br.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void close(BufferedWriter[] writers) throws IOException {
		for (BufferedWriter bw: writers) {
			bw.flush();
			bw.close();
		}
	}
	
	public static void writeValue(String value, BufferedWriter[] writers, int index) throws IOException {
		BufferedWriter bw = writers[index];
		
		bw.write(value);
		bw.write("\n");
		
	}

	public static int getSize(Map<String, Schema> schemaByColumnName) {
		int size = -1;

		for (String columnName : schemaByColumnName.keySet()) {
			int index = schemaByColumnName.get(columnName).getColumnIndex();

			if (index > size)
				size = index;
		}

		return size + 1;
	}
}
