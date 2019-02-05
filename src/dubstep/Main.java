package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Main {

	public static void main(String[] args) {
		StringReader input = new StringReader(args[0]);
		CCJSqlParser parser = new CCJSqlParser(input);
		try {
			Statement queryStatement = parser.Statement();

			if (queryStatement instanceof Select) {
				PlainSelect selectStatement = (PlainSelect) ((Select) queryStatement).getSelectBody();

				if (selectStatement instanceof PlainSelect) {
					Table tableName = (Table) selectStatement.getFromItem();
					printer(tableName.getName());
				}

			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.out.println("Can't read query" + args[0]);
			e.printStackTrace();
		}

	}
	public static void printer(String tableName) {
		String path = "data/"+tableName+".csv";
		String line = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
				while ((line = br.readLine()) != null) {
					 System.out.println(line);
				 }
				br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
