import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDao {

	public void scanTable() throws Exception {

		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class
		HTable table = new HTable(config, "emp");

		// Instantiating the Scan class
		Scan scan = new Scan();

		// Scanning the required columns
		scan.addColumn(Bytes.toBytes("personal info"), Bytes.toBytes("Name"));
		scan.addColumn(Bytes.toBytes("personal info"), Bytes.toBytes("Age"));
		scan.addColumn(Bytes.toBytes("professional info"), Bytes.toBytes("Designation"));
		scan.addColumn(Bytes.toBytes("professional info"), Bytes.toBytes("Salary"));

		// Getting the scan result
		ResultScanner scanner = table.getScanner(scan);

		// Reading values from scan result
		for (Result result = scanner.next(); result != null; result = scanner.next())

			System.out.println("Found row : " + result);
		// closing the scanner
		scanner.close();

	}

	public void scanRow(String tableName,String partialRow) throws IOException {
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, tableName);

		byte[] prefix = Bytes.toBytes(partialRow);
		PrefixFilter prefixFilter = new PrefixFilter(prefix);
		Scan scan = new Scan(prefix);
		scan.setFilter(prefixFilter);
		ResultScanner resultScanner = table.getScanner(scan);

		for (Iterator<Result> iterator = resultScanner.iterator(); iterator.hasNext();) {
			Result result = iterator.next();

			String returnString = "";
			returnString += Bytes.toString(result.getValue(Bytes.toBytes("personal info"), Bytes.toBytes("Name"))) + ", ";
			returnString += Bytes.toString(result.getValue(Bytes.toBytes("personal info"), Bytes.toBytes("Age"))) + ", ";
			returnString += Bytes.toString(result.getValue(Bytes.toBytes("professional info"), Bytes.toBytes("Designation")))+ ", ";
			returnString += Bytes.toString(result.getValue(Bytes.toBytes("professional info"), Bytes.toBytes("Salary")));
			
			System.out.println(returnString);
		}
	}

	public void updateData() {

	}

	public void insertData() throws Exception {
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class
		HTable hTable = new HTable(config, "emp");

		// Instantiating Put class
		// accepts a row name.
		String row = "1001"+new Date();
		Put p = new Put(Bytes.toBytes(row));

		// adding values using add() method
		// accepts column family name, qualifier/row name ,value
		p.add(Bytes.toBytes("personal info"), Bytes.toBytes("Name"), Bytes.toBytes("raju singh"));
		p.add(Bytes.toBytes("personal info"), Bytes.toBytes("Age"), Bytes.toBytes("hyderabad"));
		p.add(Bytes.toBytes("professional info"), Bytes.toBytes("Designation"), Bytes.toBytes("Lead"));
		p.add(Bytes.toBytes("professional info"), Bytes.toBytes("Salary"), Bytes.toBytes("1000000"));

		// Saving the put Instance to the HTable.
		hTable.put(p);
		System.out.println("data inserted");

		// closing HTable
		hTable.close();
	}

	public static void main(String[] args) throws Exception {
		//new HBaseDao().scanTable();
		//new HBaseDao().insertData();
		new HBaseDao().scanRow("emp","1001");
	}
}
