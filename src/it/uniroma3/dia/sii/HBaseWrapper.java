package it.uniroma3.dia.sii;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class HBaseWrapper {

	private Configuration config;

	public void configureConnection() {
		this.config = HBaseConfiguration.create();
		return;
	}

	public void addRecord(String tableName, String rowKey, String family,
			String qualifier, String value) throws IOException {
		HTable tb = new HTable(this.config, tableName);

		Put p = new Put(rowKey.getBytes());

		p.add(family.getBytes(), qualifier.getBytes(), value.getBytes());

		tb.put(p);

		tb.close();
	}

	public void delRecord(String tableName, String rowKey) throws IOException {
		HTable tb = new HTable(this.config, tableName);

		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		tb.delete(list);
		tb.close();
	}

	public RowBean getOneRecord (String tableName, String rowKey) throws IOException{
		HTable tb = new HTable(this.config, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = tb.get(get);
		RowBean rb = new RowBean();
		
		for (KeyValue entry : rs.raw()) {
			rb.addEntry(entry.getRow(), entry.getFamily(), entry.getQualifier(), entry.getValue());
		}
		
		tb.close();
		return rb;
	}
	
	public List<RowBean> getRowsByFamily (String tableName, String familyName) throws IOException {
		HTable tb = new HTable( HBaseConfiguration.create(), tableName);

		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		allFilters.addFilter(new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(familyName.getBytes())));
		Scan scan = new Scan();
		scan.setFilter(allFilters);
		ResultScanner scanner = tb.getScanner(scan);
		
		List<RowBean> list = new ArrayList<>();
		RowBean rb = null;
		
		for (Result result : scanner) {
			rb = new RowBean();
			for (KeyValue entry : result.raw()) {
				rb.addEntry(entry.getRow(), entry.getFamily(), entry.getQualifier(),
						entry.getValue());
			}
			list.add(rb);
		}	
		
		tb.close();

		return list;
	}
	
	public List<RowBean> getRowsByQualifier(String tableName, String familyName, String qualifierName) throws IOException {
		HTable tb = new HTable( HBaseConfiguration.create(), tableName);

		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		allFilters.addFilter(new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(familyName.getBytes())));
		allFilters.addFilter(new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(qualifierName.getBytes())));
		Scan scan = new Scan();
		scan.setFilter(allFilters);
		ResultScanner scanner = tb.getScanner(scan);
		
		List<RowBean> list = new ArrayList<>();
		RowBean rb = null;
		
		for (Result result : scanner) {
			rb = new RowBean();
			for (KeyValue entry : result.raw()) {
				rb.addEntry(entry.getRow(), entry.getFamily(), entry.getQualifier(),
						entry.getValue());
			}
			list.add(rb);
		}	
		
		tb.close();

		return list;
	}

	
	public class RowBean {
		private List<Entry> entries;
		
		public RowBean() {
			this.entries = new ArrayList<>();
		}
		
		
		public void addEntry(byte[] row,byte[] family,byte[] qualifier,byte[] value) {
			Entry e = new Entry();
			e.setValue(value);
			e.setFamily(family);
			e.setQualifier(qualifier);
			e.setRow(row);
			entries.add(e);
		}
		
		public List<Entry> getEntries() {
			return entries;
		}
		
	}

	public class Entry {
		
		private byte[] row;
		
		private byte[] family;
		
		private byte[] qualifier;
		
		private byte[] value;

		public byte[] getRow() {
			return row;
		}

		public void setRow(byte[] row) {
			this.row = row;
		}

		public byte[] getFamily() {
			return family;
		}

		public void setFamily(byte[] family) {
			this.family = family;
		}

		public byte[] getQualifier() {
			return qualifier;
		}

		public void setQualifier(byte[] qualifier) {
			this.qualifier = qualifier;
		}

		public byte[] getValue() {
			return value;
		}

		public void setValue(byte[] value) {
			this.value = value;
		}
	}
}