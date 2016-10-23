package it.uniroma3.dia.sii;

import java.io.IOException;
import java.util.List;

import it.uniroma3.dia.sii.HBaseWrapper.Entry;
import it.uniroma3.dia.sii.HBaseWrapper.RowBean;


public class QueryDia {

	public static void main(String[] args) throws IOException {
		String table = "dia";
		String infoFamily = "informatica";
		String autoFamily = "automatica";
		String iaQualifier = "intelligenza artificiale";
		String roQualifier = "ricerca operativa";
		HBaseWrapper hbw = new HBaseWrapper();
		
		hbw.configureConnection();
		
		List<RowBean> list = hbw.getRowsByFamily(table, infoFamily);
		printFamily(list);
		list = hbw.getRowsByFamily(table, autoFamily);
		printFamily(list);
		
		list = hbw.getRowsByQualifier(table, infoFamily, iaQualifier);
		printFamily(list);
		list = hbw.getRowsByQualifier(table, autoFamily, roQualifier);
		printFamily(list);
	}

	private static void printFamily(List<RowBean> list) {
		String up = "";
		for(int j = 0; j < 110; j++)
			up += "-";
		
		System.out.format("%s\n", up);
		boolean isHeader = true;
		int i = 1;
		for (RowBean result : list) {
			for (Entry entry : result.getEntries()) {
				if (isHeader) {
					System.out.format("|%-5s | %-20s | %-20s | %-30s | %-20s |\n", "n", "Row Identifier", "Family", "Qualifier", "Value");
					System.out.format("%s\n", up);
					isHeader = false; 
				}
				String row = new String(entry.getRow());
				String family = new String(entry.getFamily());
				String qualifier = new String(entry.getQualifier());
				String value = new String(entry.getValue());

				System.out.printf("|%-5s | %-20s | %-20s | %-30s | %-20s |\n", i, row, family ,qualifier, value);
				i++;
			}
			
		}	
		System.out.format("%s\n", up);
	}

}
