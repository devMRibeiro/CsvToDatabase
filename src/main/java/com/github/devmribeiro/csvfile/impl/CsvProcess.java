package com.github.devmribeiro.csvfile.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.github.devmribeiro.csvfile.dao.CsvReaderDao;
import com.github.devmribeiro.csvfile.log.Log;

public class CsvProcess {
	public static void processMunicipioCsv(String fileName) {
		processFile(fileName, true);
	}

	public static void processUnidadeFederativaCsv(String fileName) {
		processFile(fileName, false);
	}

	private static void processFile(String fileName, boolean isMunicipio) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fileName, StandardCharsets.UTF_8));
			br.readLine(); // ignora header

			Collection<List<String>> records = (isMunicipio) ? new ArrayList<List<String>>(5573) : new HashSet<List<String>>(27); 

			String line;
		    while ((line = br.readLine()) != null) 
		    	records.add(Arrays.asList(line.split(";")));

			if (CsvReaderDao.insert(records, isMunicipio))
				Log.i("Registros inseridos com sucesso");
			else
				Log.e("Erro ao inserir");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) try { br.close(); } catch (IOException e) { Log.e("Erro ao tentar liberar BufferedReader: " + e); }
		}
	}

	public static void main(String[] args) {
//		processMunicipioCsv("src/main/resources/municipios_2024.csv");
		processUnidadeFederativaCsv("src/main/resources/uf_2024.csv");
	}
}