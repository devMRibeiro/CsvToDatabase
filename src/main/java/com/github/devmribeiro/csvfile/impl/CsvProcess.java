package com.github.devmribeiro.csvfile.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.github.devmribeiro.csvfile.dao.CsvReaderDao;
import com.github.devmribeiro.csvfile.log.Log;

public class CsvProcess {
	public static void processFile() {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("src/main/resources/uf_2024.csv", StandardCharsets.UTF_8));
			br.readLine(); // ignora header

			Set<List<String>> set = new HashSet<>();

			String line;
		    while ((line = br.readLine()) != null)
		        set.add(Arrays.asList(line.split(";")));

		    if (CsvReaderDao.insert(set))
		    	Log.i("Registro inseridos com sucesso");
		    else
		    	Log.e("Erro ao inserir");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) try { br.close(); } catch (IOException e) { System.out.println("Erro ao tentar liberar BufferedReader: " + e); }
		}
	}

	public static void main(String[] args) {
		processFile();
	}
}