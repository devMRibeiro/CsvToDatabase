package com.github.devmribeiro.csvfile.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.db.utility.impl.ResUtil;
import com.github.devmribeiro.csvfile.log.Log;

public class CsvReaderDao {
	public static boolean insert(Collection<List<String>> data, boolean isMunicipio) {

		Connection conn = null;
		PreparedStatement ps = null;

		String sql = (isMunicipio) ? "insert into municipio (mun_cod, mun_nome, mun_uf_cod) values (?, ?, ?)" :
									 "insert into unidade_federativa (uf_cod, uf_nome, uf_sigla, uf_regiao) values (?, ?, ?, ?)";

		try {

			conn = ResUtil.open();

			ps = conn.prepareStatement(sql);

			Iterator<List<String>> itr = data.iterator();
			if (isMunicipio) {
				while (itr.hasNext()) {
					List<String> values = itr.next();

					ps.setInt(1, Integer.parseInt(values.get(0)));
					ps.setString(2, values.get(1));
					ps.setInt(3, Integer.parseInt(values.get(2)));

					ps.addBatch();
					Log.d(ps);
				}
			} else {
				while (itr.hasNext()) {
					List<String> values = itr.next();

					ps.setInt(1, Integer.parseInt(values.get(0)));
	                ps.setString(2, values.get(1));
	                ps.setString(3, values.get(2));
	                ps.setString(4, values.get(3));

	                ps.addBatch();
	                Log.d(ps);
				}
			}

			ps.executeBatch();
			conn.commit();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ResUtil.close(ps, conn);
		}
		return false;
	}
}