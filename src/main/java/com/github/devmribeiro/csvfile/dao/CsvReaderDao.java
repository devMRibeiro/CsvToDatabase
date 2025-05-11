package com.github.devmribeiro.csvfile.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.db.utility.impl.ResUtil;
import com.github.devmribeiro.csvfile.log.Log;

public class CsvReaderDao {
	private static boolean createTables(Connection conn) {

		PreparedStatement psUF = null;
		PreparedStatement psMunicipio = null;

		try {
			psUF = conn.prepareStatement(
					"create table if not exists unidade_federativa (" +
					" uf_id serial primary key, " +
					" uf_cod smallint not null unique, " + 
					" uf_nome varchar(50) not null, " +
					" uf_sigla varchar(2) not null, " +
					" uf_regiao varchar(20) not null)");
			
			psMunicipio = conn.prepareStatement(
					"create table if not exists municipio (" +
					"  mun_cod integer primary key not null, " +
					"  mun_nome varchar(100) not null, " +
					"  mun_uf_cod smallint not null, " +
					"  constraint fk_unidade_federativa foreign key (mun_uf_cod) references unidade_federativa(uf_cod))");

			if (psUF.executeUpdate() > 0)
				Log.d(psUF);

			if (psMunicipio.executeUpdate() > 0)
				Log.d(psMunicipio);

			return true;

		} catch (Exception e) {
			Log.e("Erro ao criar tabelas", e);
		} finally {
			ResUtil.close(psUF, psMunicipio);
		}
		return false;
	}
	
	public static boolean insert(Collection<List<String>> data, boolean isMunicipio) {

		Connection conn = null;
		PreparedStatement ps = null;

		String sql = (isMunicipio) ? "insert into municipio (mun_cod, mun_nome, mun_uf_cod) values (?, ?, ?)" :
									 "insert into unidade_federativa (uf_cod, uf_nome, uf_sigla, uf_regiao) values (?, ?, ?, ?)";

		try {

			conn = ResUtil.open();

			if (!createTables(conn))
				return false;

			ps = conn.prepareStatement(sql);

			int batchSize = 500;
			int count = 1;

			Iterator<List<String>> itr = data.iterator();
			if (isMunicipio) {
				while (itr.hasNext()) {
					List<String> values = itr.next();

					ps.setInt(1, Integer.parseInt(values.get(0)));
					ps.setString(2, values.get(1));
					ps.setInt(3, Integer.parseInt(values.get(2)));

					ps.addBatch();
					Log.d(ps);

					if (count % batchSize == 0)
	                	ps.executeBatch();

	                count++;
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