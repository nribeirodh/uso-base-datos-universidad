package edu.uoc.practica.bd.uocdb.exercise2;

import edu.uoc.practica.bd.util.DBAccessor;
import edu.uoc.practica.bd.util.FileUtilities;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class Exercise2InsertAndUpdateDataFromFile {

	private FileUtilities fileUtilities;

	public Exercise2InsertAndUpdateDataFromFile() {
		super();
		fileUtilities = new FileUtilities();
	}

	public static void main(String[] args) {
		Exercise2InsertAndUpdateDataFromFile app = new Exercise2InsertAndUpdateDataFromFile();
		app.run();
	}

	private void run() {
		List<List<String>> fileContents = null;

		try {
			fileContents = fileUtilities.readFileFromClasspath("exercise2.data");
		} catch (FileNotFoundException e) {
			System.err.println("Error: El archivo de datos no se encontro.");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error: Ocurrio un problema de entrada/salida al leer el archivo.");
			e.printStackTrace();
		}

		if (fileContents == null) {
			return;
		}

		DBAccessor dbaccessor = new DBAccessor();
		dbaccessor.init();
		Connection conn = dbaccessor.getConnection();

		if (conn == null) {
			return;
		}

		//////////////////////////////////////////////////
		//INI

		try {
			// Iniciamos una transaccion
			conn.setAutoCommit(false);

			// Definimos las consultas SQL para las operaciones necesarias
			// sqlCheckWinery: verificar si la bodega ya existe en la base de datos
			String sqlCheckWinery = "select * from winery where winery_id = ?";
			// sqlUpdateWinery: actualizar los datos de la bodega si ya existe
			String sqlUpdateWinery = "update winery set winery_phone = ?, sales_representative = ? where winery_id = ?";
			// sqlInsertWinery: insertar una nueva bodega si no existe
			String sqlInsertWinery = "insert into winery (winery_id, winery_name, town, established_year, winery_phone, sales_representative) values (?, ?, ?, ?, ?, ?)";
			// sqlSelectZone: verificar si la zona ya existe en la base de datos
			String sqlSelectZone = "select * from zone where zone_id = ?";
			// sqlInsertZone: insertar una nueva zona si no existe
			String sqlInsertZone = "insert into zone (zone_id, zone_name, capital_town, climate, region) values (?, ?, ?, ?, ?)";
			// sqlInsertWine: insertar un nuevo vino con stock inicial 0
			String sqlInsertWine = "insert into wine (wine_name, vintage, alcohol_content, category, color, winery_id, zone_id, price, stock) values (?, ?, ?, ?, ?, ?, ?, ?, 0)";

			// Creamos los preparedStatement para las consultas definidas anteriormente
			PreparedStatement checkWineryStmt = conn.prepareStatement(sqlCheckWinery);
			PreparedStatement updateWineryStmt = conn.prepareStatement(sqlUpdateWinery);
			PreparedStatement insertWineryStmt = conn.prepareStatement(sqlInsertWinery);
			PreparedStatement selectZoneStmt = conn.prepareStatement(sqlSelectZone);
			PreparedStatement insertZoneStmt = conn.prepareStatement(sqlInsertZone);
			PreparedStatement insertWineStmt = conn.prepareStatement(sqlInsertWine, PreparedStatement.RETURN_GENERATED_KEYS);

			// Iteramos sobre cada fila del archivo de datos para procesar la informacion
			for (List<String> datosFila : fileContents) {
				// 1. Verificamos si la bodega ya existe
				System.out.println("- Registro a tratar "+datosFila.get(0));
				setPSCheckWinery(checkWineryStmt, datosFila); // Configuramos los parametros del SELECT
				ResultSet rsWinery = checkWineryStmt.executeQuery();
				if (rsWinery.next()) {
					// Si la bodega existe, actualizamos los datos
					setPSUpdateWinery(updateWineryStmt, datosFila); // Configuramos los parametros del UPDATE
					updateWineryStmt.executeUpdate(); // Ejecutamos el UPDATE
					System.out.println("\tActualizacion wnery con winery_id "+datosFila.get(6));
				} else {
					// Si la bodega no existe, la insertamos en la base de datos
					setPSInsertWinery(insertWineryStmt, datosFila); // Configuramos los parametros del INSERT
					insertWineryStmt.executeUpdate(); // Ejecutamos el INSERT
					System.out.println("\tAlta winery con winery_id "+datosFila.get(6));
				}
				rsWinery.close(); // Cerramos el ResultSet para liberar recursos

				// 2. Verificamos si la zona ya existe
				setPSSelectZone(selectZoneStmt, datosFila); // Configuramos los parametros del SELECT
				if (!selectZoneStmt.executeQuery().next()) {
					// Si la zona no existe, la insertamos en la base de datos
					setPSInsertZone(insertZoneStmt, datosFila); // Configuramos los parametros del INSERT
					insertZoneStmt.executeUpdate(); // Ejecutamos el INSERT
					System.out.println("\tAlta zone con zone_id "+datosFila.get(12));
				}

				// 3. Insertamos un nuevo vino
				setPSInsertWine(insertWineStmt, datosFila); // Configuramos los parametros del INSERT
				insertWineStmt.executeUpdate(); // Ejecutamos el INSERT
				try (ResultSet generatedKeys = insertWineStmt.getGeneratedKeys()) {
					if (generatedKeys.next()) {
						// Mostramos el ID del nuevo vino insertado
						System.out.println("\tNuevo vino insertado con ID: " + generatedKeys.getInt(1));
					} else {
						throw new SQLException("No se pudo recuperar el ID del vino insertado.");
					}
				}
			}

			// Confirmamos los cambios realizados en la base de datos
			conn.commit();
		} catch (Exception e) {
			// Si ocurre un error, revertimos todos los cambios realizados en la transaccion
			System.err.println("Error: ocurrio un problema durante la transaccion. se revirtieron los cambios.");
			try {
				conn.rollback();
			} catch (Exception ex) {
				System.err.println("Error: fallo al intentar revertir los cambios.");
			}
		} finally {
			// Cerramos todos los recursos para liberar memoria y evitar problemas
			try {
				if (conn != null) conn.close();
			} catch (Exception ex) {
				System.err.println("Error: no se pudieron cerrar correctamente los recursos.");
			}
		}

		//FIN
		//////////////////////////////////////////////////
	}

	private void setPSCheckWinery(PreparedStatement stmt, List<String> row) throws SQLException {
		String[] rowArray = row.toArray(new String[0]);
		setValueOrNull(stmt, 1, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 6)));
	}

	private void setPSUpdateWinery(PreparedStatement stmt, List<String> row) throws SQLException {
		String[] rowArray = row.toArray(new String[0]);
		setValueOrNull(stmt, 1, getValueIfNotNull(rowArray, 10));
		setValueOrNull(stmt, 2, getValueIfNotNull(rowArray, 11));
		setValueOrNull(stmt, 3, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 6)));
	}

	private void setPSInsertWinery(PreparedStatement stmt, List<String> row) throws SQLException {
		String[] rowArray = row.toArray(new String[0]);
		setValueOrNull(stmt, 1, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 6)));
		setValueOrNull(stmt, 2, getValueIfNotNull(rowArray, 7));
		setValueOrNull(stmt, 3, getValueIfNotNull(rowArray, 8));
		setValueOrNull(stmt, 4, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 9)));
		setValueOrNull(stmt, 5, getValueIfNotNull(rowArray, 10));
		setValueOrNull(stmt, 6, getValueIfNotNull(rowArray, 11));
	}

	private void setPSSelectZone(PreparedStatement stmt, List<String> row) throws SQLException {
		String[] rowArray = row.toArray(new String[0]);
		setValueOrNull(stmt, 1, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 12)));
	}

	private void setPSInsertZone(PreparedStatement stmt, List<String> row) throws SQLException {
		String[] rowArray = row.toArray(new String[0]);
		setValueOrNull(stmt, 1, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 12)));
		setValueOrNull(stmt, 2, getValueIfNotNull(rowArray, 13));
		setValueOrNull(stmt, 3, getValueIfNotNull(rowArray, 14));
		setValueOrNull(stmt, 4, getValueIfNotNull(rowArray, 15));
		setValueOrNull(stmt, 5, getValueIfNotNull(rowArray, 16));
	}

	private void setPSInsertWine(PreparedStatement stmt, List<String> row) throws SQLException {
		String[] rowArray = row.toArray(new String[0]);
		setValueOrNull(stmt, 1, getValueIfNotNull(rowArray, 0));
		setValueOrNull(stmt, 2, getDoubleFromStringOrNull(getValueIfNotNull(rowArray, 1)));
		setValueOrNull(stmt, 3, getDoubleFromStringOrNull(getValueIfNotNull(rowArray, 2)));
		setValueOrNull(stmt, 4, getValueIfNotNull(rowArray, 3));
		setValueOrNull(stmt, 5, getValueIfNotNull(rowArray, 4));
		setValueOrNull(stmt, 6, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 6)));
		setValueOrNull(stmt, 7, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 12)));
		setValueOrNull(stmt, 8, getDoubleFromStringOrNull(getValueIfNotNull(rowArray, 5)));
	}

	private Integer getIntegerFromStringOrNull(String integer) {
		return (integer != null) ? Integer.valueOf(integer) : null;
	}

	private Double getDoubleFromStringOrNull(String doubl) {
		return (doubl != null) ? Double.valueOf(doubl) : null;
	}

	private String getValueIfNotNull(String[] rowArray, int index) {
		return (index < rowArray.length && rowArray[index].length() > 0) ? rowArray[index] : null;
	}

	private void setValueOrNull(PreparedStatement stmt, int parameterIndex, Integer value) throws SQLException {
		if (value == null) {
			stmt.setNull(parameterIndex, Types.INTEGER);
		} else {
			stmt.setInt(parameterIndex, value);
		}
	}

	private void setValueOrNull(PreparedStatement stmt, int parameterIndex, Double value) throws SQLException {
		if (value == null) {
			stmt.setNull(parameterIndex, Types.DOUBLE);
		} else {
			stmt.setDouble(parameterIndex, value);
		}
	}

	private void setValueOrNull(PreparedStatement stmt, int parameterIndex, String value) throws SQLException {
		if (value == null || value.length() == 0) {
			stmt.setNull(parameterIndex, Types.VARCHAR);
		} else {
			stmt.setString(parameterIndex, value);
		}
	}
}
