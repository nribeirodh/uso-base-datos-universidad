package edu.uoc.practica.bd.uocdb.exercise1;

import edu.uoc.practica.bd.util.*;
import java.sql.*;
import java.util.*;

public class Exercise1PrintReportOverQuery {

	public static void main(String[] args) {
		Exercise1PrintReportOverQuery app = new Exercise1PrintReportOverQuery();
		app.run();
	}

	private void run() {
		DBAccessor dbaccessor = new DBAccessor();
		dbaccessor.init();	
		Connection conn = dbaccessor.getConnection();

		if (conn != null) {
			Statement cstmt = null;
			ResultSet resultSet = null;

			try {
				List<Column> columns = Arrays.asList(new Column("Zone", 12, "zone_name"),
						new Column("Capital", 12, "capital_town"),
						new Column("Climate", 15, "climate"),
						new Column("Region", 20, "region"), 
						new Column("Last selling", 12, "last_selling"),
						new Column("Total", 5, "total_quantity")
						);

				Report report = new Report();
				report.setColumns(columns);
				List<Object> list = new ArrayList<Object>();

				//////////////////////////////////////////////////
				//INI

				//Crear un Statement para ejecutar la consulta SQL
				cstmt = conn.createStatement();

				//Ejecutar la consulta. La consulta usa la vista best_selling_zones
				resultSet = cstmt.executeQuery(
						"select zone_name, capital_town, climate, region, last_selling, total_quantity " +
								"from best_selling_zones"
						);
				/* Realizamos la consulta sobre la vista creada mediante el script entregado:
							create view best_selling_zones as
							select z.zone_name, 
							z.capital_town, 
							z.climate,
							z.region, 
							MAX(o.order_date) as last_selling, 
							sum(l.quantity) AS total_quantity 
							from zone z 
							NATURAL JOIN wine w 
							NATURAL JOIN order_line l
							NATURAL JOIN customer_order o
							group by z.zone_name, z.capital_town, z.climate, z.region
							order by total_quantity DESC
							LIMIT 5;
				 */

				//Crear una variable para guardar cada fila del resultado
				Exercise1Row exercise1Row = null;

				//Leer las filas del resultado una a una
				while (resultSet.next()) {
					// Por cada fila, obtener los valores de las columnas y crear un objeto Exercise1Row
					exercise1Row = new Exercise1Row(
							resultSet.getString("zone_name"),        // Columna zone_name
							resultSet.getString("capital_town"),    // Columna capital_town
							resultSet.getString("climate"),         // Columna climate
							resultSet.getString("region"),          // Columna region
							resultSet.getString("last_selling"),    // Columna last_selling
							resultSet.getLong("total_quantity")     // Columna total_quantity
							);

					// Agregar el objeto Exercise1Row a la lista
					list.add(exercise1Row);
				}

				//Verificar si se obtuvieron resultados
				if (list.isEmpty()) {
					System.out.println("No se encontraron registros en la vista best_selling_zones.");
				} else {
					// Llamar a printReport para mostrar la lista de resultados
					report.printReport(list);
				}

				//FIN
				//////////////////////////////////////////////////
			} 
			catch (Exception e) {
				// Mostrar un mensaje si ocurre un error inesperado
				System.err.println("ERROR: List not available.\n");
				e.printStackTrace();
			}
			finally {
				// Cerrar todos los recursos abiertos
				if (resultSet!=null)
					try {
						resultSet.close();
					} catch(Exception ex) {}
				if (cstmt!=null)
					try {
						cstmt.close();
					} catch(Exception ex) {}
				if (conn!=null)
					try {
						conn.close();
					} catch(Exception ex) {}
			}
		}
	}
}
