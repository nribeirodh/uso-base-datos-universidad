package edu.uoc.practica.bd.util;

import java.io.*;
import java.sql.*;
import java.util.*;

public class DBAccessor {

  private String dbname;
  private String host;
  private String port;
  private String user;
  private String passwd;
  private String schema;

  /**
   * Initializes the class loading the database properties file and assigns values to the instance
   * variables.
   *
   * @throws RuntimeException Properties file could not be found.
   */
  public void init() {
    Properties prop = new Properties();
    InputStream propStream = this.getClass().getClassLoader().getResourceAsStream("db.properties");

    try {
      prop.load(propStream);
      this.host = prop.getProperty("host");
      this.port = prop.getProperty("port");
      this.dbname = prop.getProperty("dbname");
      this.user = prop.getProperty("user");
      this.passwd = prop.getProperty("passwd");
      this.schema = prop.getProperty("schema");
    } catch (IOException e) {
      String message = "ERROR: db.properties file could not be found";
      System.err.println(message);
      throw new RuntimeException(message, e);
    }
  }

  /**
   * Obtains a {@link Connection} to the database, based on the values of the
   * <code>db.properties</code> file.
   *
   * @return DB connection or null if a problem occurred when trying to connect.
   */

  public Connection getConnection() {
	    Connection conn = null; // Variable para almacenar la conexion a la base de datos
	    // TODO Implement the DB connection
	    // TODO Sets the search_path
	    try {
	        // Construir la URL de conexion usando los parametros configurados en db.properties
	        String sUrlBBDD = "jdbc:postgresql://" + this.host + ":" + this.port + "/" + this.dbname;

	        // Intentar establecer la conexion a la base de datos
	        conn = DriverManager.getConnection(sUrlBBDD, this.user, this.passwd);

	        // Verificar si la conexion es valida antes de configurar el search_path
	        if (conn != null && !conn.isClosed()) {
	            // Crear un Statement para ejecutar la sentencia SQL que configura el search_path
	            try (Statement stSearchPath = conn.createStatement()) {
	                // Configurar el esquema predeterminado (search_path) segun el valor de la propiedad schema
	                stSearchPath.execute("SET search_path TO " + this.schema);
	            }
	        }
	    } catch (SQLException e) {
	        // Si ocurre un error relacionado con SQL, mostrar un mensaje claro en la consola
	        System.err.println("ERROR SQL: No se pudo conectar a la base de datos ni configurar el esquema.");
	        e.printStackTrace(); // Mostrar detalles del error para facilitar la depuracion
	    } catch (Exception e) {
	        // Manejo de errores inesperados o genericos
	        System.err.println("ERROR INESPERADO: Se produjo un error desconocido al intentar conectar.");
	        e.printStackTrace(); // Mostrar detalles del error para facilitar la depuracion
	    }

	    // Devolver la conexion, sea valida o nula si no se pudo establecer
	    return conn;
	}
  
  
}
