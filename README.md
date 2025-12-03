# Uso de bases de datos – Proyectos UOC
Este repositorio recopila prácticas de la asignatura **Uso de bases de datos** (UOC) sobre una base de datos de bodegas, zonas y vinos. 
El objetivo es mostrar mi forma de trabajar con PostgreSQL y PL/pgSQ para funciones, procedimientos y disparadores, también  de Java (JDBC) para acceder a la base de datos, ejecutar consultas y realizar inserciones/actualizaciones con transacciones.

Cada práctica está organizada en una carpeta propia:
**PR2 – Procedimientos y disparadores en PostgreSQL** (`PR2-procedimientos-y-disparadores`)  
   - Función `update_report_wine` que calcula estadísticas de ventas de un vino (total vendido, número de pedidos, cliente principal) y actualiza o inserta el registro en la tabla `REPORT_WINE`.  
   - Disparador sobre `order_line` que mantiene actualizada la columna `stock` de la tabla `WINE` y valida que no se hagan pedidos con stock insuficiente.  
**PR3 – Acceso a la base de datos con Java (JDBC)** (`PR3-java-jdbc-bodegas`)  
   - Clase `DBAccessor` para gestionar la conexión a PostgreSQL a partir de un fichero `db.properties`.  
   - Programa que consulta la vista `BEST_SELLING_ZONES` y genera un informe tabular.  
   - Programa que procesa un fichero de datos (`exercise2.data`) y realiza inserciones/actualizaciones en las tablas de bodegas, zonas y vinos usando transacciones y `PreparedStatement`
