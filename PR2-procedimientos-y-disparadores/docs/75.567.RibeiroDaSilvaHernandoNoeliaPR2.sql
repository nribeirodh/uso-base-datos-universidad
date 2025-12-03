SET search_path TO ubd_20241;
--Ejercicio 1
CREATE OR REPLACE FUNCTION update_report_wine(p_wine_id INT)
RETURNS REPORT_WINE_TYPE AS $$
DECLARE
    v_report REPORT_WINE_TYPE;  -- Variable para almacenar el tipo compuesto
    v_existe_en_report_wine INT;	-- Variable para comprobar si existe el vino en la tabla
BEGIN
    
	
	with estadistica_vino as (	
		select	-- estadisticas sobre el vino
			wine_id
			, sum(quantity) as total_sold	-- cajas vendidas 
			, count(distinct(order_id)) as orders	-- pedidos
		from order_line
		group by wine_id 
	), mejor_cliente_aux as (
		/*
		y el cliente que ha realizado más pedidos del vino
		(customer_id y customer_name). En caso de empate, se debe seleccionar el cliente con más cajas
		compradas, y si todavía hay empate, el cliente cuyo nombre sea primero en orden alfabético.
		 * */
		select 
			pedido.wine_id
			, cliente.customer_id
			, cliente.customer_name
			, sum(pedido.quantity) as cajas 
			, count(distinct(pedido.order_id)) as pedidos 
			, rank() over( partition by pedido.wine_id order by sum(pedido.quantity) desc, count(distinct(pedido.order_id)) desc, cliente.customer_name asc ) as ranking
		from order_line pedido 
		join customer_order pedido_cliente on ( pedido.order_id = pedido_cliente.order_id )
		join customer cliente on ( pedido_cliente.customer_id = cliente.customer_id )
		group by cliente.customer_id
				, cliente.customer_name
				, pedido.wine_id
	), mejor_cliente as (
		select * from mejor_cliente_aux 
		where ranking = 1
	) 
	select	-- Info de los vinos
	into v_report
		wine.wine_id
		, wine.wine_name
		, wine.alcohol_content
		, wine.category
		, wine.price
		, wine.prizes
		, stats.total_sold
		, stats.orders
		, cli.customer_id
		, cli.customer_name
	from wine 
	left join estadistica_vino stats on ( wine.wine_id = stats.wine_id )
	left join mejor_cliente cli on ( wine.wine_id = cli.wine_id )
	where 1 = 1 
		and wine.wine_id = p_wine_id;
	
		
	
	-- Validar si no existen registros
    IF v_report IS NULL THEN
        RAISE WARNING 'No se encontraron registros para el vino con ID %', p_wine_id;
       	return NULL;
    END IF;
   
   IF v_report.t_orders is null then 
   		RAISE INFO 'El vino con ID % no ha recibido pedidos aún', p_wine_id;
   end if;
	
  
  	
  	-- Comprobar si hay registro en la tabla de report_wine 
  	select 
  	into v_existe_en_report_wine
		count(1) as existe_en_report_wine
	from report_wine
	where wine_id = p_wine_id;
	
	if v_existe_en_report_wine > 0 then 
		-- UPDATE EN LA TABLA
		update report_wine set 
			wine_name = v_report.t_wine_name
			, alcohol_content = v_report.t_alcohol_content
			, category = v_report.t_category
			, price = v_report.t_price
			, prizes = v_report.t_prizes
			, total_sold = v_report.t_total_sold
			, orders = v_report.t_orders
			, customer_id = v_report.t_customer_id
			, customer_name = v_report.t_customer_name
		where wine_id = p_wine_id;
	
		RAISE INFO 'Actualizado el vino % en la tabla report_wine', p_wine_id;
	
	else 
		-- INSERT EN LA TABLA 
		insert into report_wine ( wine_id, wine_name, alcohol_content, category, price, prizes, total_sold, orders, customer_id, customer_name )
		values ( v_report.t_wine_id, v_report.t_wine_name, v_report.t_alcohol_content, v_report.t_category
				, v_report.t_price, v_report.t_prizes, v_report.t_total_sold, v_report.t_orders, v_report.t_customer_id
				, v_report.t_customer_name  );
		
		RAISE INFO 'Insertado el vino % en la tabla report_wine', p_wine_id;
	end if;
	
	return v_report;
	
END;
$$ LANGUAGE plpgsql;


-- select update_report_wine(2)

--Ejercicio 2
--Actualizar el stock de la tabla vinos en cada insert o actualización en la tabla order_line

create or replace function actualizar_stock()
returns trigger AS $$
	declare v_cant_stock int;
	declare v_cantidad_antigua int;
	declare v_nueva_cantidad int;
begin
	
	raise info 'Disparador: %', tg_op;
	
	
	-- Si es negativo, lanzar error
	if new.quantity < 0 then 
		raise exception 'La cantidad no puede ser inferior a 0';
	end if;

	-- Obtener el stock del vino
	select 
	into v_cant_stock
		stock 
	from wine 
	where wine_id = new.wine_id;

	v_cantidad_antigua = 0;
	if tg_op = 'UPDATE' then 
		v_cantidad_antigua = old.quantity;
	end if;


	v_nueva_cantidad = new.quantity - v_cantidad_antigua;

	-- Si no hay stock suficiente cuando es insert, lanzar error
	if  v_cant_stock < v_nueva_cantidad then 
		raise exception 'La cantidad del pedido es superior al stock actual del vino %. Cantidad de pedido: %; Cantidad de stock: %', new.wine_id, v_nueva_cantidad, v_cant_stock;
	end if;

	
	RAISE INFO 'Actualizando el stock del vino % [ % ] -> [ % ]', new.wine_id, v_cantidad_antigua , v_nueva_cantidad;

	-- Si no se ha lanzado error todavía, actualizar el stock
	update wine set 
		stock = stock - v_nueva_cantidad
	where wine_id = new.wine_id;
	
	
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;






