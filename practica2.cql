-- TABLAS VISTAS EN LA "Práctica 1" (CLIENTE, PRODUCTO, PEDIDO) Y EN " Práctica 2, Parte 1: Puesta en marcha de base de datos Cassandra"

-- Export of keyspace practica2 (ver como "Exportar el esquema de un keyspace Cassandra almacenado en un contenedor Docker desde línea de comandos")

CREATE KEYSPACE practica2 WITH durable_writes = true AND replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE practica2.cliente_producto (
	cliente_nombre text,
	cliente_dni text,
	producto_idproducto int,
	producto_existencias int,
	producto_precio float,
	PRIMARY KEY (cliente_nombre, cliente_dni, producto_idproducto)
) WITH CLUSTERING ORDER BY ( cliente_dni ASC, producto_idproducto ASC );

CREATE TABLE practica2.productos_por_id (
	id int,
	existencias int,
	nombre text,
	precio float,
	PRIMARY KEY (id)
) ;

CREATE TABLE practica2.productos_cliente (
	producto_id int,
	cliente_id int,
	PRIMARY KEY (producto_id, cliente_id)
) WITH CLUSTERING ORDER BY ( cliente_id ASC );

CREATE TABLE practica2.numpedidos (
	pedido_fecha date,
	numpedidos counter,
	PRIMARY KEY (pedido_fecha)
);

CREATE TABLE practica2.clientes_pedidos (
	pedido_fecha date,
	pedido_idpedidos int,
	cliente_direccion text,
	cliente_dni text,
	cliente_idcliente int,
	pedido_nombre text,
	PRIMARY KEY (pedido_fecha, pedido_idpedidos)
) WITH CLUSTERING ORDER BY ( pedido_idpedidos ASC );

CREATE TABLE practica2.productos (
	precio float,
	idproducto int,
	existencias int,
	nombre text,
	PRIMARY KEY (precio, idproducto)
) WITH CLUSTERING ORDER BY ( idproducto ASC );

CREATE TABLE practica2.clientes_por_id (
	id int,
	direccion text,
	dni text,
	nombre text,
	preferencia SET<text>,
	PRIMARY KEY (id)
) ;

CREATE TABLE practica2.clientes_por_id_copy1 (
	preferencia text,
	id int,
	direccion text,
	dni text,
	nombre text,
	preferencias SET<text>,
	PRIMARY KEY (preferencia, id)
) WITH CLUSTERING ORDER BY ( id ASC );


CREATE TABLE practica2.clientes_por_preferencia (
	preferencia text,
	id int,
	direccion text,
	dni text,
	nombre text,
	preferencias SET<text>,
	PRIMARY KEY (preferencia, id)
) WITH CLUSTERING ORDER BY ( id ASC );

CREATE CUSTOM INDEX id_productos ON practica2.productos (idproducto) USING 'sai';
