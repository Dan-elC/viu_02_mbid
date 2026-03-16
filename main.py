from cassandra.cluster import Cluster

# cluster = Cluster()
# session = cluster.connect('danelcampana')
# session = cluster.connect()
# print(session.execute("SELECT release_version FROM system.local").one())



class Usuario:

    def __init__(self, usuario_nombre, usuario_dni, usuario_calle, usuario_ciudad):

        # self.Usuario_id = Usuario_id
        self.usuario_nombre = usuario_nombre
        self.usuario_dni    = usuario_dni
        self.usuario_calle  = usuario_calle
        self.usuario_ciudad = usuario_ciudad


class Cancion:

    def __init__(self, cancion_id, cancion_genero, cancion_isrc, cancion_titulo, cancion_anio, cancion_generos):

        self.cancion_id = cancion_id
        self.cancion_genero = cancion_genero
        self.cancion_isrc = cancion_isrc
        self.cancion_titulo = cancion_titulo
        self.cancion_anio = cancion_anio
        self.cancion_generos = cancion_generos


# --- Creación de funciones de consulta a tablas con índices


def consultar_usuario_por_dni(usuario_dni):
    select = session.prepare(
        "SELECT * FROM Tabla1 WHERE usuario_dni = ?")
    filas = session.execute(select, [
        usuario_dni, ])
    for fila in filas:
        u = Usuario(fila.usuario_nombre, usuario_dni, fila.usuario_calle, fila.usuario_ciudad)
        return u
    print("NO SE ENCONTRARON DATOS")




def consultar_cancion_por_isrc(cancion_isrc):
    select = session.prepare(
        "SELECT * FROM Tabla5 WHERE cancion_isrc = ?")
    filas = session.execute(select, [
        cancion_isrc, ])
    for fila in filas:
        u = Cancion(fila.cancion_genero, cancion_isrc, fila.cancion_titulo, fila.cancion_anio, fila.cancion_generos)
        return u
    print("NO SE ENCONTRARON DATOS")






# --- Creación de métodos de inserción de datos

def insertar_usuario():
    # Pedimos al usuario que ingrese los datos
    print("\n--- Insertar Nuevo Usuario ---")
    dni = input("DNI: ")
    nombre = input("Nombre: ")
    calle = input("Calle: ")
    ciudad = input("Ciudad: ")
    nuevo_usuario = consultar_usuario_por_dni(dni) # Validamos si ya existe el usuario
    if(nuevo_usuario != None):
        print("EL USUARIO YA SE ENCUENTRA REGISTRADO")
        return 0
    else: # Si no está registrado, lo agregamos
        nuevo_usuario = Usuario(nombre, dni, calle, ciudad)
    insertStatement = session.prepare(
        "INSERT INTO Tabla1 (usuario_nombre, usuario_dni, usuario_calle, usuario_ciudad) VALUES (?, ?, ?, ?)")
    session.execute(insertStatement, [nuevo_usuario.usuario_nombre, nuevo_usuario.usuario_dni, nuevo_usuario.usuario_calle, nuevo_usuario.usuario_ciudad])
    print("Usuario insertado correctamente.")



def insertar_cancion():
    # Pedimos al usuario que ingrese los datos
    print("\n--- Insertar Nueva Canción ---")
    genero  = input("Genero: ")
    isrc    = input("Isrc: ")
    titulo  = input("Titulo: ")
    anio    = input("Anio: ")


    generos = set()  # iniciamos la colección (set) que contendra los generos a insertar
    genero_adicional = input("Introduzca una preferencia, vacío para parar")
    while (genero_adicional != ""):
        generos.add(genero_adicional)
        genero_adicional = input("Introduzca una preferencia, vacío para parar")


    nuevo_usuario = consultar_usuario_por_dni(dni) # Validamos si ya existe el usuario
    if(nuevo_usuario != None):
        print("EL USUARIO YA SE ENCUENTRA REGISTRADO")
        print()
        return 0
    else: # Si no está registrado, lo agregamos
        nuevo_usuario = Usuario(nombre, dni, calle, ciudad)
    insertStatement = session.prepare(
        "INSERT INTO Tabla1 (usuario_nombre, usuario_dni, usuario_calle, usuario_ciudad) VALUES (?, ?, ?, ?)")
    session.execute(insertStatement, [nuevo_usuario.usuario_nombre, nuevo_usuario.usuario_dni, nuevo_usuario.usuario_calle, nuevo_usuario.usuario_ciudad])
    print("Usuario insertado correctamente.")







def insertar_relacion_guardada(session):
    """Inserta la relación es_guardada_por en tablas 2 y 6[cite: 106]."""
    print("\n--- Registrar Grabación Guardada ---")
    cod_grabacion = int(input("Código de Grabación (int): "))
    dni_usuario = input("DNI del Usuario: ")
    fecha = input("Fecha (YYYY-MM-DD): ") # Formato date 
    
    # Inserción en Tabla 2 (Consulta 2)
    query2 = "INSERT INTO Tabla2 (Grabacion_cod, usuario_dni, Grabacion_fecha) VALUES (%s, %s, %s)"
    session.execute(query2, (cod_grabacion, dni_usuario, fecha))
    
    # Inserción en Tabla 6 (Consulta 6)
    query6 = "INSERT INTO Tabla6 (Grabacion_fecha, Grabacion_cod, usuario_dni) VALUES (%s, %s, %s)"
    session.execute(query6, (fecha, cod_grabacion, dni_usuario))
    print("Relación guardada con éxito.")



# --- Creación de funciones de actualización de datos

def actualizar_nombre_usuario(session):
    """Actualiza nombre en base al DNI (Consulta 1)[cite: 112]."""
    dni = input("DNI del usuario a modificar: ")
    nuevo_nombre = input("Nuevo nombre: ")
    
    # Nota: En Cassandra, si el nombre es parte de la PK (Partition Key), 
    # se debe borrar e insertar de nuevo o usar tablas soporte[cite: 113].
    query = "UPDATE Tabla1 SET Usuario_nombre = %s WHERE usuario_dni = %s"
    # Este comando asume que se creó una tabla soporte o el DNI es la única PK.
    session.execute(query, (nuevo_nombre, dni))
    print("Nombre actualizado.")


# --- Creación de funciones de borrado de datos

def borrar_grabaciones_por_fecha(session):
    """Borra grabaciones de una fecha específica[cite: 115]."""
    fecha = input("Fecha a borrar (YYYY-MM-DD): ")
    query = "DELETE FROM Tabla6 WHERE Grabacion_fecha = %s"
    session.execute(query, (fecha,))
    print(f"Registros del {fecha} eliminados.")









# test = consultar_usuario_por_dni('47839167')
# test = consultar_cancion_por_isrc('12345')



# session.shutdown()







# Programa principal
# Conexión con Cassandra
cluster = Cluster()
session = cluster.connect('danelcampana')


# test = consultar_usuario_por_dni('47839167')
# print(test.usuario_dni)
# print(test.usuario_nombre)
# print(test.usuario_calle)
# print(test.usuario_ciudad)


numero = -1
# Sigue pidiendo operaciones hasta que se introduzca 0
while (numero != 0):
    print("Introduzca un número para ejecutar una de las siguientes operaciones:")
    print("1. Insertar un cliente")
    print("2. Insertar un producto")
    print("3. Insertar relación entre cliente, producto y pedido (todos datos)")
    print("4. Insertar relación entre cliente, producto y pedido (solo id cliente)")
    print("5. Consultar datos cliente según su id")
    print("6. Consultar datos de los productos comprados por un cliente dando DNI y nombre")
    print("7. Consultar datos de los productos que tienen un precio dado")
    print("8. Actualizar precio producto")
    print("9. Borrar los pedidos de una fecha")
    print("10. Consultar los clientes que tienen una direccion asignada")
    print("11. Actualizar el nombre de clientes en base a su direccion")
    print("0. Cerrar aplicación")

    numero = int(input())  # Pedimos numero al usuario


    if (numero == 1):
        insertar_usuario()
#     elif (numero == 2):
#         insertProducto()
#     elif (numero == 3):
#         insertClientePedidosProductos()
#     elif (numero == 4):
#         insertClientePedidosProductosSelectCliente()
#     elif (numero == 5):
#         consultaClientePorId()
#     elif (numero == 6):
#         consultaProductosCompradosCliente()
#     elif (numero == 7):
#         consultaDatosProductosPrecio()
#     elif (numero == 8):
#         actualizarPrecioProducto()
#     elif (numero == 9):
#         borrarPedidoFecha()
#     elif (numero == 10):
#         print (consultaEjercicio4(input("Dame el nombre de una direccion")))
#     elif (numero == 11):
#         actualizacionNombreCliente()
#     else:
#         print("Número incorrecto")


cluster.shutdown()  # cerramos conexion