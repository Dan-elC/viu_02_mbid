from cassandra.cluster import Cluster
import datetime
from datetime import date

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

    def __init__(self, cancion_isrc, cancion_titulo, cancion_anio, cancion_generos):

        # self.cancion_id = cancion_id
        # self.cancion_genero = cancion_genero
        self.cancion_isrc = cancion_isrc
        self.cancion_titulo = cancion_titulo
        self.cancion_anio = cancion_anio
        self.cancion_generos = cancion_generos

###################################################################################
################################## -- PARTE 3 -- ##################################
###################################################################################


# --- Creación de funciones de consulta a tablas con índices

def consultar_usuario_por_dni(usuario_dni):
    select = session.prepare(
        "SELECT * FROM Tabla1 WHERE usuario_dni = ?")
    filas = session.execute(select, [
        usuario_dni, ])
    # u = []
    for fila in filas:
        u = Usuario(fila.usuario_nombre, usuario_dni, fila.usuario_calle, fila.usuario_ciudad)
        # u.append(fila.usuario_nombre)
        return u
    print("NO SE ENCONTRARON DATOS")



def consultar_cancion_por_isrc(cancion_isrc):
    select = session.prepare(
        "SELECT * FROM Tabla5 WHERE cancion_isrc = ?")
    filas = session.execute(select, [
        cancion_isrc, ])
    # c = []
    for fila in filas:
        c = Cancion(cancion_isrc, fila.cancion_titulo, fila.cancion_anio, fila.cancion_generos)
        # c.append(fila.cancion_titulo)
        return c
    print("NO SE ENCONTRARON DATOS")




# --- CREACIÓN DE MÉTODOS DE INSERCIÓN DE DATOS ---


# -------- Cancion en tabla de consulta 5 --------
def insertar_cancion():
    # Pedimos al usuario que ingrese los datos
    print("\n--- Insertar Nueva Canción ---")
    isrc    = input("Isrc: ")
    titulo  = input("Titulo: ")
    anio    = input("Anio: ")

    generos = set()  # iniciamos la colección (set) que contendra los generos a insertar
    nuevo_genero = input("Introduzca un género, vacío para parar: ")
    while (nuevo_genero != " "):
        generos.add(nuevo_genero)
        nuevo_genero = input("Introduzca un género, vacío para parar: ")

    nueva_cancion = consultar_cancion_por_isrc(isrc) # Validamos si ya existe el usuario
    if(nueva_cancion != None):
        print("LA CANCIÓN YA SE ENCUENTRA REGISTRADA")
        print()
        return 0
    else: # Si no está registrado, lo agregamos
        nueva_cancion = Cancion(isrc, titulo, int(anio), generos)

    insertStatement = session.prepare(
        "INSERT INTO Tabla5 (cancion_genero, cancion_isrc, cancion_titulo, cancion_anio, cancion_generos) VALUES (?, ?, ?, ?, ?)")
    # Insertar géneros por canción
    for gen in generos:
        session.execute(insertStatement, [gen, nueva_cancion.cancion_isrc, nueva_cancion.cancion_titulo, nueva_cancion.cancion_anio, nueva_cancion.cancion_generos])
    print("Canción insertada correctamente.")


# -------- Usuario en tabla de consulta 1 --------
def insertar_usuario():
    # Pedimos al usuario que ingrese los datos
    print("\n--- Insertar Nuevo Usuario ---")
    dni     = input("DNI: ")
    nombre  = input("Nombre: ")
    calle   = input("Calle: ")
    ciudad  = input("Ciudad: ")
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



# ------------ 2

# CREATE TABLE danelcampana.Tabla2 (
    
#     grabacion_cod text,         -- PK: Clave de búsqueda (cod), ya que satisface la segunda consulta de búsqueda por código de grabación
#     usuario_dni text,           -- CK: Clave de unicidad y ordenamiento, ya que el DNI de la persona es único y no se repite.    
#     grabacion_fecha date,       -- Se genera el atributo fecha de la relación entre grabación y usuario
#     usuario_nombre text,        -- Nombre del usuario
#     usuario_calle text,         -- Nombre de la calle del usuario
#     usuario_ciudad text,        -- Nombre de la ciudad del usuario
#     grabacion_duracion int,     -- Duración de la grabación. Asumo que son segundos (int).  No es información del usuario pero ayuda a identificar si la grabación es correcta.
#     grabacion_tipo text,        -- Tipo de la grabación. No es información del usuario pero ayuda a identificar si la grabación es correcta.
    
#     PRIMARY KEY (grabacion_cod, usuario_dni)
# ) WITH CLUSTERING ORDER BY (usuario_dni ASC);

# -------------- 6

# CREATE TABLE danelcampana.Tabla6 (
    
#     grabacion_fecha date,        -- PK: Clave de búsqueda (la fecha de guardado). Campo por el cual se realizará la búsqueda
#     grabacion_cod text,          -- CKs: Para un mejor ordenamiento se toma el cod como CK, ya que se necesita la grabación.
#     usuario_dni text,            -- CKs: Para un mejor ordenamiento se toma el DNI como CK, ya que se necesita información del usuario.
#     grabacion_duracion INT,      -- Campos extras de duración de la grabación
#     grabacion_tipo text,         -- Campos extras de tipo de la grabación
#     usuario_nombre text,         -- Campos extras de nombre del usuario
#     usuario_calle text,          -- Campos extras de calle del usuario
#     usuario_ciudad text,         -- Campos extras de ciudad del usuario
#     -- La información del usuario y grabación ayudan a identificar si es el resultado esperado.
    
#     PRIMARY KEY (grabacion_fecha, grabacion_cod, usuario_dni)
# ) WITH CLUSTERING ORDER BY (grabacion_cod ASC, usuario_dni ASC);



def insertar_relacion_es_guardada_por():

    dni     = input("DNI: ")
    nombre  = input("Nombre: ")
    calle   = input("Calle: ")
    ciudad  = input("Ciudad: ")

    cod         = input("Código: ")
    duracion    = input("Duración: ")
    while not duracion.isnumeric():
        duracion = input("Duración: ")
    duracion = int(duracion)
    tipo        = input("Tipo: ")

    hoy = date.today()

    usuario = consultar_usuario_por_dni(dni)
    if (usuario != None):
        insertStatementUsuarioGrabacion = session.prepare(
            "INSERT INTO Tabla2 (grabacion_cod, usuario_dni, grabacion_fecha, usuario_nombre, usuario_calle, usuario_ciudad, grabacion_duracion, grabacion_tipo) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        insertStatementFechaGrabacionUsuario = session.prepare(
            "INSERT INTO Tabla6 (grabacion_fecha, grabacion_cod, usuario_dni, grabacion_duracion, grabacion_tipo, usuario_nombre, usuario_calle, usuario_ciudad) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

        session.execute(insertStatementUsuarioGrabacion,
                        [cod, dni, hoy, nombre, calle, ciudad, duracion, tipo])
        session.execute(insertStatementFechaGrabacionUsuario,
                        [hoy, cod, dni, duracion, tipo, nombre, calle, ciudad])
        print()
    else:
        print("No existe el usuario con dni " + dni)
        print()



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










# Programa principal
# Conexión con Cassandra
cluster = Cluster()
session = cluster.connect('danelcampana')


# test = consultar_usuario_por_dni('47839167')
# print(test.usuario_dni)
# print(test.usuario_nombre)
# print(test.usuario_calle)
# print(test.usuario_ciudad)


# test = consultar_cancion_por_isrc('12345678')
# print(test.cancion_isrc)
# print(test.cancion_titulo)
# print(test.cancion_anio)
# print(test.cancion_generos)


numero = -1
# Sigue pidiendo operaciones hasta que se introduzca 0
while (numero != 0):
    print("Introduzca un número para ejecutar una de las siguientes operaciones:")
    # print("1. Consultar canción por ISRC")
    # print("2. Consultar usuario por DNI")
    print("1. Insertar una canción")
    print("2. Insertar un usuario")
    print("3. Insertar relación es_guardada_por entre usuario y grabación")

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


    # if (numero == 1):
    #     cancion = consultar_cancion_por_isrc(input("Ingresa el ISRC de la canción: "))
    #     print(cancion)
    #     print()
    # elif (numero == 2):
    #     usuario = consultar_usuario_por_dni(input("Ingresa el DNI del usuario: "))
    #     print(usuario)
    #     print()
    if   (numero == 1):
        insertar_cancion()
    elif (numero == 2):
        insertar_usuario()
    elif (numero == 3):
        insertar_relacion_es_guardada_por()


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