from cassandra.cluster import Cluster
import datetime
from datetime import date


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



# -------- Relación es_guardada_por (consultas 2 y 6) --------
def insertar_relacion_es_guardada_por():

    usuario_dni = input("DNI: ")

    grabacion_cod         = input("Código de grabación: ")
    grabacion_duracion    = input("Duración de grabación: ")
    while not grabacion_duracion.isnumeric():
        grabacion_duracion = input("Duración de grabación: ")
    grabacion_duracion = int(grabacion_duracion)
    grabacion_tipo        = input("Tipo de grabación: ")

    hoy = date.today()

    usuario = consultar_usuario_por_dni(usuario_dni)
    if (usuario != None):
        insertStatementTabla2 = session.prepare(
            "INSERT INTO Tabla2 (grabacion_cod, usuario_dni, grabacion_fecha, usuario_nombre, usuario_calle, usuario_ciudad, grabacion_duracion, grabacion_tipo) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        insertStatementTabla6 = session.prepare(
            "INSERT INTO Tabla6 (grabacion_fecha, grabacion_cod, usuario_dni, grabacion_duracion, grabacion_tipo, usuario_nombre, usuario_calle, usuario_ciudad) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

        session.execute(insertStatementTabla2,
                        [grabacion_cod, usuario_dni, hoy, usuario.usuario_nombre, usuario.usuario_calle, usuario.usuario_ciudad, grabacion_duracion, grabacion_tipo])
        session.execute(insertStatementTabla6,
                        [hoy, grabacion_cod, usuario_dni, grabacion_duracion, grabacion_tipo, usuario.usuario_nombre, usuario.usuario_calle, usuario.usuario_ciudad])
        print()
    else:
        print("No existe el usuario con dni " + usuario_dni)
        print()



# -------- Relación conjunta es_interpretada_por y es_de (consultas 3 y 4) --------
def insertar_relacion_es_interpretada_por_es_de():
    cancion_isrc    = input("ISRC de canción: ")
    artista_cod     = input("Cod de artista: ")
    artista_cod     = int(artista_cod)
    pais_nombre     = input("Nombre de país: ")
    pais_cod        = input("Cod de país: ")
    pais_cod        = int(pais_cod)
    # cancion_titulo  = input("Título de canción: ")

    artista_nombre  = input("Nombre artista: ")
    artista_premios = set()
    premio = input("Nombre de premio (espacio para terminar): ")
    while premio != " ":
        artista_premios.add(premio)
        premio = input("Nombre de premio (espacio para terminar): ")

    # Se valida si la canción existe
    cancion = consultar_cancion_por_isrc(cancion_isrc)
    if(cancion != None):
        insertStatementTabla3 = session.prepare(
            """INSERT INTO Tabla3
            (cancion_isrc, artista_cod, pais_nombre, pais_cod, cancion_titulo)
            VALUES (?, ?, ?, ?, ?)"""
        )
        insertStatementTabla4 = session.prepare(
            """INSERT INTO Tabla4
            (pais_cod, artista_cod, artista_nombre, artista_premios, pais_nombre)
            VALUES (?, ?, ?, ?, ?)"""
        )

        session.execute(insertStatementTabla3, [cancion_isrc, artista_cod, pais_nombre, pais_cod, cancion.cancion_titulo])
        session.execute(insertStatementTabla4, [pais_cod, artista_cod, artista_nombre, artista_premios, pais_nombre])

    else:
        print("No existe la cancion con isrc " + cancion_isrc)
        print()





# -------- Actualizar el nombre de un usuario en base a su DNI. (Consulta 1) --------
def actualizar_nombre_usuario():
    usuario_dni     = input("DNI del usuario a modificar: ")

    usuario = consultar_usuario_por_dni(usuario_dni)
    if(usuario != None):
        nuevo_nombre = input("Ingresa el nuevo nombre: ")

        deleteStatementTabla1 = session.prepare(
            """DELETE FROM Tabla1 WHERE usuario_dni = ? and usuario_nombre = ?"""
        )
        insertStatementTabla1 = session.prepare(
            """INSERT INTO Tabla1
            (usuario_nombre, usuario_dni, usuario_calle, usuario_ciudad)
            VALUES (?, ?, ?, ?)"""
        )
        session.execute(deleteStatementTabla1, [usuario_dni, usuario.usuario_nombre])
        session.execute(insertStatementTabla1, [nuevo_nombre, usuario_dni, usuario.usuario_calle, usuario.usuario_ciudad])
    else:
        print("No existe el usuario con dni " + usuario_dni)
        print()
    



# --- Creación de funciones de borrado de datos
def borrar_grabaciones_por_fecha():
    fecha = input("Fecha a borrar (YYYY-MM-DD): ")
    year, month, day = map(int, fecha.split('-'))
    fecha_convertida = datetime.date(year, month, day)

    deleteStatementTabla6 = session.prepare(
        """DELETE FROM Tabla6 WHERE grabacion_fecha = ?"""
    )
    session.execute(deleteStatementTabla6, [fecha_convertida, ])
    print(f"Registros del {fecha} eliminados.")


# --- Consultar datos de usuario por DNI
def consultar_datos_usuario():
    usuario_dni = input("DNI de usuario: ")
    usuario = consultar_usuario_por_dni(usuario_dni)
    if(usuario != None):
        print("Nombre: ",usuario.usuario_nombre)
        print("Calle_ ",usuario.usuario_calle)
        print("Ciudad: ",usuario.usuario_ciudad)
        print()
    else:
        print("Usuario no existe para el DNI: " + usuario_dni)
        print()


# --- Consulta tabla 1
def consulta_tabla1():
    usuario_nombre = input("Nombre del usuario: ")
    select = session.prepare(
        """SELECT * FROM Tabla1 WHERE usuario_nombre = ?"""
    )
    filas = session.execute(select, [
        usuario_nombre, ])
    for fila in filas:
        print("DNI de usuario: " + fila.usuario_dni)
        print("Calle de usuario: " + fila.usuario_calle)
        print("Ciudad de usuario: " + fila.usuario_ciudad)


# --- Consulta tabla 2
def consulta_tabla2():
    grabacion_cod = input("Código de grabación: ")

    select = session.prepare(
        """SELECT * FROM Tabla2 WHERE grabacion_cod = ?"""
    )
    filas = session.execute(select, [
        grabacion_cod, ])
    for fila in filas:
        print("DNI de usuario: " + fila.usuario_dni)
        print("Fecha de grabación: " + str(fila.grabacion_fecha))
        print("Nombre de usuario: " + fila.usuario_nombre)
        print("Calle de usuario: " + fila.usuario_calle)
        print("Ciudad de usuario: " + fila.usuario_ciudad)
        print("Duración de grabación: " + str(fila.grabacion_duracion))
        print("Tipo de grabación: " + fila.grabacion_tipo)
        print()


# --- Consulta tabla 3
def consulta_tabla3():
    cancion_isrc = input("ISRC de la canción: ")

    select = session.prepare(
        """SELECT * FROM Tabla3 WHERE cancion_isrc = ?"""
    )
    filas = session.execute(select, [
        cancion_isrc, ])
    for fila in filas:
        print("Código de artista: " + str(fila.artista_cod))
        print("Nombre del país: " + fila.pais_nombre)
        print("Código del país: " + str(fila.pais_cod))
        print("Título de la canción: " + fila.cancion_titulo)
        print()


# --- Consulta tabla 4
def consulta_tabla4():
    pais_cod = input("Código del país: ")
    pais_cod = int(pais_cod)

    select = session.prepare(
        """SELECT * FROM Tabla4 WHERE pais_cod = ?"""
    )
    filas = session.execute(select, [
        pais_cod, ])
    for fila in filas:
        print("Código de artista: " + str(fila.artista_cod))
        print("Nombre del artista: " + fila.artista_nombre)
        print("Premios del artista: " + ", ".join(sorted(fila.artista_premios)))
        print("Nombre del país: " + fila.pais_nombre)
        print()


# --- Consulta tabla 5
def consulta_tabla5():
    cancion_genero = input("Género de la canción: ")

    select = session.prepare(
        """SELECT * FROM Tabla5 WHERE cancion_genero = ?"""
    )
    filas = session.execute(select, [
        cancion_genero, ])
    for fila in filas:
        print("ISRC de la canción: " + fila.cancion_isrc)
        print("Título de la canción: " + fila.cancion_titulo)
        print("Año de la canción: " + str(fila.cancion_anio))
        print("Géneros de la canción: " + ", ".join(sorted(fila.cancion_generos)))
        print()


# --- Consulta tabla 6
def consulta_tabla6():
    grabacion_fecha = input("Fecha de grabación (YYYY-MM-DD): ")

    select = session.prepare(
        """SELECT * FROM Tabla6 WHERE grabacion_fecha = ?"""
    )
    filas = session.execute(select, [
        grabacion_fecha, ])
    for fila in filas:
        print("Código de grabación: "   + fila.grabacion_cod)
        print("DNI de usuario: "        + fila.usuario_dni)
        print("Duración de grabación: " + str(fila.grabacion_duracion))
        print("Tipo de grabación: "     + fila.grabacion_tipo)
        print("Nombre de usuario: "     + fila.usuario_nombre)
        print("Calle de usuario: "      + fila.usuario_calle)
        print("Ciudad de usuario: "     + fila.usuario_ciudad)
        print()




# Programa principal
# Conexión con Cassandra
cluster = Cluster()
session = cluster.connect('danelcampana')


numero = -1
# Sigue pidiendo operaciones hasta que se introduzca 0
while (numero != 0):
    print()
    print("Introduzca un número para ejecutar una de las siguientes operaciones:")
    # print("1. Consultar canción por ISRC")
    # print("2. Consultar usuario por DNI")
    print("1.  Insertar una canción")
    print("2.  Insertar un usuario")
    print("3.  Insertar relación es_guardada_por entre usuario y grabación")
    print("4.  Insertar relación es_interpretada_por y es_de")
    print("5.  Actualizar nombre de usuario por DNI")
    print("6.  Borrar grabaciones por fecha")
    print("7.  Consultar usuario por DNI")
    print("8.  Consulta Tabla1")
    print("9.  Consulta Tabla2")
    print("10. Consulta Tabla3")
    print("11. Consulta Tabla4")
    print("12. Consulta Tabla5")
    print("13. Consulta Tabla6")
    print("0. Cerrar aplicación")


    numero = int(input())  # Pedimos numero al usuario
    if   (numero == 1):
        insertar_cancion()
    elif (numero == 2):
        insertar_usuario()
    elif (numero == 3):
        insertar_relacion_es_guardada_por()
    elif (numero == 4):
        insertar_relacion_es_interpretada_por_es_de()
    elif (numero == 5):
        actualizar_nombre_usuario()
    elif (numero == 6):
        borrar_grabaciones_por_fecha()
    elif (numero == 7):
        consultar_datos_usuario()
    elif (numero == 8):
        consulta_tabla1()
    elif (numero == 9):
        consulta_tabla2()
    elif (numero == 10):
        consulta_tabla3()
    elif (numero == 11):
        consulta_tabla4()
    elif (numero == 12):
        consulta_tabla5()
    elif (numero == 13):
        consulta_tabla6()    
    else:
        print("Número incorrecto")


cluster.shutdown()  # cerramos conexion