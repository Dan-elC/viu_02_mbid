from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('danelcampana')
# session = cluster.connect()
# print(session.execute("SELECT release_version FROM system.local").one())



class Usuario:

    def __init__(self, Usuario_nombre, Usuario_DNI, Usuario_calle, Usuario_ciudad):

        self.Usuario_nombre = Usuario_nombre
        self.Usuario_DNI = Usuario_DNI
        self.Usuario_calle = Usuario_calle
        self.Usuario_ciudad = Usuario_ciudad


class Cancion:

    def __init__(self, Cancion_genero, Cancion_ISRC, Cancion_titulo, Cancion_anio, Cancion_generos):

        self.Cancion_genero = Cancion_genero
        self.Cancion_ISRC = Cancion_ISRC
        self.Cancion_titulo = Cancion_titulo
        self.Cancion_anio = Cancion_anio
        self.Cancion_generos = Cancion_generos




def consultar_usuario_por_dni(Usuario_DNI):
    select = session.prepare(
        "SELECT * FROM Tabla1 WHERE Usuario_DNI = ?")
    filas = session.execute(select, [
        Usuario_DNI, ])
    for fila in filas:
        u = Usuario(fila.Usuario_nombre, Usuario_DNI, fila.Usuario_calle, fila.Usuario_ciudad)
        return u
    print("SIN DATOS")



def consultar_cancion_por_isrc(Cancion_ISRC):
    select = session.prepare(
        "SELECT * FROM Tabla5 WHERE Cancion_ISRC = ?")
    filas = session.execute(select, [
        Cancion_ISRC, ])
    for fila in filas:
        u = Cancion(fila.Cancion_genero, Cancion_ISRC, fila.Cancion_titulo, fila.Cancion_anio, fila.Cancion_generos)
        return u
    print("SIN DATOS")



test = consultar_usuario_por_dni('12345')
test = consultar_cancion_por_isrc('12345')



# session.shutdown()

