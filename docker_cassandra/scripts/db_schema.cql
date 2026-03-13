
CREATE KEYSPACE danelcampana WITH replication = {'class':'SimpleStrategy','replication_factor':1};

USE danelcampana;

-- Cada tabla sumará hasta el porcentaje indicado de la calificación total de la actividad:
-- 1. Obtener toda la información de un usuario en base a su nombre. (5%).
-- 2. Obtener toda la información de los usuarios que han guardado una grabación específica (identificada por su cod). (7,5%)
-- 3. Obtener los países de los artistas con una canción en base a su ISRC. (10%)
-- 4. Obtener cuántos artistas hay de un país concreto, identificado base a su cod. (10%)
-- 5. Obtener toda la información de las canciones que tengan un género asociado común (p. ej. Rock). (7,5%)
-- 6. Buscar, según la fecha en que se guardó, las grabaciones y el usuario que las guardó. (10%)


-- #############################################################
-- Consulta 1: Obtener toda la información de un usuario en base a su nombre.
-- #############################################################
CREATE TABLE danelcampana.Tabla1 (

    Usuario_nombre text,        -- PK: Clave de búsqueda (nombre), ya que sastisface la primera consulta de búsqueda por nombre.
    Usuario_DNI text,           -- CK: Clave de unicidad y ordenamiento, ya que el DNI de la persona es único y no se repite.
    Usuario_calle text,         -- Nombre de la calle donde vive el usuario.
    Usuario_ciudad text,        -- Nombre de la ciudad donde vive el usuario.
    
    PRIMARY KEY (Usuario_nombre, Usuario_DNI)
) WITH CLUSTERING ORDER BY (Usuario_DNI ASC);

CREATE CUSTOM INDEX idx_usuario_dni ON danelcampana.Tabla1 (Usuario_DNI) USING 'sai';


-- #############################################################
-- Consulta 2: Obtener toda la información de los usuarios que han guardado una grabación específica (identificada por su cod).
-- #############################################################
CREATE TABLE danelcampana.Tabla2 (
    
    Grabacion_cod text,         -- PK: Clave de búsqueda (cod), ya que satisface la segunda consulta de búsqueda por código de grabación
    Usuario_DNI text,           -- CK: Clave de unicidad y ordenamiento, ya que el DNI de la persona es único y no se repite.    
    Grabacion_fecha date,       -- Se genera el atributo fecha de la relación entre grabación y usuario
    Usuario_nombre text,        -- Nombre del usuario
    Usuario_calle text,         -- Nombre de la calle del usuario
    Usuario_ciudad text,        -- Nombre de la ciudad del usuario
    Grabacion_duracion int,     -- Duración de la grabación. Asumo que son segundos (int).  No es información del usuario pero ayuda a identificar si la grabación es correcta.
    Grabacion_tipo text,        -- Tipo de la grabación. No es información del usuario pero ayuda a identificar si la grabación es correcta.
    
    PRIMARY KEY (Grabacion_cod, Usuario_DNI)
) WITH CLUSTERING ORDER BY (Usuario_DNI ASC);


-- #############################################################
-- Consulta 3: Obtener los países de los artistas con una canción en base a su ISRC.
-- #############################################################
CREATE TABLE danelcampana.Tabla3 (
    
    Cancion_ISRC text,          -- PK: Clave de búsqueda (ISRC de la canción), ya que se buscarán los países de los artistas con una canción.
    Artista_cod int,            -- CK: Clave de unicidad
    Pais_nombre text,           -- Nombre del pais
    Pais_cod int,               -- Código del país
    Cancion_titulo text,        -- Título de la canción. Ayuda a validar si es la canción que está buscando, por más que no sea el país.

    
    PRIMARY KEY (Cancion_ISRC, Artista_cod)
) WITH CLUSTERING ORDER BY (Artista_cod ASC);


-- #############################################################
-- Consulta 4: Obtener cuántos artistas hay de un país concreto, identificado base a su cod.
-- #############################################################
CREATE TABLE danelcampana.Tabla4 (
    
    Pais_cod int,               -- PK: Clave de búsqueda (código de país). Se realizará la búsqued por el código de país.
    Artista_cod int,            -- CK: Clave de unicidad (un artista por país)
    Artista_nombre text,        -- Nombre del artista
    Artista_premios SET<text>,  -- Atributo de conjunto de los premios que tiene el artista
    Pais_nombre text,           -- Nombre del país
    
    PRIMARY KEY (Pais_cod, Artista_cod)
) WITH CLUSTERING ORDER BY (Artista_cod ASC);


-- #############################################################
-- Consulta 5: Obtener toda la información de las canciones que tengan un género asociado común (p. ej. Rock).
-- #############################################################
CREATE TABLE danelcampana.Tabla5 (
    
    Cancion_genero text,        -- PK: Clave de búsqueda (el género, para índice invertido). La consulta solicita información apartir del género asociado.
    Cancion_ISRC text,          -- CK: Clave de unicidad (una canción por género). Con ello se identifica la grabación única.
    Cancion_titulo text,        -- Título de la canción
    Cancion_anio int,           -- Año en que se publicó la canción
    Cancion_generos SET<text>,  -- La canción puede tener varos géneros
    
    PRIMARY KEY (Cancion_genero, Cancion_ISRC)
) WITH CLUSTERING ORDER BY (Cancion_ISRC ASC);

CREATE CUSTOM INDEX idx_cancion_isrc ON danelcampana.Tabla5 (Cancion_ISRC) USING 'sai';


-- #############################################################
-- Consulta 6: Buscar, según la fecha en que se guardó, las grabaciones y el usuario que las guardó.
-- #############################################################
CREATE TABLE danelcampana.Tabla6 (
    
    Grabacion_fecha date,        -- PK: Clave de búsqueda (la fecha de guardado). Campo por el cual se realizará la búsqueda
    Grabacion_cod text,          -- CKs: Para un mejor ordenamiento se toma el cod como CK, ya que se necesita la grabación.
    Usuario_DNI text,            -- CKs: Para un mejor ordenamiento se toma el DNI como CK, ya que se necesita información del usuario.
    Grabacion_duracion INT,      -- Campos extras de duración de la grabación
    Grabacion_tipo text,         -- Campos extras de tipo de la grabación
    Usuario_nombre text,         -- Campos extras de nombre del usuario
    Usuario_calle text,          -- Campos extras de calle del usuario
    Usuario_ciudad text,         -- Campos extras de ciudad del usuario
    -- La información del usuario y grabación ayudan a identificar si es el resultado esperado.
    
    PRIMARY KEY (Grabacion_fecha, Grabacion_cod, Usuario_DNI)
) WITH CLUSTERING ORDER BY (Grabacion_cod ASC, Usuario_DNI ASC);