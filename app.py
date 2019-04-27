from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flaskext.mysql import MySQL
from collections import OrderedDict
from geopy.distance import geodesic

mysql = MySQL()
app = Flask(__name__)
api = Api(app)

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'admin'
app.config['MYSQL_DATABASE_PASSWORD'] = '0ef2cdec193436181d99624661f2c7b294dc276de9fc89b3'
app.config['MYSQL_DATABASE_DB'] = 'procalidad'
app.config['MYSQL_DATABASE_HOST'] = '104.248.63.229'
mysql.init_app(app)


class Login(Resource):
    def post(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        parser.add_argument('usuario')  # Para capturar los argumentos que enviamos
        parser.add_argument('password')
        args = parser.parse_args()  # Obtenemos un diccionario de todos lo argumentos enviados
        usuario = args['usuario']
        password = args['password']
        query = "SELECT * FROM Usuario WHERE nombre_usuario=%s AND password=%s"
        valid = cursor.execute(query, (usuario, password))
        usuario = []
        if valid == 1:
            rows = cursor.fetchall()
            for row in rows:
                d = OrderedDict()
                d['idUsuario'] = row[0]
                d['nombre'] = row[1]
                d['apellido'] = row[2]
                d['correo'] = row[3]
                d['nombre_usuario'] = row[4]
                d['dni'] = row[6]
                d['sexo'] = row[7]
                d['idDistrito_vivienda'] = row[8]
                d['ocupacion'] = row[9]
                d['grado_educativo'] = row[10]
                d['es_identificado'] = row[11]
                d['cantidad_comentario_indebido'] = row[12]
                d['es_eliminado'] = row[13]
                d['fecha'] = str(row[14])
                usuario.append(d)
        conn.close()
        return {"valid": valid, "usuario": usuario}, 200


class RegistroUsuario(Resource):
    def post(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        parser.add_argument('nombre')  # Para capturar los argumentos que enviamos
        parser.add_argument('apellido')
        parser.add_argument('correo')
        parser.add_argument('nombre_usuario')
        parser.add_argument('password')
        parser.add_argument('dni')
        parser.add_argument('sexo')
        args = parser.parse_args()  # Obtenemos un diccionario de todos lo argumentos enviados
        nombre = args['nombre']
        apellido = args['apellido']
        correo = args['correo']
        nombre_usuario = args['nombre_usuario']
        password = args['password']
        dni = args['dni']
        sexo = args['sexo']
        try:
            # Consulta si existe el dni del Usuario
            q = "SELECT * FROM Usuario WHERE dni=%s AND es_identificado=1"
            existe = cursor.execute(q, (dni,))
            if existe == 0:  # No existe un usuario con ese nro de dni
                query = "INSERT INTO Usuario (nombre, apellido, correo, nombre_usuario, password, dni, sexo) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(query, (nombre, apellido, correo,
                                       nombre_usuario, password, dni, sexo))
                conn.commit()
                conn.close()
                return {'msg': 'Usuario registrado satisfactoriamente'}, 201
            conn.close()
            return {'msg': 'Ya existe un usuario con ese nro de dni'}, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error, no se pudo registrar al usuario'}, 400


class RegistroDistrito(Resource):
    def post(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        parser.add_argument('nombre')  # Para capturar los argumentos que enviamos
        args = parser.parse_args()  # Obtenemos un diccionario de todos lo argumentos enviados
        nombre = args['nombre']
        query = "INSERT INTO Distrito (nombre) VALUES (%s)"
        try:
            cursor.execute(query, (nombre,))
            conn.commit()
            conn.close()
            return {'msg': 'Distrito agregado satisfactoriamente'}, 201
        except Exception as e:
            conn.close()
            return {'msg': 'Error al registrar distrito'}, 400


class ConsultaDistrito(Resource):
    def get(self, id_distrito):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Distrito WHERE idDistritos=%s"
        try:
            cursor.execute(query, (id_distrito,))
            rows = cursor.fetchall()
            distritos = []
            for row in rows:
                d = OrderedDict()
                d['idDistritos'] = row[0]
                d['nombre'] = row[1]
                distritos.append(d)
            conn.close()
            return distritos, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al consultar distrito'}, 400


class RegistroValoracion(Resource):
    def obtenerNombresApellidosUsuario(self, id_usuario, cursor):
        query = "SELECT * FROM Usuario WHERE idUsuario=%s"
        cursor.execute(query, (id_usuario,))
        usuario = cursor.fetchone()
        nombres = usuario[1]
        apellidos = usuario[2]
        return nombres + " " + apellidos

    def post(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        parser.add_argument('idUsuario')  # Para capturar los argumentos que enviamos
        parser.add_argument('idSucursal')
        parser.add_argument('comentario')
        parser.add_argument('puntaje')
        args = parser.parse_args()  # Obtenemos un diccionario de todos lo argumentos enviados
        id_usuario = args['idUsuario']
        id_sucursal = args['idSucursal']
        comentario = args['comentario']
        puntaje = args['puntaje']
        q = "SELECT * FROM Valoracion WHERE idUsuario=%s AND idSucursal=%s"
        try:
            existe = cursor.execute(q, (id_usuario, id_sucursal))
            if int(existe) == 0:  # No existe una valoracion hecha por este usuario a esta sucursal
                query = "INSERT INTO Valoracion (idUsuario, idSucursal, comentario, puntaje) VALUES (%s, %s, %s, %s)"
                cursor.execute(query, (id_usuario, id_sucursal, comentario, puntaje))
                conn.commit()
                cursor.execute(q, (id_usuario, id_sucursal))
                val = cursor.fetchone()
                d = OrderedDict()
                d['idUsuario'] = val[1]
                d['nombreUsuario'] = self.obtenerNombresApellidosUsuario(id_usuario, cursor)
                d['idSucursal'] = val[2]
                d['comentario'] = val[3]
                d['puntaje'] = val[4]
                d['fecha'] = str(val[5])
                conn.close()
                return {'msg': 'Valoracion registrada satisfactoriamente', 'valoracion': d}, 201
            cursor.execute(q, (id_usuario, id_sucursal))
            valoracion = cursor.fetchone()
            d = OrderedDict()
            d['idUsuario'] = valoracion[1]
            d['nombreUsuario'] = self.obtenerNombresApellidosUsuario(id_usuario, cursor)
            d['idSucursal'] = valoracion[2]
            d['comentario'] = valoracion[3]
            d['puntaje'] = valoracion[4]
            d['fecha'] = str(valoracion[5])
            conn.close()
            return {'msg': 'Usted ya registro una valoracion para esta empresa', 'valoracion': d}, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al registrar valoracion'}, 400


class ConsultaValoracionesUsuario(Resource):
    def get(self, id_usuario):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Valoracion WHERE idUsuario=%s"
        try:
            cursor.execute(query, (id_usuario,))
            rows = cursor.fetchall()
            valoraciones = []
            for row in rows:
                d = OrderedDict()
                d['idValoracion'] = row[0]
                d['idUsuario'] = row[1]
                d['idSucursal'] = row[2]
                d['comentario'] = row[3]
                d['puntaje'] = row[4]
                d['fecha'] = str(row[5])
                d['cantidad_modificacion'] = row[6]
                valoraciones.append(d)
            conn.close()
            return valoraciones, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al consultar valoraciones de usuario'}, 400


class ConsultaValoracionesSucursal(Resource):
    def get(self, id_sucursal):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Valoracion WHERE idSucursal=%s"
        try:
            cursor.execute(query, (id_sucursal,))
            rows = cursor.fetchall()
            valoraciones = []
            for row in rows:
                d = OrderedDict()
                d['idValoracion'] = row[0]
                d['idUsuario'] = row[1]
                d['idSucursal'] = row[2]
                d['comentario'] = row[3]
                d['puntaje'] = row[4]
                d['fecha'] = str(row[5])
                d['cantidad_modificacion'] = row[6]
                valoraciones.append(d)
            conn.close()
            return valoraciones
        except Exception as e:
            conn.close()
            return {'msg': 'Error al consultar valoraciones por empresa'}, 400


class RegistroEmpresa(Resource):
    def post(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        parser.add_argument('nombre')  # Para capturar los argumentos que enviamos
        parser.add_argument('sector_economico')
        args = parser.parse_args()  # Obtenemos un diccionario de todos lo argumentos enviados
        nombre = args['nombre']
        sector_economico = args['sector_economico']
        query = "INSERT INTO Empresa (nombre, sector_economico) VALUES (%s, %s)"
        try:
            cursor.execute(query, (nombre, sector_economico))
            conn.commit()
            conn.close()
            return {'msg': 'Empresa registrada satisfactoriamente'}, 201
        except Exception as e:
            conn.close()
            return {'msg': 'Error al registrar empresa'}, 400


class ConsultaEmpresa(Resource):
    def get(self, id_empresa):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Empresa WHERE idEmpresa=%s"
        try:
            cursor.execute(query, (id_empresa,))
            rows = cursor.fetchall()
            empresas = []
            for row in rows:
                d = OrderedDict()
                d['idEmpresa'] = row[0]
                d['nombre'] = row[1]
                d['sector_economico'] = row[2]
                d['fecha'] = str(row[3])
                empresas.append(d)
            conn.close()
            return empresas, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al consultar empresa'}, 400


class ListaEmpresas(Resource):
    def get(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Empresa"
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            empresas = []
            for row in rows:
                d = OrderedDict()
                d['idEmpresa'] = row[0]
                d['nombre'] = row[1]
                d['sector_economico'] = row[2]
                d['fecha'] = str(row[3])
                empresas.append(d)
            conn.close()
            return empresas, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al obtener lista de empresas'}, 400


class ConsultaServicio(Resource):
    def get(self, id_servicio):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Servicio WHERE idServicios=%s"
        try:
            cursor.execute(query, (id_servicio,))
            rows = cursor.fetchall()
            servicios = []
            for row in rows:
                d = OrderedDict()
                d['idServicios'] = row[0]
                d['nombre'] = row[1]
                servicios.append(d)
            conn.close()
            return servicios, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al consultar servicio'}, 400


class ListaServicios(Resource):
    def get(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Servicio"
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            servicios = []
            for row in rows:
                d = OrderedDict()
                d['idServicios'] = row[0]
                d['nombre'] = row[1]
                servicios.append(d)
            conn.close()
            return servicios, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al obtener lista de servicios'}, 400


class ListaDistritos(Resource):
    def get(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Distrito"
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            distritos = []
            for row in rows:
                d = OrderedDict()
                d['idDistritos'] = row[0]
                d['nombre'] = row[1]
                distritos.append(d)
            conn.close()
            return distritos, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al obtener lista de distritos'}, 400


class ListaSucursalesEmpresa(Resource):
    def get(self, id_empresa):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Empresa_sucursal WHERE idEmpresa=%s"
        try:
            cursor.execute(query, (id_empresa,))
            rows = cursor.fetchall()
            sucursales_empresa = []
            for row in rows:
                d = OrderedDict()
                d['idSucursal'] = row[0]
                d['idEmpresa'] = row[1]
                d['idDistrito'] = row[2]
                d['direccion'] = row[3]
                sucursales_empresa.append(d)
            conn.close()
            return sucursales_empresa,  200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al obtener lista de sucursales'}, 400


class ListaServiciosEmpresa(Resource):
    def get(self, id_sucursal):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        query = "SELECT * FROM Empresa_servicios WHERE idSucursal=%s"
        try:
            cursor.execute(query, (id_sucursal,))
            rows = cursor.fetchall()
            sucursales_empresa = []
            for row in rows:
                d = OrderedDict()
                d['idEmpresa_servicio'] = row[0]
                d['idSucursal'] = row[1]
                d['idServicio'] = row[2]
                sucursales_empresa.append(d)
            conn.close()
            return sucursales_empresa, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al obtener lista de servicios por sucursal'}, 400


class EmpresasCercanas(Resource):
    def post(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        parser.add_argument('latitud')  # Para capturar los argumentos que enviamos
        parser.add_argument('longitud')
        args = parser.parse_args()  # Obtenemos un diccionario de todos lo argumentos enviados
        latitud = float(args['latitud'])
        longitud = float(args['longitud'])
        mi_posicion = (latitud, longitud)
        query = "SELECT * FROM Empresa_sucursal"
        try:
            cursor.execute(query)
            sucursales = cursor.fetchall()
            empresas_cercanas = []
            for sucursal in sucursales:
                posicion_sucursal = (sucursal[4], sucursal[5])
                if geodesic(mi_posicion, posicion_sucursal).kilometers < 5.0:  # distancia menor a 5Km
                    # (idEmpresa, idSucursal, idDistrito, 'Av. Arequipa 5280', -12.118738, -77.029366)
                    d = OrderedDict()
                    d['idSucursal'] = sucursal[0]
                    query = "SELECT * FROM Empresa WHERE idEmpresa=%s"
                    cursor.execute(query, (sucursal[1],))
                    empresa = cursor.fetchall()
                    d['nombreEmpresa'] = empresa[0][1]
                    query = "SELECT * FROM Distrito WHERE idDistritos=%s"
                    cursor.execute(query, (sucursal[2],))
                    distrito = cursor.fetchall()
                    d['nombreDistrito'] = distrito[0][1]
                    d['direccion'] = sucursal[3]
                    d['latitud'] = sucursal[4]
                    d['longitud'] = sucursal[5]
                    empresas_cercanas.append(d)
            conn.close()
            return {'sucursales_cercanas': empresas_cercanas}
        except Exception as e:
            conn.close()
            return {'msg': 'Error al obtener lista de empresas cercanas'}, 400


class DetalleSucursal(Resource):
    def obtenerSucursalById(self, id_sucursal, cursor):
        query = "SELECT * FROM Empresa_sucursal WHERE idSucursal=%s"
        cursor.execute(query, (int(id_sucursal),))
        empresa_sucursal = cursor.fetchone()
        return empresa_sucursal

    def obtenerDireccionSucursalById(self, id_sucursal, cursor):
        query = "SELECT * FROM Empresa_sucursal WHERE idSucursal=%s"
        cursor.execute(query, (int(id_sucursal),))
        empresa_sucursal = cursor.fetchone()
        return empresa_sucursal[3]

    def obtenerNombreEmpresaByIdSucursal(self, id_sucursal, cursor):
        empresa_sucursal = self.obtenerSucursalById(id_sucursal, cursor)
        query = "SELECT * FROM Empresa WHERE idEmpresa=%s"
        cursor.execute(query, (int(empresa_sucursal[1]),))
        empresa = cursor.fetchone()
        return empresa[1]

    def obtenerValoracionesSucursal(self, id_sucursal, cursor):
        query = "SELECT * FROM Valoracion WHERE idSucursal=%s"
        # AND eliminado=%s
        cursor.execute(query, (int(id_sucursal),))
        valoraciones = cursor.fetchall()
        return valoraciones

    def obtenerNombresApellidosUsuario(self, id_usuario, cursor):
        query = "SELECT * FROM Usuario WHERE idUsuario=%s"
        cursor.execute(query, (int(id_usuario),))
        usuario = cursor.fetchone()
        nombres = usuario[1]
        apellidos = usuario[2]
        return nombres + " " + apellidos

    def obtenerEstadoComentario(self, id_sucursal, id_usuario, cursor):
        query = "SELECT * FROM Valoracion WHERE idUsuario=%s AND idSucursal=%s"
        cursor.execute(query, (int(id_usuario), int(id_sucursal)))
        valoraciones = cursor.fetchall()
        if int(len(valoraciones)) == 0:
            estado = 0
        else:
            for valoracion in valoraciones:
                if int(valoracion[7]) == 0:
                    estado = 1
                    break
                elif int(valoracion[7]) == 1:
                    estado = -1
                    break
        return estado

    def post(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        parser.add_argument('idSucursal')  # Para capturar los argumentos que enviamos
        parser.add_argument('idUsuario')
        args = parser.parse_args()  # Obtenemos un diccionario de todos lo argumentos enviados
        idSucursal = int(args['idSucursal'])
        idUsuario = int(args['idUsuario'])

        detalle_sucursal = {}
        comentario_usuario = []
        comentarios = []
        puntaje_promedio = 0
        nro_comentarios = 0
        # Construyendo el json a retornar
        try:
            d = OrderedDict()
            d['idSucursal'] = idSucursal
            d['nombreEmpresa'] = self.obtenerNombreEmpresaByIdSucursal(idSucursal, cursor)
            d['direccion'] = self.obtenerDireccionSucursalById(idSucursal, cursor)
            valoraciones = self.obtenerValoracionesSucursal(idSucursal, cursor)
            for valoracion in valoraciones:
                e = OrderedDict()
                e['idValoracion'] = valoracion[0]
                e['nombreUsuario'] = self.obtenerNombresApellidosUsuario(valoracion[1], cursor)
                e['comentario'] = valoracion[3]
                e['puntaje'] = valoracion[4]
                e['fecha'] = str(valoracion[5])
                nro_comentarios = nro_comentarios + 1
                puntaje_promedio = puntaje_promedio + int(valoracion[4])
                # print(valoracion)
                if int(idUsuario) != int(valoracion[1]) and int(valoracion[7]) == 0:
                    print(valoracion)
                    comentarios.append(e)
                if int(idUsuario) == int(valoracion[1]) and int(valoracion[7]) == 0:
                    comentario_usuario.append(e)
            if int(nro_comentarios) != 0:
                puntaje_promedio = puntaje_promedio/nro_comentarios
            d['puntaje_promedio'] = puntaje_promedio
            d['comentario_usuario'] = comentario_usuario
            d['estado_comentario'] = self.obtenerEstadoComentario(idSucursal, idUsuario, cursor)
            d['comentarios'] = comentarios
            detalle_sucursal = d
            conn.close()
            return detalle_sucursal, 200
        except Exception as e:
            conn.close()
            return {'msg': 'Error al obtener detalles sucursal'}, 400


class ReclamoUsuario(Resource):
    def post(self):
        conn = mysql.connect()
        cursor = conn.cursor()
        parser = reqparse.RequestParser()
        parser.add_argument('idUsuario_reportado')  # Para capturar los argumentos que enviamos
        parser.add_argument('idUsuario_que_reporta')
        parser.add_argument('motivo')
        parser.add_argument('telefono_que_reporta')
        parser.add_argument('link_dni_que_reporta')
        args = parser.parse_args()  # Obtenemos un diccionario de todos lo argumentos enviados
        id_usuario_reportado = args['idUsuario_reportado']
        id_usuario_que_reporta = args['idUsuario_que_reporta']
        motivo = args['motivo']
        telefono_que_reporta = args['telefono_que_reporta']
        link_dni_que_reporta = args['link_dni_que_reporta']
        query = "INSERT INTO Usuario_reportado (idUsuario_reportado, idUsuario_que_reporta, motivo, telefono_que_reporta, link_dni_que_reporta) VALUES (%s, %s, %s, %s, %s)"
        try:
            cursor.execute(query, (id_usuario_reportado, id_usuario_que_reporta,
                                   motivo, telefono_que_reporta, link_dni_que_reporta))
            conn.commit()
            conn.close()
            return {'msg': 'Reclamo registrado'}, 201
        except Exception as e:
            conn.close()
            return {'msg': 'Error al registrar reclamo'}, 400


api.add_resource(Login, '/login')
api.add_resource(RegistroUsuario, '/registro-usuario')
api.add_resource(RegistroValoracion, '/registro-valoracion')
api.add_resource(ConsultaValoracionesUsuario, '/valoraciones-usuario/<idUsuario>')
api.add_resource(ConsultaValoracionesSucursal, '/valoraciones-sucursal/<idSucursal>')
api.add_resource(RegistroDistrito, '/registro-distrito')
api.add_resource(ConsultaDistrito, '/consulta-distrito/<idDistrito>')
# 2do lote de Apis
api.add_resource(RegistroEmpresa, '/registro-empresa')
api.add_resource(ConsultaEmpresa, '/consulta-empresa/<idEmpresa>')
api.add_resource(ListaEmpresas, '/lista-empresas')
api.add_resource(ConsultaServicio, '/consulta-servicio/<idServicio>')
api.add_resource(ListaServicios, '/lista-servicios')
api.add_resource(ListaDistritos, '/lista-distritos')
api.add_resource(ListaSucursalesEmpresa, '/lista-sucursales-empresa/<idEmpresa>')
api.add_resource(ListaServiciosEmpresa, '/lista-servicios-empresa/<idSucursal>')
# 3er lote de Apis
api.add_resource(EmpresasCercanas, '/empresas-cercanas')
api.add_resource(DetalleSucursal, '/detalles-sucursal')
api.add_resource(ReclamoUsuario, '/reclamo-usuario')

if __name__ == "__main__":
    app.run()
