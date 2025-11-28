# ============================================
#  productor.py
#  Sistema de Simulación Montecarlo Distribuido
#  Módulo: Productor
#  Autor: Krasny(Bara)
# ============================================

"""
Este módulo actúa como el productor dentro del sistema distribuido Montecarlo.
Su función principal es:

1. Leer un modelo de simulación desde un archivo de texto (modelo.txt).
2. Publicar dicho modelo en la cola 'model_queue' para que los consumidores lo reciban.
3. Generar escenarios aleatorios a partir de las distribuciones definidas.
4. Enviar los escenarios a la cola 'scenario_queue' para que sean procesados.

El Productor se comunica con RabbitMQ, que funge como bróker de mensajes,
y se ejecuta en la máquina madre del sistema.
"""

import os
import pika
import numpy as np
import re
import json
from pathlib import Path

# ---------------- CONFIGURACIÓN POR DEFECTO ----------------
RABBITMQ_HOST = "IP"
RABBITMQ_USER = "NOMBRE HOST"
RABBITMQ_PASS = "12345"
MODEL_FILE = "modelo.txt"

# ------------------------------------------------------------
class MonteCarloProductor:
    """
    Clase principal encargada de:
    - Leer y analizar el archivo del modelo.
    - Generar escenarios de simulación.
    - Publicar modelo y escenarios en las colas correspondientes.
    """

    def __init__(self, host, usuario, contrasena, archivo_modelo):
        self.host = host
        self.usuario = usuario
        self.contrasena = contrasena
        self.archivo_modelo = Path(archivo_modelo)
        self.modelo = {}
        self.conexion = None
        self.canal = None

    # ------------------- MÉTODOS PRINCIPALES -------------------

    def conectar(self):
        """Establece conexión con RabbitMQ y crea las colas necesarias."""
        print("Conectando con RabbitMQ...")
        cred = pika.PlainCredentials(self.usuario, self.contrasena)
        params = pika.ConnectionParameters(host=self.host, credentials=cred)
        self.conexion = pika.BlockingConnection(params)
        self.canal = self.conexion.channel()

        # Declarar colas persistentes
        self.canal.queue_declare(queue="model_queue", durable=True)
        self.canal.queue_declare(queue="scenario_queue", durable=True)

        print(f"Conectado a RabbitMQ en {self.host}")

    def leer_modelo(self):
        """
        Lee y analiza el archivo del modelo.
        Puede estar en formato JSON o texto plano con secciones.
        """
        if not self.archivo_modelo.exists():
            raise FileNotFoundError(f"No se encontró el archivo {self.archivo_modelo}")

        contenido = self.archivo_modelo.read_text(encoding="utf-8").strip()

        try:
            self.modelo = json.loads(contenido)
            print("Modelo en formato JSON cargado correctamente.")
        except json.JSONDecodeError:
            print("El archivo no está en formato JSON. Intentando analizar texto...")
            self.modelo = self._parsear_modelo(contenido)

        return self.modelo

    def publicar_modelo(self):
        """Publica el modelo actual en la cola 'model_queue'."""
        print("Publicando modelo en la cola 'model_queue'...")
        self.canal.basic_publish(
            exchange='',
            routing_key='model_queue',
            body=json.dumps(self.modelo),
            properties=pika.BasicProperties(expiration='60000')  # 60 segundos de validez
        )
        print("Modelo publicado correctamente.")

    def generar_escenarios(self, cantidad=100):
        """
        Genera una lista de escenarios basados en las distribuciones
        definidas en el modelo. Cada escenario es un diccionario de variables.
        """
        print(f"Generando {cantidad} escenarios...")
        distros = self.modelo.get("variables", self.modelo.get("distributions", {}))
        escenarios = []

        for _ in range(cantidad):
            escenario = {}
            for var, expr in distros.items():
                escenario[var] = self._evaluar_distribucion(expr)
            escenarios.append(escenario)

        print("Escenarios generados correctamente.")
        return escenarios

    def publicar_escenarios(self, escenarios):
        """Publica los escenarios generados en la cola 'scenario_queue'."""
        print(f"Publicando {len(escenarios)} escenarios en la cola 'scenario_queue'...")
        for i, esc in enumerate(escenarios, 1):
            self.canal.basic_publish(
                exchange="",
                routing_key="scenario_queue",
                body=json.dumps(esc),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            if i % 10 == 0 or i == len(escenarios):
                print(f"   Escenarios enviados: {i}/{len(escenarios)}")
        print("Todos los escenarios fueron publicados correctamente.")

    def cerrar(self):
        """Cierra la conexión con RabbitMQ de forma segura."""
        if self.conexion:
            self.conexion.close()
            print("Conexión cerrada correctamente con RabbitMQ.")

    # ------------------- MÉTODOS INTERNOS -------------------

    def _parsear_modelo(self, texto):
        """
        Analiza un modelo definido en texto plano con las secciones:
        model:, constants:, distributions:
        """
        secciones = {"model": None, "constants": {}, "distributions": {}}
        seccion_actual = None

        for linea in texto.splitlines():
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue

            if linea.startswith("model:"):
                seccion_actual = "model"
                secciones["model"] = linea.replace("model:", "").strip()
            elif linea.startswith("constants:"):
                seccion_actual = "constants"
            elif linea.startswith("distributions:"):
                seccion_actual = "distributions"
            elif seccion_actual == "constants" and "=" in linea:
                k, v = linea.split("=")
                secciones["constants"][k.strip()] = float(v.strip())
            elif seccion_actual == "distributions" and "~" in linea:
                var, expr = linea.split("~")
                secciones["distributions"][var.strip()] = expr.strip()

        return secciones

    def _evaluar_distribucion(self, expresion):
        """
        Evalúa una expresión de distribución y devuelve un valor aleatorio.
        Soporta: uniform(a,b), normal(mu,sigma), triangular(a,b,c), constante(x)
        """
        if isinstance(expresion, list):
            # Compatibilidad con formato JSON tipo ["constante", valor]
            if expresion[0] == "constante":
                return float(expresion[1])

        if m := re.match(r"uniform\(([^,]+),\s*([^)]+)\)", expresion):
            a, b = map(float, m.groups())
            return np.random.uniform(a, b)
        elif m := re.match(r"normal\(([^,]+),\s*([^)]+)\)", expresion):
            mu, sigma = map(float, m.groups())
            return np.random.normal(mu, sigma)
        elif m := re.match(r"triangular\(([^,]+),\s*([^,]+),\s*([^)]+)\)", expresion):
            izq, pico, der = map(float, m.groups())
            return np.random.triangular(izq, pico, der)
        elif m := re.match(r"constante\(([^)]+)\)", expresion):
            return float(m.group(1))
        else:
            raise ValueError(f"Distribución no reconocida: {expresion}")

# ------------------- EJECUCIÓN PRINCIPAL -------------------

if __name__ == "__main__":
    print("=== Sistema Distribuido MonteCarlo: PRODUCTOR ===")
    host = input(f"IP del servidor RabbitMQ [{RABBITMQ_HOST}]: ") or RABBITMQ_HOST
    usuario = input(f"Usuario RabbitMQ [{RABBITMQ_USER}]: ") or RABBITMQ_USER
    contrasena = input(f"Contraseña RabbitMQ [{RABBITMQ_PASS}]: ") or RABBITMQ_PASS

    productor = MonteCarloProductor(host, usuario, contrasena, MODEL_FILE)
    productor.conectar()
    productor.leer_modelo()
    productor.publicar_modelo()

    try:
        cantidad = input("¿Cuántos escenarios deseas generar? [100]: ")
        cantidad = int(cantidad) if cantidad.strip() else 100
    except ValueError:
        cantidad = 100

    escenarios = productor.generar_escenarios(cantidad)
    productor.publicar_escenarios(escenarios)
    productor.cerrar()

    print("\nProductor finalizado correctamente.")
