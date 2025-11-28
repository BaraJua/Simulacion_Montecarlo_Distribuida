# ============================================
#  consumidor.py
#  Sistema de Simulación Montecarlo Distribuido
#  Módulo: Consumidor
#  Autor: Krasny(Bara)
# ============================================

"""
Este módulo representa el rol de los consumidores dentro del sistema distribuido
Montecarlo. Cada consumidor:

1. Se conecta a RabbitMQ y obtiene el modelo de simulación desde 'model_queue'.
2. Escucha la cola 'scenario_queue' para recibir escenarios de simulación.
3. Evalúa el modelo usando los valores del escenario.
4. Envía el resultado a la cola 'result_queue', junto con su identificador.

Varios consumidores pueden ejecutarse en diferentes máquinas virtuales o equipos,
lo que permite distribuir la carga de trabajo de forma concurrente.
"""

import pika
import json
import time
import math

class ConsumidorMontecarlo:
    """Clase que implementa el comportamiento de un consumidor Montecarlo."""

    def __init__(self):
        print("=== Sistema Distribuido MonteCarlo: CONSUMIDOR ===")
        self.rabbit_host = input("IP del servidor RabbitMQ [IP]: ") or "IP"
        self.rabbit_user = input("Usuario RabbitMQ [NOMBRE HOST]: ") or "NOMBRE HOST"
        self.rabbit_pass = input("Contraseña RabbitMQ [12345]: ") or "12345"

        # Identificador único del consumidor (definido manualmente por el usuario)
        self.worker_id = input("Nombre del consumidor (ej. VM1, LaptopA): ") or "Anon"

        self.modelo = None
        self.resultados_publicados = 0
        self.conectar()

    # ------------------------------------------------------------
    def conectar(self):
        """Establece conexión con RabbitMQ y prepara las colas necesarias."""
        print("Conectando con RabbitMQ...")
        credenciales = pika.PlainCredentials(self.rabbit_user, self.rabbit_pass)
        parametros = pika.ConnectionParameters(host=self.rabbit_host, credentials=credenciales)
        self.conexion = pika.BlockingConnection(parametros)
        self.canal = self.conexion.channel()

        # Declarar colas persistentes
        self.canal.queue_declare(queue="model_queue", durable=True)
        self.canal.queue_declare(queue="scenario_queue", durable=True)
        self.canal.queue_declare(queue="result_queue", durable=True)

        print(f"Conectado a RabbitMQ en {self.rabbit_host}")

    # ------------------------------------------------------------
    def obtener_modelo(self):
        """Obtiene y carga el modelo de simulación desde la cola 'model_queue'."""
        print("Esperando modelo en 'model_queue'...")
        metodo, propiedades, cuerpo = self.canal.basic_get(queue="model_queue", auto_ack=True)

        if cuerpo:
            self.modelo = json.loads(cuerpo.decode())
            print("Modelo recibido y cargado correctamente.")
        else:
            print("No hay modelo disponible en la cola. Finalizando proceso.")
            exit(1)

    # ------------------------------------------------------------
    def ejecutar_modelo(self, escenario):
        """
        Ejecuta el modelo usando los valores del escenario.
        Combina las variables del escenario con las constantes del modelo.
        """
        expresion = self.modelo["model"].replace("resultado =", "").strip()
        try:
            entorno = {}
            entorno.update(escenario)
            entorno.update(self.modelo.get("constants", {}))
            resultado = eval(expresion, {}, entorno)
            return resultado
        except Exception as e:
            print(f"Error evaluando modelo: {e}")
            return None

    # ------------------------------------------------------------
    def procesar_escenario(self, canal, metodo, propiedades, cuerpo):
        """Procesa un escenario recibido, evalúa el modelo y publica el resultado."""
        escenario = json.loads(cuerpo.decode())
        resultado = self.ejecutar_modelo(escenario)

        if resultado is not None:
            mensaje_resultado = {
                "worker_id": self.worker_id,
                "escenario": escenario,
                "resultado": resultado,
                "timestamp": time.time()
            }

            # Publicar resultado en la cola result_queue
            self.canal.basic_publish(
                exchange="",
                routing_key="result_queue",
                body=json.dumps(mensaje_resultado)
            )
            self.resultados_publicados += 1
            print(f"Resultado publicado #{self.resultados_publicados}: {resultado:.4f}")

        canal.basic_ack(delivery_tag=metodo.delivery_tag)

    # ------------------------------------------------------------
    def consumir_escenarios(self):
        """
        Se suscribe a la cola 'scenario_queue' y procesa escenarios de forma continua.
        Cada consumidor recibe y procesa escenarios de manera independiente.
        """
        print("Esperando escenarios en 'scenario_queue'...")
        self.canal.basic_qos(prefetch_count=1)
        self.canal.basic_consume(queue="scenario_queue", on_message_callback=self.procesar_escenario)
        self.canal.start_consuming()

    # ------------------------------------------------------------
    def iniciar(self):
        """Inicia la ejecución del consumidor (obtiene modelo y comienza consumo)."""
        self.obtener_modelo()
        self.consumir_escenarios()

# ------------------------------------------------------------
if __name__ == "__main__":
    consumidor = ConsumidorMontecarlo()
    consumidor.iniciar()
