# ============================================
#  colector.py
#  Sistema de Simulación Montecarlo Distribuido
#  Módulo: Recolector de Resultados
#  Autor: Krasny (Bara)
# ============================================

"""
Este módulo actúa como el Recolector dentro del sistema distribuido Montecarlo.
Su función principal es:

1. Escuchar los resultados enviados por los consumidores desde la cola 'result_queue'.
2. Guardar cada resultado en un archivo de texto dentro de la carpeta 'resultados/'.
3. Calcular estadísticas básicas (mínimo, máximo, promedio y total).
4. Reenviar los resultados al Dashboard mediante la cola 'dashboard_queue' para su visualización.

El Recolector puede ejecutarse en una máquina independiente y funciona como punto
central de consolidación de la información procesada.
"""

import pika
import json
import os
import time
import statistics
from datetime import datetime

# ---------------- CONFIGURACIÓN ----------------
RABBITMQ_HOST = "IP_SERVIDOR"
RABBITMQ_USER = "NOMBRE_HOST"
RABBITMQ_PASS = "12345"
RESULTS_DIR = "resultados"

# ------------------------------------------------------------
class ResultCollector:
    """Clase que implementa la recolección y manejo de resultados del sistema Montecarlo."""

    def __init__(self):
        print("=== Sistema Distribuido Montecarlo: RECOLECTOR ===")
        self.host = input(f"IP del servidor RabbitMQ [{RABBITMQ_HOST}]: ") or RABBITMQ_HOST
        self.user = input(f"Usuario RabbitMQ [{RABBITMQ_USER}]: ") or RABBITMQ_USER
        self.password = input(f"Contraseña RabbitMQ [{RABBITMQ_PASS}]: ") or RABBITMQ_PASS

        self.resultados = []
        self._iniciar_archivo()
        self._conectar()

    # ------------------------------------------------------------
    def _iniciar_archivo(self):
        """Crea la carpeta y el archivo de resultados donde se almacenarán los datos recibidos."""
        os.makedirs(RESULTS_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.archivo_path = os.path.join(RESULTS_DIR, f"resultados_{timestamp}.txt")
        self.archivo = open(self.archivo_path, "w", encoding="utf-8")
        print(f"Archivo de resultados creado: {self.archivo_path}")

    # ------------------------------------------------------------
    def _conectar(self):
        """Establece la conexión con RabbitMQ y prepara las colas necesarias."""
        print("Conectando con RabbitMQ...")
        cred = pika.PlainCredentials(self.user, self.password)
        params = pika.ConnectionParameters(host=self.host, credentials=cred)
        self.conexion = pika.BlockingConnection(params)
        self.canal = self.conexion.channel()
        self.canal.queue_declare(queue="result_queue", durable=True)
        self.canal.queue_declare(queue="dashboard_queue", durable=True)
        print(f"Conectado a RabbitMQ en {self.host}")

    # ------------------------------------------------------------
    def _procesar_resultado(self, canal, metodo, propiedades, cuerpo):
        """Procesa un mensaje recibido desde 'result_queue'."""
        try:
            msg = json.loads(cuerpo.decode())
            resultado = msg.get("resultado")
            escenario = msg.get("escenario", {})
            worker = msg.get("worker_id", "desconocido")
            timestamp = datetime.now().strftime("%H:%M:%S")

            # Guardar resultado en archivo
            linea = f"[{timestamp}] {worker} -> Escenario: {escenario} => Resultado: {resultado:.4f}\n"
            self.archivo.write(linea)
            self.archivo.flush()

            # Acumular resultado en memoria
            self.resultados.append(resultado)
            print(f"Resultado recibido: {resultado:.4f} (total: {len(self.resultados)})")

            # Enviar al dashboard
            msg_dashboard = {
                "worker_id": worker,
                "resultado": resultado,
                "timestamp": timestamp
            }
            self.canal.basic_publish(
                exchange="",
                routing_key="dashboard_queue",
                body=json.dumps(msg_dashboard),
                properties=pika.BasicProperties(delivery_mode=2)
            )

            canal.basic_ack(delivery_tag=metodo.delivery_tag)

        except Exception as e:
            print(f"Error procesando resultado: {e}")
            canal.basic_nack(delivery_tag=metodo.delivery_tag)

    # ------------------------------------------------------------
    def iniciar(self):
        """Comienza a escuchar la cola de resultados."""
        print("Escuchando resultados en 'result_queue'...")
        self.canal.basic_qos(prefetch_count=1)
        self.canal.basic_consume(queue="result_queue", on_message_callback=self._procesar_resultado)

        try:
            self.canal.start_consuming()
        except KeyboardInterrupt:
            print("\nFinalizando ejecución...")
            self._cerrar()

    # ------------------------------------------------------------
    def _cerrar(self):
        """Cierra el archivo, muestra estadísticas y termina la conexión."""
        if self.resultados:
            prom = statistics.mean(self.resultados)
            minimo = min(self.resultados)
            maximo = max(self.resultados)

            print("\nEstadísticas finales de resultados:")
            print(f"   Total: {len(self.resultados)}")
            print(f"   Promedio: {prom:.4f}")
            print(f"   Mínimo: {minimo:.4f}")
            print(f"   Máximo: {maximo:.4f}")
        else:
            print("\nNo se recibieron resultados durante la ejecución.")

        self.archivo.close()

        if self.conexion and not self.conexion.is_closed:
            self.conexion.close()

        print(f"Conexión cerrada. Resultados guardados en '{self.archivo_path}'.")
        print("Ejecución finalizada correctamente.")

# ------------------------------------------------------------
if __name__ == "__main__":
    collector = ResultCollector()
    collector.iniciar()