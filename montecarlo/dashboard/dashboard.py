# ============================================
#  dashboard.py
#  Sistema de Simulación Montecarlo Distribuido
#  Módulo: Dashboard
#  Autor: Krasny (Bara)
# ============================================

"""
Este módulo visualiza en tiempo real los resultados recibidos desde el colector.
Escucha la cola 'dashboard_queue' y muestra gráficamente:

1. La evolución de los resultados por consumidor.
2. El promedio global y número de resultados procesados.
3. La actividad individual de cada consumidor.

Su diseño sigue una estética de consola militar moderna, con colores suaves y
un enfoque minimalista tipo panel de control.
"""

import pygame
import pika
import json
import threading
from datetime import datetime
import os

# ---------------- CONFIGURACIÓN ----------------
RABBITMQ_HOST = "IP_SERVIDOR"
RABBITMQ_USER = "NOMBRE_HOST"
RABBITMQ_PASS = "12345"
QUEUE_NAME = "dashboard_queue"
RESULTS_DIR = "resultados_dashboard"

SCREEN_WIDTH = 1280
SCREEN_HEIGHT = 720
FPS = 30

# ---------------- ESTILO VISUAL ----------------
COLOR_FONDO = (150, 158, 124)   # #969e7c
COLOR_TITULO = (32, 39, 12)     # #20270c
COLOR_TEXTO = (101, 109, 75)    # #656d4b
COLOR_PANEL = (122, 135, 122, 180)  # #7a877a con transparencia
COLOR_CONTORNO = (122, 135, 122)    # #7a877a

COLOR_LINEAS = [
    (70, 90, 60), (80, 100, 70), (90, 110, 80),
    (100, 120, 90), (110, 130, 100), (120, 140, 110)
]

# ------------------------------------------------------------
class AlmacenDatos:
    """Gestiona la información recibida de los consumidores."""
    def __init__(self):
        self.datos = {}
        self.lock = threading.Lock()

    def agregar_resultado(self, worker_id, resultado):
        """Agrega o actualiza los datos de un consumidor."""
        with self.lock:
            if worker_id not in self.datos:
                self.datos[worker_id] = {
                    "color": COLOR_LINEAS[len(self.datos) % len(COLOR_LINEAS)],
                    "historial": [],
                    "conteo": 0,
                    "ultimo": resultado
                }
            d = self.datos[worker_id]
            d["historial"].append(resultado)
            if len(d["historial"]) > 200:
                d["historial"].pop(0)
            d["conteo"] += 1
            d["ultimo"] = resultado

    def instantanea(self):
        """Devuelve una copia segura de los datos."""
        with self.lock:
            return dict(self.datos)

# ------------------------------------------------------------
class EscuchadorRabbit(threading.Thread):
    """Hilo que escucha los mensajes provenientes del colector."""
    def __init__(self, almacen, host, usuario, contrasena):
        super().__init__(daemon=True)
        self.almacen = almacen
        self.host = host
        self.usuario = usuario
        self.contrasena = contrasena

    def run(self):
        cred = pika.PlainCredentials(self.usuario, self.contrasena)
        params = pika.ConnectionParameters(host=self.host, credentials=cred)
        conn = pika.BlockingConnection(params)
        canal = conn.channel()
        canal.queue_declare(queue=QUEUE_NAME, durable=True)
        print("Escuchando datos desde:", self.host)

        for metodo, props, cuerpo in canal.consume(QUEUE_NAME, inactivity_timeout=1):
            if cuerpo:
                try:
                    msg = json.loads(cuerpo.decode())
                    worker_id = msg.get("worker_id", "desconocido")
                    resultado = msg.get("resultado", 0.0)
                    self.almacen.agregar_resultado(worker_id, resultado)
                    canal.basic_ack(metodo.delivery_tag)
                    guardar_resultado(worker_id, resultado)
                except Exception as e:
                    print("Error procesando mensaje:", e)
        conn.close()

# ------------------------------------------------------------
def guardar_resultado(worker_id, resultado):
    """Guarda los resultados visualizados también en disco local."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    archivo = os.path.join(RESULTS_DIR, "resultados_dashboard.txt")
    with open(archivo, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {worker_id} => {resultado:.4f}\n")

# ------------------------------------------------------------
def dibujar_panel(screen, fuente, datos):
    """Dibuja el panel lateral con estado general."""
    panel_surface = pygame.Surface((320, SCREEN_HEIGHT - 60), pygame.SRCALPHA)
    pygame.draw.rect(panel_surface, COLOR_PANEL, (0, 0, 320, SCREEN_HEIGHT - 60), border_radius=10)
    screen.blit(panel_surface, (SCREEN_WIDTH - 340, 40))

    x_base = SCREEN_WIDTH - 320
    y = 80
    pygame.draw.rect(screen, COLOR_CONTORNO, (x_base - 10, 40, 320, SCREEN_HEIGHT - 60), 2, border_radius=10)

    texto_titulo = fuente.render("ESTADO GENERAL", True, COLOR_TITULO)
    screen.blit(texto_titulo, (x_base + 30, y)); y += 40

    todos = [v["ultimo"] for v in datos.values()] if datos else []
    if todos:
        promedio = sum(todos) / len(todos)
        screen.blit(fuente.render(f"Resultados totales: {len(todos)}", True, COLOR_TEXTO), (x_base + 20, y)); y += 30
        screen.blit(fuente.render(f"Promedio actual: {promedio:.4f}", True, COLOR_TEXTO), (x_base + 20, y)); y += 40
    else:
        screen.blit(fuente.render("Esperando resultados...", True, COLOR_TEXTO), (x_base + 20, y)); y += 40

    screen.blit(fuente.render("CONSUMIDORES ACTIVOS", True, COLOR_TITULO), (x_base + 30, y)); y += 30
    for wid, info in datos.items():
        c = info["color"]
        pygame.draw.rect(screen, c, (x_base + 20, y + 10, min(220, info["conteo"] * 2), 8))
        etiqueta = f"{wid[:10]} ({info['conteo']})"
        screen.blit(fuente.render(etiqueta, True, COLOR_TEXTO), (x_base + 20, y - 8))
        y += 30

# ------------------------------------------------------------
def dibujar_graficas(screen, fuente, datos):
    """Dibuja una línea tipo osciloscopio para cada consumidor."""
    base_y = SCREEN_HEIGHT // 2
    for i, (worker_id, info) in enumerate(sorted(datos.items())):
        color = info["color"]
        hist = info["historial"]
        offset = i * 70 - 150
        if len(hist) > 2:
            puntos = []
            for x, v in enumerate(hist[-200:]):
                px = int((x / 200) * (SCREEN_WIDTH - 420))
                py = base_y + offset - int((v % 60))
                puntos.append((px + 40, py))
            if len(puntos) > 1:
                pygame.draw.lines(screen, color, False, puntos, 2)
        etiqueta = f"{worker_id} [último={info['ultimo']:.2f}]"
        screen.blit(fuente.render(etiqueta, True, COLOR_TEXTO), (50, base_y + offset - 45))

# ------------------------------------------------------------
def dibujar_interfaz(screen, fuente, datos, tick):
    """Dibuja la interfaz completa."""
    screen.fill(COLOR_FONDO)
    titulo_color = COLOR_TITULO if (tick // 30) % 2 == 0 else COLOR_TEXTO
    screen.blit(fuente.render("SIMULADOR MONTECARLO", True, titulo_color), (40, 20))

    hora = datetime.now().strftime("%H:%M:%S")
    screen.blit(fuente.render(f"{hora}", True, COLOR_TEXTO), (1100, 25))

    dibujar_graficas(screen, fuente, datos)
    dibujar_panel(screen, fuente, datos)

# ------------------------------------------------------------
def main():
    """Función principal del dashboard."""
    pygame.init()
    pygame.display.set_caption("Panel de Control - Simulador Montecarlo")
    screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))
    fuente = pygame.font.SysFont("Consolas", 20, bold=True)
    reloj = pygame.time.Clock()

    almacen = AlmacenDatos()
    escuchador = EscuchadorRabbit(almacen, RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS)
    escuchador.start()

    tick = 0
    ejecutando = True
    while ejecutando:
        for evento in pygame.event.get():
            if evento.type == pygame.QUIT:
                ejecutando = False

        tick += 1
        datos = almacen.instantanea()
        dibujar_interfaz(screen, fuente, datos, tick)

        pygame.display.flip()
        reloj.tick(FPS)

    pygame.quit()
    print("Dashboard cerrado correctamente.")

# ------------------------------------------------------------
if __name__ == "__main__":
    main()
