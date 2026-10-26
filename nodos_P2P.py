#!/usr/bin/env python3
import socket
import threading
import json
import sys
from datetime import datetime

# =====================================================================
# CONFIGURACIÓN DEL CLÚSTER (Ajustar en cada VM Debian)
# =====================================================================
# Puedes pasar el ID por argumento: python3 nodo_p2p.py nodo1
MI_ID = sys.argv[1] if len(sys.argv) > 1 else "nodo1"
PUERTO = 5000

# Mapa de topología: { "id_nodo": "ip_de_la_vm" }
NODOS = {
    "nodo1": "192.168.100.11",
    "nodo2": "192.168.100.12",
    "nodo3": "192.168.100.13",
    # Agrega aquí tus "n" nodos virtuales...
}

ARCHIVO_HISTORIAL = f"mensajes_{MI_ID}.log"
lock_archivo = threading.Lock()
lock_consola = threading.Lock()

# =====================================================================
# ALMACENAMIENTO SINCRONIZADO (Thread-Safe)
# =====================================================================
def guardar_registro(direccion, nodo_remoto, contenido, estado_ack):
    """Guarda el mensaje en el disco local protegiendo la escritura concurrente."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{timestamp}] [{direccion}] Par: {nodo_remoto} | Msg: '{contenido}' | Estado: {estado_ack}\n"
    
    with lock_archivo:
        with open(ARCHIVO_HISTORIAL, "a", encoding="utf-8") as f:
            f.write(linea)

def imprimir_consola(texto):
    """Imprime en la terminal sin romper el prompt del usuario."""
    with lock_consola:
        sys.stdout.write(f"\r\033[K{texto}\n[{MI_ID}] > ")
        sys.stdout.flush()

# =====================================================================
# LÓGICA DE RECEPCIÓN (Servidor Paralelo)
# =====================================================================
def manejar_conexion_entrante(conn, addr):
    """Hilo Worker: procesa un mensaje entrante, lo guarda y responde ACK."""
    try:
        datos = conn.recv(4096).decode("utf-8")
        if not datos:
            return
        
        paquete = json.loads(datos)
        emisor = paquete.get("emisor", addr[0])
        mensaje = paquete.get("mensaje", "")
        
        # 1. Almacenar en el nodo receptor
        guardar_registro("RECIBIDO", emisor, mensaje, "ACK_ENVIADO")
        
        # 2. Responder automáticamente de recibido
        respuesta_ack = {
            "receptor": MI_ID,
            "estado": "RECIBIDO_OK",
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
        conn.sendall(json.dumps(respuesta_ack).encode("utf-8"))
        
        # 3. Notificar en pantalla
        imprimir_consola(f"📩 Mensaje de [{emisor}]: {mensaje} (ACK automático enviado)")
        
    except Exception as e:
        imprimir_consola(f"⚠️ Error al procesar conexión de {addr}: {e}")
    finally:
        conn.close()

def hilo_servidor():
    """Escucha permanentemente conexiones de los demás nodos."""
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Permite reutilizar el puerto inmediatamente si reinicias el script
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("0.0.0.0", PUERTO))
    servidor.listen(20)
    
    while True:
        try:
            conn, addr = servidor.accept()
            # Lanza un hilo paralelo por cada mensaje entrante
            t = threading.Thread(target=manejar_conexion_entrante, args=(conn, addr), daemon=True)
            t.start()
        except Exception as e:
            imprimir_consola(f"Error en servidor: {e}")

# =====================================================================
# LÓGICA DE ENVÍO (Cliente Paralelo)
# =====================================================================
def enviar_a_nodo(id_destino, ip_destino, texto_mensaje):
    """Hilo Emisor: conecta con un nodo, envía el mensaje, espera ACK y guarda."""
    paquete = {
        "emisor": MI_ID,
        "mensaje": texto_mensaje
    }
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5.0)
            s.connect((ip_destino, PUERTO))
            s.sendall(json.dumps(paquete).encode("utf-8"))
            
            # Esperar respuesta automática (ACK)
            datos_ack = s.recv(1024).decode("utf-8")
            ack = json.loads(datos_ack)
            estado = f"{ack.get('estado')} a las {ack.get('timestamp')}"
            
            # Almacenar en el nodo emisor tras confirmar recepción
            guardar_registro("ENVIADO", id_destino, texto_mensaje, estado)
            imprimir_consola(f"✅ Confirmación de [{id_destino}]: Recibido correctamente.")
            
    except Exception as e:
        guardar_registro("ENVIADO_FALLIDO", id_destino, texto_mensaje, f"ERROR: {e}")
        imprimir_consola(f"❌ No se pudo contactar a [{id_destino}] ({ip_destino}): {e}")

def despachar_envio(destino, mensaje):
    """Lanza hilos paralelos para enviar a un nodo específico o a todos (broadcast)."""
    if destino.lower() == "todos":
        hilos = []
        for id_nodo, ip in NODOS.items():
            if id_nodo != MI_ID:
                t = threading.Thread(target=enviar_a_nodo, args=(id_nodo, ip, mensaje), daemon=True)
                hilos.append(t)
                t.start()
    elif destino in NODOS:
        if destino == MI_ID:
            print("⚠️ No necesitas enviarte un mensaje a ti mismo.")
            return
        t = threading.Thread(target=enviar_a_nodo, args=(destino, NODOS[destino], mensaje), daemon=True)
        t.start()
    else:
        print(f"⚠️ El nodo '{destino}' no existe en la topología. Nodos válidos: {list(NODOS.keys())} o 'todos'.")

# =====================================================================
# INTERFAZ DE TERMINAL (Hilo Principal)
# =====================================================================
def iniciar_nodo():
    if MI_ID not in NODOS:
        print(f"Error: '{MI_ID}' no está registrado en el diccionario NODOS.")
        sys.exit(1)

    # Iniciar servidor en segundo plano
    t_srv = threading.Thread(target=hilo_servidor, daemon=True)
    t_srv.start()

    print("=" * 65)
    print(f" 🚀 NODO ACTIVO: {MI_ID} ({NODOS[MI_ID]}:{PUERTO})")
    print(f" 📁 Archivo de almacenamiento: {ARCHIVO_HISTORIAL}")
    print(" 💡 Comandos:")
    print("    - <id_nodo> <mensaje> : Envía mensaje a un nodo (ej. nodo2 Hola)")
    print("    - todos <mensaje>     : Envía mensaje en paralelo a todos los nodos")
    print("    - historial           : Muestra los mensajes almacenados en este nodo")
    print("    - salir               : Cierra el programa")
    print("=" * 65)

    try:
        while True:
            entrada = input(f"[{MI_ID}] > ").strip()
            if not entrada:
                continue
            
            if entrada.lower() == "salir":
                print("Cerrando nodo...")
                break
                
            if entrada.lower() == "historial":
                print("\n--- HISTORIAL LOCAL DE MENSAJES ---")
                try:
                    with lock_archivo:
                        with open(ARCHIVO_HISTORIAL, "r", encoding="utf-8") as f:
                            print(f.read().strip())
                except FileNotFoundError:
                    print("(Aún no hay mensajes registrados)")
                print("-----------------------------------\n")
                continue

            partes = entrada.split(" ", 1)
            if len(partes) < 2:
                print("⚠️ Formato incorrecto. Usa: <destino> <mensaje>")
                continue

            destino, mensaje = partes[0], partes[1]
            despachar_envio(destino, mensaje)

    except KeyboardInterrupt:
        print("\nCerrando nodo por interrupción del usuario...")

if __name__ == "__main__":
    iniciar_nodo()