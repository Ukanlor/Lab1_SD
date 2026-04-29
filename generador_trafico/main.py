import requests
import numpy as np
import time
import random

URL_CACHE = "http://127.0.0.1:8001"

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]

def simular_trafico(distribucion="uniforme", num_peticiones=500):
    print(f"\n--- Iniciando simulación: Distribución {distribucion.upper()} ---")
    
    for i in range(num_peticiones):
        if distribucion == "uniforme":
            zona_elegida = random.choice(ZONAS)
            
        elif distribucion == "zipf":
            a = 2.0 
            indice = np.random.zipf(a) - 1
            
            if indice >= len(ZONAS):
                indice = len(ZONAS) - 1
                
            zona_elegida = ZONAS[indice]

        # Aleatorio (Q1 a Q5)
        tipos_consultas = ["q1", "q2", "q3", "q4", "q5"]
        consulta_elegida = random.choice(tipos_consultas)

        print(f"[{i+1}/{num_peticiones}] Consultando {consulta_elegida.upper()} para la zona {zona_elegida}...")
        
        try:
            if consulta_elegida in ["q1", "q2", "q3"]:
                requests.get(f"{URL_CACHE}/{consulta_elegida}", params={"zone_id": zona_elegida, "confidence_min": 0.8})
            elif consulta_elegida == "q4":
                zona_b = random.choice([z for z in ZONAS if z != zona_elegida]) # Elegimos una segunda zona distinta
                requests.get(f"{URL_CACHE}/q4", params={"zone_a": zona_elegida, "zone_b": zona_b, "confidence_min": 0.8})
            elif consulta_elegida == "q5":
                requests.get(f"{URL_CACHE}/q5", params={"zone_id": zona_elegida, "bins": 5})
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión: {e}")
            
        time.sleep(0.05)
        
    print("Simulación terminada.")

if __name__ == "__main__":
    simular_trafico(distribucion="uniforme", num_peticiones=500)
    
    time.sleep(2) # Pausa
    
    simular_trafico(distribucion="zipf", num_peticiones=500)