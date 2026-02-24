## Pruebas de Throughput

| Prueba | Publishers | Subscribers | Payload (Bytes) | Tiempo (s) | Frecuencia (s) | Métrica | Comando .sh |
|--------|-----------|-------------|-----------------|------------|----------------|---------|-------------|
| Throughput Base (T1) | 10 | 10 | 100 | 300 | 0.001 | msgs/s | `./run_*_clients.sh 10 10 100 300 0.001` |
| Alta Carga (T2) | 20 | 20 | 100 | 300 | 0.001 | msgs/s | `./run_*_clients.sh 20 20 100 300 0.001` |
| Carga Extrema (T3) | 30 | 30 | 100 | 300 | 0.001 | msgs/s | `./run_*_clients.sh 30 30 100 300 0.001` |

---

## Pruebas de Memoria

| Prueba | Publishers | Subscribers | Payload (Bytes) | Tiempo (s) | Frecuencia (s) | Métrica | Comando .sh |
|--------|-----------|-------------|-----------------|------------|----------------|---------|-------------|
| Memoria Baja (M1) | 5 | 5 | 100 | 300 | 0.01 | RSS (KB) | `./run_*_clients.sh 5 5 100 300 0.01` |
| Memoria Media (M2) | 25 | 25 | 100 | 300 | 0.01 | RSS (KB) | `./run_*_clients.sh 25 25 100 300 0.01` |
| Memoria Alta (M3) | 50 | 50 | 100 | 300 | 0.01 | RSS (KB) | `./run_*_clients.sh 50 50 100 300 0.01` |