# Memoria_INF

Código utilizado en el desarrollo de mi memoria para optar por el título de ingeniero civil informático.

## Flujo recomendado

El flujo a continuación es el recomendado para obtener el conjunto de datos o expandirlo.

### Obtener enlaces

Ejecutar el script de python en la raíz de la carpeta y asegurarse que existen los siguientes directorios: `Links\CM`, `Links\TD`, `Links\LIC`.
Esto es para asegurar el funcionamiento del script (el reporsitorio las trae por defecto). Para obtener enlaces existen dos scripts `Retriever.py` y `Retriever_TD.py`.
El primero es solo para licitaciones y el segundo para tartos directos y convenios marco.

La ejecución del primero es la siguiente:

```bash
anio=2022
mes=01
python Retriever.py  anio mes N
```


