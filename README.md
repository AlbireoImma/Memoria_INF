# Memoria_INF

Código utilizado en el desarrollo de mi memoria para optar por el título de ingeniero civil informático.

## Flujo recomendado

El flujo a continuación es el recomendado para obtener el conjunto de datos o expandirlo.

### Obtener enlaces

Ejecutar el script de python en la raíz de la carpeta y asegurarse que existen los siguientes directorios: `Links\CM`, `Links\TD`, `Links\LIC`.

Esto es para asegurar el funcionamiento del script (el reporsitorio las trae por defecto). Para obtener enlaces existen dos scripts `Retriever.py` y `Retriever_TD.py`.

El primero es solo para licitaciones y el segundo para tratos directos y convenios marco.

La ejecución del primero es la siguiente:

```bash
anio=2022
mes=01
python Retriever.py anio mes N
```

La ejecución del segundo es la siguiente (en donde los últimos dos argumentos corresponde a la descarga de enlaces sobre trato directo y convenio marco respectivamente):

```bash
anio=2022
mes=01
python Retriever_TD.py anio mes Y Y
```

### Descargar archivos de los enlaces

Para obtener los archivos JSON de los enlaces proporcionados por los scripts anteriores se recomienda utilizar el siguiente comando, en donde se debe reemplazar `PATH_LINKS_FILE` con la ruta a alguno de los archivos generados.

```
cat PATH_LINKS_FILE | tr -d '\r' | xargs -n 1 -P 1000 wget -nc -nv
```

### Generar los conjuntos de datos

Para generar un conjunto de datos se provee un jupyter notebook (`Mongo.ipynb`) en el cual se requiere de tener instalado o acceso a una instacia de MongoDB para realizar el flujo de datos. De cualquier manera los conjuntos procesado hasta octubre del 2021 se encuentran públicamente en [Kaggle](https://www.kaggle.com/datasets/franciscoabarca).
