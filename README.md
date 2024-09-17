
# Proyecto final; Pipeline de datos para calificación del TFM.

Este proyecto ha sido desarrollado para llevar a cabo ingestas de ficheros de datos en modalidad batch, en formato csv.








## Installation

Para instalar localmente.

```bash
  pip install data_pipeline-0.0.1-py3-none-any.whl
```
    
## Usage/Examples

Importamos la clase WebScrape() desde el módulo data_pipeline:

```python
from data_pipeline.WebScrape import WebScrape
```

Instanciamos la clase, colocando como parametro la ruta al fichero de configuración de extensión '.yaml':

```python
objeto = WebScrape('ruta_al_config.yaml')
```

La clase WebScrape(), dispone de dos métodos principales que se encargan del pipeline completo.

a) Primer método => get_links()

b) Segundo método => transform()

# Llamada a métodos:

Aplicamos el primer método a la instancia de clase que ya tenemos.
```python
objeto.get_links()

# Al ejecutar este método se descargaran y se guardaran automaticamente los ficheros especificados en fichero de configuración, dentro de las rutas mencionadas en el mismo.

# Tomar en cuenta que se imprimirá un mensaje expresando que ha culminado el guardado de los ficheros.
```

Luego, se aplica el segundo método a la misma instancia de clase previamente declarada.
```python
objeto.transform()

# Al ejecutar este método se realizaran las transformaciones correspondientes a cada fichero y luego se guardaran en la ruta especificada en el fichero de configuración.

# Tomar en cuenta que se imprimirá un mensaje expresando las rutas del guardado de los ficheros.
```

# Nota:

Para la prueba de la clase creada se ha utilizado un fichero de configuración que se tiene en la carpeta adjunta "config". Cabe destacar que la clase solo recibe extensiones .yaml, y se recomienda seguir el modelo presentado.

Se adjuntan también en la carpeta "tests/resorces/csv" los ficheros que se han utilizado para temas de prueba.