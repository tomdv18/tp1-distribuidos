#!/bin/bash

VOLUMES_DIR="volumes"

echo "Eliminando todos los archivos .json dentro de subcarpetas de $VOLUMES_DIR..."

if [ -d "$VOLUMES_DIR" ]; then
    find "$VOLUMES_DIR" -type f -name "*.json" -exec rm  -f {} \;
    echo "Archivos .json eliminados en todas las subcarpetas."
else
    echo "La carpeta $VOLUMES_DIR no existe."
fi
