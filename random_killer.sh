#!/bin/bash

# Prefijo que deben tener los contenedores
PREFIX="tp1-distribuidos"

# Servicios que no queremos matar
EXCLUDE_SERVICES=("rabbitmq" "gateway" "client_1" "client_2")

# Obtener todos los contenedores en ejecución y filtrar por nombre
ALL_SERVICES=$(docker compose ps --services --filter "status=running")

# Verificar servicios válidos según prefijo y exclusión
AVAILABLE_SERVICES=()
for service in $ALL_SERVICES; do
    FULL_NAME=$(docker compose ps -q "$service" | xargs docker inspect --format '{{.Name}}' | sed 's|/||')
    if [[ "$FULL_NAME" == $PREFIX* ]]; then
        skip=false
        for exclude in "${EXCLUDE_SERVICES[@]}"; do
            if [[ "$service" == "$exclude" ]]; then
                skip=true
                break
            fi
        done
        if ! $skip; then
            AVAILABLE_SERVICES+=("$service")
        fi
    fi
done

# Validar que haya servicios para matar
if [ ${#AVAILABLE_SERVICES[@]} -eq 0 ]; then
    echo "No hay servicios disponibles para matar (con prefijo $PREFIX)."
    exit 1
fi

# Función para matar un servicio
kill_service() {
    local service="$1"
    echo "Matando servicio: $service"
    docker compose kill "$service"
}

# Modo manual
if [[ "$1" == "--manual" ]]; then
    echo "Servicios disponibles para matar (prefijo $PREFIX):"
    for i in "${!AVAILABLE_SERVICES[@]}"; do
        echo "  [$i] ${AVAILABLE_SERVICES[$i]}"
    done
    echo -n "Elegí un número o escribí 'q' para cancelar: "
    read -r selection
    if [[ "$selection" == "q" ]]; then
        echo "Cancelado."
        exit 0
    elif [[ "$selection" =~ ^[0-9]+$ ]] && [ "$selection" -ge 0 ] && [ "$selection" -lt ${#AVAILABLE_SERVICES[@]} ]; then
        kill_service "${AVAILABLE_SERVICES[$selection]}"
    else
        echo "Selección inválida."
        exit 1
    fi
else
    # Modo aleatorio
    RANDOM_INDEX=$((RANDOM % ${#AVAILABLE_SERVICES[@]}))
    kill_service "${AVAILABLE_SERVICES[$RANDOM_INDEX]}"
fi
