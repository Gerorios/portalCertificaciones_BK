# Portal de Certificaciones Serytec

Portal interno para que Serytec cargue, valide y analice las certificaciones de trabajos realizados para Naturgy (GASNOR), reemplazando el proceso manual en Excel.

## Language

**PGN (Puntos GASNOR)**:
Métrica de puntuación que Naturgy/GASNOR asigna a la producción certificada, en paralelo al monto en pesos. Se calcula como `cantidad × ptos_gasnor` de cada línea certificada. El `ptos_gasnor` de referencia debe salir del maestro de ítems (`dim_item`) cuando el archivo cargado no trae esa columna; hoy el sistema no hace ese fallback (ver pendientes en CONTEXTO_SISTEMA.md).
_Avoid_: "puntos", "puntaje"

**Certificación**:
Un lote de líneas de trabajo (ítems, cantidades, montos) presentado por Serytec a Naturgy correspondiente a un archivo cargado, un contrato y un período. Se registra como filas en `fact_certificaciones` y como una entrada de auditoría en `carga_log`.
_Avoid_: "carga" (carga es la acción de subir el archivo; certificación es el contenido)

**Maestro de ítems**:
Catálogo de referencia (`dim_item`) con el `ptos_gasnor`, contrato y tipo (OPEX/CAPEX) por defecto de cada ítem. Los valores del maestro son defaults editables en el preview de carga, no la fuente de verdad final — la certificación (archivo cargado) tiene prioridad una vez editada por el usuario.
_Avoid_: "catálogo", "tabla de ítems"

**Presupuesto de contrato**:
Monto en $ (ARS) que Naturgy asigna a un contrato K para un ciclo determinado (ej. mayo 2026 – abril 2027). Vive en `dim_presupuesto_contrato`, con `periodo_desde`/`periodo_hasta` propios — permite guardar varios ciclos históricos por contrato sin perder el anterior al cargar uno nuevo. Solo gerencia/admin lo ven en Analytics.
_Avoid_: "límite", "cupo"

**Consumido (de presupuesto)**:
Suma en $ de `fc.total_mes` de las certificaciones de un contrato cuya `fc.fecha` cae dentro del `periodo_desde`/`periodo_hasta` de su presupuesto vigente. Es en pesos, no en PGN — el presupuesto Naturgy se mide en dinero, no en puntos.
_Avoid_: "gastado", "ejecutado"
