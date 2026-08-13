-- Índices pendientes de CONTEXTO_SISTEMA.md §3 — analytics filtra por estas columnas.
-- Verificado con SHOW INDEX el 2026-08-13: idx_contrato, idx_item e idx_provincia
-- YA existían en la BD (junto con idx_tipo e idx_origen, no listados en el doc).
-- Solo faltaba idx_fecha — aplicado el 2026-08-13 vía el contenedor del VPS.

ALTER TABLE fact_certificaciones
    ADD INDEX idx_fecha (fecha);

-- Estado final de índices tras aplicar:
-- PRIMARY, idx_contrato, idx_fecha, idx_item, idx_origen, idx_provincia, idx_tipo
