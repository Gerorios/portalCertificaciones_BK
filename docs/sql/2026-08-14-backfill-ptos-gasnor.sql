-- Backfill de ptos_gasnor para cargas históricas sin la columna (ej. K12).
-- Complementa el fix de app/services/carga.py (_ptos_gasnor_con_fallback):
-- el fix corrige las cargas futuras; este UPDATE corrige las ya insertadas.
--
-- ⚠️ NO EJECUTADO AÚN — aprobado en concepto por el usuario (2026-08-14) pero
-- pendiente de que pruebe los fixes en desarrollo. La BD del portal es la MISMA
-- en dev y producción (`testing`), así que este UPDATE impacta producción:
-- correr solo con OK explícito.
--
-- Antes de ejecutar, medir el alcance:
--   SELECT COUNT(*) FROM fact_certificaciones fc
--   JOIN dim_item di ON di.id_item = fc.id_item
--   WHERE fc.ptos_gasnor IS NULL AND di.ptos_gasnor IS NOT NULL;

UPDATE fact_certificaciones fc
JOIN dim_item di ON di.id_item = fc.id_item
SET fc.ptos_gasnor = di.ptos_gasnor
WHERE fc.ptos_gasnor IS NULL
  AND di.ptos_gasnor IS NOT NULL;

-- Verificación posterior (debería dar 0):
--   SELECT COUNT(*) FROM fact_certificaciones fc
--   JOIN dim_item di ON di.id_item = fc.id_item
--   WHERE fc.ptos_gasnor IS NULL AND di.ptos_gasnor IS NOT NULL;
