# Portal de Certificaciones Serytec

Portal interno para que Serytec cargue, valide y analice las certificaciones de trabajos realizados para Naturgy (GASNOR), reemplazando el proceso manual en Excel.

## Language

**PGN (Puntos GASNOR)**:
Métrica de puntuación que Naturgy/GASNOR asigna a la producción certificada, en paralelo al monto en pesos. Se calcula como `cantidad × ptos_gasnor` de cada línea certificada. El `ptos_gasnor` sale del archivo cargado; si el archivo no trae esa columna (ej. K12), se toma del maestro de ítems como fallback.
_Avoid_: "puntos", "puntaje"

**Certificación**:
Un lote de líneas de trabajo (ítems, cantidades, montos) presentado por Serytec a Naturgy correspondiente a un archivo cargado, un contrato y un período. Se registra como filas en `fact_certificaciones` y como una entrada de auditoría en `carga_log`.
_Avoid_: "carga" (carga es la acción de subir el archivo; certificación es el contenido)

**Maestro de ítems**:
Catálogo de referencia (`dim_item`) con el `ptos_gasnor`, contrato y tipo (OPEX/CAPEX) de cada ítem. Para el **contrato**, el maestro manda sobre lo que diga el archivo (salvo edición manual del usuario en el preview, que gana siempre). Para `ptos_gasnor`, manda el archivo y el maestro es fallback.
_Avoid_: "catálogo", "tabla de ítems"

**Presupuesto de contrato**:
Monto en $ (ARS) que Naturgy asigna a un contrato K para un ciclo determinado (ej. mayo 2026 – abril 2027). Vive en `dim_presupuesto_contrato`, con `periodo_desde`/`periodo_hasta` propios — permite guardar varios ciclos históricos por contrato sin perder el anterior al cargar uno nuevo. Solo gerencia/admin lo ven en Analytics.
_Avoid_: "límite", "cupo"

**Consumido (de presupuesto)**:
Suma en $ de `fc.total_mes` de las certificaciones de un contrato cuya `fc.fecha` cae dentro del `periodo_desde`/`periodo_hasta` de su presupuesto vigente. Es en pesos, no en PGN — el presupuesto Naturgy se mide en dinero, no en puntos.
_Avoid_: "gastado", "ejecutado"

**Código de ítem**:
Identificador del ítem en el maestro. Puede tener decimales escritos con coma o punto según la fuente (ej. "431,2" ≡ "431.2"); ambas notaciones refieren al mismo ítem y se comparan normalizadas.
_Avoid_: tratar "431,2" y "431.2" como ítems distintos

**Fila cargable**:
Fila de certificación que cumple: ítem existente en el maestro + contrato K + provincia válida + cantidad ≠ 0 + total mes presente. El precio unitario puede faltar si el total está. La cargabilidad se revalida ante cada edición del usuario y de nuevo al confirmar — nunca es un veredicto congelado del parser.
_Avoid_: "fila válida" (ambiguo), tratar `tiene_error` como flag definitivo

**Fila incompleta**:
Fila con contenido monetario (total o unitario presente) que aún no es cargable (le falta cantidad, provincia, etc.). Se muestra siempre en el preview marcada con error, editable para corrección manual. Nunca se oculta: si el archivo declara plata, la fila no desaparece en silencio.
_Avoid_: "fila inválida", descartarla sin mostrarla

**Fila de plantilla**:
Fila sin cantidad (vacía o 0) y sin total con plata. El precio unitario solo NO cuenta como contenido: los archivos de Naturgy traen el catálogo completo con unitario y cantidad 0. No representa trabajo certificado; se omite del preview sin aviso.
_Avoid_: "fila vacía", mostrarla como error, contar el unitario solo como contenido

**Fila excluida**:
Fila que el usuario destildó manualmente en el preview para que no se cargue (ej. duplicados generados por errores de lectura del parser). La exclusión es siempre manual — no hay detección automática de duplicados.
_Avoid_: "fila eliminada" (no se borra, solo no se carga)

**Incidencia de MO**:
Porcentaje del mes de un contrato K que se lleva la mano de obra: `costo MO del contrato en el mes ÷ total certificado del contrato en ese mes`, en $. El numerador es el monto bruto de liquidación de Horas imputado al K (suma de las 2 quincenas del mes); el denominador es lo certificado (no lo cobrado). El costo "sin contrato asignable" se excluye del cálculo por K y se muestra como línea propia — nunca se prorratea ni se oculta.
_Avoid_: "margen" (la incidencia es costo/facturado, no ganancia), incluir el bucket sin asignar dentro de un K

**Costo MO (de un contrato)**:
Monto bruto pagado por la liquidación de las horas imputadas a ese contrato K en el sistema de Horas (prorrateo del costo de cada empleado según horas aprobadas; empleados fijos según sus contratos de imputación). Se calcula en Horas, no acá.
_Avoid_: "gasto de personal" (ambiguo), recalcularlo fuera de Horas

**Acceso al módulo Certificaciones** (app unificada):
Concesión explícita de un admin en el sistema de Horas, nunca automática. Tres niveles: `admin` (todo), `carga` (jefe con contratos K asignados), `lectura` (gerencia, ve todo sin cargar). Los jefes con `carga` ven la incidencia de MO de sus K **solo si el admin les activa ese flag adicional**.
_Avoid_: dar acceso por tener rol JefeContrato en Horas, atar la incidencia al permiso de carga

**Total declarado**:
Monto "TOTAL MES" que el certificado trae en su encabezado. Se contrasta contra la suma de filas cargables del preview como aviso no bloqueante; una diferencia indica filas perdidas, excluidas o mal leídas.
_Avoid_: usarlo como validación bloqueante
