# Unificación ERP — Etapa 1: módulo Certificaciones (solo lectura) en la app de Horas

> Diseño validado en sesión de grilling del 2026-08-31. Decisión de fondo en
> `docs/adr/0001-horas-dueno-de-identidad-y-app-unificada.md`. Glosario: `CONTEXT.md`
> (ver *Incidencia de MO*, *Costo MO*, *Acceso al módulo Certificaciones*).

## Visión (contexto, no alcance de esta etapa)

Una sola aplicación: el frontend Next.js de Formulario_Horas (Next 16 + React 19 +
shadcn/ui + Tailwind 4, look gerencial/ERP) absorbe Certificaciones como módulo.
Migración pantalla por pantalla; el portal vanilla sigue en producción hasta que cada
pantalla tenga reemplazo. El backend FastAPI del portal no se migra: queda como servicio.

Mapa final del módulo (4 entradas de sidebar bajo "Certificaciones"): **Resumen**,
**Cargar**, **Historial**, **Analytics**. La administración de usuarios se absorbe en el
admin de Horas; el maestro de ítems pasa a CRUD de admin del módulo. La estética no
copia el portal actual: filtros y gráficos se rediseñan con el lenguaje visual de Horas
(recharts, barra de filtros estándar, tokens dorado/sand, Space Grotesk + IBM Plex).

## Alcance de la Etapa 1

1. **Login unificado (prerequisito)**: FastAPI valida el JWT de Horas y deja de emitir
   tokens propios. Los permisos del módulo viven en el padrón de Horas.
2. **Sección "Certificaciones" en la app de Horas con las pantallas de solo lectura**:
   - **Resumen**: KPIs del período, estado de cargas por contrato (quién certificó el
     mes y quién falta), presupuesto consumido, e **incidencia de MO** por K con
     semáforo.
   - **Analytics**: las 4 secciones actuales (Resumen/Tendencia/Desagregado/Operativo)
     rediseñadas con recharts y la barra de filtros de Horas — gráficos más
     representativos, no un port 1:1 de los Chart.js actuales.
3. **La carga sigue en el portal vanilla** (recién estabilizada; migra en la última
   etapa). Historial tampoco entra en esta etapa.

## Permisos (decisión cerrada)

Acceso al módulo = concesión explícita de un admin desde Horas, nunca automática:

| Nivel | Ve | Carga | Incidencia MO |
|---|---|---|---|
| `admin` | Todo | Sí (cuando migre la carga) | Todos los K |
| `lectura` (gerencia) | Todo | No | Todos los K |
| `carga` (jefe) | Sus K | Sus K | Solo con flag adicional otorgado por admin, solo sus K |

Datos del permiso por usuario: `nivel`, `contratos K asignados` (para `carga`),
`flag ver_incidencia` (para `carga`).

## Incidencia de MO (decisión cerrada)

- `incidencia = costo MO del K en el mes ÷ total certificado del K en ese mes` (en $).
- Numerador: monto bruto de liquidación imputado al K en Horas — ya lo calcula
  `GET /liquidacion/analisis?anio&mes&quincena` (prorrateo por horas aprobadas;
  fijos por `PerfilContratoImputacion`). Mes = suma de las 2 quincenas.
- Denominador: certificado del mes (no cobrado) — lo sabe el backend del portal.
- El bucket **"Sin contrato asignable"** se excluye del cálculo por K y se muestra como
  línea propia con su monto (presión para corregir imputaciones en Horas). Nunca se
  prorratea ni se oculta.
- Semáforo con umbral **configurable por admin** (valor inicial: a definir por el
  usuario cuando vea números reales; no inventar un default silencioso).
- El cruce se computa en el **frontend** (React Query: un fetch a cada backend, join
  por código K en el cliente). Si crece la complejidad, recién ahí evaluar un endpoint
  agregador — no construirlo de entrada (YAGNI).

## Arquitectura de la Etapa 1

- **Identidad**: Horas emite el JWT como hoy; FastAPI lo valida (mismo secret/alg —
  detalle exacto se releva del backend Node al armar el plan). Se eliminan login y
  emisión de tokens del portal cuando el vanilla se apague; mientras conviven, el
  portal vanilla puede seguir con su login actual (no bloquea la etapa).
- **Permisos del módulo**: tablas nuevas en el dominio de Horas (Prisma):
  acceso por usuario con nivel, K asignados y flag de incidencia. El backend FastAPI
  lee rol/contratos del JWT (claims) — no consulta la BD de Horas.
- **Frontend**: nueva sección en `navForRole` + páginas bajo `/certificaciones/*` en la
  app Next, componentes en `src/features/certificaciones/`. Consume la API FastAPI
  existente (`/analytics/*`, `/certificaciones/resumen`, `/analytics/presupuesto`) y
  la de liquidación de Horas para la incidencia.
- **Deploy**: la app unificada vive donde vive Horas (`misregistros.serytec.com.ar`);
  `certificaciones.serytec.com.ar` sigue sirviendo el portal vanilla hasta el final de
  la migración. CORS o proxy Nginx para que el frontend de Horas llegue al FastAPI —
  se decide en el plan (proxy same-origin recomendado, ya hay patrón en el VPS).

## Fuera de alcance (Etapa 1)

- Migrar la carga (wizard), el historial y el maestro de ítems.
- Migrar usuarios del portal que no existan en Horas (se dan de alta a mano — son pocos).
- Apagar el portal vanilla o sus URLs de respaldo.
- Endpoint agregador de incidencia.

## Riesgos y supuestos verificados

- ✅ Verificado en código: `Contrato.codigo` de Horas usa literalmente los códigos K
  ("K5", "K9"...) — mismo lenguaje que `dim_contrato.codigo_k`. La igualdad textual de
  códigos K es contrato entre sistemas.
- ✅ Verificado: el monto de liquidación por K ya existe como cálculo on-the-fly en
  Horas (no hay que persistir nada nuevo para la incidencia).
- ⚠️ Supuesto a validar al armar el plan: mecánica exacta de firma/validación del JWT
  de Horas (secret compartido vs otra cosa) y qué claims trae hoy.
- ⚠️ La BD del portal es la misma en dev y prod: las pruebas de la etapa no deben
  escribir en ella (la etapa es solo lectura — riesgo bajo por diseño).
