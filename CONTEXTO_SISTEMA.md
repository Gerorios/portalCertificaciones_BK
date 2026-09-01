# Sistema de Certificaciones Serytec — Contexto Completo

> Documento de referencia para retomar el desarrollo sin perder contexto.
> Última actualización: Julio 2026

---

## 1. Descripción general

Portal web interno para que **Serytec** cargue, valide y analice las certificaciones de trabajos realizados para **Naturgy (GASNOR)**. Reemplaza el proceso manual en Excel.

**Usuarios:**
- **Admin** — acceso total
- **Jefe de contrato** — sube certificaciones de sus contratos K, ve su dashboard
- **Gerente** — solo lectura, ve analytics completo

---

## 2. Stack tecnológico

| Capa | Tecnología |
|------|-----------|
| Backend | FastAPI + Python 3.11, SQLAlchemy, MySQL |
| Frontend | HTML/JS puro, CSS variables, Chart.js 4.4 |
| Deploy backend | VPS propio 179.198.99.30 — Docker (`python:3.11-slim`), ver `docs/arquitectura-produccion-vps.md`. Respaldo: Render.com free |
| Deploy frontend | VPS propio — estáticos por Nginx. Respaldo: Netlify |
| Repo | https://github.com/Gerorios/portalCertificaciones_BK |

**URLs producción:**
- Backend: `https://portalcertificaciones-bk.onrender.com`
- Frontend: `https://portalcertificaciones.netlify.app`

**UptimeRobot** pinguea `/health` cada 5 min para mantener el backend activo.

**URL producción (VPS, principal):** `https://certificaciones.serytec.com.ar`
(las URLs de Render/Netlify quedan como respaldo)

**Frontend futuro (Unificación ERP, Etapa 1 — ver §16):** el módulo Certificaciones
se está integrando dentro de la app de Horas (`https://misregistros.serytec.com.ar`),
que consume este mismo backend FastAPI como API (`apiCert`, `NEXT_PUBLIC_CERT_API_URL`).
Hasta que termine la migración, `certificaciones.serytec.com.ar` sigue sirviendo el
frontend vanilla actual sin cambios — conviven las dos vías de acceso al mismo backend.

---

## 3. Base de datos

### Tablas principales

```sql
dim_contrato      -- K2, K5, K6, K8, K9, K10, K11, K12
dim_item          -- maestro de ítems con ptos_gasnor, contrato asignado
ma_provincias     -- Salta, Jujuy, Tucumán, Santiago del Estero (activo=1)
fact_certificaciones  -- tabla de hechos principal
usuarios          -- roles: admin | jefe | gerente
carga_log         -- historial de cargas (archivo, período, filas, estado)
```

### Columnas clave de `fact_certificaciones`

```
id_item, id_contrato, id_provincia, fecha,
cantidades, ptos_gasnor, precio_unitario, total_mes,
tarea, tipo (OPEX/CAPEX), contratista, observaciones,
hoja_origen, archivo_origen, cargado_por
```

> **Importante:** `ptos_gasnor` se guarda desde el archivo (certificación), NO desde el maestro.
> El cálculo de PGN en analytics usa `fc.ptos_gasnor` para coincidir con Power BI.

### Índices (aplicados 2026-08-13)

`idx_contrato`, `idx_item`, `idx_provincia` (y también `idx_tipo`, `idx_origen`) ya existían en `fact_certificaciones`; solo hizo falta crear `idx_fecha`. Ver `docs/sql/2026-08-13-indices-fact-certificaciones.sql`.

```sql
ALTER TABLE fact_certificaciones
    ADD INDEX idx_fecha (fecha);
```

### Query para insertar ítem en el maestro

```sql
INSERT INTO dim_item (item_codigo, id_contrato, tarea, ptos_gasnor, unidad_medida, tipo, contratista)
VALUES (
    '693',
    (SELECT id_contrato FROM dim_contrato WHERE codigo_k = 'K9'),
    'Construcción de cámara', 7500.00, 'N°', 'CAPEX', 'SER&TEC'
);
```

---

## 4. Estructura de archivos backend

```
app/
├── main.py
├── config.py
├── database.py
├── models.py              # Usuario con roles y contratos_list
└── routers/
│   ├── auth.py
│   ├── certificaciones.py  # preview, confirmar, resumen, detalle
│   ├── admin.py
│   ├── items.py            # CRUD maestro ítems
│   └── analytics.py        # todos los endpoints de gráficos
└── services/
    ├── auth.py             # JWT, bcrypt==4.0.1
    ├── parser.py           # parser Excel robusto por nombre columna
    ├── parser_pdf.py       # parser PDF por coordenadas x del header
    ├── carga.py            # inserta en fact_certificaciones
    ├── cache.py
    └── onedrive.py         # Microsoft Graph API
```

---

## 5. Flujo de carga (4 pasos)

1. **Subir archivo** (.xlsx/.xls/.xlsm/.pdf) → backend parsea, devuelve `cache_id`
2. **Seleccionar hojas** → chips de hojas; jefe solo ve las de sus contratos K (las demás con 🔒)
3. **Preview editable** — filas de plantilla (sin cantidad y sin total con plata) se ocultan
   del preview (`app/services/validacion.py::es_fila_plantilla`; el unitario solo no cuenta
   como contenido). Tabla con columnas editables:
   - **Contrato** — default resuelto por `resolver_contrato_final` (el maestro manda sobre el
     archivo), editable (se guarda como `contrato_editado`). Si el maestro reasigna el K del
     archivo, el preview muestra el aviso «archivo: K11 → K6»
   - **Provincia** — select, obligatorio antes de confirmar
   - **Cantidad** — editable
   - **$ Total** — editable
4. **Confirmar** → envía `cache_id` + `hojas` + `filas_editadas`; valida duplicados por nombre de archivo

### Resolución de contrato — el maestro es la fuente única

`resolver_contrato_final` / `anotar_contrato_final` en `app/services/carga.py` es la ÚNICA
función que decide el K de una fila; la usan tanto el preview como la carga (determinismo
garantizado, no puede haber preview y carga en desacuerdo):

1. `contrato_editado` (usuario lo cambió en el preview) → máxima prioridad
2. Contrato del maestro de ítems (`dim_item`) — **manda sobre el del archivo aunque difieran**;
   si el ítem está en varios contratos del maestro, se prefiere el K del archivo si coincide
   con alguno de ellos, si no el primero en orden determinista (por `id_item`)
3. Fallback: contrato del archivo, solo si el ítem no está en el maestro

El preview anota `contrato` (K final), `contrato_archivo` (K original del archivo, siempre
preservado), `contrato_fuente` (`"editado"|"maestro"|"archivo"`) y `contrato_del_maestro`.
`confirmar` recalcula sobre el K final (no confía en lo que mandó el frontend) para chequear
permisos del jefe y loguear correctamente.

---

## 6. Parsers

### Excel (`parser.py`)

- Busca columnas por nombre, no por posición (`COL_ALIAS` dict)
- Normaliza saltos de línea y espacios en nombres de columna antes de comparar
- Aliases K12: `DESCRIPCION→tarea`, `CANTIDAD→cantidades`, `PUNTOS→ptos_gasnor`, `$ UNIT→precio_unitario`, `TOTAL CERTIFICADO→total_mes`
- `_encontrar_header_idx` busca `ÍTEMS/ITEMS/ÍTEM/ITEM` en cualquier columna
- Fallback calamine si openpyxl falla
- Provincia vacía **NO** es error bloqueante (usuario la completa en preview)

### PDF (`parser_pdf.py`)

- Detecta columnas automáticamente desde x0 real del header (punto medio entre columnas)
- Incluye columna `NOMBRE` en el header para reconocer nombre_contrato
- Hereda `item_codigo` mirando adelante cuando hay línea huérfana (solo números)
- Hereda `tarea`, `tipo`, `contratista` de la última fila completa cuando hay línea huérfana
- Pega números partidos (gap < 8px entre dígitos)
- Funciona con A3 (K9SUR) y Letter (K2), suma exacta verificada

---

## 7. Analytics

### Endpoints (`/analytics/`)

Todos soportan filtros: `contratos[]`, `provincias[]`, `tipo` (OPEX/CAPEX), `desde`, `hasta`.
El jefe siempre queda restringido a sus propios contratos.

| Endpoint | Descripción |
|----------|-------------|
| `/evolucion-mensual` | Monto y PGN por período |
| `/por-contrato-mes` | Monto y PGN por contrato y período |
| `/por-provincia` | Monto, PGN y líneas por provincia |
| `/top-items` | Top N ítems (usa JOIN a dim_item para nombre) |
| `/interanual` | Comparación año actual vs anterior con variación % |
| `/contratos` | Lista de K disponibles (jefe: solo los suyos) |
| `/provincias` | Lista desde ma_provincias activo=1 |
| `/estado-cargas` | Estado por contrato×período desde 2025-01 |
| `/kpis-jefe` | KPIs mes actual vs mismo mes año anterior |
| `/presupuesto` | Consumo en $ vs presupuesto Naturgy por contrato (solo admin/gerente) |

### Cálculo de PGN

```sql
SUM(fc.cantidades * COALESCE(fc.ptos_gasnor, 0))
```

Usa el `ptos_gasnor` de la **certificación** (no del maestro) para coincidir con Power BI.
`COALESCE` maneja archivos que no traen esa columna (ej: K12).

### Provincias — comparación case-insensitive

```sql
AND UPPER(pv.provincia) IN ('JUJUY', 'SALTA', ...)
```

Esto resuelve el error 500 cuando el frontend envía las provincias en mayúsculas.

---

## 8. Frontend — páginas

| Página | Ruta | Rol |
|--------|------|-----|
| `index.html` | `/` | Login |
| `dashboard.html` | `/pages/` | Admin/Jefe |
| `analytics.html` | `/pages/` | Admin/Gerente |
| `upload.html` | `/pages/` | Admin/Jefe |
| `historial.html` | `/pages/` | Admin/Jefe/Gerente |
| `items.html` | `/pages/` | Admin |
| `admin.html` | `/pages/` | Admin |

### Dashboard (jefe)

- KPIs interanuales: facturación y PGN mes actual vs mismo mes año anterior
- Barra de filtros: dropdown multiselect contratos + período desde/hasta + toggle $/PGN
- Total del período en bloque separado debajo de la barra
- Gráfico línea evolución + gráfico interanual con toggle $/PGN
- Tabla de certificaciones con paginación y filtros
- Modal pendientes (vencidos en rojo, pendientes en amarillo)

### Analytics (gerente)

- Barra de filtros horizontal compacta con dropdown colapsable para contratos y provincias
- Toggle pastilla: Ambos/OPEX/CAPEX
- Toggle pastilla: $/PGN (afecta TODOS los gráficos sin refetch)
- Badge naranja en el botón de filtros cuando hay contratos/provincias excluidos
- Descripción de filtros activos en texto debajo del título
- **Presupuesto Naturgy por contrato** (solo admin/gerente, no depende de la barra de filtros de fecha): KPIs de presupuesto total/consumido/% global/contratos en alerta + un medidor de progreso por contrato con presupuesto cargado, ordenado de mayor a menor % consumido. Colores de estado: verde real `#16A34A` <80%, `--amarillo` 80-99%, `--rojo` ≥100% (`--verde` del sistema es alias del dorado de marca y no sirve para esto, ver sección 15)
- Gráficos: línea evolución, barras por contrato, barras horizontales por provincia, interanual, top ítems
- Estado de cargas al final con filtros por período/contrato/tipo

### Presupuesto por contrato

- Tabla `dim_presupuesto_contrato` (id_contrato, periodo_desde, periodo_hasta, monto_presupuesto, activo) — permite múltiples ciclos históricos por contrato, no pisa el anterior
- CRUD en `admin.html` → sección "Presupuesto por contrato", solo admin (`/admin/presupuestos`)
- El consumo se calcula con `SUM(fc.total_mes)` filtrando `fc.fecha BETWEEN periodo_desde AND periodo_hasta` — mismo criterio de fecha que el resto de analytics
- Ciclo actual cargado: mayo 2026 – abril 2027

---

## 9. Variables de entorno

> Desde 2026-08-13 las variables de producción viven en `/var/www/PortalCertificaciones_back/.env`
> del VPS (chmod 600, NO commiteado). Render conserva una copia como respaldo.

```
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
SECRET_KEY, ALGORITHM=HS256
ALLOWED_ORIGINS=https://certificaciones.serytec.com.ar,https://portalcertificaciones.netlify.app
AZURE_TENANT_ID=08487b0c-70cd-473c-80da-193f2f00be92
AZURE_CLIENT_ID=1b3d7b6d-23c8-412b-b223-d5188e4df9c6
AZURE_CLIENT_SECRET=⚠️ REGENERAR — quedó expuesto
ONEDRIVE_USER=administracion@serytecsas.onmicrosoft.com
OPENAI_API_KEY   (disponible pero parser PDF IA deprecado — usar parser_pdf.py)
```

---

## 10. Deploy

### VPS propio (principal desde 2026-08)

Ver `docs/arquitectura-produccion-vps.md` (mapa completo, operación diaria y decisiones)
y el plan `docs/superpowers/plans/2026-08-13-migracion-vps-docker.md`.

### Render (backend)

- `.python-version` = 3.11.9
- `apt.txt`: `poppler-utils` (para pdf2image si se usa)
- `requirements.txt` incluye: `bcrypt==4.0.1`, `pydantic-settings`, `pandas --only-binary=:all:`, `pdfplumber`, `openpyxl`, `calamine`
- Build command: `pip install -r requirements.txt`

### Netlify (frontend)

- `js/api.js` es multi-entorno desde 2026-08-13: localhost → :8000, `*.netlify.app` → Render, cualquier otro host → mismo origen + `/api`.
- Paleta: `--primario: #DCA028`, `--secundario: #4A4A4A`

---

## 11. OneDrive

Sube PDFs/Excel a la carpeta de OneDrive corporativo al confirmar carga.

```
Certificaciones / K8 / 2026-05 / archivo.xlsx
```

---

## 12. Problemas conocidos y soluciones

| Problema | Causa | Solución |
|----------|-------|----------|
| CORS al entrar al portal | Backend durmiendo en Render gratuito | UptimeRobot pingueando /health |
| "Web Page Blocked" en PCs Naturgy | Firewall corporativo bloquea onrender.com | Usar hotspot o dominio propio |
| Error SSL en PCs con Citrix | Citrix intercepta certificados | Aceptar advertencia o usar hotspot |
| PGN diferente al de PBI | Se usaba di.ptos_gasnor del maestro | Ahora usa fc.ptos_gasnor de la certificación |
| Provincias no filtran (error 500) | Comparación case-sensitive | UPPER() en el WHERE |
| PDF K9SUR campos vacíos | pdfplumber parte filas en dos líneas | Herencia de contexto en filas huérfanas |
| K12 Excel no detectado | Header dice "ITEM" sin S | Agregado a _encontrar_header_idx |
| Contrato editado en preview no impacta | carga.py ignoraba contrato_editado | Verificar campo contrato_editado en fila |
| Lentitud general (arranque en frío) | Render free duerme tras 15 min | Migrado al VPS propio 2026-08-13 (ver docs/arquitectura-produccion-vps.md) |

---

## 13. Pendientes

> Ver `docs/arquitectura-produccion-vps.md` y plan `docs/superpowers/plans/2026-08-13-migracion-vps-docker.md` para la migración al VPS (será completada en Task 10).

- [ ] **Urgente:** Regenerar secreto Azure (`AZURE_CLIENT_SECRET` quedó expuesto en chat) — pendiente; programado como Task 9 de la migración al VPS
- [x] **Bug UX (resuelto):** un ítem que aparece en más de una fila del preview (ej. mismo ítem en Jujuy y en Salta) no agrupaba las filas — si el usuario editaba el contrato en una fila y no en la otra, la fila no editada se cargaba con el contrato del maestro en vez del elegido. `carga.py` y el resto del backend funcionaban correctamente; el problema era que `editarContrato()` en `upload.html` solo tocaba la fila editada. Solución: al cambiar el contrato de una fila, se aplica automáticamente a todas las filas con el mismo `item_codigo` en el preview.
- [ ] **UX:** el mensaje de error al re-subir un archivo duplicado (`certificaciones.py:126`) le dice a cualquier usuario "eliminá la carga anterior desde el historial", pero solo el admin puede eliminar cargas (decisión confirmada: se mantiene admin-only). Corregir el texto para que el jefe sepa que debe pedírselo a un admin, en vez de sugerir una acción que no puede hacer.
- [ ] **Bug:** cuando el archivo no trae la columna `ptos_gasnor` (ej. K12), `carga.py` guarda `NULL` y el PGN queda en 0 en analytics (`COALESCE(fc.ptos_gasnor, 0)`). Debería, en ese caso, tomar `ptos_gasnor` del maestro (`dim_item`) y multiplicarlo por `cantidades`. Hoy no hay ningún fallback a `dim_item` en `carga.py` ni en `parser.py`.
- [x] Aplicar índices en `fact_certificaciones` — hecho 2026-08-13; solo faltaba `idx_fecha` (los otros ya existían), ver docs/sql/2026-08-13-indices-fact-certificaciones.sql
- [x] Dominio propio para el backend — hecho 2026-08-13: https://certificaciones.serytec.com.ar (VPS propio)
- [x] ~~Upgrade Render a plan Starter~~ — innecesario: migrado al VPS propio 2026-08-13, sin costo mensual extra
- [x] Verificar que `sidebar.js` tiene link "analytics" para gerente — confirmado, ya está (`sidebar.js:13`, roles `["admin","gerente"]`)
- [ ] Repuntar el monitor de UptimeRobot a `https://certificaciones.serytec.com.ar/api/health` (ahora como monitoreo real, ya no anti-sleep)
- [ ] Decidir si Render/Netlify se dan de baja o quedan como respaldo permanente

---

## 14. Contratos activos

| Código | Descripción |
|--------|-------------|
| K2 | Instalaciones domiciliarias — Salta/Jujuy |
| K5 | Mantenimiento de redes |
| K6 | Mantenimiento de redes |
| K8 | Obras |
| K9 | Instalaciones auxiliares — Tucumán/Santiago del Estero |
| K10 | Mantenimiento |
| K11 | Mantenimiento |
| K12 | Medidores |
| OTROS | Estructuras o trabajos de taller |

---

## 15. Paleta de colores CSS

```css
--primario:      #DCA028
--primario-dark: #B8841F
--primario-light:#FDF3DC
--secundario:    #4A4A4A
--rojo:          #DC2626
--amarillo:      #D97706
--azul:          #2563EB

/* --verde es alias de --primario (#DCA028) por compatibilidad histórica —
   NO es un verde real. Para indicadores de estado que necesiten un verde
   distinguible de --amarillo (ej. medidores de presupuesto), usar #16A34A
   directo en vez de var(--verde). */
--verde:         var(--primario)
```

---

## 16. Registro de sesiones de desarrollo

> Regla de trabajo desde 2026-08-14 (pedido del usuario): cada sesión sobre el portal
> se documenta acá en detalle, y **nada se deploya sin PR** (`desarollo` → `main`)
> con sus commits correspondientes.

### 2026-08-13 — Migración al VPS propio (backend dockerizado)

Ejecutada completa; detalle en `docs/arquitectura-produccion-vps.md` y el plan
`docs/superpowers/plans/2026-08-13-migracion-vps-docker.md`. Resumen: backend en Docker
(`python:3.11-slim`, **1 worker** por el cache de preview en memoria — no escalar sin sacar
el cache del proceso), frontend estático por Nginx, `https://certificaciones.serytec.com.ar`
con SSL, índice `idx_fecha` aplicado (los otros 3 del §3 ya existían), `js/api.js`
multi-entorno. Render/Netlify quedan de respaldo. Pendientes: rotar `AZURE_CLIENT_SECRET`,
repuntar UptimeRobot.

### 2026-08-14 — Fixes del backlog: PGN de K12 y mensaje de duplicados (commit `872fd45`)

- **`app/services/carga.py`** — nueva `_ptos_gasnor_con_fallback(db, valor_archivo, id_item)`:
  si el archivo no trae `ptos_gasnor` (None o "", caso K12), toma el del maestro `dim_item`;
  si lo trae, se respeta el del archivo (criterio Power BI, §3). Usada en el INSERT de
  `cargar_certificaciones`. Resuelve el bug "PGN queda en 0" del §13.
- **`app/routers/certificaciones.py`** — nueva `mensaje_archivo_duplicado(archivo, rol)`:
  el mensaje de archivo duplicado del confirmar ahora distingue rol (admin: "eliminá desde
  el historial"; jefe/gerente: "pedile a un administrador"). Resuelve el ítem UX del §13.
- **Tests (TDD)**: `tests/test_carga_ptos_gasnor.py` (5 tests, fake db) y
  `tests/test_mensaje_duplicado.py` (3 tests). Suite completa: 36/36 verde.
- **Backfill del histórico**: `docs/sql/2026-08-14-backfill-ptos-gasnor.sql` preparado y
  **NO ejecutado** — ⚠️ la BD del portal es la misma en dev y producción (`testing`);
  correr solo con OK explícito. Probado por el usuario en local: funciona bien.
- **Estado**: commiteado y pusheado en `desarollo`; deploy al VPS pendiente de PR + merge
  a `main` (flujo nuevo) y del backfill.

### 2026-08-31 — PR + merge a `main`, redeploy al VPS y baja de la rama `desarollo`

- **PRs mergeados** (pedido del usuario): backend
  [BK #47](https://github.com/Gerorios/portalCertificaciones_BK/pull/47) (migración VPS +
  fixes PGN K12 y mensaje de duplicados; 36/36 tests verdes antes del merge) y frontend
  [FE #30](https://github.com/Gerorios/portalCertificaciones_FE/pull/30) (api.js multi-entorno).
- **Redeploy en el VPS**: ambos clones (`/var/www/PortalCertificaciones_back` y `_front`)
  **cambiados de `desarollo` a `main`** — los deploys futuros son `git pull` de `main`.
  Backend reconstruido (`docker compose up -d --build`); verificado `/health` 200 y
  portal 200 en `https://certificaciones.serytec.com.ar`.
- **Rama `desarollo` eliminada** (local y remota, en ambos repos) a pedido del usuario.
  Para el próximo ciclo de trabajo: crear la rama de desarrollo desde `main`.
- **Sigue pendiente**: backfill de `ptos_gasnor` (`docs/sql/2026-08-14-backfill-ptos-gasnor.sql`,
  solo con OK explícito — BD compartida dev/prod), rotar `AZURE_CLIENT_SECRET` si no se hizo,
  y repuntar UptimeRobot.

### 2026-08-31 — Fix: preview sin cantidad 0 + contrato del maestro como fuente única (rama fix/preview-cantidad-cero-contrato-maestro)

Plan: `docs/superpowers/plans/2026-08-31-preview-cantidad-cero-contrato-maestro.md`.

- **Qué cambió**:
  - Regla de plantilla nueva: una fila sin cantidad (None o 0) **y** sin total con plata
    se oculta del preview (ruido de catálogo); el unitario solo, sin cantidad, no cuenta
    como contenido certificado. `app/services/validacion.py::es_fila_plantilla`.
  - Regla única de resolución de contrato: `resolver_contrato_final` /
    `anotar_contrato_final` en `app/services/carga.py` — editado > maestro > archivo,
    determinista, preferencia por el K del archivo si el ítem está en varios contratos
    del maestro. Es la única función que decide el K; la usan preview y carga por igual.
  - El preview ahora anota y muestra el contrato final resuelto (no el crudo del archivo)
    y, si el maestro reasigna, el frontend muestra el aviso «archivo: K11 → K6».
  - `confirmar` (`app/routers/certificaciones.py`) chequea permisos del jefe y loguea sobre
    el K final, no sobre el del archivo.
  - Frontend (`pages/upload.html`): eliminada `_resolverContratosDesdeDB` (la resolución de
    contrato queda 100% en el backend); agregado el aviso de reasignación.
- **Decisiones del usuario**: el maestro de ítems manda sobre el contrato del archivo,
  incluso si difieren; una fila con cantidad 0 no se carga nunca; en el preview solo se ve
  si trae total con plata (anomalía a corregir), el resto se oculta.
- **Archivos tocados**:
  - Backend: `app/services/validacion.py`, `app/services/carga.py`,
    `app/routers/certificaciones.py`, `tests/test_validacion.py`,
    `tests/test_resolver_contrato.py` (nuevo).
  - Frontend: `pages/upload.html`.
  - Ver también §5 (flujo de carga y resolución de contrato), actualizado para no dejar la
    vieja prioridad de resolución de contrato contradiciendo esto (era la única sección que
    describía la regla vieja; §13 no la mencionaba).
- **Tests**: suite completa `python -m pytest -q` → 50 passed.
- **Estado**: CERRADO el mismo día. Probado por el usuario en local (backend :8000 +
  frontend :5500), mergeado vía PRs [BK #48](https://github.com/Gerorios/portalCertificaciones_BK/pull/48)
  y [FE #31](https://github.com/Gerorios/portalCertificaciones_FE/pull/31), deployado al VPS
  (health y portal 200) y rama del fix eliminada en ambos repos.

### 2026-08-31 — Unificación ERP Etapa 1: módulo Certificaciones en la app de Horas (ramas feat/erp-certificaciones-etapa1)

Trabajo en 3 repos en paralelo, cada uno en la rama `feat/erp-certificaciones-etapa1`. Objetivo:
que la app de Horas (`misregistros.serytec.com.ar`, dueña de la identidad de usuarios) incorpore
un módulo Certificaciones que consume este mismo backend FastAPI, sin duplicar login.

- **Qué se construyó, por repo:**
  - **Portal backend** (este repo): acepta también un JWT firmado por Horas
    (`HORAS_JWT_SECRET`, mismo secret HS256 que `JWT_SECRET` en Horas), con claims
    `cuil`/`email`/`rol`/`cert`; mapeo cert→rol interno del portal (`PrincipalHoras`,
    solo lectura: `nombre`, `rol` ya mapeado, `contratos_list`, `ver_incidencia`, `id=0`).
    CORS ampliado en `app/main.py` para aceptar el origen de esa app (Etapa 1: Task 8).
  - **Horas Backend**: modelos y CRUD de accesos al módulo (nivel `admin`/`carga`/`lectura`
    + lista de contratos K + flag `incidencia`); claim `cert` sumado al JWT y al endpoint de
    perfil; endpoint nuevo `GET /certificaciones/incidencia-mo` (suma de ambas quincenas por
    contrato, con el sub-total de horas/montos sin contrato asignable en un bucket aparte,
    `contratoId: null, codigo: 'Sin contrato asignable'`, excluido del agregado por código y
    devuelto separado como `sinAsignar`; solo visible a `admin`/`lectura` o con flag
    `incidencia`). Migración Prisma generada pero NO aplicada (se aplica en el deploy con
    `migrate deploy`, documentado en `prisma/migrations/README.md`).
  - **Horas Frontend**: sección Certificaciones dentro de la app (nav gateado por
    `perfil.cert != null`), cliente HTTP `apiCert` (`NEXT_PUBLIC_CERT_API_URL`) apuntando al
    portal backend, página Resumen (KPIs, tabla de incidencia con semáforo, card de
    Presupuesto) y Analytics.
- **Decisiones del usuario:**
  - App única con **Horas como dueño de la identidad**: un solo login, el portal de
    certificaciones no vuelve a emitir sesión propia para estos usuarios.
  - **Accesos por concesión de admin**: nivel `admin`/`carga`/`lectura` + flag `incidencia`
    independiente del nivel (alguien con `lectura` puede o no ver incidencia de MO).
  - **Incidencia de mano de obra**: lo sin asignar a contrato va a un bucket aparte
    (`sinAsignar`), nunca mezclado en el agregado por código de contrato — así no ensucia
    el % de incidencia por contrato con horas que no se pudieron atribuir.
  - **Umbral de alerta de incidencia = 30%**, hardcodeado en código del frontend
    (`UMBRAL_INCIDENCIA_PCT` en `src/features/certificaciones/config.ts` de Horas FE) como
    valor inicial acordado; UI de configuración por admin queda para una etapa posterior.
- **Verificado hoy**: la base de datos del portal y la de Horas son **la misma instancia
  física** (`191.101.235.7`, esquema `testing`) — confirmado antes de decidir cómo compartir
  identidad/accesos entre ambas apps.
- **Checklist de deploy pendiente** (nada de esto se ejecutó — queda para el usuario):
  - Aplicar la migración Prisma pendiente en Horas Backend con `migrate deploy`.
  - Setear `HORAS_JWT_SECRET` en el `.env` del portal (VPS) = mismo valor que `JWT_SECRET`
    de Horas — a mano, nunca commiteado.
  - Definir y configurar `NEXT_PUBLIC_CERT_API_URL` en Horas Frontend, o el proxy Nginx
    same-origin alternativo (`location /certapi/` → `127.0.0.1:8000`) — decisión al deployar.
  - Abrir y mergear los PRs de los 3 repos (portal, Horas Backend, Horas Frontend).
- **Suites al cierre de la Etapa 1** (Task 8, corridas en local, nada deployado):
  portal `python -m pytest -q` → 54 passed; Horas BE `npm test` → 297 passed (27 suites);
  Horas FE `npm test` (Vitest, corrida única) → 523 passed (74 archivos), sin flakes.
- **Estado**: pendiente de prueba del usuario → PRs de los 3 repos → deploy. Nada deployado.

- **Iteración post-mockup (2026-09-01)**: sobre la misma rama `feat/erp-certificaciones-etapa1`
  de los 3 repos, ajuste de UI guiado por un mockup revisado con el usuario.
  - **Qué cambió**:
    - Página **Resumen** rediseñada según el mockup: KPIs de certificado / certificaron / faltan,
      gráfico de evolución de incidencia de los últimos 12 meses (con cache de meses cerrados en
      Horas Backend — solo el mes corriente se recalcula en cada request), y una tabla compacta
      con semáforo; se elimina la tabla de detalle que tenía la primera versión.
    - `estado-cargas` (avance de carga por K) abierto también a jefes (nivel `carga`), filtrado
      a sus propios K — antes era solo para `admin`/`lectura`.
    - Página **Analytics**: torta por provincia y listado de top items con formato más legible.
  - **Decisiones**:
    - Se mantiene una **estructura única de página por rol** (no una página distinta por rol);
      una vista personalizada para el jefe (recorte propio de KPIs/serie) queda anotada como
      **FUTURO**, no se construye en esta iteración.
    - Top items **sin fila "Otros"**: el endpoint devuelve directamente el top N, no hay
      agregado de "resto" a mostrar.
  - El mockup de referencia vive en el artifact `47f9feac`
    (`claude.ai/code/artifact/47f9feac-9cf0-4e8f-8f61-b1e7fb16433a`).
  - **Suites al cierre de esta iteración** (corridas en local, nada deployado):
    portal `python -m pytest -q` → 61 passed; Horas BE `npm test` → 302 passed (27 suites) —
    de paso se corrigió un test de `incidencia.service.spec.ts` que dependía de la fecha real
    del sistema (usaba el mes corriente sin fijarlo con fake timers, y quedó expuesto al cruzar
    el calendario a septiembre: el cache de "mes cerrado" hacía que un test reutilizara el
    resultado cacheado de otro); Horas FE `npm test` (Vitest, corrida única, ~538 tests) →
    537 passed, 1 timeout intermitente en
    `analytics-page.test.tsx` (torta por provincia) bajo la carga de la corrida completa —
    verificado que ese mismo test pasa limpio en aislamiento (~7s de test, timeout de 5s), no
    es un fallo lógico sino contención de recursos del entorno jsdom con las ~538 pruebas
    corriendo en la misma tanda.
