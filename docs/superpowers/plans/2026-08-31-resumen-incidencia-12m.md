# Resumen rediseñado + evolución de incidencia 12m — Plan de implementación

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar el mockup aprobado (artifact 47f9feac): Resumen con 3 KPIs + gráfico de evolución de incidencia de MO a 12 meses + tabla compacta con semáforo (sin la tabla de detalle), y en Analytics la torta por provincia y el top de ítems rediseñado.

**Architecture:** La serie de 12 meses la sirve Horas con un endpoint hermano de `incidencia-mo` que reutiliza el cálculo mensual existente con **cache en memoria de meses cerrados** (inmutables; el mes corriente se calcula fresco). El % de incidencia por mes/K se cruza en el frontend con `/analytics/por-contrato-mes` (certificado por K y mes — ya existe). El portal abre `/analytics/estado-cargas` al jefe filtrado a sus K, habilitando los KPIs de certificaron/faltan para ambos roles con una sola estructura de pantalla.

**Tech Stack:** NestJS + Jest (Horas BE) · FastAPI + pytest (portal) · Next 16 + recharts + Vitest (Horas FE).

**Spec:** Mockup aprobado https://claude.ai/code/artifact/47f9feac-9cf0-4e8f-8f61-b1e7fb16433a + decisiones de la sesión 2026-08-31 (estructura única por rol con alcance distinto; 12 meses; gráfico + tabla compacta; sin tabla detalle; torta provincias; top items legible). Complementa el spec de la Etapa 1 (`docs/superpowers/specs/2026-08-31-unificacion-erp-etapa1-design.md`).

## Global Constraints

- Los 3 repos siguen en la rama `feat/erp-certificaciones-etapa1` (aún sin PR; estos cambios viajan en ella).
- TDD; suites actuales: portal 57, Horas BE 297, Horas FE 523. FE: solo corridas dirigidas por task (`npx vitest run <paths>` + `npx tsc --noEmit`); las suites completas de los 3 repos se corren en la task de cierre.
- Ningún comando conecta a la BD salvo los servicios ya corriendo; los tests usan mocks/fakes.
- Estructura única por rol: jefe = sus K, gerencia/admin = todo. Piezas exclusivas de gerencia/admin: presupuesto, línea "sin asignar", sección Operativo de Analytics (el gate de Operativo en Analytics SE MANTIENE aunque el backend ahora permita jefe en estado-cargas — decisión del mockup).
- Contrato del endpoint nuevo (lo consume el FE): `GET /certificaciones/incidencia-mo/serie?anio&mes&meses=12` → `[{ anio: number, mes: number, contratos: [{codigo, montoMo}], sinAsignar: number|null }]` (orden cronológico ascendente, el último es (anio,mes); mismas reglas de visibilidad por claim que `incidencia-mo`; `meses` acotado a 1..24).
- Semáforo: reutilizar `semaforo()`/`UMBRAL_INCIDENCIA_PCT` existentes (`features/certificaciones/resumen/incidencia.ts`, `config.ts`). Banda de alerta del gráfico: 30–45%.
- Top items: SIN fila "Otros" (el endpoint devuelve solo el top N; agregar "Otros" requeriría otro endpoint — YAGNI; difiere del mockup, decisión registrada).
- Textos español (voseo); commits con sufijo `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; push solo a la rama del fix.

---

### Task 1: Horas BE — serie de incidencia con cache de meses cerrados

**Files:**
- Modify: `src/certificaciones/incidencia.service.ts`, `src/certificaciones/certificaciones.controller.ts`
- Test: `src/certificaciones/incidencia.service.spec.ts` (ampliar)

**Interfaces:**
- Consumes: `AnalisisService.getAnalisis(anio, mes, quincena)` y `CertClaim` (existentes).
- Produces: `IncidenciaService.obtenerSerie(anio: number, mes: number, meses: number, cert: CertClaim | null): Promise<IncidenciaMesSerie[]>` con `interface IncidenciaMesSerie extends IncidenciaMo { anio: number; mes: number }`; ruta `GET /certificaciones/incidencia-mo/serie` (JwtAuthGuard, sin @Roles; `anio`/`mes` con ParseIntPipe, `meses` con `new DefaultValuePipe(12), ParseIntPipe`).

- [ ] **Step 1: Tests que fallan** — agregar a `incidencia.service.spec.ts` (mismo mock de `AnalisisService` con `getAnalisis: jest.fn()`):

```ts
describe('IncidenciaService.obtenerSerie', () => {
  it('devuelve N meses en orden cronológico terminando en (anio, mes)', async () => {
    // getAnalisis mockeado devolviendo contratos: [{contratoId:1, codigo:'K6', monto:10}]
    const serie = await service.obtenerSerie(2026, 8, 3, CERT_ADMIN);
    expect(serie.map((m) => `${m.anio}-${m.mes}`)).toEqual(['2026-6', '2026-7', '2026-8']);
    expect(serie[2].contratos).toEqual([{ codigo: 'K6', montoMo: 20 }]); // 10 + 10 quincenas
  });

  it('cruza el límite de año hacia atrás', async () => {
    const serie = await service.obtenerSerie(2026, 1, 3, CERT_ADMIN);
    expect(serie.map((m) => `${m.anio}-${m.mes}`)).toEqual(['2025-11', '2025-12', '2026-1']);
  });

  it('cachea meses cerrados: dos llamadas no recalculan meses pasados', async () => {
    await service.obtenerSerie(2026, 8, 2, CERT_ADMIN); // calcula jul y ago
    const llamadasTrasPrimera = analisisMock.getAnalisis.mock.calls.length;
    await service.obtenerSerie(2026, 8, 2, CERT_ADMIN);
    // solo el mes corriente (si (2026,8) es el mes actual del test: fijar "hoy" con jest.useFakeTimers
    // en 2026-08-15) se recalcula: +2 llamadas (q1 y q2), no +4
    expect(analisisMock.getAnalisis.mock.calls.length).toBe(llamadasTrasPrimera + 2);
  });

  it('aplica visibilidad por claim en cada mes (carga: sus ks, sinAsignar null)', async () => {
    const serie = await service.obtenerSerie(2026, 8, 2, { nivel: 'carga', ks: ['K6'], inc: true });
    for (const m of serie) { expect(m.sinAsignar).toBeNull(); expect(m.contratos.every((c) => c.codigo === 'K6')).toBe(true); }
  });

  it('sin acceso lanza Forbidden y meses se acota a 1..24', async () => {
    await expect(service.obtenerSerie(2026, 8, 12, null)).rejects.toBeInstanceOf(ForbiddenException);
    const serie = await service.obtenerSerie(2026, 8, 99, CERT_ADMIN);
    expect(serie.length).toBe(24);
  });
});
```

(`CERT_ADMIN = { nivel: 'admin', ks: [], inc: true }`; usar `jest.useFakeTimers().setSystemTime(new Date('2026-08-15'))` en el describe para que "mes corriente" sea determinista, con `jest.useRealTimers()` en afterAll.)

- [ ] **Step 2: RED** — `npm test -- incidencia` FAIL.
- [ ] **Step 3: Implementación** — refactor interno de `incidencia.service.ts`:

```ts
export interface IncidenciaMesSerie extends IncidenciaMo { anio: number; mes: number }

// cache de meses CERRADOS (inmutables): clave "anio-mes" → agregado SIN filtrar por claim.
// El mes corriente nunca se cachea. 1 solo proceso (PM2) — mismo criterio que el cache del portal.
private readonly cacheMes = new Map<string, { contratos: { codigo: string; montoMo: number }[]; sinAsignar: number }>();

private esMesCerrado(anio: number, mes: number): boolean {
  const hoy = new Date();
  return anio < hoy.getFullYear() || (anio === hoy.getFullYear() && mes < hoy.getMonth() + 1);
}

private async calcularMes(anio: number, mes: number) { /* el cuerpo actual de obtenerIncidencia
  SIN la autorización ni el filtro por claim: suma q1+q2, redondea, devuelve
  { contratos: [...todos los K...], sinAsignar } ; consulta/escribe cacheMes solo si esMesCerrado */ }

private aplicarVisibilidad(mes: { contratos: ...; sinAsignar: number }, cert: CertClaim): IncidenciaMo {
  if (cert.nivel === 'carga')
    return { contratos: mes.contratos.filter((c) => cert.ks.includes(c.codigo)), sinAsignar: null };
  return { contratos: mes.contratos, sinAsignar: mes.sinAsignar };
}

async obtenerIncidencia(anio, mes, cert) { /* autoriza (igual que hoy) → calcularMes → aplicarVisibilidad */ }

async obtenerSerie(anio: number, mes: number, meses: number, cert: CertClaim | null): Promise<IncidenciaMesSerie[]> {
  if (!cert || (cert.nivel === 'carga' && !cert.inc)) throw new ForbiddenException('No tenés acceso a la incidencia de mano de obra.');
  const n = Math.min(Math.max(meses, 1), 24);
  const out: IncidenciaMesSerie[] = [];
  for (let i = n - 1; i >= 0; i--) {
    const d = new Date(anio, mes - 1 - i, 1); // JS normaliza el cruce de año
    const a = d.getFullYear(), m = d.getMonth() + 1;
    const bruto = await this.calcularMes(a, m);
    out.push({ anio: a, mes: m, ...this.aplicarVisibilidad(bruto, cert) });
  }
  return out;
}
```

Controller: `@Get('incidencia-mo/serie')` con `@Query('anio', ParseIntPipe)`, `@Query('mes', ParseIntPipe)`, `@Query('meses', new DefaultValuePipe(12), ParseIntPipe)` → `this.incidencia.obtenerSerie(anio, mes, meses, req.user?.cert ?? null)`. OJO: declarar la ruta `incidencia-mo/serie` ANTES de cualquier ruta con parámetro si la hubiera (no la hay hoy — verificar).

- [ ] **Step 4: GREEN + suite** — `npm test` PASS (297 + nuevos), `npm run build` OK.
- [ ] **Step 5: Commit** — `git add src/certificaciones/` · `feat(certificaciones): serie de incidencia 12m con cache de meses cerrados` + sufijo.

---

### Task 2: Portal — estado-cargas abierto al jefe (filtrado a sus K)

**Files:**
- Modify: `app/routers/analytics.py:265-` (endpoint `estado_cargas`)
- Test: `tests/test_estado_cargas_jefe.py` (nuevo)

**Interfaces:**
- Produces: `/analytics/estado-cargas` acepta rol `jefe` (dependencia `require_analytics` en lugar de `require_gerente_or_admin`) y para jefe devuelve solo filas de `current.contratos_list`. Helper puro testeable `contratos_visibles(current, todos: list[str]) -> list[str]` en `analytics.py`. Task 3 depende del cambio de permiso.

- [ ] **Step 1: Tests que fallan** — `tests/test_estado_cargas_jefe.py`:

```python
"""El estado de cargas se abre al jefe, acotado a sus contratos (mockup 2026-08-31)."""
from types import SimpleNamespace
from app.routers.analytics import contratos_visibles

TODOS = ["K2", "K5", "K6", "K8", "K9", "K10", "K11", "K12"]

def _u(rol, ks=None):
    return SimpleNamespace(rol=rol, contratos_list=ks or [])

def test_admin_y_gerente_ven_todos():
    assert contratos_visibles(_u("admin"), TODOS) == TODOS
    assert contratos_visibles(_u("gerente"), TODOS) == TODOS

def test_jefe_ve_solo_sus_k_en_el_orden_del_maestro():
    assert contratos_visibles(_u("jefe", ["K11", "K6"]), TODOS) == ["K6", "K11"]

def test_jefe_sin_contratos_ve_lista_vacia():
    assert contratos_visibles(_u("jefe"), TODOS) == []

def test_k_asignado_que_no_existe_en_maestro_se_ignora():
    assert contratos_visibles(_u("jefe", ["K6", "K99"]), TODOS) == ["K6"]
```

- [ ] **Step 2: RED** — `python -m pytest tests/test_estado_cargas_jefe.py -v` FAIL (ImportError).
- [ ] **Step 3: Implementación** — en `analytics.py`, arriba del endpoint:

```python
def contratos_visibles(current, todos: list[str]) -> list[str]:
    """Jefe: solo sus K (en el orden del maestro); admin/gerente: todos."""
    if current.rol == "jefe":
        propios = {k.upper() for k in current.contratos_list}
        return [k for k in todos if k.upper() in propios]
    return todos
```

y en `estado_cargas`: cambiar la dependencia `_: Usuario = Depends(require_gerente_or_admin)` por `current: Usuario = Depends(require_analytics)`; después de armar `todos_contratos`, hacer `todos_contratos = contratos_visibles(current, todos_contratos)` y filtrar las filas de `cargas` a esos contratos (`cargas = [r for r in cargas if r.contrato in set(todos_contratos)]` o el equivalente en el armado del dict `cargados`). El resto del armado de la matriz no cambia.

- [ ] **Step 4: GREEN + suite** — `python -m pytest -q` PASS (57 + 4).
- [ ] **Step 5: Commit** — `git add app/routers/analytics.py tests/test_estado_cargas_jefe.py` · `feat: estado-cargas para jefes, filtrado a sus contratos K` + sufijo.

---

### Task 3: Horas FE — Resumen rediseñado (KPIs + evolución de incidencia + tabla compacta)

**Files:**
- Modify: `src/app/(protected)/certificaciones/page.tsx` (reescritura), `src/lib/api/certificaciones.ts` (hook nuevo + quitar gate)
- Create: `src/features/certificaciones/resumen/serie-incidencia.ts` (lógica pura), `src/features/certificaciones/resumen/evolucion-incidencia.tsx` (chart recharts, client, importado con `next/dynamic({ ssr:false })`)
- Test: `src/features/certificaciones/resumen/serie-incidencia.test.ts`, actualizar el test de página existente

**Interfaces:**
- Consumes: `GET /certificaciones/incidencia-mo/serie` (Task 1, forma exacta de Global Constraints, vía axios `api` de Horas); `usePorContratoMes` y `useResumenCert`/`useEstadoCargas` existentes (estado-cargas ya sin gate por Task 2); `calcularIncidencia`/`semaforo`/`UMBRAL_INCIDENCIA_PCT` existentes.
- Produces: `construirSerie(certPorMes: {periodo: string; contrato: string; monto: number}[], moSerie: IncidenciaMesSerie[]): PuntoSerie[]` con `PuntoSerie = { etiqueta: string; global: number | null; porK: Record<string, number | null> }` (pct = mo/certificado*100 redondeado a 1 decimal; null si certificado 0; `global` = Σmo/Σcertificado del mes). La consume solo esta task.

- [ ] **Step 1: Tests de la lógica pura** (`serie-incidencia.test.ts`, Vitest):

```ts
import { construirSerie } from './serie-incidencia';

const mo = [{ anio: 2026, mes: 7, contratos: [{ codigo: 'K6', montoMo: 30 }], sinAsignar: null },
            { anio: 2026, mes: 8, contratos: [{ codigo: 'K6', montoMo: 12 }, { codigo: 'K9', montoMo: 9 }], sinAsignar: null }];
const cert = [{ periodo: '2026-07', contrato: 'K6', monto: 100 },
              { periodo: '2026-08', contrato: 'K6', monto: 40 }, { periodo: '2026-08', contrato: 'K9', monto: 30 }];

it('calcula pct por K y global por mes', () => {
  const s = construirSerie(cert, mo);
  expect(s[0]).toMatchObject({ etiqueta: 'jul 26', global: 30, porK: { K6: 30 } });
  expect(s[1].porK).toEqual({ K6: 30, K9: 30 });
  expect(s[1].global).toBe(30); // (12+9)/(40+30)
});
it('K con MO pero sin certificado en el mes: pct null (no infinito)', () => {
  const s = construirSerie([], mo);
  expect(s[1].porK.K6).toBeNull();
  expect(s[1].global).toBeNull();
});
```

- [ ] **Step 2: RED** — `npx vitest run src/features/certificaciones/resumen/serie-incidencia.test.ts` FAIL.
- [ ] **Step 3: Implementación**
  - `serie-incidencia.ts`: `construirSerie` según el contrato de arriba (etiqueta `"jul 26"` con mes corto es-AR); exportar también `MESES_CORTOS` si hace falta.
  - Hook `useIncidenciaSerie(anio: number, mes: number, habilitado: boolean)` en `lib/api/certificaciones.ts` (axios `api` de Horas, ruta `/certificaciones/incidencia-mo/serie?anio&mes&meses=12`, `enabled: habilitado`, `retry: false`).
  - `evolucion-incidencia.tsx`: recharts `LineChart` con `ResponsiveContainer` (`div` h-72, `role="img"`, `aria-label="Evolución mensual de la incidencia de MO"`): línea `global` destacada (color tinta, strokeWidth 3), una línea por K (colores de `colores.ts` en orden fijo, strokeWidth 2, `connectNulls`), banda de alerta 30–45% con `ReferenceArea` (fill ámbar 6% de opacidad), eje Y en % con dominio [0, auto], tooltip compartido, leyenda. UN SOLO eje Y.
  - `page.tsx`: (1) KPIs: certificado del mes (suma de `useResumenCert` filtrado al período) con delta vs mes anterior (mismos datos, período -1; mostrar ▲/▼ y % con color ok/danger; si no hay mes anterior, sin delta); contratos certificados `X / Y` y "aún sin subir" desde `useEstadoCargas(periodo)` — **quitar el gate `puedeVerEstadoCargas`** (Task 2 lo habilitó server-side; el backend filtra por K). (2) Sección incidencia (mismo gate de siempre: `cert.inc || nivel !== 'carga'`): `<EvolucionIncidencia>` + tabla compacta del mes (contrato/certificado/MO/pct/semáforo — la tabla actual, compactada) + nota "sin asignar" (solo si `sinAsignar !== null`, del último punto de la serie). (3) **Eliminar la tabla de detalle por contrato/tipo** y sus imports muertos. Presupuesto queda igual. El gate de Operativo en Analytics NO se toca.
- [ ] **Step 4: Tests de página + tipos** — actualizar el test de página: KPIs presentes, tabla detalle AUSENTE (`queryByText` del header viejo → null), sección incidencia con el chart (mock `next/dynamic`), gate de estado-cargas eliminado (query habilitada para nivel carga). `npx vitest run <paths tocados>` PASS + `npx tsc --noEmit` limpio.
- [ ] **Step 5: Commit** — `feat(certificaciones): resumen rediseñado — KPIs, evolucion de incidencia 12m y tabla compacta` + sufijo.

---

### Task 4: Horas FE — Analytics: torta por provincia y top ítems legible

**Files:**
- Modify: `src/features/certificaciones/analytics/por-provincia-chart.tsx` (barras → torta), `src/features/certificaciones/analytics/top-items.tsx` (rediseño)
- Test: los `.test.tsx` que cubren ambos componentes (actualizar asserts)

**Interfaces:**
- Consumes: hooks existentes `usePorProvincia(filtros)` (`[{provincia, monto, pgn, lineas}]`) y `useTopItems(filtros)` (`[{item_codigo, tarea, monto, contrato, pgn_total}]`). Nada nuevo de backend.
- Produces: nada para otras tasks.

- [ ] **Step 1: Tests que fallan** — actualizar: por-provincia renderiza un `PieChart` (assert por `aria-label="Distribución del certificado por provincia"` y presencia de % en las etiquetas con datos mockeados); top-items renderiza código en chip, tarea con `title` (tooltip nativo) y monto formateado alineado (assert de celdas con los datos mock).
- [ ] **Step 2: RED** — corrida dirigida FAIL.
- [ ] **Step 3: Implementación**
  - Torta: recharts `PieChart`+`Pie` tipo donut (`innerRadius` ~55%, `paddingAngle` 2 — el gap de 2px entre gajos), colores de `colores.ts` en orden fijo por provincia (alfabético), `label` con el % (1 decimal), tooltip con monto formateado + %, leyenda al costado, centro con el total del período. Provincias con monto 0 se omiten. Máximo esperado: 4 gajos.
  - Top items: tabla con 3 columnas — código en `<span>` tipo chip mono, tarea en celda flexible con `overflow:hidden; text-overflow:ellipsis; white-space:nowrap` y `title={tarea}`, monto a la derecha con `tabular-nums`; barra proporcional de fondo (div absoluto con el degradé dorado suave, ancho `monto/max*100%`) detrás del texto de la tarea. SIN fila "Otros" (Global Constraints). Quitar el layout viejo colapsado.
- [ ] **Step 4: GREEN + tipos** — corridas dirigidas PASS + `npx tsc --noEmit`.
- [ ] **Step 5: Commit** — `feat(certificaciones): torta por provincia y top items legible en analytics` + sufijo.

---

### Task 5: Cierre — suites completas, docs y push

**Files:**
- Modify: `CONTEXTO_SISTEMA.md` (addendum a la entrada §16 "Unificación ERP Etapa 1")

**Interfaces:** consume todo lo anterior verde.

- [ ] **Step 1: Suites completas** — portal `python -m pytest -q` (esperado 61), Horas BE `npm test` (esperado ~302), Horas FE `npm test` (una corrida con la máquina tranquila; si flakea SOLO en archivos ajenos por timeout, documentar la lista exacta).
- [ ] **Step 2: Docs** — en la entrada §16 de la Etapa 1, agregar sub-bloque "Iteración post-mockup (mismo día)": qué cambió (Resumen rediseñado, serie 12m con cache, estado-cargas para jefes, torta+top items), decisiones (estructura única por rol con vista personalizada del jefe anotada como futuro; sin fila "Otros" en top items), y que el mockup vive en el artifact 47f9feac.
- [ ] **Step 3: Commits + push** — commit de docs en el portal; `git push` en los 3 repos (rama del fix).
- [ ] **Step 4: Recarga para prueba local** — reiniciar los servicios locales que estén corriendo (backend Horas y frontend levantan solos con watch; si no, relanzar) y avisar al usuario que pruebe contra el mockup.
