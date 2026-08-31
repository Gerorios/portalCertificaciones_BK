# Preview sin cantidad 0 + contrato del maestro como fuente única — Plan de implementación

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** (1) Ocultar del preview las filas con cantidad 0/vacía que no tengan total con plata; (2) una única regla de resolución de contrato (editado > maestro > archivo) compartida entre preview y carga, con aviso en el preview cuando el contrato final difiere del archivo.

**Architecture:** Toda la lógica nueva vive en el backend: la regla de plantilla en `app/services/validacion.py` y la resolución de contrato en `app/services/carga.py` (funciones puras testeables con fake db, patrón de `tests/test_carga_ptos_gasnor.py`). El endpoint `/certificaciones/preview` anota en cada fila el contrato final que se va a cargar; el frontend **deja de resolver el contrato por su cuenta** (`_resolverContratosDesdeDB` se elimina) y solo muestra lo que el backend decidió, con un aviso cuando hubo reasignación.

**Tech Stack:** FastAPI + SQLAlchemy (text SQL) + pytest (backend); HTML/JS vanilla (frontend, `pages/upload.html`).

**Spec:** Diseño acordado en chat (sesión 2026-08-31, registrada en `CONTEXTO_SISTEMA.md` §16). Decisiones del usuario: el **maestro manda** sobre el archivo; las filas con cantidad 0 no deben verse en el preview ni cargarse (lo segundo ya se cumple hoy vía `revalidar_fila`).

## Global Constraints

- Repos: backend `PortalCertificaciones_BK`, frontend `PortalCertificaciones_FE` (carpetas hermanas `PortalCertificaciones_backend` / `PortalCertificaciones_frontend`).
- Rama de trabajo en ambos repos: `fix/preview-cantidad-cero-contrato-maestro` (ya creada desde `main`).
- TDD estricto: test que falla → implementación mínima → suite completa verde. Suite actual: 36/36.
- Correr tests desde la raíz del backend: `python -m pytest -q` (o `.\venv\Scripts\python.exe -m pytest -q` si `python` no está en PATH).
- No tocar la BD compartida (dev y prod usan la misma, `testing`). Los tests usan fakes, nunca la BD real.
- Comparaciones de `item_codigo` siempre con normalización `.` ↔ `,` (convención existente: `REPLACE(item_codigo, '.', ',')`).
- Regla de dominio que NO cambia: una fila con cantidad 0/vacía **nunca** se carga (`revalidar_fila` → "Falta cantidad").
- Mensajes al usuario en español rioplatense (voseo), como el resto del portal.
- Commits con sufijo `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Regla de plantilla — cantidad 0 sin total se oculta del preview

**Files:**
- Modify: `app/services/validacion.py` (funciones `tiene_contenido_monetario` y `es_fila_plantilla`, líneas 20-29)
- Test: `tests/test_validacion.py`

**Interfaces:**
- Consumes: nada de otras tasks.
- Produces: `es_fila_plantilla(fila: dict) -> bool` con la regla nueva: plantilla = cantidad vacía/0 **y** total vacío/0 (el precio unitario ya no cuenta como contenido). `tiene_contenido_monetario` se **elimina** (solo la usaba `es_fila_plantilla` y sus propios tests). `filtrar_visibles_preview` no cambia de firma.

- [ ] **Step 1: Escribir los tests que fallan**

En `tests/test_validacion.py`, **eliminar** los tests `test_fila_con_total_tiene_contenido_monetario` y `test_fila_solo_unitario_tiene_contenido_monetario` (líneas 35-39) y el import de `tiene_contenido_monetario` (línea 11). En su lugar, agregar en la sección "contenido monetario / plantilla":

```python
def test_fila_cantidad_cero_con_solo_unitario_es_plantilla():
    # Ruido de catálogo Naturgy: unitario cargado, nada certificado → se oculta
    f = fila_base(cantidades="0", total_mes=None)
    assert es_fila_plantilla(f)

def test_fila_sin_cantidad_con_solo_unitario_es_plantilla():
    f = fila_base(cantidades=None, total_mes=None)
    assert es_fila_plantilla(f)

def test_fila_cantidad_cero_con_total_cero_es_plantilla():
    f = fila_base(cantidades="0", total_mes="0.00")
    assert es_fila_plantilla(f)

def test_fila_cantidad_cero_con_total_con_plata_no_es_plantilla():
    # Anomalía real: hay plata declarada sin cantidad → debe verse (con error)
    f = fila_base(cantidades="0")
    assert not es_fila_plantilla(f)

def test_fila_con_cantidad_y_total_no_es_plantilla():
    assert not es_fila_plantilla(fila_base())
```

Y actualizar el test de visibilidad existente `test_preview_muestra_incompleta_y_oculta_plantilla` (línea 101) para que el caso "solo unitario" quede del lado oculto:

```python
def test_preview_muestra_incompleta_y_oculta_plantilla():
    incompleta = fila_base(cantidades=None)                        # total con plata → se ve
    catalogo   = fila_base(cantidades="0", total_mes=None)         # solo unitario → se oculta
    plantilla  = fila_base(cantidades=None, precio_unitario=None,
                           total_mes=None)                         # ruido → se oculta
    normal     = fila_base()
    visibles = filtrar_visibles_preview([incompleta, catalogo, plantilla, normal])
    assert incompleta in visibles
    assert normal in visibles
    assert catalogo not in visibles
    assert plantilla not in visibles
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `python -m pytest tests/test_validacion.py -v`
Expected: FAIL — los tests nuevos de "solo unitario es plantilla" fallan porque `es_fila_plantilla` todavía considera el unitario como contenido.

- [ ] **Step 3: Implementación mínima**

En `app/services/validacion.py`, reemplazar las funciones de las líneas 20-29:

```python
def es_fila_plantilla(fila: dict) -> bool:
    """
    Sin cantidad y sin total con plata: ruido de plantilla, no se muestra.
    El unitario solo NO cuenta — los archivos de Naturgy traen el catálogo
    completo con unitario y cantidad 0, y eso no es contenido certificado.
    """
    cant  = _num(fila.get("cantidades"))
    total = _num(fila.get("total_mes"))
    return (cant is None or cant == 0) and (total is None or total == 0)
```

(`tiene_contenido_monetario` se elimina; verificar con grep que nadie más la importa: `grep -rn "tiene_contenido_monetario" app/ tests/`.)

Actualizar también el docstring del módulo (línea 2-7) y el comentario del preview en `app/routers/certificaciones.py` líneas 53-55:

```python
    # Se muestran todas las filas salvo las de plantilla (sin cantidad y sin
    # total con plata — el catálogo con unitario y cantidad 0 se oculta).
    # Las filas con total pero sin cantidad/provincia quedan visibles con
    # error para que el usuario las corrija — nunca se ocultan.
```

- [ ] **Step 4: Correr la suite completa**

Run: `python -m pytest -q`
Expected: PASS (los 2 tests eliminados se compensan con los 5 nuevos; ningún otro test debe romperse).

- [ ] **Step 5: Commit**

```bash
git add app/services/validacion.py app/routers/certificaciones.py tests/test_validacion.py
git commit -m "fix: ocultar del preview el catalogo con cantidad 0 (unitario solo no es contenido)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Resolución única de contrato en `carga.py`

**Files:**
- Modify: `app/services/carga.py`
- Test: `tests/test_resolver_contrato.py` (nuevo)

**Interfaces:**
- Consumes: nada de otras tasks.
- Produces (usadas por Tasks 3 y 4):
  - `resolver_contrato_final(db, item_codigo: str, contrato_archivo: str | None, contrato_editado: str | None = None) -> tuple[str | None, str]` — devuelve `(codigo_k, fuente)` con `fuente ∈ {"editado", "maestro", "archivo"}`.
  - `anotar_contrato_final(db, fila: dict) -> dict` — muta la fila: setea `contrato_archivo` (preservando el original del archivo), `contrato` (el final), `contrato_fuente` y `contrato_del_maestro` (el K si la fuente es el maestro, si no `None`).
  - Se **eliminan** `_id_contrato_desde_maestro` y `_resolver_id_contrato` (reemplazadas por la lógica nueva).

- [ ] **Step 1: Escribir los tests que fallan**

Crear `tests/test_resolver_contrato.py`:

```python
"""
Tests de la resolución única de contrato (sesión 2026-08-31):

Prioridad: contrato editado por el usuario > contrato del maestro (dim_item)
> contrato del archivo (solo si el ítem no está en el maestro).

Si el ítem existe en VARIOS contratos del maestro, se prefiere el del archivo
si está entre ellos; si no, el primero en orden determinista (ORDER BY id_item).
"""
from app.services.carga import resolver_contrato_final, anotar_contrato_final


class _FakeDB:
    """db.execute(...).fetchall() devuelve los contratos del maestro para el ítem."""

    def __init__(self, contratos_maestro):
        self._rows = [(k,) for k in contratos_maestro]
        self.consultas = 0

    def execute(self, *_args, **_kw):
        self.consultas += 1
        return self

    def fetchall(self):
        return self._rows


def test_contrato_editado_gana_siempre_y_no_consulta_maestro():
    db = _FakeDB(["K6"])
    k, fuente = resolver_contrato_final(db, "431,2", "K11", contrato_editado="K12")
    assert (k, fuente) == ("K12", "editado")
    assert db.consultas == 0


def test_maestro_gana_sobre_el_archivo():
    db = _FakeDB(["K6"])
    k, fuente = resolver_contrato_final(db, "431,2", "K11")
    assert (k, fuente) == ("K6", "maestro")


def test_item_en_varios_contratos_prefiere_el_del_archivo():
    db = _FakeDB(["K6", "K11"])
    k, fuente = resolver_contrato_final(db, "431,2", "K11")
    assert (k, fuente) == ("K11", "maestro")


def test_item_en_varios_contratos_sin_match_toma_el_primero():
    db = _FakeDB(["K6", "K11"])
    k, fuente = resolver_contrato_final(db, "431,2", "K99")
    assert (k, fuente) == ("K6", "maestro")


def test_item_fuera_del_maestro_usa_el_del_archivo():
    db = _FakeDB([])
    k, fuente = resolver_contrato_final(db, "999,9", "K11")
    assert (k, fuente) == ("K11", "archivo")


def test_item_fuera_del_maestro_y_sin_archivo_queda_none():
    db = _FakeDB([])
    k, fuente = resolver_contrato_final(db, "999,9", "")
    assert (k, fuente) == (None, "archivo")


def test_item_vacio_no_consulta_maestro():
    db = _FakeDB(["K6"])
    k, fuente = resolver_contrato_final(db, "", "K11")
    assert (k, fuente) == ("K11", "archivo")
    assert db.consultas == 0


# ── anotar_contrato_final ────────────────────────────────────

def test_anotar_reasignacion_del_maestro():
    fila = {"item_codigo": "431,2", "contrato": "K11"}
    anotar_contrato_final(_FakeDB(["K6"]), fila)
    assert fila["contrato"] == "K6"
    assert fila["contrato_archivo"] == "K11"
    assert fila["contrato_fuente"] == "maestro"
    assert fila["contrato_del_maestro"] == "K6"


def test_anotar_preserva_contrato_archivo_en_pasadas_sucesivas():
    # El preview puede anotar y el confirmar volver a anotar: el original
    # del archivo no debe pisarse con el contrato ya resuelto.
    fila = {"item_codigo": "431,2", "contrato": "K11"}
    anotar_contrato_final(_FakeDB(["K6"]), fila)
    anotar_contrato_final(_FakeDB(["K6"]), fila)
    assert fila["contrato_archivo"] == "K11"
    assert fila["contrato"] == "K6"


def test_anotar_item_fuera_del_maestro_deja_el_del_archivo():
    fila = {"item_codigo": "999,9", "contrato": "K11"}
    anotar_contrato_final(_FakeDB([]), fila)
    assert fila["contrato"] == "K11"
    assert fila["contrato_fuente"] == "archivo"
    assert fila["contrato_del_maestro"] is None


def test_anotar_respeta_contrato_editado():
    fila = {"item_codigo": "431,2", "contrato": "K6", "contrato_editado": "K12"}
    anotar_contrato_final(_FakeDB(["K6"]), fila)
    assert fila["contrato"] == "K12"
    assert fila["contrato_fuente"] == "editado"
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `python -m pytest tests/test_resolver_contrato.py -v`
Expected: FAIL con `ImportError: cannot import name 'resolver_contrato_final'`.

- [ ] **Step 3: Implementación**

En `app/services/carga.py`, **eliminar** `_id_contrato_desde_maestro` (líneas 44-51) y `_resolver_id_contrato` (líneas 54-71), y agregar en su lugar:

```python
def _contratos_maestro(db: Session, item_codigo: str) -> list[str]:
    """Códigos K de todos los contratos donde el maestro tiene este ítem,
    en orden determinista."""
    if not item_codigo:
        return []
    rows = db.execute(text("""
        SELECT dc.codigo_k
        FROM dim_item di
        JOIN dim_contrato dc ON di.id_contrato = dc.id_contrato
        WHERE REPLACE(di.item_codigo, '.', ',') = :item
        ORDER BY di.id_item
    """), {"item": item_codigo.replace(".", ",")}).fetchall()
    return [r[0] for r in rows]


def resolver_contrato_final(
    db: Session,
    item_codigo: str,
    contrato_archivo: str | None,
    contrato_editado: str | None = None,
) -> tuple[str | None, str]:
    """
    Regla única de resolución de contrato (preview y carga usan ESTA función):
    1. editado por el usuario en el preview → gana siempre
    2. maestro (dim_item); si el ítem está en varios contratos, se prefiere
       el del archivo si coincide, si no el primero en orden determinista
    3. archivo, solo si el ítem no está en el maestro
    Devuelve (codigo_k | None, fuente) con fuente en {"editado","maestro","archivo"}.
    """
    if contrato_editado:
        return contrato_editado, "editado"
    ks = _contratos_maestro(db, item_codigo)
    if ks:
        if contrato_archivo in ks:
            return contrato_archivo, "maestro"
        return ks[0], "maestro"
    return (contrato_archivo or None), "archivo"


def anotar_contrato_final(db: Session, fila: dict) -> dict:
    """Anota en la fila el contrato que efectivamente se va a cargar.
    Idempotente: `contrato_archivo` preserva siempre el K original del archivo."""
    if "contrato_archivo" not in fila:
        fila["contrato_archivo"] = fila.get("contrato") or ""
    k_final, fuente = resolver_contrato_final(
        db,
        fila.get("item_codigo") or "",
        fila["contrato_archivo"],
        fila.get("contrato_editado"),
    )
    fila["contrato"] = k_final or ""
    fila["contrato_fuente"] = fuente
    fila["contrato_del_maestro"] = k_final if fuente == "maestro" else None
    return fila
```

Y en `cargar_certificaciones`, reemplazar las líneas 112-117 (resolución vieja):

```python
        # Regla única de resolución (la misma que vio el usuario en el preview)
        k_final, _fuente = resolver_contrato_final(
            db,
            fila["item_codigo"],
            fila.get("contrato_archivo") or fila.get("contrato"),
            fila.get("contrato_editado"),
        )

        id_item      = _resolver_id_item(db, fila["item_codigo"], k_final or "")
        id_contrato  = _id_contrato_por_k(db, k_final) if k_final else None
        id_provincia = _resolver_id_provincia(db, fila["provincia"])
```

y el mensaje de error de la línea 121 pasa a usar `k_final`:

```python
        if not id_contrato:
            errores.append({"fila": i, "mensaje": f"Contrato {k_final} no encontrado"})
            omitidas += 1
            continue
```

Actualizar el docstring del módulo (líneas 4-7) a la regla nueva.

- [ ] **Step 4: Correr la suite completa**

Run: `python -m pytest -q`
Expected: PASS (los tests nuevos + los 5 de `test_carga_ptos_gasnor.py` intactos; nada más importaba las funciones eliminadas — verificar con `grep -rn "_resolver_id_contrato\|_id_contrato_desde_maestro" app/ tests/`).

- [ ] **Step 5: Commit**

```bash
git add app/services/carga.py tests/test_resolver_contrato.py
git commit -m "fix: resolucion unica de contrato (editado > maestro > archivo), determinista

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: El preview devuelve el contrato final que se va a cargar

**Files:**
- Modify: `app/routers/certificaciones.py` (endpoint `preview`, loop de las líneas 64-79)

**Interfaces:**
- Consumes: `anotar_contrato_final(db, fila)` de Task 2.
- Produces: cada fila del response de `/certificaciones/preview` trae `contrato` (el K final), `contrato_archivo`, `contrato_fuente` y `contrato_del_maestro`. El frontend (Task 5) depende de estos cuatro campos.

- [ ] **Step 1: Modificar el endpoint**

En `app/routers/certificaciones.py`, importar la función (línea 11):

```python
from app.services.carga import cargar_certificaciones, anotar_contrato_final
```

y en el loop del preview (líneas 65-79), anotar el contrato **antes** de revalidar (la revalidación valida el contrato final, que es el que cuenta):

```python
    for fila in filas_visibles:
        anotar_contrato_final(db, fila)

        codigo = (fila.get("item_codigo") or "").replace(".", ",")
        if codigo not in items_existentes:
            items_existentes[codigo] = db.execute(text("""
                SELECT 1 FROM dim_item
                WHERE REPLACE(item_codigo, '.', ',') = :item
                LIMIT 1
            """), {"item": codigo}).fetchone() is not None

        fila["tiene_error"], fila["error_detalle"] = revalidar_fila(
            fila,
            item_existe=items_existentes[codigo],
            provincias_validas=provincias_validas,
        )
        fila["item_en_maestro"] = items_existentes[codigo]
```

- [ ] **Step 2: Verificación**

Run: `python -m pytest -q`
Expected: PASS (la lógica anotada ya está cubierta por los tests de Task 2; el endpoint es cableado fino).

Verificación manual (con el backend local levantado y un Excel real de certificación):
subir un archivo desde la pantalla de carga y confirmar en la respuesta del preview
(pestaña Network del navegador) que las filas traen `contrato_archivo` y que `contrato`
es el del maestro cuando difieren.

- [ ] **Step 3: Commit**

```bash
git add app/routers/certificaciones.py
git commit -m "fix: el preview anota y muestra el contrato final (regla del maestro)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: El confirmar chequea permisos y loguea sobre el contrato final

**Files:**
- Modify: `app/routers/certificaciones.py` (endpoint `confirmar`, líneas 153-160)

**Interfaces:**
- Consumes: `anotar_contrato_final(db, fila)` de Task 2 (idempotente: preserva `contrato_archivo` si el frontend ya lo mandó).
- Produces: `check_contrato_access` y el `carga_log.contrato` usan el K final, no el del archivo.

- [ ] **Step 1: Modificar el endpoint**

En `confirmar`, después de `filtrar_cargables` (línea 153), anotar el contrato final de cada fila cargable antes de calcular `contratos_cargados`:

```python
    filas_ok = filtrar_cargables(candidatas, provincias_validas=provincias_validas)
    for f in filas_ok:
        f["tiene_error"] = False
        anotar_contrato_final(db, f)
    contratos_cargados = {f["contrato"] for f in filas_ok if f.get("contrato")}
```

(el loop existente `for f in filas_ok: f["tiene_error"] = False` de las líneas 154-155 queda absorbido acá; no duplicarlo).

- [ ] **Step 2: Verificación**

Run: `python -m pytest -q`
Expected: PASS.

Verificación manual: cargar un archivo cuyo item esté asignado en el maestro a un
contrato distinto del que dice el archivo, confirmar, y verificar que (a) el registro
del historial (`carga_log.contrato`) muestra el K del maestro, y (b) la fila en
`fact_certificaciones` quedó con el `id_contrato` del maestro (mismo K que mostró el preview).

- [ ] **Step 3: Commit**

```bash
git add app/routers/certificaciones.py
git commit -m "fix: permisos y log del confirmar sobre el contrato final, no el del archivo

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Frontend — usar la resolución del backend y avisar la reasignación

**Files:**
- Modify: `../PortalCertificaciones_frontend/pages/upload.html` (funciones `ejecutarPreview` líneas 304-328, `_resolverContratosDesdeDB` líneas 330-368, `renderTabla` líneas 382-413)

**Interfaces:**
- Consumes: campos `contrato`, `contrato_archivo`, `contrato_fuente`, `contrato_del_maestro` que el backend agrega a cada fila del preview (Task 3).
- Produces: nada para otras tasks. El frontend sigue mandando `contrato` y `contrato_editado` en `filas_editadas` al confirmar (el formato de datos del confirmar no cambia).

- [ ] **Step 1: Eliminar la resolución propia del frontend**

En `ejecutarPreview` (líneas 311-319), quitar la llamada y sus comentarios — queda:

```javascript
  // El contrato de cada fila ya viene resuelto por el backend
  // (regla: editado > maestro > archivo) en contrato / contrato_archivo /
  // contrato_del_maestro. Acá solo se revalida y se renderiza.
  filasEditables = filas.map(f => ({ ...f }));
  filasEditables.forEach(revalidarFila);
```

y **eliminar completa** la función `_resolverContratosDesdeDB` (líneas 330-368).

- [ ] **Step 2: Aviso de reasignación en la tabla**

En `renderTabla`, junto a la lógica del contrato (líneas 382-383), agregar:

```javascript
    const contrato   = f.contrato || "";
    const esMaestro  = contrato === f.contrato_del_maestro;
    const reasignada = !f.contrato_editado && f.contrato_archivo
                       && contrato && contrato !== f.contrato_archivo;
```

y después del `</select>` y el indicador ✓/✏ (línea 413), agregar el aviso:

```javascript
        ${reasignada
          ? `<div style="font-size:10px;color:var(--amarillo);white-space:nowrap"
                  title="El archivo decía ${f.contrato_archivo}; según el maestro de ítems se carga en ${contrato}">
               archivo: ${f.contrato_archivo} → ${contrato}
             </div>`
          : ""}
```

- [ ] **Step 3: Verificación manual**

Con el backend local corriendo (`uvicorn app.main:app --port 8000`) y el frontend abierto
desde localhost:
1. Subir un Excel real → las filas con cantidad 0 y solo unitario ya no aparecen.
2. Un item cuyo maestro diga otro contrato → el select muestra el K del maestro con el
   aviso «archivo: K11 → K6» y tooltip explicativo.
3. Editar el contrato a mano → aparece ✏ y el aviso desaparece (gana el editado).
4. Confirmar la carga → el historial muestra el K final.

- [ ] **Step 4: Commit (repo frontend)**

```bash
cd ../PortalCertificaciones_frontend
git add pages/upload.html
git commit -m "fix: preview usa el contrato resuelto por el backend + aviso de reasignacion

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Verificación integral y documentación

**Files:**
- Modify: `CONTEXTO_SISTEMA.md` (§ reglas de carga si existe la sección, y §16 registro de sesiones)

**Interfaces:**
- Consumes: todo lo anterior implementado y verde.
- Produces: documentación al día; ramas listas para PR.

- [ ] **Step 1: Suite completa + smoke manual**

Run: `python -m pytest -q`
Expected: PASS completo (36 tests previos − 2 eliminados + ~16 nuevos ≈ 50).

Repetir el smoke de Task 5 Step 3 de punta a punta si no se hizo con datos reales.

- [ ] **Step 2: Actualizar CONTEXTO_SISTEMA.md**

Agregar entrada en §16 con fecha del día: qué se cambió (regla de plantilla nueva, regla
única de contrato con el maestro como fuente de verdad, aviso en preview), decisiones del
usuario (maestro manda; cantidad 0 ni se ve ni se carga) y los archivos tocados. Si alguna
sección anterior describe la regla vieja de plantilla ("sin cantidad y sin ningún monto")
o la resolución vieja de contrato, actualizarla para que no queden reglas contradictorias.

- [ ] **Step 3: Commit de docs**

```bash
git add CONTEXTO_SISTEMA.md docs/superpowers/plans/2026-08-31-preview-cantidad-cero-contrato-maestro.md
git commit -m "docs: reglas nuevas de plantilla y contrato del maestro (sesion 2026-08-31)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Push de ambas ramas**

```bash
git push
cd ../PortalCertificaciones_frontend && git push
```

Los PRs a `main` y el deploy quedan para después de que el usuario pruebe en local
(flujo acordado: nada se deploya sin PR).
