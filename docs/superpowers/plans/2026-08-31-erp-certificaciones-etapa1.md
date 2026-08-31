# Unificación ERP Etapa 1 — Plan de implementación

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Login unificado (FastAPI valida el JWT de Horas) + sección "Certificaciones" de solo lectura (Resumen con incidencia de MO + Analytics) dentro de la app Next.js de Horas.

**Architecture:** Los permisos del módulo viven en el dominio de Horas (Prisma: acceso por usuario con nivel/K/flag incidencia) y viajan como claim `cert` dentro del JWT que Horas ya emite; el backend FastAPI del portal acepta ese token con el mismo secret y mapea el claim a su modelo de roles sin tocar la BD de Horas. La incidencia se sirve desde un endpoint nuevo en Horas (los jefes no pueden usar `/liquidacion/analisis`, que es de Admin/Liquidador) y el frontend la cruza por código K con lo certificado que devuelve FastAPI.

**Tech Stack:** NestJS + Prisma + Jest (Horas backend) · Next 16 + React 19 + shadcn/ui + TanStack Query + recharts + Vitest (Horas frontend) · FastAPI + python-jose + pytest (portal backend).

**Spec:** `docs/superpowers/plans/../specs/2026-08-31-unificacion-erp-etapa1-design.md` (repo del portal backend). ADR: `docs/adr/0001-horas-dueno-de-identidad-y-app-unificada.md`.

## Global Constraints

- Repos y ramas (crear cada rama desde su `main`/rama principal): Horas Backend `C:\Users\Administrador\Desktop\SE Gero\Aplicaciones Web\Formulario_Horas\Backend`, Horas Frontend `...\Formulario_Horas\Frontend`, Portal backend `...\PortalCertificaciones\PortalCertificaciones_backend` — rama `feat/erp-certificaciones-etapa1` en los tres.
- TDD en cada task; suites: Horas BE `npm test` (Jest), Horas FE `npm test` (Vitest), Portal `python -m pytest -q` (hoy 50 passed).
- La BD del portal es la misma en dev y prod: la etapa es solo lectura sobre ella — ningún test escribe en BD real (fakes/mocks como en `tests/test_resolver_contrato.py` del portal).
- Los códigos K coinciden textualmente entre sistemas (`Contrato.codigo` ≡ `dim_contrato.codigo_k`); todo join es por ese string en mayúsculas.
- Claim del módulo en el JWT de Horas: `cert: { nivel: "admin"|"carga"|"lectura", ks: string[], inc: boolean } | null` — nombre y forma exactos, los consumen Tasks 3-7.
- Mapeo de nivel→rol del portal (Task 5): `admin`→`admin`, `lectura`→`gerente`, `carga`→`jefe`.
- Secret compartido: la env var `JWT_SECRET` de Horas; en el portal se llama `HORAS_JWT_SECRET` (mismo valor, se setea en `.env`). HS256 en ambos.
- Rulings de planificación: (1) endpoint dedicado `GET /certificaciones/incidencia-mo` en Horas — no se reusa `/liquidacion/analisis` porque está `@Roles('Admin','Liquidador')`; (2) el bucket "Sin contrato asignable" solo se devuelve a `admin`/`lectura`; (3) umbral del semáforo = constante `UMBRAL_INCIDENCIA_PCT = 30` en `features/certificaciones/config.ts`, editable en código — la UI de configuración por admin queda para una etapa posterior.
- Textos de UI y mensajes en español (voseo). Commits con sufijo `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Nada se deploya: el plan termina con las tres ramas pusheadas; PR y deploy los decide el usuario después.

---

### Task 1: Horas BE — modelos Prisma de acceso al módulo + migración

**Files:**
- Modify: `Formulario_Horas\Backend\prisma\schema.prisma`
- Create: migración generada por Prisma (`prisma/migrations/*_certificaciones_accesos/`)

**Interfaces:**
- Produces: modelos `CertificacionAcceso` (tabla `sth_certificaciones_accesos`; campos `cuil String @id`, `nivel String` con valores `admin|carga|lectura`, `verIncidencia Boolean @default(false)`, relación 1:1 a `Usuario`) y `CertificacionContratoHabilitado` (tabla `sth_certificaciones_contratos`; `cuil String` + `contratoId Int`, PK compuesta, FKs a `Usuario` y `Contrato`). Tasks 2-4 consumen ambos por esos nombres exactos.

- [ ] **Step 1: Agregar los modelos al schema**

En `prisma/schema.prisma`, junto a los modelos existentes (`Usuario`, `Contrato`, `ContratoHabilitado` como referencia de estilo):

```prisma
model CertificacionAcceso {
  cuil          String  @id @db.Char(13)
  nivel         String  // admin | carga | lectura
  verIncidencia Boolean @default(false)
  usuario       Usuario @relation(fields: [cuil], references: [cuil])

  @@map("sth_certificaciones_accesos")
}

model CertificacionContratoHabilitado {
  cuil       String   @db.Char(13)
  contratoId Int
  usuario    Usuario  @relation(fields: [cuil], references: [cuil])
  contrato   Contrato @relation(fields: [contratoId], references: [id])

  @@id([cuil, contratoId])
  @@map("sth_certificaciones_contratos")
}
```

Agregar los back-relations que Prisma exija en `Usuario` (`certificacionAcceso CertificacionAcceso?`, `certificacionContratos CertificacionContratoHabilitado[]`) y en `Contrato` (`certificacionUsuarios CertificacionContratoHabilitado[]`). Si el campo PK de `Contrato` no se llama `id`, usar el nombre real del schema.

- [ ] **Step 2: Generar y aplicar la migración**

Run: `npx prisma migrate dev --name certificaciones-accesos` (en la carpeta Backend, contra la BD de desarrollo que use el `.env` local — NO contra producción).
Expected: migración creada y `prisma generate` regenerado sin errores.

- [ ] **Step 3: Verificar que el proyecto compila y la suite sigue verde**

Run: `npm run build && npm test`
Expected: build OK, tests existentes sin regresiones.

- [ ] **Step 4: Commit**

```bash
git add prisma/
git commit -m "feat(certificaciones): modelos de acceso al modulo (nivel, contratos K, flag incidencia)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Horas BE — módulo `certificaciones`: CRUD de accesos (solo Admin)

**Files:**
- Create: `src/certificaciones/certificaciones.module.ts`, `src/certificaciones/accesos.service.ts`, `src/certificaciones/certificaciones.controller.ts`, `src/certificaciones/dto/upsert-acceso.dto.ts`
- Test: `src/certificaciones/accesos.service.spec.ts`
- Modify: `src/app.module.ts` (registrar `CertificacionesModule`)

**Interfaces:**
- Consumes: modelos de Task 1; `PrismaService`; guards existentes `JwtAuthGuard`, `RolesGuard`, decorator `@Roles('Admin')`.
- Produces: `AccesosService.obtenerAcceso(cuil): Promise<{ nivel: string; ks: string[]; inc: boolean } | null>` (la consumen Tasks 3 y 4); endpoints `GET /certificaciones/accesos` (lista), `PUT /certificaciones/accesos/:cuil` (upsert), `DELETE /certificaciones/accesos/:cuil`.

- [ ] **Step 1: Test del service (mock de Prisma, patrón de los spec existentes)**

`src/certificaciones/accesos.service.spec.ts`:

```ts
import { AccesosService } from './accesos.service';

describe('AccesosService.obtenerAcceso', () => {
  const prisma = {
    certificacionAcceso: { findUnique: jest.fn() },
    certificacionContratoHabilitado: { findMany: jest.fn() },
  } as any;
  const service = new AccesosService(prisma);

  it('sin fila de acceso devuelve null', async () => {
    prisma.certificacionAcceso.findUnique.mockResolvedValue(null);
    expect(await service.obtenerAcceso('20-11111111-1')).toBeNull();
  });

  it('nivel carga devuelve los K habilitados y el flag', async () => {
    prisma.certificacionAcceso.findUnique.mockResolvedValue({
      cuil: '20-1', nivel: 'carga', verIncidencia: true,
    });
    prisma.certificacionContratoHabilitado.findMany.mockResolvedValue([
      { contrato: { codigo: 'K6' } }, { contrato: { codigo: 'K11' } },
    ]);
    expect(await service.obtenerAcceso('20-1')).toEqual({
      nivel: 'carga', ks: ['K6', 'K11'], inc: true,
    });
  });

  it('admin y lectura devuelven ks vacio (ven todo, no se enumera)', async () => {
    prisma.certificacionAcceso.findUnique.mockResolvedValue({
      cuil: '20-1', nivel: 'lectura', verIncidencia: false,
    });
    expect(await service.obtenerAcceso('20-1')).toEqual({
      nivel: 'lectura', ks: [], inc: false,
    });
  });
});
```

- [ ] **Step 2: Correr y ver fallar** — Run: `npm test -- accesos.service` · Expected: FAIL (módulo inexistente).

- [ ] **Step 3: Implementar service, DTO, controller y module**

`accesos.service.ts` (la forma del claim es contrato del plan — Global Constraints):

```ts
import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

export interface CertClaim { nivel: string; ks: string[]; inc: boolean }

@Injectable()
export class AccesosService {
  constructor(private readonly prisma: PrismaService) {}

  async obtenerAcceso(cuil: string): Promise<CertClaim | null> {
    const acceso = await this.prisma.certificacionAcceso.findUnique({ where: { cuil } });
    if (!acceso) return null;
    let ks: string[] = [];
    if (acceso.nivel === 'carga') {
      const filas = await this.prisma.certificacionContratoHabilitado.findMany({
        where: { cuil }, include: { contrato: true },
      });
      ks = filas.map((f) => f.contrato.codigo);
    }
    return { nivel: acceso.nivel, ks, inc: acceso.verIncidencia };
  }

  listar() {
    return this.prisma.certificacionAcceso.findMany({
      include: { usuario: { select: { email: true } } },
    });
  }

  async upsert(cuil: string, dto: { nivel: string; verIncidencia: boolean; contratoIds: number[] }) {
    await this.prisma.$transaction([
      this.prisma.certificacionAcceso.upsert({
        where: { cuil },
        update: { nivel: dto.nivel, verIncidencia: dto.verIncidencia },
        create: { cuil, nivel: dto.nivel, verIncidencia: dto.verIncidencia },
      }),
      this.prisma.certificacionContratoHabilitado.deleteMany({ where: { cuil } }),
      this.prisma.certificacionContratoHabilitado.createMany({
        data: dto.contratoIds.map((contratoId) => ({ cuil, contratoId })),
      }),
    ]);
  }

  async eliminar(cuil: string) {
    await this.prisma.$transaction([
      this.prisma.certificacionContratoHabilitado.deleteMany({ where: { cuil } }),
      this.prisma.certificacionAcceso.delete({ where: { cuil } }),
    ]);
  }
}
```

`dto/upsert-acceso.dto.ts` con `class-validator` (patrón de los DTO existentes): `nivel` `@IsIn(['admin','carga','lectura'])`, `verIncidencia` `@IsBoolean()`, `contratoIds` `@IsArray()` de `@IsInt()`. `certificaciones.controller.ts` con `@Controller('certificaciones')`, `@UseGuards(JwtAuthGuard, RolesGuard)` y `@Roles('Admin')` en las tres rutas de accesos. Registrar `CertificacionesModule` en `app.module.ts`.

- [ ] **Step 4: Verde + suite completa** — Run: `npm test` · Expected: PASS.
- [ ] **Step 5: Commit** — `git add src/certificaciones src/app.module.ts` y mensaje `feat(certificaciones): CRUD de accesos al modulo (solo Admin)` + sufijo.

---

### Task 3: Horas BE — claim `cert` en el JWT y en el perfil

**Files:**
- Modify: `src/auth/auth.service.ts` (payload del login), `src/auth/auth.module.ts` (importar `CertificacionesModule` o proveer `AccesosService`), y el endpoint de perfil que consume `fetchPerfil()` del frontend (localizarlo: es la ruta que llama `src/lib/auth/session.tsx` del Frontend — típicamente `GET /auth/perfil` en `auth.controller.ts`).
- Test: `src/auth/auth.service.spec.ts` (ampliar el existente).

**Interfaces:**
- Consumes: `AccesosService.obtenerAcceso(cuil)` de Task 2.
- Produces: el JWT firmado pasa de `{ cuil, email, rol }` a `{ cuil, email, rol, cert }` con `cert: CertClaim | null`; el perfil devuelve el mismo campo `cert`. Tasks 4-7 dependen de ese nombre.

- [ ] **Step 1: Test** — ampliar `auth.service.spec.ts`: mockear `AccesosService` para que devuelva `{ nivel: 'carga', ks: ['K6'], inc: false }` y assertar que el objeto pasado a `jwtService.sign` incluye `cert` con ese valor; segundo caso: `obtenerAcceso` → `null` ⇒ `cert: null`.
- [ ] **Step 2: Ver fallar** — Run: `npm test -- auth.service` · Expected: FAIL.
- [ ] **Step 3: Implementar** — en `auth.service.ts`, donde hoy se arma `{ cuil, email, rol: usuario.rol.nombre }`, inyectar `AccesosService` y armar:

```ts
const cert = await this.accesosService.obtenerAcceso(usuario.cuil);
const payload = { cuil: usuario.cuil, email: usuario.email, rol: usuario.rol.nombre, cert };
```

Mismo agregado en la respuesta del endpoint de perfil (el objeto que hidrata `useSession().perfil`). `JwtStrategy.validate` debe devolver también `cert` para que llegue a `req.user`.

- [ ] **Step 4: Verde + suite** — `npm test` PASS.
- [ ] **Step 5: Commit** — `feat(certificaciones): claim cert en JWT y perfil` + sufijo.

---

### Task 4: Horas BE — endpoint `GET /certificaciones/incidencia-mo`

**Files:**
- Create: `src/certificaciones/incidencia.service.ts`
- Modify: `src/certificaciones/certificaciones.controller.ts`, `src/certificaciones/certificaciones.module.ts`
- Test: `src/certificaciones/incidencia.service.spec.ts`

**Interfaces:**
- Consumes: `AnalisisService` de `src/liquidacion/analisis.service.ts` (método que resuelve `?anio&mes&quincena` y devuelve `contratos: [{ contratoId, codigo, nombre, monto, horas, pctDelTotal }]` con bucket `codigo: 'Sin contrato asignable'`); `req.user.cert` de Task 3.
- Produces: `GET /certificaciones/incidencia-mo?anio=2026&mes=8` → `{ contratos: [{ codigo: string, montoMo: number }], sinAsignar: number | null }`. Reglas: suma quincena 1 + quincena 2 por código; `carga` recibe solo sus `ks` y `sinAsignar: null`; `admin`/`lectura` reciben todos + `sinAsignar` (monto del bucket); `carga` sin flag `inc`, o usuario sin `cert`, reciben **403**. Task 6/7 consumen esta forma exacta.

- [ ] **Step 1: Test** — `incidencia.service.spec.ts` con `AnalisisService` mockeado:

```ts
const analisisMock = { analisis: jest.fn() } as any; // usar el nombre real del método al implementar
// quincena 1 y 2 devuelven K6=100 y 50, K9=30 y 0, sin-asignar=10 y 5
```

Casos: (1) suma por código: K6→150, K9→30; (2) `cert = { nivel:'carga', ks:['K6'], inc:true }` ⇒ solo K6, `sinAsignar: null`; (3) `nivel:'admin'` ⇒ todos + `sinAsignar: 15`; (4) `carga` con `inc:false` ⇒ lanza `ForbiddenException`; (5) `cert: null` ⇒ `ForbiddenException`.

- [ ] **Step 2: Ver fallar** — `npm test -- incidencia` FAIL.
- [ ] **Step 3: Implementar** — service que llama al método real de `AnalisisService` para `quincena: 1` y `quincena: 2` (usar la firma real; si el método exige quincena, son dos llamadas), acumula `monto` por `codigo`, separa el bucket cuyo `contratoId` es `null`, y aplica las reglas de visibilidad del claim. Controller: `@Get('incidencia-mo')` bajo `JwtAuthGuard` (sin `@Roles` — la autorización es por claim `cert`, dentro del service).
- [ ] **Step 4: Verde + suite** — `npm test` PASS.
- [ ] **Step 5: Commit** — `feat(certificaciones): endpoint incidencia-mo (suma quincenas, visibilidad por acceso)` + sufijo.

---

### Task 5: Portal BE — aceptar el JWT de Horas

**Files:**
- Modify: `app/config.py` (nueva setting `horas_jwt_secret: str = ""`), `app/services/auth.py`
- Test: `tests/test_auth_horas.py` (nuevo)

**Interfaces:**
- Consumes: forma del token de Horas (claims `cuil`, `email`, `rol`, `cert` — Global Constraints).
- Produces: `get_current_user` acepta AMBOS tokens. Para el de Horas devuelve un `PrincipalHoras` con la interfaz que los endpoints de lectura usan: `.rol` (mapeo `admin→admin`, `lectura→gerente`, `carga→jefe`), `.contratos_list` (los `ks`), `.nombre` (email), `.id = 0`, `.ver_incidencia` (bool). Un token de Horas **sin** claim `cert` (o `cert: null`) ⇒ 403 "Sin acceso al módulo Certificaciones". Los tokens legacy del portal siguen funcionando igual.

- [ ] **Step 1: Tests** — `tests/test_auth_horas.py` generando tokens reales con `jose.jwt.encode` y un secret de prueba (monkeypatch de settings):

```python
from jose import jwt
import pytest
from fastapi import HTTPException
from app.services import auth as auth_mod

SECRET_HORAS = "secret-horas-test"

def token_horas(cert):
    return jwt.encode(
        {"cuil": "20-1", "email": "jefe@serytec.com", "rol": "JefeContrato", "cert": cert},
        SECRET_HORAS, algorithm="HS256",
    )

@pytest.fixture(autouse=True)
def _settings(monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "horas_jwt_secret", SECRET_HORAS, raising=False)

def test_token_horas_nivel_carga_mapea_a_jefe():
    p = auth_mod.principal_desde_token_horas(
        jwt.decode(token_horas({"nivel": "carga", "ks": ["K6"], "inc": True}),
                   SECRET_HORAS, algorithms=["HS256"]))
    assert p.rol == "jefe" and p.contratos_list == ["K6"] and p.ver_incidencia is True

def test_nivel_lectura_mapea_a_gerente_y_admin_a_admin():
    g = auth_mod.principal_desde_token_horas({"cuil": "1", "email": "g@s", "rol": "Admin",
                                              "cert": {"nivel": "lectura", "ks": [], "inc": False}})
    a = auth_mod.principal_desde_token_horas({"cuil": "1", "email": "a@s", "rol": "Admin",
                                              "cert": {"nivel": "admin", "ks": [], "inc": True}})
    assert g.rol == "gerente" and a.rol == "admin"

def test_sin_claim_cert_es_403():
    with pytest.raises(HTTPException) as e:
        auth_mod.principal_desde_token_horas({"cuil": "1", "email": "x@s", "rol": "Supervisor", "cert": None})
    assert e.value.status_code == 403

def test_decode_any_prueba_portal_y_luego_horas():
    payload = auth_mod.decode_any_token(token_horas({"nivel": "admin", "ks": [], "inc": True}))
    assert payload["cuil"] == "20-1"
```

- [ ] **Step 2: Ver fallar** — `python -m pytest tests/test_auth_horas.py -v` FAIL (funciones inexistentes).
- [ ] **Step 3: Implementar** en `app/services/auth.py`:

```python
from dataclasses import dataclass, field

@dataclass
class PrincipalHoras:
    """Usuario autenticado con token de Horas — solo lectura en Etapa 1."""
    nombre: str
    rol: str                      # admin | gerente | jefe (ya mapeado)
    contratos_list: list = field(default_factory=list)
    ver_incidencia: bool = False
    id: int = 0                   # sin fila en la BD del portal

_NIVEL_A_ROL = {"admin": "admin", "lectura": "gerente", "carga": "jefe"}

def principal_desde_token_horas(payload: dict) -> PrincipalHoras:
    cert = payload.get("cert")
    if not cert or cert.get("nivel") not in _NIVEL_A_ROL:
        raise HTTPException(status_code=403, detail="Sin acceso al módulo Certificaciones")
    return PrincipalHoras(
        nombre=payload.get("email", ""),
        rol=_NIVEL_A_ROL[cert["nivel"]],
        contratos_list=[k.upper() for k in cert.get("ks", [])],
        ver_incidencia=bool(cert.get("inc")),
    )

def decode_any_token(token: str) -> dict:
    """Prueba primero el secret del portal (legacy), después el de Horas."""
    try:
        return jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    except JWTError:
        pass
    try:
        return jwt.decode(token, settings.horas_jwt_secret, algorithms=["HS256"])
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido o expirado",
                            headers={"WWW-Authenticate": "Bearer"})
```

y en `get_current_user`, reemplazar `decode_token` por `decode_any_token`; si el payload trae `"cuil"` es de Horas ⇒ `return principal_desde_token_horas(payload)`; si no, sigue el camino legacy actual (lookup por `sub` en la BD). En `app/config.py` agregar `horas_jwt_secret: str = ""` al `Settings`.

- [ ] **Step 4: Suite completa** — `python -m pytest -q` PASS (50 + nuevos).
- [ ] **Step 5: Commit** — `feat: aceptar JWT de Horas (login unificado, claims cert)` + sufijo.

---

### Task 6: Horas FE — cliente API del portal, nav y página Resumen

**Files:**
- Create: `src/lib/api/certificaciones.ts`, `src/features/certificaciones/config.ts`, `src/features/certificaciones/resumen/*.tsx`, `src/app/(protected)/certificaciones/page.tsx`
- Modify: `src/components/layout/nav.ts` (ítem + excepción por claim `cert`)
- Test: `src/lib/api/certificaciones.test.ts`, `src/features/certificaciones/resumen/incidencia.test.ts`

**Interfaces:**
- Consumes: `perfil.cert` (Task 3), `GET /certificaciones/incidencia-mo` (Task 4, vía axios `api` existente), y del FastAPI: `GET /certificaciones/resumen`, `GET /analytics/estado-cargas`, `GET /analytics/presupuesto` (existentes). Cliente nuevo: instancia axios `apiCert` con `baseURL: process.env.NEXT_PUBLIC_CERT_API_URL ?? 'http://localhost:8000'` y el mismo interceptor Bearer (`getToken()` de `src/lib/api/token.ts`).
- Produces: función pura `calcularIncidencia(certificadoPorK: Record<string, number>, moPorK: Record<string, number>): { codigo, certificado, mo, pct }[]` y `semaforo(pct: number): 'ok' | 'alerta' | 'excedido'` — las reutiliza Task 7 si hace falta.

- [ ] **Step 1: Tests de la lógica pura** (`incidencia.test.ts`, Vitest):

```ts
import { calcularIncidencia, semaforo } from './incidencia';

it('cruza por codigo K y calcula pct', () => {
  const r = calcularIncidencia({ K6: 40_000_000 }, { K6: 12_000_000 });
  expect(r[0]).toMatchObject({ codigo: 'K6', pct: 30 });
});
it('K certificado sin MO imputada muestra pct 0 y mo 0', () => {
  expect(calcularIncidencia({ K9: 10 }, {})[0]).toMatchObject({ mo: 0, pct: 0 });
});
it('K con MO pero sin certificado no divide por cero', () => {
  expect(calcularIncidencia({}, { K9: 5 })[0].pct).toBeNull();
});
it('semaforo usa el umbral configurado (30)', () => {
  expect(semaforo(29)).toBe('ok');
  expect(semaforo(31)).toBe('alerta');
  expect(semaforo(50)).toBe('excedido'); // umbral * 1.5
});
```

- [ ] **Step 2: Ver fallar** — `npm test -- incidencia` FAIL.
- [ ] **Step 3: Implementar** — `config.ts`: `export const UMBRAL_INCIDENCIA_PCT = 30; // valor inicial acordado como editable en código; UI de configuración por admin: etapa posterior`. `incidencia.ts` con las dos funciones (`pct = mo / certificado * 100` redondeado a 1 decimal; `null` si certificado 0; `excedido` desde `UMBRAL * 1.5`). `lib/api/certificaciones.ts`: `apiCert` + hooks `useResumenCert(periodo)`, `useEstadoCargas()`, `usePresupuesto()`, `useIncidenciaMo(anio, mes)` con `useQuery` (patrón de `lib/api/liquidacion.ts`).
- [ ] **Step 4: Página Resumen** — `src/app/(protected)/certificaciones/page.tsx` (`'use client'`): cards de KPIs (total certificado del período, líneas, contratos que certificaron vs faltantes desde estado-cargas), tabla de incidencia por K con badge de semáforo (verde `ok` / `--warn` `alerta` / `--danger` `excedido`), fila "Sin contrato asignable" visible solo si el endpoint la devuelve (`sinAsignar !== null`), y card de presupuesto (solo si `usePresupuesto` no da 403). La sección de incidencia solo se renderiza si `perfil.cert.inc || perfil.cert.nivel !== 'carga'`. En `nav.ts`: ítem `{ label: 'Certificaciones', href: '/certificaciones', roles: ['Admin'] }` + excepción en `navForRole` (patrón de las existentes): incluir el ítem si `perfil.cert != null`.
- [ ] **Step 5: Suite + smoke visual** — `npm test` PASS; `npm run dev` y abrir `/certificaciones` con un usuario con acceso (requiere Tasks 1-4 corriendo en el backend local de Horas y el FastAPI local en :8000).
- [ ] **Step 6: Commit** — `feat(certificaciones): seccion en la app, cliente API del portal y pagina Resumen con incidencia MO` + sufijo.

---

### Task 7: Horas FE — página Analytics (4 secciones, recharts)

**Files:**
- Create: `src/app/(protected)/certificaciones/analytics/page.tsx`, `src/features/certificaciones/analytics/{evolucion-chart,por-contrato-chart,por-provincia-chart,top-items,estado-operativo}.tsx`, `src/features/certificaciones/analytics/colores.ts`
- Modify: `src/lib/api/certificaciones.ts` (hooks de los endpoints de analytics), `src/components/layout/nav.ts` si Analytics va como sub-ítem
- Test: `src/features/certificaciones/analytics/analytics-page.test.tsx`

**Interfaces:**
- Consumes: endpoints FastAPI existentes `GET /analytics/evolucion-mensual`, `/analytics/por-contrato-mes`, `/analytics/por-provincia`, `/analytics/top-items`, `/analytics/interanual`, `/analytics/estado-cargas` (todos aceptan `contratos[]`, `provincias[]`, `tipo`, `desde`, `hasta`); componente `barra-filtros.tsx` existente; hooks de `apiCert` (Task 6).
- Produces: nada para otras tasks (hoja del árbol).

- [ ] **Step 1: Hooks + test de página** — agregar a `lib/api/certificaciones.ts` los hooks `useEvolucionMensual(filtros)`, `usePorContratoMes(filtros)`, `usePorProvincia(filtros)`, `useTopItems(filtros)`, `useInteranual(filtros)`, tipando `filtros` como `{ contratos?: string[]; provincias?: string[]; tipo?: 'OPEX'|'CAPEX'; desde?: string; hasta?: string }`. Test de página (patrón `analisis-page.test.tsx`): mockear los hooks y assertar que las 4 secciones se renderizan con sus `aria-label` ("Evolución mensual", "Por contrato", "Desagregado", "Operativo").
- [ ] **Step 2: Ver fallar** — `npm test -- analytics-page` FAIL.
- [ ] **Step 3: Implementar la página** — layout en 4 secciones (Resumen del período / Tendencia / Desagregado / Operativo, mismo orden que el portal actual), `barra-filtros` arriba, cada chart en `features/certificaciones/analytics/` como client component importado con `next/dynamic({ ssr: false })` (patrón de `/liquidacion/analisis`). Charts recharts con `ResponsiveContainer`, ejes con `var(--color-line)` / `var(--color-slate)` y colores por serie en `colores.ts` (constantes hex propias, patrón `chart-colors.ts` de control-general — coherentes con la marca dorada). Evolución mensual: línea/área monto + PGN; por contrato: barras apiladas por mes; por provincia: barras horizontales; top items: tabla con mini-barra; operativo: matriz contrato×período desde estado-cargas (celdas ok/falta).
- [ ] **Step 4: Suite + smoke visual** — `npm test` PASS; revisar en `npm run dev` con datos reales que los gráficos cuenten una historia clara (pedido explícito del usuario: "gráficos que signifiquen más" — si un gráfico del portal viejo no aporta, reemplazarlo por uno mejor y anotarlo en el reporte para revisión).
- [ ] **Step 5: Commit** — `feat(certificaciones): pagina Analytics con recharts y barra de filtros` + sufijo.

---

### Task 8: Integración — CORS/proxy, envs y documentación

**Files:**
- Modify (portal): `app/main.py` (CORS), `CONTEXTO_SISTEMA.md` (§16 + §2), `docs/arquitectura-produccion-vps.md` (mapa: app unificada consume FastAPI)
- Modify (Horas FE): `.env.example` (`NEXT_PUBLIC_CERT_API_URL`)
- Modify (Horas BE): `.env.example` (nota: `JWT_SECRET` ahora también lo consume el portal como `HORAS_JWT_SECRET`)

**Interfaces:**
- Consumes: todo lo anterior verde.
- Produces: los tres repos pusheados en `feat/erp-certificaciones-etapa1`; documentación al día. Nada deployado.

- [ ] **Step 1: CORS del FastAPI** — en `app/main.py`, agregar a los orígenes permitidos `https://misregistros.serytec.com.ar` y `http://localhost:3000` (dev de Next). Nota para producción (documentar, no ejecutar): la alternativa same-origin es un `location /certapi/` en el server block de misregistros proxyando a `127.0.0.1:8000` — decisión final al deployar.
- [ ] **Step 2: Envs** — `.env.example` del Frontend: `NEXT_PUBLIC_CERT_API_URL=http://localhost:8000`; documentar en el portal que `.env` necesita `HORAS_JWT_SECRET` (mismo valor que `JWT_SECRET` de Horas — se setea a mano en el VPS, nunca se commitea).
- [ ] **Step 3: Suites completas en los tres repos** — portal `python -m pytest -q`, Horas BE `npm test`, Horas FE `npm test`. Expected: todo verde.
- [ ] **Step 4: Docs** — entrada nueva en §16 de `CONTEXTO_SISTEMA.md` (qué se construyó, decisiones, estado: pendiente prueba del usuario → PRs → deploy); actualizar §2 (la app unificada como frontend futuro) y el mapa del doc de arquitectura.
- [ ] **Step 5: Commit + push de las tres ramas** — commits por repo con sufijo; `git push -u origin feat/erp-certificaciones-etapa1` en los tres. PRs y deploy quedan para el usuario.
