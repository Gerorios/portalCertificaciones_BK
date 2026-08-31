# Migración del PortalCertificaciones al VPS propio (backend dockerizado) — Plan de implementación

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mover el PortalCertificaciones desde Render (free, duerme) + Netlify al VPS propio de Serytec (179.198.99.30), con el backend FastAPI corriendo en Docker y el frontend estático servido por Nginx bajo `certificaciones.serytec.com.ar`.

**Architecture:** El VPS ya corre Formulario_Horas (Node bajo PM2 + Nginx como reverse proxy). Se agrega **un solo contenedor Docker** para el backend Python (la app pide Python 3.11.9 y el VPS trae 3.12 — el contenedor fija la versión y las dependencias de sistema como `poppler-utils`). El frontend es HTML/JS puro sin build: Nginx lo sirve directo como archivos estáticos. Las apps de Horas NO se tocan. La BD MySQL es externa y no se migra.

**Tech Stack:** FastAPI + Python 3.11 (imagen `python:3.11-slim`), Docker + docker compose v2, Nginx, Certbot (Let's Encrypt), MySQL externa (pymysql).

**Spec:** `docs/arquitectura-produccion-vps.md` (se crea en la Task 1 de este plan; contiene el diseño completo de la convivencia Docker/PM2/Nginx y el porqué de cada decisión).

## Global Constraints

- **Regla de oro del VPS:** NUNCA tocar el VPS (instalar, reiniciar, editar nginx, `docker`/`pm2` que muten estado) sin OK explícito del usuario en esa sesión. Avisar antes de cualquier corte, por breve que sea.
- Las apps existentes `forms-horas-back` (:3001) y `forms-horas-front` (:3000) bajo PM2 de root **no se modifican en ninguna task**.
- Puerto del backend del Portal en el VPS: **8000**, publicado SOLO en `127.0.0.1` (nunca expuesto al exterior; ufw ya limita a 22/80/443).
- Python de la app: **3.11.9** (`.python-version`) — la imagen Docker es `python:3.11-slim`, no otra.
- Rama de trabajo de ambos repos: `desarollo` (así está escrita, con una sola "r" — no "corregirla").
- Credenciales: el `.env` NUNCA se commitea; en el VPS vive en `/var/www/PortalCertificaciones_back/.env`. Los valores actuales están en el dashboard de Render (Environment) y en el `.env` local.
- SSH al VPS: `ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30` (comandos privilegiados con `sudo`).
- Repos GitHub: backend `https://github.com/Gerorios/portalCertificaciones_BK.git`, frontend `https://github.com/Gerorios/portalCertificaciones_FE.git`.
- No hay Docker en la máquina local de desarrollo: la imagen se construye y prueba **en el VPS** (Task 5).
- Render + Netlify + UptimeRobot quedan **encendidos como respaldo** hasta que el usuario decida darlos de baja (misma política que se usó con Vercel/Render en Formulario_Horas).

---

### Task 1: Documentar la arquitectura en el repo del Portal

**Files:**
- Create: `docs/arquitectura-produccion-vps.md`
- Modify: `CONTEXTO_SISTEMA.md` (secciones 2, 10 y 13)

**Interfaces:**
- Consumes: nada (task inicial).
- Produces: el documento de arquitectura que el resto de las tasks citan como spec.

- [ ] **Step 1: Crear `docs/arquitectura-produccion-vps.md`** con este contenido exacto:

````markdown
# Arquitectura de producción en el VPS propio (2026-08)

> Decisión y diseño de la migración desde Render (free) + Netlify al VPS de Serytec.
> Plan de ejecución: `docs/superpowers/plans/2026-08-13-migracion-vps-docker.md`.

## Por qué migrar

- **Render free duerme tras 15 min** → arranque en frío de 30-60 s; el UptimeRobot que
  pinguea `/health` cada 5 min es un parche, no una solución. Esta es la causa principal
  de la lentitud percibida.
- **El firewall de las PCs de Naturgy bloquea `onrender.com`** ("Web Page Blocked");
  con dominio propio (`certificaciones.serytec.com.ar`) el problema desaparece.
- El VPS ya existe (Hostinger KVM 2, 2 vCPU / 8 GB RAM / 96 GB) y corre Formulario_Horas
  usando ~11 % de RAM y 5 % de disco: hay capacidad de sobra.

## Por qué Docker, y por qué SOLO para el backend

El VPS es "territorio Node": Node 22 + PM2, y su Python de sistema es **3.12**
(Ubuntu 24.04). Esta app fija **Python 3.11.9** y necesita `poppler-utils` (PDFs).
Dockerizar el backend encapsula la versión de Python y las dependencias de sistema en
una imagen reproducible, sin ensuciar el host ni arriesgar a las apps de Horas.

- **Backend FastAPI → Docker** (imagen `python:3.11-slim` + poppler): un contenedor,
  puerto 8000 publicado solo en 127.0.0.1.
- **Frontend → SIN Docker**: es HTML/JS puro sin build; Nginx lo sirve directo desde
  una carpeta. Un contenedor acá sería complejidad gratuita.
- **Apps de Horas → siguen en PM2** tal como están: no se dockeriza lo que ya funciona.

Un contenedor, para Nginx, es solo un proceso que escucha en un puerto — igual que los
procesos PM2. Por eso ambos mundos conviven sin conflicto.

## Mapa del VPS después de la migración

```
VPS 179.198.99.30 (Ubuntu 24.04, Hostinger KVM 2)
│
├── Nginx (host)                              ← única puerta de entrada, 80/443 + SSL
│   ├── misregistros.serytec.com.ar/          → proxy → localhost:3000 (front Horas)
│   ├── misregistros.serytec.com.ar/api/      → proxy → localhost:3001 (back Horas)
│   ├── certificaciones.serytec.com.ar/       → estáticos /var/www/PortalCertificaciones_front
│   └── certificaciones.serytec.com.ar/api/   → proxy → localhost:8000 (back Portal)
│
├── PM2 de root (host)                        ← SIN CAMBIOS
│   ├── forms-horas-back   (Node, :3001)
│   └── forms-horas-front  (Node, :3000)
│
├── Docker
│   └── portal-certificaciones-back           (Python 3.11 + poppler + FastAPI, 127.0.0.1:8000)
│       repo: /var/www/PortalCertificaciones_back  (rama `desarollo`)
│       .env: /var/www/PortalCertificaciones_back/.env  (NO commiteado)
│
└── /var/www/PortalCertificaciones_front      (clon del repo FE, servido por Nginx)
```

## Operación diaria

| | Apps de Horas | Portal backend |
|---|---|---|
| Estado | `sudo pm2 ls` | `sudo docker compose ps` (desde el dir del repo) |
| Logs | `sudo pm2 logs forms-horas-back` | `sudo docker compose logs -f` |
| Reiniciar | `sudo pm2 restart ...` | `sudo docker compose restart` |
| Deploy | `git pull` + build + restart | `git pull` + `sudo docker compose up -d --build` |
| Reinicio del VPS | `pm2 startup` (ya configurado) | `restart: unless-stopped` en el compose |

Deploy del frontend: `git pull` en `/var/www/PortalCertificaciones_front` — nada más
(no hay build ni proceso).

## Decisiones tomadas

1. **Subdominio propio** `certificaciones.serytec.com.ar` (registro A → 179.198.99.30,
   se administra en Optimus Panel, igual que `misregistros`). SSL con Certbot.
2. **`/api/` con strip del prefijo** en Nginx (proxy a `127.0.0.1:8000/`): el backend
   no sabe que existe el prefijo; el frontend usa `${location.origin}/api`.
3. **`js/api.js` multi-entorno**: elige la URL de la API según el hostname
   (localhost → :8000 local; netlify.app → Render; cualquier otro → mismo origen + `/api`).
   Así Netlify sigue funcionando como respaldo sin tocar nada.
4. **`client_max_body_size 25m`** en el server block: los Excel/PDF de certificaciones
   superan el default de 1 MB de Nginx (lección aprendida del 413 en Horas).
5. **Render + Netlify + UptimeRobot quedan como respaldo** hasta decisión del usuario.
6. **La BD no se toca**: sigue siendo la MySQL externa compartida. Sí se aplican los
   índices pendientes de `fact_certificaciones` (estaban en el backlog).
7. **Rotación del `AZURE_CLIENT_SECRET`** (estaba expuesto, pendiente urgente del
   backlog) se hace durante la migración, al armar el `.env` definitivo.
````

- [ ] **Step 2: Actualizar `CONTEXTO_SISTEMA.md`**

En la **sección 2 (Stack tecnológico)**, reemplazar las filas de deploy de la tabla:

```markdown
| Deploy backend | VPS propio 179.198.99.30 — Docker (`python:3.11-slim`), ver `docs/arquitectura-produccion-vps.md`. Respaldo: Render.com free |
| Deploy frontend | VPS propio — estáticos por Nginx. Respaldo: Netlify |
```

y debajo de las URLs de producción agregar:

```markdown
**URL producción (VPS, principal):** `https://certificaciones.serytec.com.ar`
(las URLs de Render/Netlify quedan como respaldo)
```

En la **sección 10 (Deploy)**, agregar al principio:

```markdown
### VPS propio (principal desde 2026-08)

Ver `docs/arquitectura-produccion-vps.md` (mapa completo, operación diaria y decisiones)
y el plan `docs/superpowers/plans/2026-08-13-migracion-vps-docker.md`.
```

En la **sección 13 (Pendientes)**, marcar como resueltos (cuando el plan termine) los
ítems "Aplicar índices", "Dominio propio para el backend" y "Upgrade Render a plan
Starter" (este último tachado con nota "resuelto migrando al VPS, sin costo mensual").
En esta task solo agregar la referencia; el tildado final es parte de la Task 10.

- [ ] **Step 3: Commit**

```bash
git add docs/arquitectura-produccion-vps.md CONTEXTO_SISTEMA.md
git commit -m "docs: arquitectura de producción en VPS propio (backend dockerizado)"
```

---

### Task 2: Dockerfile, .dockerignore y docker-compose.yml en el repo backend

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `docker-compose.yml`
- Modify: `.gitignore` (crear si no existe; dejar de trackear `__pycache__`)

**Interfaces:**
- Consumes: `requirements.txt` y `app/` existentes (no se modifican).
- Produces: imagen que expone uvicorn en `0.0.0.0:8000` dentro del contenedor; servicio compose llamado `portal-back`, contenedor `portal-certificaciones-back`, publicado en `127.0.0.1:8000`. La Task 5 hace el build en el VPS y la Task 7 apunta Nginx a ese puerto.

- [ ] **Step 1: Crear `Dockerfile`**

```dockerfile
FROM python:3.11-slim

# poppler-utils: requerido por pdfplumber/pdf2image para parsear PDFs de certificaciones
RUN apt-get update \
    && apt-get install -y --no-install-recommends poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
```

- [ ] **Step 2: Crear `.dockerignore`**

```
__pycache__/
*.pyc
.env
.env.example
.git/
.claude/
tests/
docs/
*.md
crear_admin.py
run.sh
apt.txt
skills-lock.json
```

- [ ] **Step 3: Crear `docker-compose.yml`**

```yaml
services:
  portal-back:
    build: .
    container_name: portal-certificaciones-back
    restart: unless-stopped
    env_file: .env
    ports:
      - "127.0.0.1:8000:8000"
```

Nota: `env_file` inyecta el `.env` como variables de entorno — `pydantic-settings`
(`app/config.py`) y `os.environ` (`app/services/onedrive.py`) las leen igual que con
el archivo; no hace falta montar el `.env` como volumen.

- [ ] **Step 4: Sacar `__pycache__` del control de versiones**

Hoy el repo trackea `.pyc` (aparecen como modificados en `git status`). Crear/ampliar
`.gitignore` con:

```
__pycache__/
*.pyc
.env
```

y destrackear:

```bash
git rm -r --cached app/__pycache__ app/routers/__pycache__ app/services/__pycache__
```

- [ ] **Step 5: Verificación estática** (no hay Docker local; el build real es en Task 5)

```bash
docker --version 2>/dev/null || echo "sin docker local — build diferido a la Task 5 (VPS)"
python -c "import yaml,sys; yaml.safe_load(open('docker-compose.yml')); print('compose YAML OK')" 2>/dev/null || echo "verificar YAML a ojo si no hay pyyaml local"
```

Expected: mensaje de build diferido; YAML válido.

- [ ] **Step 6: Commit y push**

```bash
git add Dockerfile .dockerignore docker-compose.yml .gitignore
git commit -m "feat: dockerización del backend (python:3.11-slim + poppler) para deploy en VPS"
git push origin desarollo
```

---

### Task 3: `js/api.js` multi-entorno en el repo frontend

**Files:**
- Modify: `js/api.js:6` (repo `PortalCertificaciones_frontend`)

**Interfaces:**
- Consumes: nada.
- Produces: constante global `API` que el resto del frontend ya usa (sin cambios en los llamadores). Contrato: en el VPS la API se sirve en el mismo origen bajo `/api` (la Task 7 configura ese proxy en Nginx).

- [ ] **Step 1: Reemplazar la línea hardcodeada**

Reemplazar:

```js
const API = "https://portalcertificaciones-bk.onrender.com";  // cambiar por la URL de Render en producción
```

por:

```js
// URL de la API según dónde corre el frontend:
//  - dev local (Live Server): backend local en :8000
//  - Netlify (respaldo): backend de Render
//  - VPS propio: mismo origen bajo /api (Nginx hace el proxy y saca el prefijo)
const API =
  window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
    ? "http://localhost:8000"
    : window.location.hostname.endsWith("netlify.app")
      ? "https://portalcertificaciones-bk.onrender.com"
      : `${window.location.origin}/api`;
```

- [ ] **Step 2: Verificar que ningún otro archivo hardcodea la URL de Render**

```bash
grep -rn "onrender.com" --include="*.js" --include="*.html" .
```

Expected: única aparición en `js/api.js`. Si aparece en otro lado, aplicar el mismo patrón.

- [ ] **Step 3: Commit y push**

```bash
git add js/api.js
git commit -m "feat: API multi-entorno (local / Netlify respaldo / VPS mismo origen via /api)"
git push origin desarollo
```

Nota: si Netlify auto-deploya la rama `desarollo`, este push redeploya el respaldo —
es inocuo: la rama netlify.app sigue apuntando a Render.

---

### Task 4: Instalar Docker en el VPS ⚠️ requiere OK explícito del usuario

**Files:** ninguno local — solo el VPS.

**Interfaces:**
- Consumes: acceso SSH (Global Constraints).
- Produces: `docker` + `docker compose` v2 operativos para las Tasks 5+.

- [ ] **Step 1: Pedir OK al usuario** (instala paquetes en el VPS; sin corte de servicio).

- [ ] **Step 2: Instalar**

```bash
ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30 \
  "sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2"
```

- [ ] **Step 3: Verificar**

```bash
ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30 \
  "sudo docker run --rm hello-world && sudo docker compose version"
```

Expected: "Hello from Docker!" y versión de compose v2.x.

- [ ] **Step 4: Confirmar que las apps de Horas siguen online**

```bash
ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30 "sudo pm2 ls"
curl -s -o /dev/null -w "%{http_code}" https://misregistros.serytec.com.ar
```

Expected: ambos procesos `online`, front responde 200.

---

### Task 5: Clonar el backend en el VPS, armar `.env` y levantar el contenedor ⚠️ requiere OK

**Files (en el VPS):**
- Create: `/var/www/PortalCertificaciones_back` (clon del repo, rama `desarollo`)
- Create: `/var/www/PortalCertificaciones_back/.env`

**Interfaces:**
- Consumes: Dockerfile/compose de la Task 2 (ya pusheados); valores de env del dashboard de Render (Environment) o del `.env` local.
- Produces: API respondiendo en `127.0.0.1:8000` dentro del VPS; la Task 7 le apunta Nginx.

- [ ] **Step 1: Pedir OK al usuario** (no toca las apps de Horas; sin corte).

- [ ] **Step 2: Clonar**

```bash
ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30 \
  "cd /var/www && sudo git clone -b desarollo https://github.com/Gerorios/portalCertificaciones_BK.git PortalCertificaciones_back"
```

- [ ] **Step 3: Crear el `.env`** con TODAS las claves que hoy tiene Render (mismos valores; el `AZURE_CLIENT_SECRET` se rota en la Task 9):

```
DB_HOST=..., DB_PORT=..., DB_NAME=..., DB_USER=..., DB_PASSWORD=...
SECRET_KEY=...            # el MISMO de Render (no invalidar sesiones)
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=480
ALLOWED_ORIGINS=https://certificaciones.serytec.com.ar,https://portalcertificaciones.netlify.app
AZURE_TENANT_ID=..., AZURE_CLIENT_ID=..., AZURE_CLIENT_SECRET=...
ONEDRIVE_USER=administracion@serytecsas.onmicrosoft.com
OPENAI_API_KEY=...        # opcional, parser IA deprecado
```

Crearlo por SSH con heredoc o `sudo tee` (nunca commitearlo). Ojo con caracteres
especiales en contraseñas dentro del `.env` de compose: sin comillas, valor crudo.

- [ ] **Step 4: Build y arranque**

```bash
ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30 \
  "cd /var/www/PortalCertificaciones_back && sudo docker compose up -d --build"
```

- [ ] **Step 5: Verificar salud y conexión a BD**

```bash
ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30 \
  "curl -s http://127.0.0.1:8000/health && sudo docker compose -f /var/www/PortalCertificaciones_back/docker-compose.yml logs --tail 20"
```

Expected: `{"status":"ok","db":true}` y en logs "✓ Conexión a la base de datos OK".
Si `db:false`: revisar credenciales del `.env` y que el MySQL externo permita conexiones desde la IP del VPS (allowlist del hosting de la BD).

---

### Task 6: Registro DNS del subdominio (acción del usuario)

**Files:** ninguno.

**Interfaces:**
- Consumes: IP del VPS (179.198.99.30).
- Produces: `certificaciones.serytec.com.ar` resolviendo al VPS — requisito de Certbot en la Task 7.

- [ ] **Step 1: Pedir al usuario** que cree en Optimus Panel (donde administra `serytec.com.ar`, igual que hizo con `misregistros`) un registro **A**: `certificaciones` → `179.198.99.30`.

- [ ] **Step 2: Verificar propagación**

```bash
nslookup certificaciones.serytec.com.ar
```

Expected: respuesta con `179.198.99.30`. Si no resuelve, esperar propagación (minutos a horas) antes de la Task 7.

---

### Task 7: Frontend + Nginx + SSL en el VPS ⚠️ requiere OK (reload de Nginx, sin corte perceptible)

**Files (en el VPS):**
- Create: `/var/www/PortalCertificaciones_front` (clon del repo FE, rama `desarollo`)
- Create: `/etc/nginx/sites-available/certificaciones.serytec.com.ar` (+ symlink en `sites-enabled`)

**Interfaces:**
- Consumes: contenedor en `127.0.0.1:8000` (Task 5), DNS resuelto (Task 6), `api.js` multi-entorno (Task 3).
- Produces: `https://certificaciones.serytec.com.ar` operativo (frontend + `/api/`).

- [ ] **Step 1: Pedir OK al usuario** (agrega un site y recarga Nginx — `reload` no corta conexiones de Horas).

- [ ] **Step 2: Clonar el frontend**

```bash
ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30 \
  "cd /var/www && sudo git clone -b desarollo https://github.com/Gerorios/portalCertificaciones_FE.git PortalCertificaciones_front"
```

- [ ] **Step 3: Crear el server block** `/etc/nginx/sites-available/certificaciones.serytec.com.ar`:

```nginx
server {
    listen 80;
    server_name certificaciones.serytec.com.ar;

    root /var/www/PortalCertificaciones_front;
    index index.html;

    # Los Excel/PDF de certificaciones superan el default de 1MB (lección del 413 en Horas)
    client_max_body_size 25m;

    location /api/ {
        proxy_pass http://127.0.0.1:8000/;   # barra final: saca el prefijo /api
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        try_files $uri $uri/ =404;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/certificaciones.serytec.com.ar /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

- [ ] **Step 4: SSL con Certbot** (agrega `listen 443`, certificado y redirect http→https; Certbot edita el server block solo):

```bash
ssh -i ~/.ssh/forms_horas_vps2 coworker@179.198.99.30 \
  "sudo certbot --nginx -d certificaciones.serytec.com.ar --non-interactive --agree-tos -m reportelaasturiana@gmail.com --redirect"
```

- [ ] **Step 5: Verificar de punta a punta**

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://certificaciones.serytec.com.ar          # 200 (login)
curl -s https://certificaciones.serytec.com.ar/api/health                                 # {"status":"ok","db":true}
curl -s -o /dev/null -w "%{http_code}\n" https://misregistros.serytec.com.ar              # 200 (Horas intacto)
```

- [ ] **Step 6: Prueba funcional en browser** (usuario o Claude in Chrome): login con un usuario real en `https://certificaciones.serytec.com.ar`, ver dashboard y analytics con datos. Expected: sin errores CORS ni 502 en la consola.

---

### Task 8: Aplicar los índices pendientes en `fact_certificaciones`

**Files:**
- Create: `docs/sql/2026-08-13-indices-fact-certificaciones.sql` (repo backend)

**Interfaces:**
- Consumes: acceso a la BD MySQL del portal (credenciales del `.env`).
- Produces: analytics más rápido (los endpoints filtran por contrato/fecha/ítem/provincia).

- [ ] **Step 1: Verificar qué índices existen hoy** (para no duplicar):

```sql
SHOW INDEX FROM fact_certificaciones;
```

- [ ] **Step 2: Crear `docs/sql/2026-08-13-indices-fact-certificaciones.sql`** (omitir los que ya existan):

```sql
-- Índices pendientes de CONTEXTO_SISTEMA.md §3 — analytics filtra por estas columnas
ALTER TABLE fact_certificaciones
    ADD INDEX idx_contrato (id_contrato),
    ADD INDEX idx_fecha (fecha),
    ADD INDEX idx_item (id_item),
    ADD INDEX idx_provincia (id_provincia);
```

- [ ] **Step 3: Aplicar** contra la BD (cliente MySQL local o el que use el usuario para esa BD; la tabla tiene pocos MB, el ALTER tarda segundos).

- [ ] **Step 4: Verificar**

```sql
SHOW INDEX FROM fact_certificaciones;  -- deben aparecer los 4
```

y comprobar en el portal que Analytics carga notablemente más rápido.

- [ ] **Step 5: Commit**

```bash
git add docs/sql/2026-08-13-indices-fact-certificaciones.sql
git commit -m "perf: índices de fact_certificaciones para analytics (pendiente del backlog)"
git push origin desarollo
```

---

### Task 9: Rotar `AZURE_CLIENT_SECRET` (pendiente urgente del backlog) ⚠️ requiere acción del usuario + OK para restart

**Files (en el VPS):**
- Modify: `/var/www/PortalCertificaciones_back/.env`

**Interfaces:**
- Consumes: contenedor corriendo (Task 5); acceso del usuario al portal de Azure.
- Produces: secreto nuevo activo; subida a OneDrive funcionando con credencial no expuesta.

- [ ] **Step 1: El usuario genera el secreto nuevo** en portal.azure.com → App registrations → (app `1b3d7b6d-23c8-412b-b223-d5188e4df9c6`) → Certificates & secrets → New client secret. Copiar el **Value** (se muestra una sola vez).

- [ ] **Step 2: Actualizar el `.env` del VPS** (`AZURE_CLIENT_SECRET=<nuevo>`) y, previo OK, reiniciar:

```bash
sudo docker compose -f /var/www/PortalCertificaciones_back/docker-compose.yml up -d
# (up -d relee env_file al recrear; si no recrea: sudo docker compose ... up -d --force-recreate)
```

- [ ] **Step 3: Actualizar también en Render** (Environment → AZURE_CLIENT_SECRET) para que el respaldo siga sano, y **borrar el secreto viejo** en Azure.

- [ ] **Step 4: Verificar**: hacer una carga de certificación real (o de prueba) y confirmar que el archivo aparece en OneDrive (`Certificaciones/K.../<período>/`). Expected: subida OK sin errores en `docker compose logs`.

---

### Task 10: Cutover, monitoreo y cierre de documentación

**Files:**
- Modify: `CONTEXTO_SISTEMA.md` (secciones 2, 9, 12 y 13)

**Interfaces:**
- Consumes: todo lo anterior operativo.
- Produces: portal en producción en el VPS, docs al día, respaldos definidos.

- [ ] **Step 1: Smoke test completo en `https://certificaciones.serytec.com.ar`** (usuario o Claude in Chrome): login de los 3 roles (admin, jefe, gerente), carga de un archivo real con preview y confirmación, analytics con filtros, historial. Expected: todo funcional, sin lentitud de arranque en frío.

- [ ] **Step 2: Avisar a los usuarios del portal** el cambio de URL (Netlify sigue viva como respaldo, no urge).

- [ ] **Step 3: UptimeRobot**: repuntar el monitor a `https://certificaciones.serytec.com.ar/api/health` (ahora como monitoreo real de caídas, ya no como parche anti-sleep). El monitor viejo de Render puede quedar si se conserva el respaldo.

- [ ] **Step 4: Actualizar `CONTEXTO_SISTEMA.md`**: URLs de producción (sección 2), variables de entorno ahora en el `.env` del VPS (sección 9), fila nueva en "Problemas conocidos" (sección 12: "Lentitud por sleep de Render → resuelto migrando al VPS"), y tildar en pendientes (sección 13): índices aplicados, dominio propio hecho, upgrade de Render innecesario, secreto Azure rotado.

- [ ] **Step 5: Commit y push**

```bash
git add CONTEXTO_SISTEMA.md
git commit -m "docs: portal en producción en el VPS propio — cierre de la migración"
git push origin desarollo
```

- [ ] **Step 6: Decisión del usuario (registrarla, no ejecutarla):** ¿se dan de baja Render/Netlify o quedan como respaldo permanente? Mientras no decida, quedan encendidos.
