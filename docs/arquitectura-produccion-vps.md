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
