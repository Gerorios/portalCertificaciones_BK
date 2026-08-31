# El sistema de Horas es el dueño de la identidad; el portal se convierte en un módulo de su app

Decidido 2026-08-31 (sesión de grilling). El destino de los dos sistemas (Formulario de Horas y Portal de Certificaciones) es **una sola aplicación**: el frontend Next.js de Horas absorbe Certificaciones como módulo, migrando pantalla por pantalla (solo-lectura primero, la carga al final) mientras el portal vanilla sigue en producción. Hay **un solo padrón de usuarios y un solo login — el de Horas**: el backend FastAPI deja de emitir JWT propios y pasa a validar el JWT de Horas; el acceso al módulo Certificaciones es una concesión explícita del admin (niveles admin/carga/lectura + flag de incidencia MO), nunca automática por rol de Horas.

## Considered Options

- Federar los dos padrones de usuarios (rechazado: dos fuentes de verdad, sincronización eterna).
- Servicio de identidad neutral aparte (rechazado: infraestructura de más para una empresa interna).
- Re-skin del portal vanilla con el look de Horas (rechazado: trabajo doblemente pagado — todo lo tocado en vanilla se rehace en React al migrar).

## Consequences

- El backend FastAPI sigue existiendo como servicio (la API no se migra a Node); el frontend unificado consume ambos backends.
- La razón de negocio de la unificación es la **incidencia de MO** por contrato K: solo existe cruzando la liquidación de Horas (costo por K) con lo certificado (facturado por K). Los códigos K coinciden textualmente en ambos sistemas (`Contrato.codigo` en Horas ≡ `dim_contrato.codigo_k` en el portal) — esa igualdad es ahora un contrato entre sistemas que no debe romperse.
