# Portolan como Iceberg REST Catalog Estático

**Propuesta:** Permitir publicar un Portolan como un Iceberg REST Catalog estático sobre object storage — sin servidor, queryable directamente desde DuckDB, y federable con otros Portolans en la misma sesión analítica.

---

## Contexto

Portolan ya distribuye sus contenidos como archivos estáticos: STAC para el catálogo, COG para raster, GeoParquet para vector. Toda la pila cloud-native que hemos adoptado encaja en el mismo patrón: subir archivos a object storage, sin servicios que mantener.

Lo que falta es un **índice queryable** que cumpla con un estándar que las herramientas analíticas modernas hablen de forma nativa. La buena noticia: **la spec Iceberg REST se puede materializar como un árbol de JSONs estáticos**. Eso cierra el círculo — Portolan publica catálogos cumpliendo Iceberg REST sin necesidad de levantar Polaris, Nessie, o cualquier otro servicio.

## Cómo funciona

El generador (ya implementado en la rama `add-sdi-experiment` del portolan original) emite un árbol `v1/` que respeta la spec REST de Iceberg:

```
v1/
├── config
└── catalog/namespaces/
    ├── ne_admin_0/
    │   └── tables/countries     (metadata JSON apuntando a Parquet existente)
    └── ne_admin_1/
        └── tables/provinces
```

Subes ese árbol a R2 (o S3, GCS, Azure — cualquiera que sirva archivos estáticos vía HTTP). DuckDB se conecta directamente al endpoint:

```sql
INSTALL iceberg; LOAD iceberg;
ATTACH '' AS portolan (
  TYPE ICEBERG,
  ENDPOINT 'https://catalog.example.com',
  AUTHORIZATION_TYPE 'none'
);
SELECT * FROM portolan.ne_admin_0.countries LIMIT 10;
```

Sin servidor. Sin proceso corriendo. Sin DB que respaldar. CDN-friendly por defecto. Hosting de un catálogo público completo cuesta del orden de **$5/mes en R2**.

## Federación: múltiples Portolans en una sesión

Esta es la parte que más diferencia este enfoque. En la misma sesión de DuckDB se pueden adjuntar varios Portolans y hacer queries que crucen catálogos:

```sql
ATTACH '' AS public_admin (
  TYPE ICEBERG,
  ENDPOINT 'https://admin.example.com'
);

ATTACH '' AS imagery_carto (
  TYPE ICEBERG,
  ENDPOINT 'https://imagery.carto.com',
  AUTHORIZATION_TYPE 'oauth2',
  TOKEN 'eyJ...'
);

SELECT i.scene_id, c.name_es
FROM imagery_carto.satellite.scenes i
JOIN public_admin.ne_admin_0.countries c
  ON ST_Intersects(i.geom, c.geom)
WHERE i.acquisition_date > '2025-01-01';
```

Un join entre un dataset privado (imagery con auth) y uno público (límites administrativos), resuelto en una sola query analítica. Eso es realmente novedoso en el ecosistema geoespacial.

## Autenticación por catálogo

Cada Portolan tiene su propia identidad. El cliente configura las credenciales por cada conexión, igual que ya hacemos con buckets de S3 distintos.

Tres modos posibles:

- **Público total** — sin credenciales, lectura abierta (NaturalEarth, datos open de gobierno)
- **Privado total** — credenciales requeridas para todo (catálogos corporativos internos)
- **Híbrido** — un subconjunto público por defecto; autenticarse desbloquea namespaces o tables adicionales

El primer prototipo usa auth a nivel de storage (políticas de bucket / signed URLs). El modo "híbrido" requiere un endpoint ligero de auth, pero el grueso del catálogo sigue siendo estático.

## Casos de uso

- Publicar catálogos públicos por unos pocos dólares al mes
- Publishers comerciales que monetizan acceso a datasets premium
- Organizaciones con datos sobre su propio object storage que quieren exponerlo internamente
- Federación entre organizaciones (CARTO + HDX, por ejemplo) sin centralizar nada

## Cómo encaja con la dirección actual de portolake

[ADR-0003](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0003-plugin-architecture.md) ya define una plugin architecture. **El static-catalog y el lakehouse server-based pueden convivir como dos backends del mismo plugin**, eligibles vía configuración:

| Modo | Para qué sirve |
|---|---|
| **Lakehouse server-based** (lo actual) | Writes concurrentes, ACID estricto, time-travel transaccional |
| **Static catalog** | Publicación, lectura, federación, hosting barato |

No son competidores — resuelven problemas distintos. La elección depende del caso de uso del publisher, no de una decisión de arquitectura global.

## Estado del prototipo

Ya implementado y funcional en la rama `add-sdi-experiment` del portolan original (último commit `d645577`):

- Generador completo del Iceberg REST static tree (`iceberg_rest_catalog.py`)
- Generación de metadata + manifests Iceberg (`iceberg_metadata.py`)
- Sync a R2 vía obstore (también S3, GCS, Azure) (`catalog_state.py`)
- Probado end-to-end con DuckDB
- Sin TODOs/FIXMEs pendientes

Listo para hacer un walk-through y decidir cómo lo llevamos al portolake actual.

## Preguntas para discutir

1. ¿Sumamos este modo a portolake como segundo backend, o vive mejor como plugin separado?
2. ¿Auth como capa transversal del plugin architecture, o configurada por backend?
3. ¿Cuál es el caso de uso prioritario para CARTO ahora — federación inter-org, o publicación pública pura?
4. Si seguimos adelante, ¿qué pieza portamos primero: el generador estático, o el modelo de auth por catálogo?
