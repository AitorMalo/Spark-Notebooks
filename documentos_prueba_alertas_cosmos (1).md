# Documentos de Prueba para Disparar Alertas en PRE

> **Fecha de referencia (`{FECHA}`)**: Debe ser el dia anterior al dia de ejecucion de la funcion, formato `YYYYMMDD`.
> Ejemplo: si se ejecuta el 05/05/2026, usar `20260504`.
>
> Los documentos se generan para **Portugal (00411)**. Para otros paises, sustituir `00411` por: `00051` (CH), `00053` (UY) o `00012` (BR).

---

## ALERTA 1 — Alertas_Motor_War_TM

**Base de datos**: `core`
**Coleccion**: `core_uv_dbk_output`

```json
{
  "id": "test-war-tm-00411-001",
  "ingestBlobPath": "landing/00411_clients_20260504_Alert_tm_batch1.csv",
  "rowsKo": 0,
  "rowsOk": 500,
  "total": 500,
  "totalErrors": 0,
  "warningsSummary": "FORMAT_MISMATCH=12 registros con formato incorrecto",
  "errorsSummary": ""
}
```

```json
{
  "id": "test-war-tm-00411-002",
  "ingestBlobPath": "landing/00411_transactions_20260504_Alert_tm_batch2.csv",
  "rowsKo": 0,
  "rowsOk": 300,
  "total": 300,
  "totalErrors": 0,
  "warningsSummary": "NULL_VALUE=8 campos nulos detectados",
  "errorsSummary": ""
}
```

**Documentos auxiliares opcionales** en coleccion `core_uv_dbk_vr_output` (misma base de datos `core`), para que el ticket incluya detalle de columnas afectadas:

```json
{
  "id": "test-war-tm-00411-001",
  "sample": [
    { "COL_WITH_ERROR": "ACCOUNT_NUMBER" },
    { "COL_WITH_ERROR": "TRANSACTION_DATE" }
  ]
}
```

```json
{
  "id": "test-war-tm-00411-002",
  "sample": [
    { "COL_WITH_ERROR": "CURRENCY_CODE" }
  ]
}
```

---

## ALERTA 2 — Alertas_Motor_War_CM

**Base de datos**: `core`
**Coleccion**: `core_uv_dbk_output`

```json
{
  "id": "test-war-cm-00411-001",
  "ingestBlobPath": "landing/00411_cases_20260504_cm_batch1.csv",
  "rowsKo": 0,
  "rowsOk": 200,
  "total": 200,
  "totalErrors": 0,
  "warningsSummary": "TRUNCATED_FIELD=5 campos truncados",
  "errorsSummary": ""
}
```

```json
{
  "id": "test-war-cm-00411-002",
  "ingestBlobPath": "landing/00411_cases_20260504_cm_batch2.csv",
  "rowsKo": 0,
  "rowsOk": 150,
  "total": 150,
  "totalErrors": 0,
  "warningsSummary": "ENCODING_WARNING=3 caracteres no reconocidos",
  "errorsSummary": ""
}
```

---

## ALERTA 3 — Alertas_Motor_War_SC

**Base de datos**: `core`
**Coleccion**: `core_uv_dbk_output`

```json
{
  "id": "test-war-sc-00411-001",
  "ingestBlobPath": "landing/00411_screening_20260504_sc1_batch1.csv",
  "rowsKo": 0,
  "rowsOk": 400,
  "total": 400,
  "totalErrors": 0,
  "warningsSummary": "DATE_FORMAT=7 fechas con formato inesperado",
  "errorsSummary": ""
}
```

```json
{
  "id": "test-war-sc-00411-002",
  "ingestBlobPath": "landing/00411_screening_20260504_sc1_batch2.csv",
  "rowsKo": 0,
  "rowsOk": 250,
  "total": 250,
  "totalErrors": 0,
  "warningsSummary": "MISSING_OPTIONAL=4 campos opcionales vacios",
  "errorsSummary": ""
}
```

---

## ALERTA 4 — Alertas_Motor_War_KYC

**Base de datos**: `core`
**Coleccion**: `core_uv_dbk_output`

```json
{
  "id": "test-war-kyc-00411-001",
  "ingestBlobPath": "landing/00411_kyc_20260504_kyc_batch1.csv",
  "rowsKo": 0,
  "rowsOk": 350,
  "total": 350,
  "totalErrors": 0,
  "warningsSummary": "ID_FORMAT=6 identificadores con formato legacy",
  "errorsSummary": ""
}
```

```json
{
  "id": "test-war-kyc-00411-002",
  "ingestBlobPath": "landing/00411_kyc_20260504_kyc_batch2.csv",
  "rowsKo": 0,
  "rowsOk": 180,
  "total": 180,
  "totalErrors": 0,
  "warningsSummary": "COUNTRY_CODE=2 codigos de pais no estandar",
  "errorsSummary": ""
}
```

---

## ALERTA 5 — Alertas_Motor_KO_TM

**Base de datos**: `core`
**Coleccion**: `core_uv_dbk_output`

```json
{
  "id": "test-ko-tm-00411-001",
  "ingestBlobPath": "landing/00411_clients_20260504_Alert_tm_batch3.csv",
  "rowsKo": 45,
  "rowsOk": 455,
  "total": 500,
  "totalErrors": 5,
  "warningsSummary": "VALIDATION_WARNING=3",
  "errorsSummary": "PARSE_ERROR=5 campos no parseables"
}
```

```json
{
  "id": "test-ko-tm-00411-002",
  "ingestBlobPath": "landing/00411_transactions_20260504_Alert_tm_batch4.csv",
  "rowsKo": 20,
  "rowsOk": 280,
  "total": 300,
  "totalErrors": 2,
  "warningsSummary": "",
  "errorsSummary": "SCHEMA_MISMATCH=2 columnas no coinciden con esquema"
}
```

---

## ALERTA 6 — Alertas_Motor_KO_CM

**Base de datos**: `core`
**Coleccion**: `core_uv_dbk_output`

```json
{
  "id": "test-ko-cm-00411-001",
  "ingestBlobPath": "landing/00411_cases_20260504_cm_batch3.csv",
  "rowsKo": 30,
  "rowsOk": 170,
  "total": 200,
  "totalErrors": 4,
  "warningsSummary": "",
  "errorsSummary": "DUPLICATE_KEY=4 registros duplicados"
}
```

```json
{
  "id": "test-ko-cm-00411-002",
  "ingestBlobPath": "landing/00411_cases_20260504_cm_batch4.csv",
  "rowsKo": 12,
  "rowsOk": 138,
  "total": 150,
  "totalErrors": 1,
  "warningsSummary": "ENCODING_WARNING=1",
  "errorsSummary": "NULL_REQUIRED=1 campo obligatorio nulo"
}
```

---

## ALERTA 7 — Alertas_Motor_KO_SC

**Base de datos**: `core`
**Coleccion**: `core_uv_dbk_output`

```json
{
  "id": "test-ko-sc-00411-001",
  "ingestBlobPath": "landing/00411_screening_20260504_sc1_batch3.csv",
  "rowsKo": 60,
  "rowsOk": 340,
  "total": 400,
  "totalErrors": 8,
  "warningsSummary": "",
  "errorsSummary": "INVALID_REFERENCE=8 referencias no encontradas"
}
```

```json
{
  "id": "test-ko-sc-00411-002",
  "ingestBlobPath": "landing/00411_screening_20260504_sc1_batch4.csv",
  "rowsKo": 25,
  "rowsOk": 225,
  "total": 250,
  "totalErrors": 3,
  "warningsSummary": "",
  "errorsSummary": "TYPE_MISMATCH=3 tipos de dato incorrectos"
}
```

---

## ALERTA 8 — Alertas_Motor_KO_KYC

**Base de datos**: `core`
**Coleccion**: `core_uv_dbk_output`

```json
{
  "id": "test-ko-kyc-00411-001",
  "ingestBlobPath": "landing/00411_kyc_20260504_kyc_batch3.csv",
  "rowsKo": 15,
  "rowsOk": 335,
  "total": 350,
  "totalErrors": 2,
  "warningsSummary": "",
  "errorsSummary": "MISSING_ID=2 identificadores ausentes"
}
```

```json
{
  "id": "test-ko-kyc-00411-002",
  "ingestBlobPath": "landing/00411_kyc_20260504_kyc_batch4.csv",
  "rowsKo": 8,
  "rowsOk": 172,
  "total": 180,
  "totalErrors": 1,
  "warningsSummary": "",
  "errorsSummary": "RANGE_ERROR=1 valor fuera de rango"
}
```

---

## ALERTA 9 — Fallo_Ingesta_RE

**Base de datos**: `core`
**Coleccion**: `core_ingests_log_output`

```json
{
  "id": "test-ingest-fail-00411-001",
  "code": 409,
  "rows": 0,
  "rowsOK": 0,
  "uploadFile": {
    "originalFileName": "00411_transactions_20260504_daily_load.csv",
    "blobSize": 1048576,
    "ingestedFilePath": "landing/00411/transactions/00411_transactions_20260504_daily_load.csv"
  }
}
```

```json
{
  "id": "test-ingest-fail-00411-002",
  "code": 409,
  "rows": 0,
  "rowsOK": 0,
  "uploadFile": {
    "originalFileName": "00411_clients_20260504_monthly_refresh.csv",
    "blobSize": 2097152,
    "ingestedFilePath": "landing/00411/clients/00411_clients_20260504_monthly_refresh.csv"
  }
}
```

---

## Tabla Resumen

| # | Alerta | Base de datos | Coleccion | Docs |
|---|---|---|---|---|
| 1 | `Alertas_Motor_War_TM` | `core` | `core_uv_dbk_output` | 2 + 2 auxiliares en `core_uv_dbk_vr_output` |
| 2 | `Alertas_Motor_War_CM` | `core` | `core_uv_dbk_output` | 2 |
| 3 | `Alertas_Motor_War_SC` | `core` | `core_uv_dbk_output` | 2 |
| 4 | `Alertas_Motor_War_KYC` | `core` | `core_uv_dbk_output` | 2 |
| 5 | `Alertas_Motor_KO_TM` | `core` | `core_uv_dbk_output` | 2 |
| 6 | `Alertas_Motor_KO_CM` | `core` | `core_uv_dbk_output` | 2 |
| 7 | `Alertas_Motor_KO_SC` | `core` | `core_uv_dbk_output` | 2 |
| 8 | `Alertas_Motor_KO_KYC` | `core` | `core_uv_dbk_output` | 2 |
| 9 | `Fallo_Ingesta_RE` | `core` | `core_ingests_log_output` | 2 |
| | | | **Total** | **18 docs** (+ 2 auxiliares opcionales) |

> **Recordatorio**: Los documentos de Warning (alertas 1-4) y KO (alertas 5-8) no pueden compartir el mismo documento porque Warning exige `rowsKo=0` y `totalErrors=0`, mientras que KO exige `rowsKo!=0`.
>
> **Deduplicacion**: No se creara ticket si ya existe uno del mismo tipo + pais + dia en Snowflake (`BAU_SNOW_TICKETS`).
