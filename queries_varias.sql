-- query sqlsever

--ATTR_DATA_VALU_MAYO
-- DATA
DROP VIEW IF EXISTS public.frequency_report_data_mayo;
CREATE VIEW public.frequency_report_data_mayo AS 
SELECT
	ntile,
	CAST(avg(ATTR_DATA_VALU_MAYO) AS INTEGER) AS avgAmount,
	CAST(max(ATTR_DATA_VALU_MAYO) AS INTEGER) AS maxAmount,
	CAST(min(ATTR_DATA_VALU_MAYO) AS INTEGER) AS minAmount,
	count(distinct numa) as freq
	FROM (SELECT numa,ATTR_DATA_VALU_MAYO, ntile(10) OVER (ORDER BY ATTR_DATA_VALU_MAYO) AS ntile FROM agg_parque_att_v05) x
	GROUP BY ntile
	ORDER BY ntile;

DROP VIEW IF EXISTS public.frequency_report_data_juni;
CREATE VIEW public.frequency_report_data_juni AS 
SELECT
	ntile,
	CAST(avg(ATTR_DATA_VALU_JUNI) AS INTEGER) AS avgAmount,
	CAST(max(ATTR_DATA_VALU_JUNI) AS INTEGER) AS maxAmount,
	CAST(min(ATTR_DATA_VALU_JUNI) AS INTEGER) AS minAmount,
	count(distinct numa) as freq
	FROM (SELECT numa,ATTR_DATA_VALU_JUNI, ntile(10) OVER (ORDER BY ATTR_DATA_VALU_JUNI) AS ntile FROM agg_parque_att_v05) x
	GROUP BY ntile
	ORDER BY ntile;

DROP VIEW IF EXISTS public.frequency_report_data_juli;
CREATE VIEW public.frequency_report_data_juli AS 
SELECT
	ntile,
	CAST(avg(ATTR_DATA_VALU_JULI) AS INTEGER) AS avgAmount,
	CAST(max(ATTR_DATA_VALU_JULI) AS INTEGER) AS maxAmount,
	CAST(min(ATTR_DATA_VALU_JULI) AS INTEGER) AS minAmount,
	count(distinct numa) as freq
	FROM (SELECT numa,ATTR_DATA_VALU_JULI, ntile(10) OVER (ORDER BY ATTR_DATA_VALU_JULI) AS ntile FROM agg_parque_att_v05) x
	GROUP BY ntile
	ORDER BY ntile;

--- VOZ ---

DROP VIEW IF EXISTS public.frequency_report_voz_mayo;
CREATE VIEW public.frequency_report_voz_mayo AS 
SELECT
	ntile,
	CAST(avg(ATTR_VOZ_VALU_MAYO) AS INTEGER) AS avgAmount,
	CAST(max(ATTR_VOZ_VALU_MAYO) AS INTEGER) AS maxAmount,
	CAST(min(ATTR_VOZ_VALU_MAYO) AS INTEGER) AS minAmount,
	count(distinct numa) as freq
	FROM (SELECT numa,ATTR_VOZ_VALU_MAYO, ntile(10) OVER (ORDER BY ATTR_VOZ_VALU_MAYO) AS ntile FROM agg_parque_att_v05) x
	GROUP BY ntile
	ORDER BY ntile;

DROP VIEW IF EXISTS public.frequency_report_voz_juni;
CREATE VIEW public.frequency_report_voz_juni AS 
SELECT
	ntile,
	CAST(avg(ATTR_VOZ_VALU_JUNI) AS INTEGER) AS avgAmount,
	CAST(max(ATTR_VOZ_VALU_JUNI) AS INTEGER) AS maxAmount,
	CAST(min(ATTR_VOZ_VALU_JUNI) AS INTEGER) AS minAmount,
	count(distinct numa) as freq
	FROM (SELECT numa,ATTR_VOZ_VALU_JUNI, ntile(10) OVER (ORDER BY ATTR_VOZ_VALU_JUNI) AS ntile FROM agg_parque_att_v05) x
	GROUP BY ntile
	ORDER BY ntile;

DROP VIEW IF EXISTS public.frequency_report_voz_juli;
CREATE VIEW public.frequency_report_voz_juli AS 
SELECT
	ntile,
	CAST(avg(ATTR_VOZ_VALU_JULI) AS INTEGER) AS avgAmount,
	CAST(max(ATTR_VOZ_VALU_JULI) AS INTEGER) AS maxAmount,
	CAST(min(ATTR_VOZ_VALU_JULI) AS INTEGER) AS minAmount,
	count(distinct numa) as freq
	FROM (SELECT numa,ATTR_VOZ_VALU_JULI, ntile(10) OVER (ORDER BY ATTR_VOZ_VALU_JULI) AS ntile FROM agg_parque_att_v05) x
	GROUP BY ntile
	ORDER BY ntile;
    
    
--levantamineto de saldo    
--DROP TABLE public.raw_portados;
CREATE TABLE public.raw_saldo(
	COD_DIA date,
	COD_ABONADO_ORI integer,
	COD_ABONADO_DES integer,
	COD_SERVICIO VARCHAR(50),
	COD_HORA integer,
	MINUTO integer,
	SEGUNDO integer,
	COD_TIPO_MENSAJE integer,
	COD_CLIENTE_ORIGEN integer,
	COD_CLIENTE_DESTINO integer,
	IMPORTE_TOTAL integer,
	NUM_CELULAR integer
) DISTRIBUTED RANDOMLY;

--DROP EXTERNAL TABLE karim_recargas_external;
CREATE EXTERNAL TABLE saldo_external(
	COD_DIA date,
	COD_ABONADO_ORI integer,
	COD_ABONADO_DES integer,
	COD_SERVICIO VARCHAR(50),
	COD_HORA integer,
	MINUTO integer,
	SEGUNDO integer,
	COD_TIPO_MENSAJE integer,
	COD_CLIENTE_ORIGEN integer,
	COD_CLIENTE_DESTINO integer,
	IMPORTE_TOTAL integer,
	NUM_CELULAR integer
) LOCATION (
    'pxf://hdm3:50070/tmp/datalake/consulta_salto_05-07.txt?profile=HdfsTextSimple'
)FORMAT 'text' (header delimiter E'\t' null '' escape 'OFF' fill missing fields)
ENCODING 'UTF8';

insert into public.raw_saldo (select * from saldo_external);

drop external table public.saldo_external;

DROP TABLE IF EXISTS public.agg_trx_prepago_all_raw;
create table public.agg_trx_prepago_all_raw as
select num_celular,cod_dia,cod_hora,minuto,segundo,cod_servicio,importe_total 
from raw_saldo where importe_total is not null

-- multi join
/*
Older version

create table public.aggregated_prepago_dato_all as
select * from (
select * from public.aggregated_cdr_dat_may union
select * from public.aggregated_cdr_dat_jun union
select * from public.aggregated_cdr_dat_jul) D
DISTRIBUTED RANDOMLY
*/

create table public.agg_dat_prepago_all as
select * from (
select * from public.agg_dat_may union
select * from public.agg_dat_jun union
select * from public.agg_dat_jul) D
DISTRIBUTED RANDOMLY