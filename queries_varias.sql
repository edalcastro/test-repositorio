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


--Funciona en Teradata, revisar si lo hace en otras bbdd

 SELECT ADD_MONTHS(date,-1) dia_mesant
,CAST(((ADD_MONTHS(DATE, -1)/100)*100+1) AS DATE) firtdia_mesant,
       CAST(((ADD_MONTHS(DATE, 0)/100)*100+1) AS DATE)-1 lastdia_mesant;


 SELECT ADD_MONTHS(date,-1)
,CAST(((ADD_MONTHS(DATE, -1)/100)*100+1) AS DATE) FDo2MAGO,
       CAST(((ADD_MONTHS(DATE, 0)/100)*100+1) AS DATE)-1 LDo2MAGO;


--- para omitir los segundos
--  ya que la line_recharge_tm viene como numeros de 6 digitos no quedamos con 4 y redondeamos los :00 finales asi tenemos las salida 
--  tipo: 2016-03-13 01:18:00


(line_recharge_dt||' '||substr(line_recharge_tm,1,5)||':00') as hora,


where Line_Recharge_Dt between CURRENT_DATE- INTERVAL '1' YEAR and CURRENT_DATE
where Line_Recharge_Dt between '2016-01-01' and CURRENT_DATE

select extract (month from CURRENT_DATE)

-- convertir string a integert y fecha
select
cast(substr(cast(periodo as varchar(10)),1,4) ||''|| substr(cast(periodo as varchar(10)),5,2) ||''|| '01' as integer) as fecha,
cast(fecha as char(8)) as fecha1,
cast(fecha1 as date format 'YYYYMMDD') as fecha2,
CAST(((ADD_MONTHS(fecha2, -1)/100)*100+1) AS DATE)
from ubd15.BI_PARQUE_PREPAGO_EVO_R



-- espacio esquemas teradata

SELECT tablename,sum(currentperm)/1024/1024 MB
FROM dbc.allspace
WHERE databasename='UBD15'
GROUP BY 1
ORDER BY 2 DESC;

-- para desplazar la fila con datos previos


SELECT Periodo as Periodo,
cast('rec_mes_ant' as VARCHAR(20)) as concepto,
sum(case when Estado in ('REC GANADOS','REC MANTENIDOS') then Q else null end) as valor,
       MAX(valor) OVER (ORDER BY Periodo
                       ROWS BETWEEN 1 PRECEDING
                       AND 1 PRECEDING) ID_N_PREV, --previo
       MAX(valor) OVER (ORDER BY Periodo
                       ROWS BETWEEN 1 FOLLOWING
                       AND 1 FOLLOWING) ID_N_POST -- anterior
From ubd15.BI_PARQUE_PREPAGO_EVO_R1
group by 1
order by 1 desc