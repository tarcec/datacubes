DROP FUNCTION PROC_ETL_INSERT_EVOLUCION_INVENTARIO_CENTRO_HISTORICO@
CREATE OR REPLACE FUNCTION PROC_ETL_INSERT_EVOLUCION_INVENTARIO_CENTRO_HISTORICO(
                                        IN_ANNO_PROCESO VARCHAR(255),
					IN_MES_PROCESO VARCHAR(255))
RETURNS VOID AS $func$
BEGIN

        INSERT INTO SCM_EVOLUCION_INVENTARIO_CENTRO(
                        ANNO_PROCESO,
                        MES_PROCESO,
                        SOCIEDAD,
                        CENTRO,
                        MATERIAL, 
                        CLASE_ARTICULO, 
                        CANTIDAD_LINEAS,
                        
                        --INICIO
                        ANNO_INICIO_CONSIG,
                        MES_INICIO_CONSIG,
                        CANTIDAD_INICIO_CONSIG,
                        CANTIDAD_INICIO_CONTROL_CALIDAD_CONSIG,

                        --MES
                        CANTIDAD_ENTRADA_CONSIG,
                        CANTIDAD_ENTRADA_CONTROL_CALIDAD_CONSIG,
                        CANTIDAD_SALIDA_CONSIG,
                        CANTIDAD_SALIDA_CONTROL_CALIDAD_CONSIG,
                        
                        --E7
                        CANT_OC_EM_NORMAL_LIBRE,
                        CANT_OC_EM_URGENTE_LIBRE,
                        
                        --E4
                        CENTRO_BENEFICIO,

                        --TERMINO
                        ANNO_TERMINO_CONSIG,
                        MES_TERMINO_CONSIG,
                        CANTIDAD_TERMINO_CONSIG,
                        CANTIDAD_TERMINO_CONTROL_CALIDAD_CONSIG,

                        FECHA_ACTUALIZACION,
                        APLICACION_ACTUALIZACION)
        SELECT
                        IN_ANNO_PROCESO
                                                AS ANNO_PROCESO,
                        IN_MES_PROCESO
                                                AS MES_PROCESO,
                        t001k.bukrs             AS SOCIEDAD,
                        --DONDE OCURRE LA SALIDA DE MATERIAL
                        mard.werks              AS CENTRO,
                        mard.matnr              AS MATERIAL,
                        mara.matkl              AS CLASE_ARTICULO,
                        COUNT(mseg.id)          AS CANTIDAD_LINEAS,
                        
                        --INICIO
                        mskuh.ini_lfgja                      AS ANNO_INICIO_CONSIG,
                        mskuh.ini_lfmon                      AS MES_INICIO_CONSIG,
                        COALESCE(MAX(mskuh.ini_kulab), 0)    AS CANTIDAD_INICIO_CONSIG,
                        COALESCE(MAX(mskuh.ini_kuins), 0)    AS CANTIDAD_INICIO_CONTROL_CALIDAD_CONSIG,
                        
                        --ENTRADA
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND mseg.sobkz = 'W' AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) 
                                        AND mseg.bwart NOT IN ('641', '322', '344', '350', '505', '718') THEN
                                        
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                 AS CANTIDAD_ENTRADA_CONSIG,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND mseg.insmk IN ('X', '2') AND mseg.sobkz = 'W' THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                 AS CANTIDAD_ENTRADA_CONTROL_CALIDAD_CONSIG,
                        
                        --SALIDA
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND mseg.sobkz = 'W' AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) 
                                        AND mseg.bwart NOT IN ('641', '322', '344', '350', '505', '718') THEN
                                        
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                 AS CANTIDAD_SALIDA_CONSIG,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND mseg.insmk IN ('X', '2') AND mseg.sobkz = 'W' THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                 AS CANTIDAD_SALIDA_CONTROL_CALIDAD_CONSIG,
                        
                        --E7
                        COALESCE(MAX(oc_stat_norm.tot_norm), 0)
                                                        AS CANT_OC_EM_NORMAL_LIBRE,
                        COALESCE(MAX(oc_stat_urg.tot_urg), 0)
                                                        AS CANT_OC_EM_URGENTE_LIBRE,
                        
                        --E4
                        marc.prctr              AS CENTRO_BENEFICIO,
                        
                        --TERMINO
                        mskuh.ter_lfgja                    AS ANNO_TERMINO_CONSIG,
                        mskuh.ter_lfmon                    AS MES_TERMINO_CONSIG,
                        COALESCE(MAX(mskuh.ter_kulab), 0)  AS CANTIDAD_TERMINO_CONSIG,
                        COALESCE(MAX(mskuh.ter_kuins), 0)  AS CANTIDAD_TERMINO_CONTROL_CALIDAD_CONSIG,
                                                
                        localtimestamp          AS FECHA_ACTUALIZACION,
                        'PROC_ETL_INSERT_EVOLUCION_INVENTARIO_CENTRO_HISTORICO'                      
                                                AS APLICACION_ACTUALIZACION
                        
        FROM            (
                        (
                        SELECT          mard_1.matnr,
                                        mard_1.werks
                        FROM            MARD mard_1
                        GROUP BY        mard_1.matnr,
                                        mard_1.werks
                        )
                        UNION
                        (
                        SELECT          msku_1.matnr,
                                        msku_1.werks
                        FROM            MSKU msku_1
                        GROUP BY        msku_1.matnr,
                                        msku_1.werks
                        )
                        UNION
                        (
                        SELECT          mskuh_1.matnr,
                                        mskuh_1.werks
                        FROM            MSKUH mskuh_1
                        GROUP BY        mskuh_1.matnr,
                                        mskuh_1.werks
                        )
                        )AS mard
        
        
        INNER JOIN      T001W t001w
        ON              mard.werks = t001w.werks
        INNER JOIN      T001K t001k
        ON              t001w.bwkey = t001k.bwkey
        

        INNER JOIN      MARC marc
        ON              marc.werks = mard.werks
        AND             marc.matnr = mard.matnr

        LEFT JOIN       MARA mara
        ON              mara.matnr = mard.matnr
        AND             mara.mandt = 300 
        
       -------------------------------------------------------------------------------
        --MSKU
        -------------------------------------------------------------------------------        
        LEFT JOIN       (
                        SELECT          mskuh_1_ini.matnr,
                                        mskuh_1_ini.werks,
                                        mskuh_1_ini.lfgja       AS ini_lfgja,
                                        mskuh_1_ini.lfmon       AS ini_lfmon,
                                        mskuh_1_ini.kulab       AS ini_kulab, --LIBRE
                                        mskuh_1_ini.kuins       AS ini_kuins, --CALIDAD
                                        mskuh_1_ter.lfgja       AS ter_lfgja,
                                        mskuh_1_ter.lfmon       AS ter_lfmon,
                                        mskuh_1_ter.kulab       AS ter_kulab, --LIBRE
                                        mskuh_1_ter.kuins       AS ter_kuins, --CALIDAD
                                        mskuh_1_count.count_mskuh
                                                                AS count_mskuh
                        FROM            (
                                        (
                                        SELECT          msku_1_1.matnr,
                                                        msku_1_1.werks
                                        FROM            MSKU msku_1_1
                                        GROUP BY        msku_1_1.matnr,
                                                        msku_1_1.werks
                                        )
                                        UNION
                                        (
                                        SELECT          mskuh_1_1.matnr,
                                                        mskuh_1_1.werks
                                        FROM            MSKUH mskuh_1_1
                                        GROUP BY        mskuh_1_1.matnr,
                                                        mskuh_1_1.werks
                                        )
                                        ) AS msku_1
                        LEFT JOIN
                                        (
                                        SELECT
                                        DISTINCT ON     (mskuh_1_1.matnr, mskuh_1_1.werks)
                                                        mskuh_1_1.matnr,
                                                        mskuh_1_1.werks,
                                                        mskuh_1_1.lfgja,
                                                        mskuh_1_1.lfmon,
                                                        mskuh_1_1.kulab, --LIBRE
                                                        mskuh_1_1.kuins --CALIDAD
                                        
                                        FROM            (
                                                        (
                                                        SELECT          mskuh_1_1_1.matnr,
                                                                        mskuh_1_1_1.werks,
                                                                        mskuh_1_1_1.lfgja,
                                                                        mskuh_1_1_1.lfmon,
                                                                        SUM(mskuh_1_1_1.kulab) AS kulab, --LIBRE
                                                                        SUM(mskuh_1_1_1.kuins) AS kuins  --CALIDAD
                                                        FROM            MSKUH mskuh_1_1_1
                                                        WHERE           to_date(mskuh_1_1_1.lfgja::text || LPAD(mskuh_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                        
                                                        GROUP BY        mskuh_1_1_1.matnr,
                                                                        mskuh_1_1_1.werks,
                                                                        mskuh_1_1_1.lfgja,
                                                                        mskuh_1_1_1.lfmon
                                                        )
                                                        UNION ALL
                                                        (
                                                        SELECT          msku_1_1_1.matnr,
                                                                        msku_1_1_1.werks,
                                                                        msku_1_1_1.lfgja,
                                                                        msku_1_1_1.lfmon,
                                                                        SUM(msku_1_1_1.kulab) AS kulab, --LIBRE
                                                                        SUM(msku_1_1_1.kuins) AS kuins  --CALIDAD
                                                        FROM            MSKU msku_1_1_1
                                                        WHERE           to_date(msku_1_1_1.lfgja::text || LPAD(msku_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                        
                                                        GROUP BY        msku_1_1_1.matnr,
                                                                        msku_1_1_1.werks,
                                                                        msku_1_1_1.lfgja,
                                                                        msku_1_1_1.lfmon
                                                        )
                                                        ) AS mskuh_1_1
                                        ORDER BY        mskuh_1_1.matnr, 
                                                        mskuh_1_1.werks, 
                                                        mskuh_1_1.lfgja DESC,
                                                        mskuh_1_1.lfmon DESC
                                        ) AS mskuh_1_ini
                        ON              msku_1.matnr = mskuh_1_ini.matnr
                        AND             msku_1.werks = mskuh_1_ini.werks                
                        LEFT JOIN      (
                                        SELECT
                                        DISTINCT ON     (mskuh_1_1.matnr, mskuh_1_1.werks)
                                                        mskuh_1_1.matnr,
                                                        mskuh_1_1.werks,
                                                        mskuh_1_1.lfgja,
                                                        mskuh_1_1.lfmon,
                                                        mskuh_1_1.kulab, --LIBRE
                                                        mskuh_1_1.kuins --CALIDAD
                                        
                                        FROM            (
                                                        (
                                                        SELECT          mskuh_1_1_1.matnr,
                                                                        mskuh_1_1_1.werks,
                                                                        mskuh_1_1_1.lfgja,
                                                                        mskuh_1_1_1.lfmon,
                                                                        SUM(mskuh_1_1_1.kulab) AS kulab, --LIBRE
                                                                        SUM(mskuh_1_1_1.kuins) AS kuins  --CALIDAD
                                                        FROM            MSKUH mskuh_1_1_1
                                                        WHERE           to_date(mskuh_1_1_1.lfgja::text || LPAD(mskuh_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')                                                
                                                        GROUP BY        mskuh_1_1_1.matnr,
                                                                        mskuh_1_1_1.werks,
                                                                        mskuh_1_1_1.lfgja,
                                                                        mskuh_1_1_1.lfmon
                                                        )
                                                        UNION ALL
                                                        (
                                                        SELECT          msku_1_1_1.matnr,
                                                                        msku_1_1_1.werks,
                                                                        msku_1_1_1.lfgja,
                                                                        msku_1_1_1.lfmon,
                                                                        SUM(msku_1_1_1.kulab) AS kulab, --LIBRE
                                                                        SUM(msku_1_1_1.kuins) AS kuins  --CALIDAD
                                                        FROM            MSKU msku_1_1_1
                                                        WHERE           to_date(msku_1_1_1.lfgja::text || LPAD(msku_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')                                                
                                                        GROUP BY        msku_1_1_1.matnr,
                                                                        msku_1_1_1.werks,
                                                                        msku_1_1_1.lfgja,
                                                                        msku_1_1_1.lfmon                                                                       
                                                        )
                                                        ) AS mskuh_1_1

                                        ORDER BY        mskuh_1_1.matnr, 
                                                        mskuh_1_1.werks, 
                                                        mskuh_1_1.lfgja ASC,
                                                        mskuh_1_1.lfmon ASC
                                        ) AS mskuh_1_ter
                        ON              msku_1.matnr = mskuh_1_ter.matnr
                        AND             msku_1.werks = mskuh_1_ter.werks
                        
                        --PARA HACER UN COUNT DE CUANTOS REGISTROS TIENE EL MATERIAL
                        LEFT JOIN      (
                                        SELECT
                                                        mskuh_1_1.matnr,
                                                        mskuh_1_1.werks,
                                                        COUNT(*) AS count_mskuh
                                        FROM            MSKUH mskuh_1_1
                                        WHERE           to_date(mskuh_1_1.lfgja::text || LPAD(mskuh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                        GROUP BY        mskuh_1_1.matnr,
                                                        mskuh_1_1.werks
                                        ) AS mskuh_1_count
                        ON              msku_1.matnr = mskuh_1_count.matnr
                        AND             msku_1.werks = mskuh_1_count.werks
                        
                        WHERE           
                                        mskuh_1_ini.lfgja IS NOT NULL                         
                        --ELIMINA REGISTROS VIEJOS
                        --Y QUE SOLO TIENEN EL HITO DE CREACION
                        --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        AND NOT             
                                (
                                        mskuh_1_ini.lfgja < 2015
                                AND
                                        mskuh_1_ter IS NULL
                                AND             
                                        mskuh_1_ini.kulab = 0.0
                                AND             
                                        mskuh_1_ini.kuins = 0.0
                                )                        
        
        
                        ) AS mskuh
        ON              mskuh.matnr = mard.matnr
        AND             mskuh.werks = mard.werks
        
        LEFT JOIN       MSEG mseg
        ON              mard.matnr = mseg.matnr
        AND             mard.werks = mseg.werks
        AND             mseg.budat_mkpf::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
        AND             mseg.budat_mkpf::bigint < 
                                to_char(
                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                        + INTERVAL '1 month', 'YYYYMMDD')::bigint

        --------------------------------------------------------------------------------------------
        -- STATS DE URGENCIA
        --------------------------------------------------------------------------------------------
        --NORMAL
        LEFT JOIN       (
                        --NORMAL
                        SELECT          qry.matnr,
                                        qry.werks,
                                        SUM(qry.tot_norm) AS tot_norm
                        FROM            (
                                        --MUNDO UB
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        SUM(CASE WHEN likp_1.vsbed NOT IN ('02', '05', '06') THEN 1 ELSE 0 END) AS tot_norm
                                                        
                                        FROM            LIKP likp_1
                                        INNER JOIN      LIPS lips_1
                                        ON              lips_1.vbeln = likp_1.vbeln
                                        AND             lips_1.mandt = likp_1.mandt
                                        AND             lips_1.kzbew = 'L'
                                        INNER JOIN      EKPO ekpo_1
                                        ON              lips_1.vgbel = ekpo_1.ebeln
                                        AND             lips_1.vgpos = ekpo_1.ebelp                
                                        AND             lips_1.mandt = ekpo_1.mandt
                                        INNER JOIN      EKKO ekko_1
                                        ON              ekko_1.ebeln = lips_1.vgbel
                                        AND             ekko_1.mandt = lips_1.mandt
                                        
                                        WHERE           ekko_1.bsart IN ('UB')
                                        AND             ekko_1.aedat::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
                                        AND             ekko_1.aedat::bigint < 
                                                                to_char(
                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                                        AND             ekpo_1.loekz IS NULL
                                     
                                        AND             likp_1.mandt = 300
                                     
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks
                                        UNION ALL
                                        --MUNDO STOCK         
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        SUM(CASE WHEN ekpo_1.prio_req NOT IN (10, 20) THEN 1 ELSE 0 END) AS tot_norm
                                        FROM            EKPO ekpo_1
                                        INNER JOIN      EKKO ekko_1
                                        ON              ekko_1.ebeln = ekpo_1.ebeln
                                        AND             ekko_1.mandt = ekpo_1.mandt
                                        WHERE           ekko_1.bsart NOT IN ('UB')
                                        AND             ekko_1.aedat::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
                                        AND             ekko_1.aedat::bigint < 
                                                                to_char(
                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                                        AND             ekpo_1.loekz IS NULL
                        
                                        AND             ekpo_1.mandt = 300 --KCC
                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks
                                        ) AS qry
                        GROUP BY        qry.matnr,
                                        qry.werks
                        ) AS oc_stat_norm
        ON              mard.matnr = oc_stat_norm.matnr
        AND             mard.werks = oc_stat_norm.werks
        
        --URGENCIA
        LEFT JOIN       (
                        --URGENCIA
                        SELECT          qry.matnr,
                                        qry.werks,
                                        SUM(qry.tot_urg) AS tot_urg
                        FROM            (
                                        --MUNDO UB
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        SUM(CASE WHEN likp_1.vsbed IN ('02', '05', '06') THEN 1 ELSE 0 END) AS tot_urg
                                                        
                                        FROM            LIKP likp_1
                                        INNER JOIN      LIPS lips_1
                                        ON              lips_1.vbeln = likp_1.vbeln
                                        AND             lips_1.mandt = likp_1.mandt
                                        AND             lips_1.kzbew = 'L'
                                        INNER JOIN      EKPO ekpo_1
                                        ON              lips_1.vgbel = ekpo_1.ebeln
                                        AND             lips_1.vgpos = ekpo_1.ebelp
                                        AND             lips_1.mandt = ekpo_1.mandt                
                                        INNER JOIN      EKKO ekko_1
                                        ON              ekko_1.ebeln = lips_1.vgbel
                                        AND             ekko_1.mandt = lips_1.mandt
                                        
                                        WHERE           ekko_1.bsart IN ('UB')
                                        AND             ekko_1.aedat::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
                                        AND             ekko_1.aedat::bigint < 
                                                                to_char(
                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                                        AND             ekpo_1.loekz IS NULL
                                     
                                        AND             likp_1.mandt = 300
                                     
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks
                                        UNION ALL
                                        --MUNDO STOCK         
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        SUM(CASE WHEN ekpo_1.prio_req IN (10, 20) THEN 1 ELSE 0 END) AS tot_urg
                                        FROM            EKPO ekpo_1
                                        INNER JOIN      EKKO ekko_1
                                        ON              ekko_1.ebeln = ekpo_1.ebeln
                                        AND             ekko_1.mandt = ekpo_1.mandt
                                        WHERE           ekko_1.bsart NOT IN ('UB')
                                        AND             ekko_1.aedat::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
                                        AND             ekko_1.aedat::bigint < 
                                                                to_char(
                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                                        AND             ekpo_1.loekz IS NULL
                                        
                                        ANd             ekpo_1.mandt = 300 --KCC
                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks
                                        ) AS qry
                        GROUP BY        qry.matnr,
                                        qry.werks
                        ) AS oc_stat_urg
        ON              mard.matnr = oc_stat_urg.matnr
        AND             mard.werks = oc_stat_urg.werks

     
        --SOLO CENTROS VIGENTES
        WHERE           mard.werks IN ('0000','0001','0002','0003','0005','0006','0007','0008','0009','0010','0011','0012')
      
        AND             (
                        mskuh.matnr IS NOT NULL
                        )      
      
        --RESTRICCION DE DATOS A PROCESAR
        AND             t001k.bukrs IN ('SAP1', 'SAP2')
      
        GROUP BY        t001k.bukrs,
                        mard.werks,
                        mard.matnr,
                        mara.matkl,
                        
                        --E4
                        marc.prctr,
                       
                        mskuh.ini_lfgja,
                        mskuh.ini_lfmon,
                        mskuh.ter_lfgja,
                        mskuh.ter_lfmon
        ;

END;
$func$
LANGUAGE plpgsql@