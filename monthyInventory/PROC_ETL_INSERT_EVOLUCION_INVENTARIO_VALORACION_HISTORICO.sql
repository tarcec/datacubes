DROP FUNCTION PROC_ETL_INSERT_EVOLUCION_INVENTARIO_VALORACION_HISTORICO@
CREATE OR REPLACE FUNCTION PROC_ETL_INSERT_EVOLUCION_INVENTARIO_VALORACION_HISTORICO(
                                        IN_ANNO_PROCESO VARCHAR(255),
					IN_MES_PROCESO VARCHAR(255))
RETURNS VOID AS $func$
BEGIN

        INSERT INTO SCM_EVOLUCION_INVENTARIO_VALORACION(
                        ANNO_PROCESO,
                        MES_PROCESO,
                        SOCIEDAD,
                        CENTRO,
                        AMBITO_VALORACION,
                        CLASE_VALORACION,
                        
                        MATERIAL, 
                        CLASE_ARTICULO, 
                        CANTIDAD_LINEAS,
                        
                        ANNO_INICIO_VALORACION,
                        MES_INICIO_VALORACION,
                        CANTIDAD_INICIO_VALORACION,
                        MONTO_INICIO_VALORACION,
                        ANNO_INICIO_VALORACION_PROY,
                        MES_INICIO_VALORACION_PROY,
                        CANTIDAD_INICIO_VALORACION_PROY,
                        MONTO_INICIO_VALORACION_PROY,

                        CANTIDAD_ENTRADA_VALORACION,
                        MONTO_ENTRADA_VALORACION,
                        CANTIDAD_SALIDA_VALORACION,
                        MONTO_SALIDA_VALORACION,

                        --E4
                        CENTRO_BENEFICIO,
                        
                        ANNO_TERMINO_VALORACION,
                        MES_TERMINO_VALORACION,
                        CANTIDAD_TERMINO_VALORACION,
                        MONTO_TERMINO_VALORACION,
                        ANNO_TERMINO_VALORACION_PROY,
                        MES_TERMINO_VALORACION_PROY,
                        CANTIDAD_TERMINO_VALORACION_PROY,
                        MONTO_TERMINO_VALORACION_PROY,
                        
                        FECHA_ACTUALIZACION,
                        APLICACION_ACTUALIZACION)
        SELECT
                        IN_ANNO_PROCESO
                                                AS ANNO_PROCESO,
                        IN_MES_PROCESO
                                                AS MES_PROCESO,
                        t001k.bukrs             AS SOCIEDAD,
                        --DONDE OCURRE LA SALIDA DE MATERIAL
                        t001w.werks             AS CENTRO,
                        mbew.bwkey              AS AMBITO_VALORACION,
                        
                        --NO SE USA CLASE_VALORACION
                        --mbew.bwtar              AS CLASE_VALORACION,
                        NULL                    AS CLASE_VALORACION,
                        
                        mbew.matnr              AS MATERIAL,
                        mara.matkl              AS CLASE_ARTICULO,
                        COUNT(bsim.id)          AS CANTIDAD_LINEAS,
                        
                        mbewh.ini_lfgja         AS ANNO_INICIO_VALORACION,
                        mbewh.ini_lfmon         AS MES_INICIO_VALORACION,
                        COALESCE(MAX(mbewh.ini_lbkum), 0)
                                                AS CANTIDAD_INICIO_VALORACION,
                        COALESCE(MAX(mbewh.ini_salk3), 0)
                                                AS MONTO_INICIO_VALORACION,
                        qbewh.ini_lfgja         AS ANNO_INICIO_VALORACION_PROY,
                        qbewh.ini_lfmon         AS MES_INICIO_VALORACION_PROY,
                        COALESCE(MAX(qbewh.ini_lbkum), 0)
                                                AS CANTIDAD_INICIO_VALORACION_PROY,
                        COALESCE(MAX(qbewh.ini_salk3), 0)
                                                AS MONTO_INICIO_VALORACION_PROY,                         
                                                
                        --ENTRADA
                        SUM(
                                CASE WHEN bsim.shkzg = 'S' AND bsim.blart IN ('WA', 'WE', 'WL') THEN
                                        bsim.menge
                                ELSE
                                        0
                                END
                        )                       AS CANTIDAD_ENTRADA_VALORACION,
                        SUM(
                                CASE WHEN bsim.shkzg = 'S' THEN
                                        bsim.dmbtr
                                ELSE
                                        0
                                END
                        )               AS MONTO_ENTRADA_VALORACION,
                        
                        --SALIDA
                        SUM(
                                CASE WHEN bsim.shkzg = 'H' AND bsim.blart IN ('WA', 'WE', 'WL') THEN
                                        bsim.menge
                                ELSE
                                        0
                                END
                        )                       AS CANTIDAD_SALIDA_VALORACION,
                        SUM(
                                CASE WHEN bsim.shkzg = 'H' THEN
                                        bsim.dmbtr
                                ELSE
                                        0
                                END
                        )                       AS MONTO_SALIDA_VALORACION,
                        
                        --E4
                        marc.prctr              AS CENTRO_BENEFICIO,
                                                
                        mbewh.ter_lfgja         AS ANNO_TERMINO_VALORACION,
                        mbewh.ter_lfmon         AS MES_TERMINO_VALORACION,
                        COALESCE(MAX(mbewh.ter_lbkum), 0)    
                                                AS CANTIDAD_TERMINO_VALORACION,
                        COALESCE(MAX(mbewh.ter_salk3), 0)
                                                AS MONTO_TERMINO_VALORACION,
                        qbewh.ter_lfgja         AS ANNO_TERMINO_VALORACION_PROY,
                        qbewh.ter_lfmon         AS MES_TERMINO_VALORACION_PROY,
                        COALESCE(MAX(qbewh.ter_lbkum), 0)    
                                                AS CANTIDAD_TERMINO_VALORACION_PROY,
                        COALESCE(MAX(qbewh.ter_salk3), 0)
                                                AS MONTO_TERMINO_VALORACION_PROY,                                                
                        
                        localtimestamp          AS FECHA_ACTUALIZACION,
                        'PROC_ETL_INSERT_EVOLUCION_INVENTARIO_VALORACION_HISTORICO'                      
                                                AS APLICACION_ACTUALIZACION
                        
        
       
        FROM            (
                        (
                        SELECT          mbew_1.matnr,
                                        mbew_1.bwkey
                        FROM            MBEWH mbew_1
                        GROUP BY        mbew_1.matnr,
                                        mbew_1.bwkey
                        )
                        UNION
                        (
                        SELECT          mbew_1.matnr,
                                        mbew_1.bwkey
                        FROM            MBEW mbew_1
                        GROUP BY        mbew_1.matnr,
                                        mbew_1.bwkey
                        )
                        UNION
                        (
                        SELECT          qbew_1.matnr,
                                        qbew_1.bwkey
                        FROM            QBEWH qbew_1
                        GROUP BY        qbew_1.matnr,
                                        qbew_1.bwkey
                        )
                        UNION
                        (
                        SELECT          qbew_1.matnr,
                                        qbew_1.bwkey
                        FROM            QBEW qbew_1
                        GROUP BY        qbew_1.matnr,
                                        qbew_1.bwkey
                        )                
                        ) AS mbew
        
        INNER JOIN      T001K t001k
        ON              t001k.bwkey = mbew.bwkey

        INNER JOIN      T001W t001w
        ON              mbew.bwkey = t001w.bwkey
        
        INNER JOIN      MARC marc
        ON              marc.werks = t001w.werks
        AND             marc.matnr = mbew.matnr
        
        LEFT JOIN       MARA mara
        ON              mara.matnr = mbew.matnr
        AND             mara.mandt = 300   
        
        --------------------------------------------------------------------------------------------------------------------
        --MBEW STOCK LIBRE, E, W
        --------------------------------------------------------------------------------------------------------------------
        LEFT JOIN      (
                                SELECT          mbewh_1_ini.matnr,
                                                mbewh_1_ini.bwkey,
                                                --mbewh_1_ini.bwtar,
                                                
                                                mbewh_1_ini.lfgja       AS ini_lfgja,
                                                mbewh_1_ini.lfmon       AS ini_lfmon,
                                                mbewh_1_ini.lbkum       AS ini_lbkum,
                                                mbewh_1_ini.salk3       AS ini_salk3,
                                                
                                                mbewh_1_ter.lfgja       AS ter_lfgja,
                                                mbewh_1_ter.lfmon       AS ter_lfmon,
                                                mbewh_1_ter.lbkum       AS ter_lbkum,
                                                mbewh_1_ter.salk3       AS ter_salk3,
                                                
                                                mbewh_1_count.count_mbewh
                                                                        AS count_mbewh
                                --SELECTOR DE REGISTROS
                                FROM            (
                                                (
                                                SELECT          mbew_1_1.matnr,
                                                                mbew_1_1.bwkey
                                                FROM            MBEW mbew_1_1
                                                GROUP BY        mbew_1_1.matnr,
                                                                mbew_1_1.bwkey
                                                )
                                                UNION
                                                (
                                                SELECT          mbewh_1_1.matnr,
                                                                mbewh_1_1.bwkey
                                                FROM            MBEWH mbewh_1_1
                                                GROUP BY        mbewh_1_1.matnr,
                                                                mbewh_1_1.bwkey
                                                )
                                                ) AS mbew_1
                                --VALORACION INICIO
                                LEFT JOIN       (
                                                        SELECT
                                                        DISTINCT ON     (mbewh_1_1.matnr, mbewh_1_1.bwkey)
                                                                        mbewh_1_1.matnr,
                                                                        mbewh_1_1.bwkey,
                                                                        mbewh_1_1.lfgja,
                                                                        mbewh_1_1.lfmon,
                                                                        mbewh_1_1.lbkum,
                                                                        mbewh_1_1.salk3
                                                        FROM(                 
                                                                        --------------------------------------------------------
                                                                        --MBEWH
                                                                        --------------------------------------------------------
                                                                        (
                                                                                SELECT
                                                                                                mbewh_1_1.matnr,
                                                                                                mbewh_1_1.bwkey,
                                                                                                mbewh_1_1.lfgja,
                                                                                                mbewh_1_1.lfmon,
                                                                                                SUM(mbewh_1_1.lbkum) AS lbkum,
                                                                                                SUM(mbewh_1_1.salk3) AS salk3                                                                        
                                                                                
                                                                                FROM            MBEWH mbewh_1_1
                                                                                
                                                                                INNER JOIN(
                                                                                                        SELECT
                                                                                                        DISTINCT ON     (mbewh_1_1.matnr, mbewh_1_1.bwkey)
                                                                                                                        mbewh_1_1.matnr,
                                                                                                                        mbewh_1_1.bwkey,
                                                                                                                        mbewh_1_1.lfgja,
                                                                                                                        mbewh_1_1.lfmon
                                                                                                        FROM            MBEWH mbewh_1_1
                                                                                                        WHERE           to_date(mbewh_1_1.lfgja::text || LPAD(mbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                                       
                                                                                                        ORDER BY        mbewh_1_1.matnr, 
                                                                                                                        mbewh_1_1.bwkey,
                                                                                                                        mbewh_1_1.lfgja DESC,
                                                                                                                        mbewh_1_1.lfmon DESC
                                                                                )               AS mbewh_1_2 
                                                                                ON              mbewh_1_1.matnr = mbewh_1_2.matnr
                                                                                AND             mbewh_1_1.bwkey = mbewh_1_2.bwkey
                                                                                AND             mbewh_1_1.lfgja = mbewh_1_2.lfgja
                                                                                AND             mbewh_1_1.lfmon = mbewh_1_2.lfmon
                                                                                
                                                                                WHERE           to_date(mbewh_1_1.lfgja::text || LPAD(mbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                GROUP BY        mbewh_1_1.matnr,
                                                                                                mbewh_1_1.bwkey,
                                                                                                mbewh_1_1.lfgja,
                                                                                                mbewh_1_1.lfmon
                                                                                HAVING          bool_and(mbewh_1_1.bwtar IS NOT NULL)
                                                                                ORDER BY        mbewh_1_1.lfgja DESC,
                                                                                                mbewh_1_1.lfmon DESC
                                                                        )
                                                                        --PARTE QUE SOLO TRAE BWTAR NULO
                                                                        --COSA QUE DEBIERA OCURRIR EN LA MAYORIA DE LOS CASOS                
                                                                        UNION ALL
                                                                        (
                                                                                SELECT
                                                                                DISTINCT ON     (mbewh_1_1.matnr, mbewh_1_1.bwkey)
                                                                                                mbewh_1_1.matnr,
                                                                                                mbewh_1_1.bwkey,
                                                                                                mbewh_1_1.lfgja,
                                                                                                mbewh_1_1.lfmon,
                                                                                                mbewh_1_1.lbkum,
                                                                                                mbewh_1_1.salk3
                                                                                FROM            MBEWH mbewh_1_1
                                                                                WHERE           to_date(mbewh_1_1.lfgja::text || LPAD(mbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                AND             mbewh_1_1.bwtar IS NULL
                                                                                
                                                                                ORDER BY        mbewh_1_1.matnr, 
                                                                                                mbewh_1_1.bwkey,
                                                                                                mbewh_1_1.lfgja DESC,
                                                                                                mbewh_1_1.lfmon DESC
                                                                        )
                                                                        --------------------------------------------------------
                                                                        --MBEW
                                                                        --------------------------------------------------------
                                                                        UNION ALL
                                                                        (
                                                                                SELECT
                                                                                DISTINCT ON     (mbew_1_1.matnr, mbew_1_1.bwkey)
                                                                                                mbew_1_1.matnr,
                                                                                                mbew_1_1.bwkey,
                                                                                                mbew_1_1.lfgja,
                                                                                                mbew_1_1.lfmon,
                                                                                                mbew_1_1.lbkum,
                                                                                                mbew_1_1.salk3
                                                                                FROM            MBEW mbew_1_1
                                                                                WHERE           to_date(mbew_1_1.lfgja::text || LPAD(mbew_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                AND             mbew_1_1.bwtar IS NULL
                                                                                
                                                                                ORDER BY        mbew_1_1.matnr, 
                                                                                                mbew_1_1.bwkey,
                                                                                                mbew_1_1.lfgja DESC,
                                                                                                mbew_1_1.lfmon DESC  
                                                                        )
                                                        )               AS mbewh_1_1
                                                        ORDER BY        mbewh_1_1.matnr, 
                                                                        mbewh_1_1.bwkey,
                                                                        mbewh_1_1.lfgja DESC,
                                                                        mbewh_1_1.lfmon DESC
                                                ) AS mbewh_1_ini
                                
                                --VALORACION TERMINO
                                ON              mbew_1.matnr = mbewh_1_ini.matnr
                                AND             mbew_1.bwkey = mbewh_1_ini.bwkey                                
                                LEFT JOIN      (
                                                        SELECT
                                                        DISTINCT ON     (mbewh_1_1.matnr, mbewh_1_1.bwkey)
                                                                        mbewh_1_1.matnr,
                                                                        mbewh_1_1.bwkey,
                                                                        mbewh_1_1.lfgja,
                                                                        mbewh_1_1.lfmon,
                                                                        mbewh_1_1.lbkum,
                                                                        mbewh_1_1.salk3
                                                        FROM(                 
                                                                        --------------------------------------------------------
                                                                        --MBEWH
                                                                        --------------------------------------------------------
                                                                        (
                                                                                SELECT
                                                                                                mbewh_1_1.matnr,
                                                                                                mbewh_1_1.bwkey,
                                                                                                mbewh_1_1.lfgja,
                                                                                                mbewh_1_1.lfmon,
                                                                                                SUM(mbewh_1_1.lbkum) AS lbkum,
                                                                                                SUM(mbewh_1_1.salk3) AS salk3                                                                        
                                                                                
                                                                                FROM            MBEWH mbewh_1_1
                                                                                
                                                                                INNER JOIN(
                                                                                                        SELECT
                                                                                                        DISTINCT ON     (mbewh_1_1.matnr, mbewh_1_1.bwkey)
                                                                                                                        mbewh_1_1.matnr,
                                                                                                                        mbewh_1_1.bwkey,
                                                                                                                        mbewh_1_1.lfgja,
                                                                                                                        mbewh_1_1.lfmon
                                                                                                        FROM            MBEWH mbewh_1_1
                                                                                                        WHERE           to_date(mbewh_1_1.lfgja::text || LPAD(mbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                                        
                                                                                                        ORDER BY        mbewh_1_1.matnr, 
                                                                                                                        mbewh_1_1.bwkey,
                                                                                                                        mbewh_1_1.lfgja ASC,
                                                                                                                        mbewh_1_1.lfmon ASC
                                                                                )               AS mbewh_1_2 
                                                                                ON              mbewh_1_1.matnr = mbewh_1_2.matnr
                                                                                AND             mbewh_1_1.bwkey = mbewh_1_2.bwkey
                                                                                AND             mbewh_1_1.lfgja = mbewh_1_2.lfgja
                                                                                AND             mbewh_1_1.lfmon = mbewh_1_2.lfmon
                                                                                
                                                                                WHERE           to_date(mbewh_1_1.lfgja::text || LPAD(mbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                
                                                                                
                                                                                GROUP BY        mbewh_1_1.matnr,
                                                                                                mbewh_1_1.bwkey,
                                                                                                mbewh_1_1.lfgja,
                                                                                                mbewh_1_1.lfmon
                                                                                HAVING          bool_and(mbewh_1_1.bwtar IS NOT NULL)
                                                                                ORDER BY        mbewh_1_1.lfgja ASC,
                                                                                                mbewh_1_1.lfmon ASC
                                                                        )
                                                                        --PARTE QUE SOLO TRAE BWTAR NULO
                                                                        --COSA QUE DEBIERA OCURRIR EN LA MAYORIA DE LOS CASOS                
                                                                        UNION ALL
                                                                        (
                                                                                SELECT
                                                                                DISTINCT ON     (mbewh_1_1.matnr, mbewh_1_1.bwkey)
                                                                                                mbewh_1_1.matnr,
                                                                                                mbewh_1_1.bwkey,
                                                                                                mbewh_1_1.lfgja,
                                                                                                mbewh_1_1.lfmon,
                                                                                                mbewh_1_1.lbkum,
                                                                                                mbewh_1_1.salk3
                                                                                FROM            MBEWH mbewh_1_1
                                                                                WHERE           to_date(mbewh_1_1.lfgja::text || LPAD(mbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >=
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                AND             mbewh_1_1.bwtar IS NULL
                                                                                
                                                                                
                                                                                ORDER BY        mbewh_1_1.matnr, 
                                                                                                mbewh_1_1.bwkey,
                                                                                                mbewh_1_1.lfgja ASC,
                                                                                                mbewh_1_1.lfmon ASC
                                                                        )
                                                                        --------------------------------------------------------
                                                                        --MBEW
                                                                        --------------------------------------------------------
                                                                        UNION ALL
                                                                        (
                                                                                SELECT
                                                                                DISTINCT ON     (mbew_1_1.matnr, mbew_1_1.bwkey)
                                                                                                mbew_1_1.matnr,
                                                                                                mbew_1_1.bwkey,
                                                                                                mbew_1_1.lfgja,
                                                                                                mbew_1_1.lfmon,
                                                                                                mbew_1_1.lbkum,
                                                                                                mbew_1_1.salk3
                                                                                FROM            MBEW mbew_1_1
                                                                                WHERE           to_date(mbew_1_1.lfgja::text || LPAD(mbew_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >=
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                AND             mbew_1_1.bwtar IS NULL
                                                                                
                                                                                
                                                                                ORDER BY        mbew_1_1.matnr, 
                                                                                                mbew_1_1.bwkey,
                                                                                                mbew_1_1.lfgja ASC,
                                                                                                mbew_1_1.lfmon ASC  
                                                                        )
                                                        )               AS mbewh_1_1
                                                        ORDER BY        mbewh_1_1.matnr, 
                                                                        mbewh_1_1.bwkey,
                                                                        mbewh_1_1.lfgja ASC,
                                                                        mbewh_1_1.lfmon ASC
                                                ) AS mbewh_1_ter
                                ON              mbew_1.matnr = mbewh_1_ter.matnr
                                AND             mbew_1.bwkey = mbewh_1_ter.bwkey

                                LEFT JOIN      (
                                                        SELECT
                                                                        mbewh_1_1.matnr,
                                                                        mbewh_1_1.bwkey,
                                                                        mbewh_1_1.bwtar,
                                                                        COUNT(*) AS count_mbewh
                                                        FROM            MBEWH mbewh_1_1
                                                        WHERE           to_date(mbewh_1_1.lfgja::text || LPAD(mbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                        AND             mbewh_1_1.bwtar IS NULL
                                                        GROUP BY        mbewh_1_1.matnr,
                                                                        mbewh_1_1.bwkey,
                                                                        mbewh_1_1.bwtar
                                                ) AS mbewh_1_count
                                ON              mbew_1.matnr = mbewh_1_count.matnr
                                AND             mbew_1.bwkey = mbewh_1_count.bwkey
                               
                                WHERE           

                                                mbewh_1_ini.lfgja IS NOT NULL
                                                        
                                --ELIMINA REGISTROS VIEJOS
                                --Y QUE SOLO TIENEN EL HITO DE CREACION
                                --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                AND NOT             
                                        (
                                                mbewh_1_ter.lfgja IS NULL
                                        AND             
                                                mbewh_1_ini.salk3 = 0.0
                                        )                                
                        ) AS mbewh
        ON              mbewh.matnr = mbew.matnr
        AND             mbewh.bwkey = mbew.bwkey --REEMPLAZO DE WERKS
       
        --------------------------------------------------------------------------------------------------------------------
        --QBEW STOCK PROYECTO "Q"
        --------------------------------------------------------------------------------------------------------------------
        LEFT JOIN      (
                                SELECT          qbewh_1_ini.matnr,
                                                qbewh_1_ini.bwkey,
                                                
                                                qbewh_1_ini.lfgja       AS ini_lfgja,
                                                qbewh_1_ini.lfmon       AS ini_lfmon,
                                                qbewh_1_ini.lbkum       AS ini_lbkum,
                                                qbewh_1_ini.salk3       AS ini_salk3,
                                                
                                                qbewh_1_ter.lfgja       AS ter_lfgja,
                                                qbewh_1_ter.lfmon       AS ter_lfmon,
                                                qbewh_1_ter.lbkum       AS ter_lbkum,
                                                qbewh_1_ter.salk3       AS ter_salk3,
                                                
                                                qbewh_1_count.count_qbewh
                                                                        AS count_qbewh
                                --SELECTOR DE REGISTROS
                                FROM            (
                                                (
                                                SELECT          qbew_1_1.matnr,
                                                                qbew_1_1.bwkey
                                                FROM            QBEW qbew_1_1
                                                GROUP BY        qbew_1_1.matnr,
                                                                qbew_1_1.bwkey
                                                )
                                                UNION
                                                (
                                                SELECT          qbewh_1_1.matnr,
                                                                qbewh_1_1.bwkey
                                                FROM            QBEWH qbewh_1_1
                                                GROUP BY        qbewh_1_1.matnr,
                                                                qbewh_1_1.bwkey
                                                )
                                                ) AS qbew_1
                                --VALORACION INICIO

                                LEFT JOIN       (
                                                        SELECT
                                                        DISTINCT ON     (qbewh_1_1.matnr, qbewh_1_1.bwkey)
                                                                        qbewh_1_1.matnr,
                                                                        qbewh_1_1.bwkey,
                                                                        qbewh_1_1.lfgja,
                                                                        qbewh_1_1.lfmon,
                                                                        qbewh_1_1.lbkum,
                                                                        qbewh_1_1.salk3
                                                        FROM(                 
                                                                        --------------------------------------------------------
                                                                        --QBEWH
                                                                        --------------------------------------------------------
                                                                        (
                                                                                SELECT
                                                                                                qbewh_1_1.matnr,
                                                                                                qbewh_1_1.bwkey,
                                                                                                qbewh_1_1.lfgja,
                                                                                                qbewh_1_1.lfmon,
                                                                                                SUM(qbewh_1_1.lbkum) AS lbkum,
                                                                                                SUM(qbewh_1_1.salk3) AS salk3                                                                        
                                                                                
                                                                                FROM            QBEWH qbewh_1_1
                                                                                
                                                                                INNER JOIN(
                                                                                                        SELECT
                                                                                                        DISTINCT ON     (qbewh_1_1.matnr, qbewh_1_1.bwkey)
                                                                                                                        qbewh_1_1.matnr,
                                                                                                                        qbewh_1_1.bwkey,
                                                                                                                        qbewh_1_1.lfgja,
                                                                                                                        qbewh_1_1.lfmon
                                                                                                        FROM            QBEWH qbewh_1_1
                                                                                                        WHERE           to_date(qbewh_1_1.lfgja::text || LPAD(qbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                                        
                                                                                                        ORDER BY        qbewh_1_1.matnr, 
                                                                                                                        qbewh_1_1.bwkey,
                                                                                                                        qbewh_1_1.lfgja DESC,
                                                                                                                        qbewh_1_1.lfmon DESC
                                                                                )               AS qbewh_1_2 
                                                                                ON              qbewh_1_1.matnr = qbewh_1_2.matnr
                                                                                AND             qbewh_1_1.bwkey = qbewh_1_2.bwkey
                                                                                AND             qbewh_1_1.lfgja = qbewh_1_2.lfgja
                                                                                AND             qbewh_1_1.lfmon = qbewh_1_2.lfmon
                                                                                
                                                                                WHERE           to_date(qbewh_1_1.lfgja::text || LPAD(qbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                
                                                                                
                                                                                GROUP BY        qbewh_1_1.matnr,
                                                                                                qbewh_1_1.bwkey,
                                                                                                qbewh_1_1.lfgja,
                                                                                                qbewh_1_1.lfmon
                                                                                HAVING          bool_and(qbewh_1_1.bwtar IS NOT NULL)
                                                                                ORDER BY        qbewh_1_1.lfgja DESC,
                                                                                                qbewh_1_1.lfmon DESC
                                                                        )
                                                                        --PARTE QUE SOLO TRAE BWTAR NULO
                                                                        --COSA QUE DEBIERA OCURRIR EN LA MAYORIA DE LOS CASOS                
                                                                        UNION ALL
                                                                        (
                                                                                SELECT
                                                                                DISTINCT ON     (qbewh_1_1.matnr, qbewh_1_1.bwkey)
                                                                                                qbewh_1_1.matnr,
                                                                                                qbewh_1_1.bwkey,
                                                                                                qbewh_1_1.lfgja,
                                                                                                qbewh_1_1.lfmon,
                                                                                                qbewh_1_1.lbkum,
                                                                                                qbewh_1_1.salk3
                                                                                FROM            QBEWH qbewh_1_1
                                                                                WHERE           to_date(qbewh_1_1.lfgja::text || LPAD(qbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                AND             qbewh_1_1.bwtar IS NULL
                                                                                
                                                                                
                                                                                ORDER BY        qbewh_1_1.matnr, 
                                                                                                qbewh_1_1.bwkey,
                                                                                                qbewh_1_1.lfgja DESC,
                                                                                                qbewh_1_1.lfmon DESC
                                                                        )
                                                                        --------------------------------------------------------
                                                                        --QBEW
                                                                        --------------------------------------------------------
                                                                        UNION ALL
                                                                        (
                                                                                SELECT
                                                                                DISTINCT ON     (qbew_1_1.matnr, qbew_1_1.bwkey)
                                                                                                qbew_1_1.matnr,
                                                                                                qbew_1_1.bwkey,
                                                                                                qbew_1_1.lfgja,
                                                                                                qbew_1_1.lfmon,
                                                                                                qbew_1_1.lbkum,
                                                                                                qbew_1_1.salk3
                                                                                FROM            QBEW qbew_1_1
                                                                                WHERE           to_date(qbew_1_1.lfgja::text || LPAD(qbew_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                AND             qbew_1_1.bwtar IS NULL
                                                                                
                                                                                
                                                                                ORDER BY        qbew_1_1.matnr, 
                                                                                                qbew_1_1.bwkey,
                                                                                                qbew_1_1.lfgja DESC,
                                                                                                qbew_1_1.lfmon DESC  
                                                                        )
                                                                        UNION ALL
                                                                        
                                                                        (
                                                                                SELECT
                                                                                                qbew_1_1.matnr,
                                                                                                qbew_1_1.bwkey,
                                                                                                qbew_1_1.lfgja,
                                                                                                qbew_1_1.lfmon,
                                                                                                SUM(qbew_1_1.lbkum) AS lbkum,
                                                                                                SUM(qbew_1_1.salk3) AS salk3                                                                        
                                                                                
                                                                                FROM            QBEW qbew_1_1
                                                                                
                                                                                INNER JOIN(
                                                                                                        SELECT
                                                                                                        DISTINCT ON     (qbew_1_1.matnr, qbew_1_1.bwkey)
                                                                                                                        qbew_1_1.matnr,
                                                                                                                        qbew_1_1.bwkey,
                                                                                                                        qbew_1_1.lfgja,
                                                                                                                        qbew_1_1.lfmon
                                                                                                        FROM            QBEW qbew_1_1
                                                                                                        WHERE           to_date(qbew_1_1.lfgja::text || LPAD(qbew_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                                        
                                                                                                        ORDER BY        qbew_1_1.matnr, 
                                                                                                                        qbew_1_1.bwkey,
                                                                                                                        qbew_1_1.lfgja DESC,
                                                                                                                        qbew_1_1.lfmon DESC
                                                                                )               AS qbew_1_2 
                                                                                ON              qbew_1_1.matnr = qbew_1_2.matnr
                                                                                AND             qbew_1_1.bwkey = qbew_1_2.bwkey
                                                                                AND             qbew_1_1.lfgja = qbew_1_2.lfgja
                                                                                AND             qbew_1_1.lfmon = qbew_1_2.lfmon
                                                                                
                                                                                WHERE           to_date(qbew_1_1.lfgja::text || LPAD(qbew_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                                                
                                                                                
                                                                                GROUP BY        qbew_1_1.matnr,
                                                                                                qbew_1_1.bwkey,
                                                                                                qbew_1_1.lfgja,
                                                                                                qbew_1_1.lfmon
                                                                                HAVING          bool_and(qbew_1_1.bwtar IS NOT NULL)
                                                                                ORDER BY        qbew_1_1.lfgja DESC,
                                                                                                qbew_1_1.lfmon DESC
                                                                        )
                                                        )               AS qbewh_1_1
                                                        ORDER BY        qbewh_1_1.matnr, 
                                                                        qbewh_1_1.bwkey,
                                                                        qbewh_1_1.lfgja DESC,
                                                                        qbewh_1_1.lfmon DESC
                                                ) AS qbewh_1_ini
                                
                                --VALORACION TERMINO

                                ON              qbew_1.matnr = qbewh_1_ini.matnr
                                AND             qbew_1.bwkey = qbewh_1_ini.bwkey                                
                                LEFT JOIN      (
                                                        SELECT
                                                        DISTINCT ON     (qbewh_1_1.matnr, qbewh_1_1.bwkey)
                                                                        qbewh_1_1.matnr,
                                                                        qbewh_1_1.bwkey,
                                                                        qbewh_1_1.lfgja,
                                                                        qbewh_1_1.lfmon,
                                                                        qbewh_1_1.lbkum,
                                                                        qbewh_1_1.salk3
                                                        FROM(                 
                                                                        --------------------------------------------------------
                                                                        --QBEWH
                                                                        --------------------------------------------------------

                                                                        (
                                                                                SELECT
                                                                                                qbewh_1_1.matnr,
                                                                                                qbewh_1_1.bwkey,
                                                                                                qbewh_1_1.lfgja,
                                                                                                qbewh_1_1.lfmon,
                                                                                                SUM(qbewh_1_1.lbkum) AS lbkum,
                                                                                                SUM(qbewh_1_1.salk3) AS salk3                                                                        
                                                                                
                                                                                FROM            QBEWH qbewh_1_1
                                                                                
                                                                                INNER JOIN(
                                                                                                        SELECT
                                                                                                        DISTINCT ON     (qbewh_1_1.matnr, qbewh_1_1.bwkey)
                                                                                                                        qbewh_1_1.matnr,
                                                                                                                        qbewh_1_1.bwkey,
                                                                                                                        qbewh_1_1.lfgja,
                                                                                                                        qbewh_1_1.lfmon
                                                                                                        FROM            QBEWH qbewh_1_1
                                                                                                        WHERE           to_date(qbewh_1_1.lfgja::text || LPAD(qbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                                        
                                                                                                        ORDER BY        qbewh_1_1.matnr, 
                                                                                                                        qbewh_1_1.bwkey,
                                                                                                                        qbewh_1_1.lfgja ASC,
                                                                                                                        qbewh_1_1.lfmon ASC
                                                                                )               AS qbewh_1_2 
                                                                                ON              qbewh_1_1.matnr = qbewh_1_2.matnr
                                                                                AND             qbewh_1_1.bwkey = qbewh_1_2.bwkey
                                                                                AND             qbewh_1_1.lfgja = qbewh_1_2.lfgja
                                                                                AND             qbewh_1_1.lfmon = qbewh_1_2.lfmon
                                                                                
                                                                                WHERE           to_date(qbewh_1_1.lfgja::text || LPAD(qbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                
                                                                                
                                                                                GROUP BY        qbewh_1_1.matnr,
                                                                                                qbewh_1_1.bwkey,
                                                                                                qbewh_1_1.lfgja,
                                                                                                qbewh_1_1.lfmon
                                                                                HAVING          bool_and(qbewh_1_1.bwtar IS NOT NULL)
                                                                                ORDER BY        qbewh_1_1.lfgja ASC,
                                                                                                qbewh_1_1.lfmon ASC
                                                                        )
                                                                        --PARTE QUE SOLO TRAE BWTAR NULO
                                                                        --COSA QUE DEBIERA OCURRIR EN LA MAYORIA DE LOS CASOS                
                                                                        UNION ALL
                                                                        (
                                                                                SELECT
                                                                                DISTINCT ON     (qbewh_1_1.matnr, qbewh_1_1.bwkey)
                                                                                                qbewh_1_1.matnr,
                                                                                                qbewh_1_1.bwkey,
                                                                                                qbewh_1_1.lfgja,
                                                                                                qbewh_1_1.lfmon,
                                                                                                qbewh_1_1.lbkum,
                                                                                                qbewh_1_1.salk3
                                                                                FROM            QBEWH qbewh_1_1
                                                                                WHERE           to_date(qbewh_1_1.lfgja::text || LPAD(qbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >=
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                AND             qbewh_1_1.bwtar IS NULL
                                                                                
                                                                                
                                                                                ORDER BY        qbewh_1_1.matnr, 
                                                                                                qbewh_1_1.bwkey,
                                                                                                qbewh_1_1.lfgja ASC,
                                                                                                qbewh_1_1.lfmon ASC
                                                                        )
                                                                        --------------------------------------------------------
                                                                        --MBEW
                                                                        --------------------------------------------------------
                                                                        UNION ALL
                                                                        (
                                                                                SELECT
                                                                                DISTINCT ON     (qbew_1_1.matnr, qbew_1_1.bwkey)
                                                                                                qbew_1_1.matnr,
                                                                                                qbew_1_1.bwkey,
                                                                                                qbew_1_1.lfgja,
                                                                                                qbew_1_1.lfmon,
                                                                                                qbew_1_1.lbkum,
                                                                                                qbew_1_1.salk3
                                                                                FROM            QBEW qbew_1_1
                                                                                WHERE           to_date(qbew_1_1.lfgja::text || LPAD(qbew_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >=
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                AND             qbew_1_1.bwtar IS NULL
                                                                                
                                                                                
                                                                                ORDER BY        qbew_1_1.matnr, 
                                                                                                qbew_1_1.bwkey,
                                                                                                qbew_1_1.lfgja ASC,
                                                                                                qbew_1_1.lfmon ASC  
                                                                        )
                                                                        UNION ALL

                                                                        (
                                                                                SELECT
                                                                                                qbew_1_1.matnr,
                                                                                                qbew_1_1.bwkey,
                                                                                                qbew_1_1.lfgja,
                                                                                                qbew_1_1.lfmon,
                                                                                                SUM(qbew_1_1.lbkum) AS lbkum,
                                                                                                SUM(qbew_1_1.salk3) AS salk3                                                                        
                                                                                
                                                                                FROM            QBEW qbew_1_1
                                                                                
                                                                                INNER JOIN(
                                                                                                        SELECT
                                                                                                        DISTINCT ON     (qbew_1_1.matnr, qbew_1_1.bwkey)
                                                                                                                        qbew_1_1.matnr,
                                                                                                                        qbew_1_1.bwkey,
                                                                                                                        qbew_1_1.lfgja,
                                                                                                                        qbew_1_1.lfmon
                                                                                                        FROM            QBEW qbew_1_1
                                                                                                        WHERE           to_date(qbew_1_1.lfgja::text || LPAD(qbew_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                                        
                                                                                                        ORDER BY        qbew_1_1.matnr, 
                                                                                                                        qbew_1_1.bwkey,
                                                                                                                        qbew_1_1.lfgja ASC,
                                                                                                                        qbew_1_1.lfmon ASC
                                                                                )               AS qbew_1_2 
                                                                                ON              qbew_1_1.matnr = qbew_1_2.matnr
                                                                                AND             qbew_1_1.bwkey = qbew_1_2.bwkey
                                                                                AND             qbew_1_1.lfgja = qbew_1_2.lfgja
                                                                                AND             qbew_1_1.lfmon = qbew_1_2.lfmon
                                                                                
                                                                                WHERE           to_date(qbew_1_1.lfgja::text || LPAD(qbew_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                                                
                                                                                
                                                                                GROUP BY        qbew_1_1.matnr,
                                                                                                qbew_1_1.bwkey,
                                                                                                qbew_1_1.lfgja,
                                                                                                qbew_1_1.lfmon
                                                                                HAVING          bool_and(qbew_1_1.bwtar IS NOT NULL)
                                                                                ORDER BY        qbew_1_1.lfgja ASC,
                                                                                                qbew_1_1.lfmon ASC
                                                                        )
                                                        )               AS qbewh_1_1
                                                        ORDER BY        qbewh_1_1.matnr, 
                                                                        qbewh_1_1.bwkey,
                                                                        qbewh_1_1.lfgja ASC,
                                                                        qbewh_1_1.lfmon ASC
                                                ) AS qbewh_1_ter
                                ON              qbew_1.matnr = qbewh_1_ter.matnr
                                AND             qbew_1.bwkey = qbewh_1_ter.bwkey

                                LEFT JOIN      (
                                                        SELECT
                                                                        qbewh_1_1.matnr,
                                                                        qbewh_1_1.bwkey,
                                                                        qbewh_1_1.bwtar,
                                                                        COUNT(*) AS count_qbewh
                                                        FROM            QBEWH qbewh_1_1
                                                        WHERE           to_date(qbewh_1_1.lfgja::text || LPAD(qbewh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                        AND             qbewh_1_1.bwtar IS NULL
                                                        GROUP BY        qbewh_1_1.matnr,
                                                                        qbewh_1_1.bwkey,
                                                                        qbewh_1_1.bwtar
                                                ) AS qbewh_1_count
                                ON              qbew_1.matnr = qbewh_1_count.matnr
                                AND             qbew_1.bwkey = qbewh_1_count.bwkey
                                
                                WHERE           
                                                qbewh_1_ini.lfgja IS NOT NULL
                                                        
                                --ELIMINA REGISTROS VIEJOS
                                --Y QUE SOLO TIENEN EL HITO DE CREACION
                                --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                AND NOT             
                                        (
                                                qbewh_1_ter.lfgja IS NULL
                                        AND             
                                                qbewh_1_ini.salk3 = 0.0
                                        )            
                       ) AS qbewh
        ON              qbewh.matnr = mbew.matnr
        AND             qbewh.bwkey = mbew.bwkey --REEMPLAZO DE WERKS
        
        --------------------------------------------------------------------------------------------------------------------
        --BSIM
        --------------------------------------------------------------------------------------------------------------------        
        --REVALORACIONES

        LEFT JOIN
                (
                        SELECT                  bsim_1.id,
                                                bsim_1.matnr,
                                                bsim_1.bwkey,
                                                bsim_1.bwtar,
                                                bsim_1.buzid,
                                                bsim_1.shkzg,
                                                bsim_1.dmbtr,
                                                bsim_1.menge,
                                                bsim_1.budat,
                                                bsim_1.bldat,
                                                bsim_1.blart,
                                                t001k_1.bukrs,
                                                bsim_1.gjahr,
                                                bsim_1.belnr,
                                                bsim_1.buzei,
                                                SUBSTRING(bkpf_1.awkey FROM 1 FOR 10) AS mblnr,
                                                CAST(SUBSTRING(bkpf_1.awkey FROM 11 FOR 4) AS BIGINT) AS mjahr                                                 
                        FROM                    BSIM bsim_1
                        INNER JOIN              T001K t001k_1
                        ON                      bsim_1.bwkey = t001k_1.bwkey
                        INNER JOIN              BKPF bkpf_1
                        ON                      bkpf_1.belnr = bsim_1.belnr
                        AND                     bkpf_1.gjahr = bsim_1.gjahr
                        AND                     bkpf_1.bukrs = t001k_1.bukrs
                ) AS bsim
        ON              mbew.matnr = bsim.matnr
        AND             mbew.bwkey = bsim.bwkey
        AND             bsim.budat::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
        AND             bsim.budat::bigint < 
                                to_char(
                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                        + INTERVAL '1 month', 'YYYYMMDD')::bigint
        
        --SOLO CENTROS VIGENTES
        WHERE           t001w.werks IN ('0000','0001','0002','0003','0005','0006','0007','0008','0009','0010','0011','0012')
        
       
        --RESTRICCION DE DATOS A PROCESAR
        AND             t001k.bukrs IN ('SAP1', 'SAP2')        
        
        AND             (
                                (mbewh.ini_lfgja IS NOT NULL AND qbewh.ini_lfgja IS NULL)
                        OR
                                (mbewh.ini_lfgja IS NULL AND qbewh.ini_lfgja IS NOT NULL)
                        OR
                                (mbewh.ini_lfgja IS NOT NULL AND qbewh.ini_lfgja IS NOT NULL)
                        )
                                
        AND NOT         (
                                (mbewh.ter_lfgja IS NULL AND mbewh.ini_salk3 = 0.0)
                        AND
                                (qbewh.ter_lfgja IS NULL AND qbewh.ini_salk3 = 0.0)
                        AND
                                (mbewh.ini_lfgja < 2015 AND qbewh.ini_lfgja < 2015)
                        )

        
        GROUP BY        t001k.bukrs,
                        t001w.werks,
                        mbew.bwkey,
                        mbew.matnr,
                        mara.matkl,
                        
                        --E4
                        marc.prctr,
        
                        mbewh.ini_lfgja,
                        mbewh.ini_lfmon,
                        mbewh.ter_lfgja,
                        mbewh.ter_lfmon,
                        
                        qbewh.ini_lfgja,
                        qbewh.ini_lfmon,
                        qbewh.ter_lfgja,
                        qbewh.ter_lfmon
        ;

END;
$func$
LANGUAGE plpgsql@
