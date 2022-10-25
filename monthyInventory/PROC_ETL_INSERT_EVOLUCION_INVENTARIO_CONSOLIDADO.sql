DROP FUNCTION PROC_ETL_INSERT_EVOLUCION_INVENTARIO_CONSOLIDADO@
CREATE OR REPLACE FUNCTION PROC_ETL_INSERT_EVOLUCION_INVENTARIO_CONSOLIDADO(
                                        IN_ANNO_PROCESO VARCHAR(255),
					IN_MES_PROCESO VARCHAR(255))
RETURNS VOID AS $func$
BEGIN

        INSERT INTO SCM_EVOLUCION_INVENTARIO(
                ANNO_PROCESO,
                MES_PROCESO,
                SOCIEDAD,
                CENTRO,
                MATERIAL, 
                CLASE_ARTICULO, 
                
                --INICIO
                CANTIDAD_INICIO,
                CANTIDAD_INICIO_TRANSITO_ALMACEN,
                CANTIDAD_INICIO_CONTROL_CALIDAD,
                CANTIDAD_INICIO_BLOQUEADA,
                CANTIDAD_INICIO_VTA,
                CANTIDAD_INICIO_CONTROL_CALIDAD_VTA,
                CANTIDAD_INICIO_BLOQUEADA_VTA,
                CANTIDAD_INICIO_CONSIG,
                CANTIDAD_INICIO_CONTROL_CALIDAD_CONSIG,
                CANTIDAD_INICIO_VALORACION,
                MONTO_INICIO_VALORACION,
                CANTIDAD_INICIO_VALORACION_PROY,
                MONTO_INICIO_VALORACION_PROY,
                
                --MES
                CANTIDAD_ENTRADA,
                CANTIDAD_ENTRADA_TRANSITO_ALMACEN,
                CANTIDAD_ENTRADA_CONTROL_CALIDAD,
                CANTIDAD_ENTRADA_BLOQUEADA,
                CANTIDAD_EM_LIBRE,
                
                --E3
                CANTIDAD_EM_IMPORTACION_LIBRE,
                CANTIDAD_EM_STOCK_NAC_LIBRE,
                CANTIDAD_EM_TRASLADO_LIBRE,
                
                --E5
                CANT_OC_EM_NORMAL_LIBRE,
                CANT_OC_EM_URGENTE_LIBRE,
                CANT_OC_EM_IMPORTACION_LIBRE,
                CANT_OC_EM_STOCK_NAC_LIBRE,
                CANT_OC_EM_TRASLADO_LIBRE,
                
                --E8
                FECHA_ULT_EM_LIBRE,
                
                CANTIDAD_ENTRADA_VTA,
                CANTIDAD_ENTRADA_CONTROL_CALIDAD_VTA,
                CANTIDAD_ENTRADA_BLOQUEADA_VTA,
                CANTIDAD_ENTRADA_CONSIG,
                CANTIDAD_ENTRADA_CONTROL_CALIDAD_CONSIG,
                CANTIDAD_ENTRADA_VALORACION,
                MONTO_ENTRADA_VALORACION,
                CANTIDAD_SALIDA,
                CANTIDAD_SALIDA_TRANSITO_ALMACEN,
                CANTIDAD_SALIDA_CONTROL_CALIDAD,
                CANTIDAD_SALIDA_BLOQUEADA,
                CANTIDAD_SM_LIBRE,
                
                --E2
                CANTIDAD_SM_ORDEN_LIBRE,
                CANTIDAD_SM_TRASLADO_LIBRE,
                CANTIDAD_SM_CONSIGNACION_LIBRE,
                CANTIDAD_SM_PROYECTO_LIBRE,
                --E3
                CANTIDAD_SM_TRASP_STOCK_ESPECIAL_LIBRE,
                --E7
                CANTIDAD_SM_DESGUACE_LIBRE,
                CANTIDAD_SM_CECO_LIBRE,
                CANTIDAD_SM_OTROS_LIBRE,
                CANTIDAD_SM_BLOQUEADO_LIBRE,
                
                --E8
                FECHA_ULT_SM_CONSUMO_LIBRE,
                FECHA_ULT_SM_TRASLADO_LIBRE,
                FECHA_ULT_SM_TRASPASO_LIBRE,
                FECHA_ULT_SM_LIBRE,
                                
                CANTIDAD_SALIDA_VTA,
                CANTIDAD_SALIDA_CONTROL_CALIDAD_VTA,
                CANTIDAD_SALIDA_BLOQUEADA_VTA,
                CANTIDAD_SALIDA_CONSIG,
                CANTIDAD_SALIDA_CONTROL_CALIDAD_CONSIG,
                CANTIDAD_SALIDA_VALORACION,
                MONTO_SALIDA_VALORACION,
                
                --E4
                CENTRO_BENEFICIO,
                
                --TERMINO
                CANTIDAD_TERMINO,
                CANTIDAD_TERMINO_TRANSITO_ALMACEN,
                CANTIDAD_TERMINO_CONTROL_CALIDAD,
                CANTIDAD_TERMINO_BLOQUEADA,
                CANTIDAD_TERMINO_VTA,
                CANTIDAD_TERMINO_CONTROL_CALIDAD_VTA,
                CANTIDAD_TERMINO_BLOQUEADA_VTA,
                CANTIDAD_TERMINO_CONSIG,
                CANTIDAD_TERMINO_CONTROL_CALIDAD_CONSIG,
                CANTIDAD_TERMINO_VALORACION,
                MONTO_TERMINO_VALORACION,
                CANTIDAD_TERMINO_VALORACION_PROY,
                MONTO_TERMINO_VALORACION_PROY,
                
                FECHA_ACTUALIZACION,
                APLICACION_ACTUALIZACION
        )
        
        SELECT
                        IN_ANNO_PROCESO                                         AS ANNO_PROCESO,
                        IN_MES_PROCESO                                          AS MES_PROCESO,
                        t001k.bukrs                                             AS SOCIEDAD,
                        mard.CENTRO                                             AS CENTRO,
                        mard.MATERIAL                                           AS MATERIAL, 
                        mara.matkl                                              AS CLASE_ARTICULO, 
                        
                        COALESCE(SUM(almacen.CANTIDAD_INICIO), 0.0)                             AS CANTIDAD_INICIO,
                        COALESCE(SUM(almacen.CANTIDAD_INICIO_TRANSITO_ALMACEN), 0.0)            AS CANTIDAD_INICIO_TRANSITO_ALMACEN,
                        COALESCE(SUM(almacen.CANTIDAD_INICIO_CONTROL_CALIDAD), 0.0)             AS CANTIDAD_INICIO_CONTROL_CALIDAD,
                        COALESCE(SUM(almacen.CANTIDAD_INICIO_BLOQUEADA), 0.0)                   AS CANTIDAD_INICIO_BLOQUEADA,
                        COALESCE(SUM(almacen.CANTIDAD_INICIO_VTA), 0.0)                         AS CANTIDAD_INICIO_VTA,
                        COALESCE(SUM(almacen.CANTIDAD_INICIO_CONTROL_CALIDAD_VTA), 0.0)         AS CANTIDAD_INICIO_CONTROL_CALIDAD_VTA,
                        COALESCE(SUM(almacen.CANTIDAD_INICIO_BLOQUEADA_VTA), 0.0)               AS CANTIDAD_INICIO_BLOQUEADA_VTA,
                        COALESCE(MAX(centro.CANTIDAD_INICIO_CONSIG), 0.0)                       AS CANTIDAD_INICIO_CONSIG,
                        COALESCE(MAX(centro.CANTIDAD_INICIO_CONTROL_CALIDAD_CONSIG), 0.0)       AS CANTIDAD_INICIO_CONTROL_CALIDAD_CONSIG,
                        COALESCE(MAX(valoracion.CANTIDAD_INICIO_VALORACION), 0.0)               AS CANTIDAD_INICIO_VALORACION,
                        COALESCE(MAX(valoracion.MONTO_INICIO_VALORACION), 0.0)                  AS MONTO_INICIO_VALORACION,
                        COALESCE(MAX(valoracion.CANTIDAD_INICIO_VALORACION_PROY), 0.0)          AS CANTIDAD_INICIO_VALORACION_PROY,
                        COALESCE(MAX(valoracion.MONTO_INICIO_VALORACION_PROY), 0.0)             AS MONTO_INICIO_VALORACION_PROY,
                
                        --MES
                        COALESCE(SUM(almacen.CANTIDAD_ENTRADA), 0.0)                            AS CANTIDAD_ENTRADA,
                        COALESCE(SUM(almacen.CANTIDAD_ENTRADA_TRANSITO_ALMACEN), 0.0)           AS CANTIDAD_ENTRADA_TRANSITO_ALMACEN,
                        COALESCE(SUM(almacen.CANTIDAD_ENTRADA_CONTROL_CALIDAD), 0.0)            AS CANTIDAD_ENTRADA_CONTROL_CALIDAD,
                        COALESCE(SUM(almacen.CANTIDAD_ENTRADA_BLOQUEADA), 0.0)                  AS CANTIDAD_ENTRADA_BLOQUEADA,
                        COALESCE(SUM(almacen.CANTIDAD_EM_LIBRE), 0.0)                           AS CANTIDAD_EM_LIBRE,
                        
                        --E3
                        COALESCE(SUM(almacen.CANTIDAD_EM_IMPORTACION_LIBRE), 0.0)               AS CANTIDAD_EM_IMPORTACION_LIBRE,
                        COALESCE(SUM(almacen.CANTIDAD_EM_STOCK_NAC_LIBRE), 0.0)                 AS CANTIDAD_EM_STOCK_NAC_LIBRE,
                        COALESCE(SUM(almacen.CANTIDAD_EM_TRASLADO_LIBRE), 0.0)                  AS CANTIDAD_EM_TRASLADO_LIBRE,
                        
                        --E5
                        COALESCE(SUM(almacen.CANT_OC_EM_NORMAL_LIBRE), 0.0)                     AS CANT_OC_EM_NORMAL_LIBRE,
                        COALESCE(SUM(almacen.CANT_OC_EM_URGENTE_LIBRE), 0.0)                    AS CANT_OC_EM_URGENTE_LIBRE,
                        COALESCE(SUM(almacen.CANT_OC_EM_IMPORTACION_LIBRE), 0.0)                AS CANT_OC_EM_IMPORTACION_LIBRE,
                        COALESCE(SUM(almacen.CANT_OC_EM_STOCK_NAC_LIBRE), 0.0)                  AS CANT_OC_EM_STOCK_NAC_LIBRE,
                        COALESCE(SUM(almacen.CANT_OC_EM_TRASLADO_LIBRE), 0.0)                   AS CANT_OC_EM_TRASLADO_LIBRE,
                        
                        --E8
                        MAX(almacen.FECHA_ULT_EM_LIBRE::bigint)                                 AS FECHA_ULT_EM_LIBRE,
                        
                        COALESCE(SUM(almacen.CANTIDAD_ENTRADA_VTA), 0.0)                        AS CANTIDAD_ENTRADA_VTA,
                        COALESCE(SUM(almacen.CANTIDAD_ENTRADA_CONTROL_CALIDAD_VTA), 0.0)        AS CANTIDAD_ENTRADA_CONTROL_CALIDAD_VTA,
                        COALESCE(SUM(almacen.CANTIDAD_ENTRADA_BLOQUEADA_VTA), 0.0)              AS CANTIDAD_ENTRADA_BLOQUEADA_VTA,
                        COALESCE(MAX(centro.CANTIDAD_ENTRADA_CONSIG), 0.0)                      AS CANTIDAD_ENTRADA_CONSIG,
                        COALESCE(MAX(centro.CANTIDAD_ENTRADA_CONTROL_CALIDAD_CONSIG), 0.0)      AS CANTIDAD_ENTRADA_CONTROL_CALIDAD_CONSIG,
                        COALESCE(MAX(valoracion.CANTIDAD_ENTRADA_VALORACION), 0.0)              AS CANTIDAD_ENTRADA_VALORACION,
                        COALESCE(MAX(valoracion.MONTO_ENTRADA_VALORACION), 0.0)                 AS MONTO_ENTRADA_VALORACION,
                        COALESCE(SUM(almacen.CANTIDAD_SALIDA), 0.0)                             AS CANTIDAD_SALIDA,
                        COALESCE(SUM(almacen.CANTIDAD_SALIDA_TRANSITO_ALMACEN), 0.0)            AS CANTIDAD_SALIDA_TRANSITO_ALMACEN,
                        COALESCE(SUM(almacen.CANTIDAD_SALIDA_CONTROL_CALIDAD), 0.0)             AS CANTIDAD_SALIDA_CONTROL_CALIDAD,
                        COALESCE(SUM(almacen.CANTIDAD_SALIDA_BLOQUEADA), 0.0)                   AS CANTIDAD_SALIDA_BLOQUEADA,
                        COALESCE(SUM(almacen.CANTIDAD_SM_LIBRE), 0.0)                           AS CANTIDAD_SM_LIBRE,
                        
                        --E2
                        COALESCE(SUM(almacen.CANTIDAD_SM_ORDEN_LIBRE), 0.0)                     AS CANTIDAD_SM_ORDEN_LIBRE,
                        COALESCE(SUM(almacen.CANTIDAD_SM_TRASLADO_LIBRE), 0.0)                  AS CANTIDAD_SM_TRASLADO_LIBRE,
                        COALESCE(SUM(almacen.CANTIDAD_SM_CONSIGNACION_LIBRE), 0.0)              AS CANTIDAD_SM_CONSIGNACION_LIBRE,
                        COALESCE(SUM(almacen.CANTIDAD_SM_PROYECTO_LIBRE), 0.0)                  AS CANTIDAD_SM_PROYECTO_LIBRE,
                        --E3
                        COALESCE(SUM(almacen.CANTIDAD_SM_TRASP_STOCK_ESPECIAL_LIBRE), 0.0)      AS CANTIDAD_SM_TRASP_STOCK_ESPECIAL_LIBRE,
                        --E7
                        COALESCE(SUM(almacen.CANTIDAD_SM_DESGUACE_LIBRE), 0.0)                  AS CANTIDAD_SM_DESGUACE_LIBRE,
                        COALESCE(SUM(almacen.CANTIDAD_SM_CECO_LIBRE), 0.0)                      AS CANTIDAD_SM_CECO_LIBRE,
                        COALESCE(SUM(almacen.CANTIDAD_SM_OTROS_LIBRE), 0.0)                     AS CANTIDAD_SM_OTROS_LIBRE,
                        COALESCE(SUM(almacen.CANTIDAD_SM_BLOQUEADO_LIBRE), 0.0)                 AS CANTIDAD_SM_BLOQUEADO_LIBRE,
                        
                        --E8
                        MAX(almacen.FECHA_ULT_SM_CONSUMO_LIBRE::bigint)                         AS FECHA_ULT_SM_CONSUMO_LIBRE,
                        MAX(almacen.FECHA_ULT_SM_TRASLADO_LIBRE::bigint)                        AS FECHA_ULT_SM_TRASLADO_LIBRE,
                        MAX(almacen.FECHA_ULT_SM_TRASPASO_LIBRE::bigint)                        AS FECHA_ULT_SM_TRASPASO_LIBRE,
                        MAX(almacen.FECHA_ULT_SM_LIBRE::bigint)                                 AS FECHA_ULT_SM_LIBRE,
                                                
                        COALESCE(SUM(almacen.CANTIDAD_SALIDA_VTA), 0.0)                         AS CANTIDAD_SALIDA_VTA,
                        COALESCE(SUM(almacen.CANTIDAD_SALIDA_CONTROL_CALIDAD_VTA), 0.0)         AS CANTIDAD_SALIDA_CONTROL_CALIDAD_VTA,
                        COALESCE(SUM(almacen.CANTIDAD_SALIDA_BLOQUEADA_VTA), 0.0)               AS CANTIDAD_SALIDA_BLOQUEADA_VTA,
                        COALESCE(SUM(centro.CANTIDAD_SALIDA_CONSIG), 0.0)                       AS CANTIDAD_SALIDA_CONSIG,
                        COALESCE(SUM(centro.CANTIDAD_SALIDA_CONTROL_CALIDAD_CONSIG), 0.0)       AS CANTIDAD_SALIDA_CONTROL_CALIDAD_CONSIG,
                        COALESCE(MAX(valoracion.CANTIDAD_SALIDA_VALORACION), 0.0)               AS CANTIDAD_SALIDA_VALORACION,
                        COALESCE(MAX(valoracion.MONTO_SALIDA_VALORACION), 0.0)                  AS MONTO_SALIDA_VALORACION,
                        
                        --E4
                        mard.CENTRO_BENEFICIO                                   AS CENTRO_BENEFICIO,
                        
                        COALESCE(SUM(
                                CASE WHEN almacen.ANNO_TERMINO IS NULL THEN
                                        almacen.CANTIDAD_INICIO
                                ELSE
                                        almacen.CANTIDAD_TERMINO
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO,
                        COALESCE(SUM(
                                CASE WHEN almacen.ANNO_TERMINO IS NULL THEN
                                        almacen.CANTIDAD_INICIO_TRANSITO_ALMACEN
                                ELSE
                                        almacen.CANTIDAD_TERMINO_TRANSITO_ALMACEN
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_TRANSITO_ALMACEN,
                        COALESCE(SUM(
                                CASE WHEN almacen.ANNO_TERMINO IS NULL THEN
                                        almacen.CANTIDAD_INICIO_CONTROL_CALIDAD
                                ELSE
                                        almacen.CANTIDAD_TERMINO_CONTROL_CALIDAD
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_CONTROL_CALIDAD,
                        COALESCE(SUM(
                                CASE WHEN almacen.ANNO_TERMINO IS NULL THEN
                                        almacen.CANTIDAD_INICIO_BLOQUEADA
                                ELSE
                                        almacen.CANTIDAD_TERMINO_BLOQUEADA
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_BLOQUEADA,
                        COALESCE(SUM(
                                CASE WHEN almacen.ANNO_TERMINO_VTA IS NULL THEN
                                        almacen.CANTIDAD_INICIO_VTA
                                ELSE
                                        almacen.CANTIDAD_TERMINO_VTA
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_VTA,
                        COALESCE(SUM(
                                CASE WHEN almacen.ANNO_TERMINO_VTA IS NULL THEN
                                        almacen.CANTIDAD_INICIO_CONTROL_CALIDAD_VTA
                                ELSE
                                        almacen.CANTIDAD_TERMINO_CONTROL_CALIDAD_VTA
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_CONTROL_CALIDAD_VTA,
                        COALESCE(SUM(
                                CASE WHEN almacen.ANNO_TERMINO_VTA IS NULL THEN
                                        almacen.CANTIDAD_INICIO_BLOQUEADA_VTA
                                ELSE
                                        almacen.CANTIDAD_TERMINO_BLOQUEADA_VTA
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_BLOQUEADA_VTA,
                        COALESCE(MAX(
                                CASE WHEN centro.ANNO_TERMINO_CONSIG IS NULL THEN
                                        centro.CANTIDAD_INICIO_CONSIG
                                ELSE
                                        centro.CANTIDAD_TERMINO_CONSIG
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_CONSIG,
                        COALESCE(MAX(
                                CASE WHEN centro.ANNO_TERMINO_CONSIG IS NULL THEN
                                        centro.CANTIDAD_INICIO_CONTROL_CALIDAD_CONSIG
                                ELSE
                                        centro.CANTIDAD_TERMINO_CONTROL_CALIDAD_CONSIG
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_CONTROL_CALIDAD_CONSIG,
                        COALESCE(MAX(
                                CASE WHEN valoracion.ANNO_TERMINO_VALORACION IS NULL THEN
                                        valoracion.CANTIDAD_INICIO_VALORACION
                                ELSE
                                        valoracion.CANTIDAD_TERMINO_VALORACION
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_VALORACION,
                        COALESCE(MAX(
                                CASE WHEN valoracion.ANNO_TERMINO_VALORACION IS NULL THEN
                                        valoracion.MONTO_INICIO_VALORACION
                                ELSE
                                        valoracion.MONTO_TERMINO_VALORACION
                                END
                        ), 0.0)                                                 AS MONTO_TERMINO_VALORACION,
                        COALESCE(MAX(
                                CASE WHEN valoracion.ANNO_TERMINO_VALORACION_PROY IS NULL THEN
                                        valoracion.CANTIDAD_INICIO_VALORACION_PROY
                                ELSE
                                        valoracion.CANTIDAD_TERMINO_VALORACION_PROY
                                END
                        ), 0.0)                                                 AS CANTIDAD_TERMINO_VALORACION_PROY,
                        COALESCE(MAX(
                                CASE WHEN valoracion.ANNO_TERMINO_VALORACION_PROY IS NULL THEN
                                        valoracion.MONTO_INICIO_VALORACION_PROY
                                ELSE
                                        valoracion.MONTO_TERMINO_VALORACION_PROY
                                END
                        ), 0.0)                                                 AS MONTO_TERMINO_VALORACION_PROY,
                        
                        localtimestamp                                          AS FECHA_ACTUALIZACION,
                        'PROC_ETL_INSERT_EVOLUCION_INVENTARIO_CONSOLIDADO'                      
                                                                                AS APLICACION_ACTUALIZACION
        
        FROM            (
                        (
                        SELECT          almacen.MATERIAL,
                                        almacen.CENTRO,
                                        almacen.CENTRO_BENEFICIO                
                        FROM            SCM_EVOLUCION_INVENTARIO_ALMACEN almacen
                        GROUP BY        almacen.MATERIAL,
                                        almacen.CENTRO,
                                        almacen.CENTRO_BENEFICIO
                        )
                        UNION
                        (
                        SELECT          centro.MATERIAL,
                                        centro.CENTRO,
                                        centro.CENTRO_BENEFICIO                
                        FROM            SCM_EVOLUCION_INVENTARIO_CENTRO centro
                        GROUP BY        centro.MATERIAL,
                                        centro.CENTRO,
                                        centro.CENTRO_BENEFICIO
                        )
                        UNION
                        (
                        SELECT          valoracion.MATERIAL,
                                        valoracion.CENTRO,
                                        valoracion.CENTRO_BENEFICIO                
                        FROM            SCM_EVOLUCION_INVENTARIO_VALORACION valoracion
                        GROUP BY        valoracion.MATERIAL,
                                        valoracion.CENTRO,
                                        valoracion.CENTRO_BENEFICIO                        
                        )
                        ) AS mard
        
        INNER JOIN      T001W t001w
        ON              mard.CENTRO = t001w.werks
        
        INNER JOIN      T001K t001k
        ON              t001w.bwkey = t001k.bwkey
        
        LEFT JOIN       MARA mara
        ON              mara.matnr = mard.MATERIAL
        AND             mara.mandt = 300
        
        
        LEFT JOIN       SCM_EVOLUCION_INVENTARIO_ALMACEN almacen
        ON              mard.MATERIAL = almacen.material
        AND             mard.CENTRO = almacen.CENTRO
        AND             almacen.ANNO_PROCESO = IN_ANNO_PROCESO
        AND             almacen.MES_PROCESO = IN_MES_PROCESO
        
        
        LEFT JOIN       SCM_EVOLUCION_INVENTARIO_CENTRO centro
        ON              mard.MATERIAL = centro.MATERIAL
        AND             mard.CENTRO = centro.CENTRO
        AND             centro.ANNO_PROCESO = IN_ANNO_PROCESO
        AND             centro.MES_PROCESO = IN_MES_PROCESO
        
        LEFT JOIN       SCM_EVOLUCION_INVENTARIO_VALORACION valoracion                
        ON              mard.MATERIAL = valoracion.MATERIAL
        AND             mard.CENTRO = valoracion.CENTRO
        AND             valoracion.ANNO_PROCESO = IN_ANNO_PROCESO
        AND             valoracion.MES_PROCESO = IN_MES_PROCESO
       
        WHERE           (
                        almacen.ANNO_PROCESO IS NOT NULL
                        OR
                        centro.ANNO_PROCESO IS NOT NULL
                        OR
                        valoracion.ANNO_PROCESO IS NOT NULL
                        )        
       
        GROUP BY        t001k.bukrs,
                        mard.CENTRO,
                        mard.MATERIAL, 
                        mara.matkl,
                        mard.CENTRO_BENEFICIO
                        
        ;
END;
$func$
LANGUAGE plpgsql@