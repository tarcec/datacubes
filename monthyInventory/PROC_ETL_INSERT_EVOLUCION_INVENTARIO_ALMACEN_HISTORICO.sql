DROP FUNCTION PROC_ETL_INSERT_EVOLUCION_INVENTARIO_ALMACEN_HISTORICO@
CREATE OR REPLACE FUNCTION PROC_ETL_INSERT_EVOLUCION_INVENTARIO_ALMACEN_HISTORICO(
                                        IN_ANNO_PROCESO VARCHAR(255),
					IN_MES_PROCESO VARCHAR(255))
RETURNS VOID AS $func$
BEGIN

        INSERT INTO SCM_EVOLUCION_INVENTARIO_ALMACEN(
                        ANNO_PROCESO,
                        MES_PROCESO,
                        SOCIEDAD,
                        CENTRO,
                        ALMACEN,
                        MATERIAL, 
                        CLASE_ARTICULO, 
                        CANTIDAD_LINEAS,
                        
                        --INICIO
                        ANNO_INICIO,
                        MES_INICIO,
                        CANTIDAD_INICIO,
                        CANTIDAD_INICIO_TRANSITO_ALMACEN,
                        CANTIDAD_INICIO_CONTROL_CALIDAD,
                        CANTIDAD_INICIO_BLOQUEADA,
                        ANNO_INICIO_VTA,
                        MES_INICIO_VTA,
                        CANTIDAD_INICIO_VTA,
                        CANTIDAD_INICIO_CONTROL_CALIDAD_VTA,
                        CANTIDAD_INICIO_BLOQUEADA_VTA,

                        --MES
                        CANTIDAD_ENTRADA,
                        CANTIDAD_ENTRADA_TRANSITO_ALMACEN,
                        CANTIDAD_ENTRADA_CONTROL_CALIDAD,
                        CANTIDAD_ENTRADA_BLOQUEADA,
                        CANTIDAD_ENTRADA_VTA,
                        CANTIDAD_ENTRADA_CONTROL_CALIDAD_VTA,
                        CANTIDAD_ENTRADA_BLOQUEADA_VTA,
                        CANTIDAD_SALIDA,
                        CANTIDAD_SALIDA_TRANSITO_ALMACEN,
                        CANTIDAD_SALIDA_CONTROL_CALIDAD,
                        CANTIDAD_SALIDA_BLOQUEADA,
                        CANTIDAD_SALIDA_VTA,
                        CANTIDAD_SALIDA_CONTROL_CALIDAD_VTA,
                        CANTIDAD_SALIDA_BLOQUEADA_VTA,

                        --E1
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
                        
                        --E4
                        CENTRO_BENEFICIO,
                        
                        --TERMINO
                        ANNO_TERMINO,
                        MES_TERMINO,                        
                        CANTIDAD_TERMINO,
                        CANTIDAD_TERMINO_TRANSITO_ALMACEN,
                        CANTIDAD_TERMINO_CONTROL_CALIDAD,
                        CANTIDAD_TERMINO_BLOQUEADA,
                        ANNO_TERMINO_VTA,
                        MES_TERMINO_VTA,
                        CANTIDAD_TERMINO_VTA,
                        CANTIDAD_TERMINO_CONTROL_CALIDAD_VTA,
                        CANTIDAD_TERMINO_BLOQUEADA_VTA,

                        FECHA_ACTUALIZACION,
                        APLICACION_ACTUALIZACION)

        SELECT
                        IN_ANNO_PROCESO                         AS ANNO_PROCESO,
                        IN_MES_PROCESO                          AS MES_PROCESO,
                        t001k.bukrs                             AS SOCIEDAD,
                        mard.werks                              AS CENTRO,
                        mard.lgort                              AS ALMACEN,
                        mard.matnr                              AS MATERIAL,
                        mara.matkl                              AS CLASE_ARTICULO,
                        COUNT(mseg.id)                          AS CANTIDAD_LINEAS,
                        
                        --INICIO
                        mardh.ini_lfgja                       AS ANNO_INICIO,
                        mardh.ini_lfmon                       AS MES_INICIO,
                        COALESCE(MAX(mardh.ini_labst), 0)     AS CANTIDAD_INICIO,
                        COALESCE(MAX(mardh.ini_umlme), 0)     AS CANTIDAD_INICIO_TRANSITO_ALMACEN,
                        COALESCE(MAX(mardh.ini_insme), 0)     AS CANTIDAD_INICIO_CONTROL_CALIDAD,
                        COALESCE(MAX(mardh.ini_speme), 0)     AS CANTIDAD_INICIO_BLOQUEADA,                         
                        mskah.ini_lfgja                       AS ANNO_INICIO_VTA,
                        mskah.ini_lfmon                       AS MES_INICIO_VTA,
                        COALESCE(MAX(mskah.ini_kalab), 0)     AS CANTIDAD_INICIO_VTA,
                        COALESCE(MAX(mskah.ini_kains), 0)     AS CANTIDAD_INICIO_CONTROL_CALIDAD_VTA,
                        COALESCE(MAX(mskah.ini_kaspe), 0)     AS CANTIDAD_INICIO_BLOQUEADA_VTA,
                        
                        --ENTRADA
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        --CASO TRASLADO ENTRE CENTROS
                                        
                                        CASE WHEN mseg.bwart NOT IN ('641', '322', '344', '505', '718') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'S' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart NOT IN ('641', '322', '344', '505', '718') THEN
                                                mseg.menge
                                        ELSE
                                                --20190829      CASO ENTRADA STOCK EN LAMPA
                                                --              EN ALMACEN DE REPUESTOS EN GARANTIA KCH
                                                --              AGREGAR ALMACEN
                                                CASE WHEN mseg.shkzg = 'S' 
                                                        AND mseg.werks IN ('5091')
                                                        AND mseg.lgort IN ('3157')
                                                        AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                        AND mseg.insmk = 'F'    
                                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                        AND mseg.bwart NOT IN ('641', '322', '344', '505', '718') THEN
                                                        mseg.menge
                                                ELSE
                                                        0
                                                END
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_ENTRADA,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND mseg.bwart IN ('313', '315') THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_ENTRADA_TRANSITO_ALMACEN,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND mseg.insmk IN ('X', '2') THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_ENTRADA_CONTROL_CALIDAD,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND mseg.insmk IN ('S', '3') THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_ENTRADA_BLOQUEADA,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND mseg.sobkz = 'E' AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) 
                                        AND mseg.bwart NOT IN ('641', '322', '344', '350', '505', '718') THEN
                                        
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_ENTRADA_VTA,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND mseg.insmk IN ('X', '2') AND mseg.sobkz = 'E' THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_ENTRADA_CONTROL_CALIDAD_VTA,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND mseg.insmk IN ('S', '3') AND mseg.sobkz = 'E' THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_ENTRADA_BLOQUEADA_VTA,
                        
                        --SALIDA
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart NOT IN ('321','343', '506', '717', '713') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart NOT IN ('321','343', '506', '717', '713') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SALIDA,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H'  AND mseg.bwart IN ('313', '315') THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_SALIDA_TRANSITO_ALMACEN,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND mseg.insmk IN ('X', '2') THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_SALIDA_CONTROL_CALIDAD,
                        COALESCE(SUM(
                                --505 506
                                CASE WHEN mseg.shkzg = 'H' AND mseg.insmk IN ('S', '3') THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_SALIDA_BLOQUEADA,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND mseg.sobkz = 'E' AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) 
                                        AND mseg.bwart NOT IN ('641', '322', '344', '350', '505', '718') THEN
                                        
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_SALIDA_VTA,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND mseg.insmk IN ('X', '2') AND mseg.sobkz = 'E' THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_SALIDA_CONTROL_CALIDAD_VTA,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND mseg.insmk IN ('S', '3') AND mseg.sobkz = 'E' THEN
                                        mseg.menge
                                ELSE
                                        0
                                END
                        ), 0.0)                                 AS CANTIDAD_SALIDA_BLOQUEADA_VTA,
                       
                        --E1
                        --REVISAR!!!!!!!!!!!!!!!!!!!!!!!!!
                        --ENTRADA DE MERCANCIA
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('101') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'S' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('101') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_EM_LIBRE,
                        
                        --E2
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('101') AND mseg.ebeln LIKE '4502%' THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'S' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('101') 
                                                AND mseg.ebeln LIKE '4502%' THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_EM_IMPORTACION_LIBRE,
                        COALESCE(SUM(
                                --CASE WHEN mseg.shkzg = 'S' THEN
                                CASE WHEN mseg.shkzg = 'S' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('101') AND mseg.ebeln LIKE '41%' THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'S' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('101') 
                                                AND mseg.ebeln LIKE '41%' THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_EM_STOCK_NAC_LIBRE,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'S' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('101') AND mseg.ebeln LIKE '40%' THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'S' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('101') 
                                                AND mseg.ebeln LIKE '40%' THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_EM_TRASLADO_LIBRE,
                        
                        --E5
                        COALESCE(MAX(oc_stat_norm.tot_norm), 0)
                                                                AS CANT_OC_EM_NORMAL_LIBRE,
                        COALESCE(MAX(oc_stat_urg.tot_urg), 0)
                                                                AS CANT_OC_EM_URGENTE_LIBRE,
                        COALESCE(MAX(oc_stat_tras_stock.tot_stock_fab), 0)
                                                                AS CANT_OC_EM_IMPORTACION_LIBRE,
                        COALESCE(MAX(oc_stat_tras_stock.tot_stock_nac), 0)
                                                                AS CANT_OC_EM_STOCK_NAC_LIBRE,
                        COALESCE(MAX(oc_stat_tras_stock.tot_tras), 0)
                                                                AS CANT_OC_EM_TRASLADO_LIBRE,
                                                                
                        --E8
                        COALESCE(MAX(mseg_ult_em_libre.budat::bigint), '20000101')
                                                                AS FECHA_ULT_EM_LIBRE,
                        
                        --SALIDA DE MERCANCIA
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('601') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('601') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_LIBRE,
                        --SALIDA DE MERCANCIA ORDEN
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        --Z27 -> OS
                                        --Z21 -> OS
                                        --Z15 -> OS
                                        --Z17 -> OS REL
                                        --261 -> SM ORDEN
                                        --ZPF -> OS
                                        --Z85 -> MAN
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) 
                                        THEN
                                        CASE WHEN mseg.bwart IN ('ZPE', 'Z27', 'Z21', 'Z15', 'Z17', '261', 'ZPF', 'Z85') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                       CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('ZPE', 'Z27', 'Z21', 'Z15', 'Z17', '261', 'ZPF', 'Z85') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_ORDEN_LIBRE,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('641', '301') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                       CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('641', '301') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_TRASLADO_LIBRE,
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('631') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('631') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_CONSIGNACION_LIBRE,
                        
                        COALESCE(SUM(
                                --CASE WHEN mseg.shkzg = 'H' THEN
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('221') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('221') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_PROYECTO_LIBRE,
                        --E3
                        --CASO TRASPASOS A STOCK ESPECIAL
                        COALESCE(SUM(
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                       AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        CASE WHEN mseg.bwart IN ('412') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                       CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('412') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_TRASP_STOCK_ESPECIAL_LIBRE,
                        --E7                        
                        COALESCE(SUM(
                                --CASE WHEN mseg.shkzg = 'H' THEN
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        --551 SM DESGUACE
                                        --Z25 SM DESPIECE
                                        CASE WHEN mseg.bwart IN ('551', 'Z25') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('551', 'Z25') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_DESGUACE_LIBRE,
                        COALESCE(SUM(
                                --CASE WHEN mseg.shkzg = 'H' THEN
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        --201 SALIDA A CECO 
                                        CASE WHEN mseg.bwart IN ('201') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('201') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_CECO_LIBRE,
                        
                        --SE CREA CASO OTROS PARA CASOS ESPECIALES
                        --NO CONSTITUYE OCURRENCIA
                        COALESCE(SUM(
                                --CASE WHEN mseg.shkzg = 'H' THEN
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        --717 SM POR AJUSTE INVENTARIO
                                        --Z03 SM POR RECLAMO A FABRICA
                                        --Z07 SM POR SINIESTRO
                                        --241 SM POR ACTIVO FIJO
                                        --ZYG SM POR RECLAMO A TRANSPORTISTA
                                        
                                        CASE WHEN mseg.bwart IN ('717', 'Z03', 'Z07', '241', 'ZYG') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart IN ('717', 'Z03', 'Z07', '241', 'ZYG') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_OTROS_LIBRE,
                        --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        --IDENTIFICACION DE SALIDAS A BLOQUEADO Y CONTROL DE CALIDAD
                        --NO DEBE CONSIDERARSE COMO OCURRENCIA!!!!!
                        --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        COALESCE(SUM(
                                --CASE WHEN mseg.shkzg = 'H' THEN
                                CASE WHEN mseg.shkzg = 'H' AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) AND mseg.insmk IS NULL
                                        AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL) THEN
                                        --322 AN TRASP CALIDAD A LIBRE (O TRASPASO LIBRE A CALIDAD)
                                        --344 AN TRASP BLOQ A LIBRE (O TRASPASO LIBRE A BLOQ)
                                        CASE WHEN mseg.bwart IN ('322', '344') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                ELSE
                                        CASE WHEN mseg.shkzg = 'H' 
                                                AND mseg.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')
                                                AND (mseg.sobkz = ' ' OR mseg.sobkz IS NULL) 
                                                AND mseg.insmk = 'F'    
                                                AND (mseg.bestq NOT IN ('Q', 'S') OR mseg.bestq IS NULL)
                                                AND mseg.bwart  IN ('322', '344') THEN
                                                mseg.menge
                                        ELSE
                                                0
                                        END
                                END
                        ), 0.0)                                 AS CANTIDAD_SM_BLOQUEADO_LIBRE,
                        
                        --E8
                        COALESCE(MAX(mseg_ult_sm_consumo_libre.budat::bigint), '20000101')
                                                                AS FECHA_ULT_SM_CONSUMO_LIBRE,
                        COALESCE(MAX(mseg_ult_sm_traslado_libre.budat::bigint), '20000101')
                                                                AS FECHA_ULT_SM_TRASLADO_LIBRE,
                        COALESCE(MAX(mseg_ult_sm_traspaso_libre.budat::bigint), '20000101')
                                                                AS FECHA_ULT_SM_TRASPASO_LIBRE,
                        COALESCE(MAX(mseg_ult_sm_libre.budat::bigint), '20000101')
                                                                AS FECHA_ULT_SM_LIBRE,
                        
                        --E4
                        marc.prctr                              AS CENTRO_BENEFICIO,
                        
                        --TERMINO
                        mardh.ter_lfgja                       AS ANNO_TERMINO,
                        mardh.ter_lfmon                       AS MES_TERMINO,
                        COALESCE(MAX(mardh.ter_labst), 0)     AS CANTIDAD_TERMINO,
                        COALESCE(MAX(mardh.ter_umlme), 0)     AS CANTIDAD_TERMINO_TRANSITO_ALMACEN,
                        COALESCE(MAX(mardh.ter_insme), 0)     AS CANTIDAD_TERMINO_CONTROL_CALIDAD,
                        COALESCE(MAX(mardh.ter_speme), 0)     AS CANTIDAD_TERMINO_BLOQUEADA, 
                        mskah.ter_lfgja                       AS ANNO_TERMINO_VTA,
                        mskah.ter_lfmon                       AS MES_TERMINO_VTA,
                        COALESCE(MAX(mskah.ter_kalab), 0)     AS CANTIDAD_TERMINO_VTA,
                        COALESCE(MAX(mskah.ter_kains), 0)     AS CANTIDAD_TERMINO_CONTROL_CALIDAD_VTA,
                        COALESCE(MAX(mskah.ter_kaspe), 0)     AS CANTIDAD_TERMINO_BLOQUEADA_VTA,
                        
                        localtimestamp                          AS FECHA_ACTUALIZACION,
                        'PROC_ETL_INSERT_EVOLUCION_INVENTARIO_HISTORICO'                      
                                                                AS APLICACION_ACTUALIZACION
        FROM            (
                        (
                        SELECT          mard_1.matnr,
                                        mard_1.werks,
                                        mard_1.lgort 
                        FROM            MARD mard_1
                        GROUP BY        mard_1.matnr,
                                        mard_1.werks,
                                        mard_1.lgort
                        )
                        UNION
                        (
                        SELECT          mardh_1.matnr,
                                        mardh_1.werks,
                                        mardh_1.lgort 
                        FROM            MARDH mardh_1
                        GROUP BY        mardh_1.matnr,
                                        mardh_1.werks,
                                        mardh_1.lgort
                        )
                        UNION
                        (
                        SELECT          mska_1.matnr,
                                        mska_1.werks,
                                        mska_1.lgort 
                        FROM            MSKA mska_1
                        GROUP BY        mska_1.matnr,
                                        mska_1.werks,
                                        mska_1.lgort
                        ) 
                        UNION
                        (
                        SELECT          mskah_1.matnr,
                                        mskah_1.werks,
                                        mskah_1.lgort 
                        FROM            MSKAH mskah_1
                        GROUP BY        mskah_1.matnr,
                                        mskah_1.werks,
                                        mskah_1.lgort
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
        
        --STOCK ANTES / DESPUES
        --SE CAMBIA POR INNER
        --STOCK NORMAL
        -------------------------------------------------------------------------------
        --MARD
        -------------------------------------------------------------------------------
        LEFT JOIN       (
                        SELECT          mardh_1_ini.matnr,
                                        mardh_1_ini.werks,
                                        mardh_1_ini.lgort,
                                        mardh_1_ini.lfgja       AS ini_lfgja,
                                        mardh_1_ini.lfmon       AS ini_lfmon,
                                        mardh_1_ini.labst       AS ini_labst, --LIBRE
                                        mardh_1_ini.umlme       AS ini_umlme, --TRANSITO
                                        mardh_1_ini.insme       AS ini_insme, --CALIDAD
                                        mardh_1_ini.speme       AS ini_speme, --BLOQUEADO
                                        mardh_1_ter.lfgja       AS ter_lfgja,
                                        mardh_1_ter.lfmon       AS ter_lfmon,
                                        mardh_1_ter.labst       AS ter_labst, --LIBRE
                                        mardh_1_ter.umlme       AS ter_umlme, --TRANSITO
                                        mardh_1_ter.insme       AS ter_insme, --CALIDAD
                                        mardh_1_ter.speme       AS ter_speme, --BLOQUEADO
                                        mardh_1_count.count_mardh
                                                                AS stk_count_mardh
                        FROM            (
                                        (
                                        SELECT          mard_1_1.matnr,
                                                        mard_1_1.werks,
                                                        mard_1_1.lgort 
                                        FROM            MARD mard_1_1
                                        GROUP BY        mard_1_1.matnr,
                                                        mard_1_1.werks,
                                                        mard_1_1.lgort
                                        )
                                        UNION
                                        (
                                        SELECT          mardh_1_1.matnr,
                                                        mardh_1_1.werks,
                                                        mardh_1_1.lgort 
                                        FROM            MARDH mardh_1_1
                                        GROUP BY        mardh_1_1.matnr,
                                                        mardh_1_1.werks,
                                                        mardh_1_1.lgort
                                        )
                                        ) AS mard_1
                        LEFT JOIN
                                        (
                                        SELECT
                                        DISTINCT ON     (mardh_1_1.matnr, mardh_1_1.werks, mardh_1_1.lgort)
                                                        mardh_1_1.matnr,
                                                        mardh_1_1.werks,
                                                        mardh_1_1.lgort,
                                                        mardh_1_1.lfgja,
                                                        mardh_1_1.lfmon,
                                                        mardh_1_1.labst, --LIBRE
                                                        mardh_1_1.umlme, --TRANSITO
                                                        mardh_1_1.insme, --CALIDAD
                                                        mardh_1_1.speme  --BLOQUEADO
                                        
                                        FROM            (
                                                        (
                                                        SELECT          mardh_1_1_1.matnr,
                                                                        mardh_1_1_1.werks,
                                                                        mardh_1_1_1.lgort,
                                                                        mardh_1_1_1.lfgja,
                                                                        mardh_1_1_1.lfmon,
                                                                        mardh_1_1_1.labst, --LIBRE
                                                                        mardh_1_1_1.umlme, --TRANSITO
                                                                        mardh_1_1_1.insme, --CALIDAD
                                                                        mardh_1_1_1.speme  --BLOQUEADO 
                                                        FROM            MARDH mardh_1_1_1
                                                        WHERE           to_date(mardh_1_1_1.lfgja::text || LPAD(mardh_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                                        )
                                                        UNION ALL
                                                        (
                                                        SELECT          mard_1_1_1.matnr,
                                                                        mard_1_1_1.werks,
                                                                        mard_1_1_1.lgort,
                                                                        mard_1_1_1.lfgja,
                                                                        mard_1_1_1.lfmon,
                                                                        mard_1_1_1.labst, --LIBRE
                                                                        mard_1_1_1.umlme, --TRANSITO
                                                                        mard_1_1_1.insme, --CALIDAD
                                                                        mard_1_1_1.speme  --BLOQUEADO 
                                                        FROM            MARD mard_1_1_1
                                                        WHERE           to_date(mard_1_1_1.lfgja::text || LPAD(mard_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'

                                                        )
                                                        ) AS mardh_1_1

                                        ORDER BY        mardh_1_1.matnr, 
                                                        mardh_1_1.werks,
                                                        mardh_1_1.lgort, 
                                                        mardh_1_1.lfgja DESC,
                                                        mardh_1_1.lfmon DESC
                                        ) AS mardh_1_ini
                        ON              mard_1.matnr = mardh_1_ini.matnr
                        AND             mard_1.werks = mardh_1_ini.werks
                        AND             mard_1.lgort = mardh_1_ini.lgort
                        LEFT JOIN       (
                                        SELECT
                                        DISTINCT ON     (mardh_1_1.matnr, mardh_1_1.werks, mardh_1_1.lgort)
                                                        mardh_1_1.matnr,
                                                        mardh_1_1.werks,
                                                        mardh_1_1.lgort,
                                                        mardh_1_1.lfgja,
                                                        mardh_1_1.lfmon,
                                                        mardh_1_1.labst, --LIBRE
                                                        mardh_1_1.umlme, --TRANSITO
                                                        mardh_1_1.insme, --CALIDAD
                                                        mardh_1_1.speme  --BLOQUEADO
                                        
                                        FROM            (
                                                        (
                                                        SELECT          mardh_1_1_1.matnr,
                                                                        mardh_1_1_1.werks,
                                                                        mardh_1_1_1.lgort,
                                                                        mardh_1_1_1.lfgja,
                                                                        mardh_1_1_1.lfmon,
                                                                        mardh_1_1_1.labst, --LIBRE
                                                                        mardh_1_1_1.umlme, --TRANSITO
                                                                        mardh_1_1_1.insme, --CALIDAD
                                                                        mardh_1_1_1.speme  --BLOQUEADO 
                                                        FROM            MARDH mardh_1_1_1
                                                        WHERE           to_date(mardh_1_1_1.lfgja::text || LPAD(mardh_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                        )
                                                        UNION ALL
                                                        (
                                                        SELECT          mard_1_1_1.matnr,
                                                                        mard_1_1_1.werks,
                                                                        mard_1_1_1.lgort,
                                                                        mard_1_1_1.lfgja,
                                                                        mard_1_1_1.lfmon,
                                                                        mard_1_1_1.labst, --LIBRE
                                                                        mard_1_1_1.umlme, --TRANSITO
                                                                        mard_1_1_1.insme, --CALIDAD
                                                                        mard_1_1_1.speme  --BLOQUEADO 
                                                        FROM            MARD mard_1_1_1
                                                        WHERE           to_date(mard_1_1_1.lfgja::text || LPAD(mard_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                                        )
                                                        ) AS mardh_1_1
                                        ORDER BY        mardh_1_1.matnr, 
                                                        mardh_1_1.werks,
                                                        mardh_1_1.lgort, 
                                                        mardh_1_1.lfgja ASC,
                                                        mardh_1_1.lfmon ASC
                                        ) AS mardh_1_ter
                        ON              mard_1.matnr = mardh_1_ter.matnr
                        AND             mard_1.werks = mardh_1_ter.werks
                        AND             mard_1.lgort = mardh_1_ter.lgort
                        
                        --PARA HACER UN COUNT DE CUANTOS REGISTROS TIENE EL MATERIAL
                        LEFT JOIN       (
                                        SELECT
                                                        mardh_1_1.matnr,
                                                        mardh_1_1.werks,
                                                        mardh_1_1.lgort,
                                                        COUNT(*) AS count_mardh
                                        FROM            MARDH mardh_1_1
                                        WHERE           to_date(mardh_1_1.lfgja::text || LPAD(mardh_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                        GROUP BY        mardh_1_1.matnr,
                                                        mardh_1_1.werks,
                                                        mardh_1_1.lgort
                                        ) AS mardh_1_count
                        ON              mard_1.matnr = mardh_1_count.matnr
                        AND             mard_1.werks = mardh_1_count.werks
                        AND             mard_1.lgort = mardh_1_count.lgort
                        
                        WHERE           
--                                        
                                        
                                        mardh_1_ini.lfgja IS NOT NULL
                                                
                        --ELIMINA REGISTROS VIEJOS
                        --Y QUE SOLO TIENEN EL HITO DE CREACION

                        --VERSION 2
                        AND NOT             
                                (
                                        mardh_1_ini.lfgja < 2015
                                AND
                                        mardh_1_ter.lfgja IS NULL
                                AND             
                                        mardh_1_ini.labst = 0.0
                                AND             
                                        mardh_1_ini.umlme = 0.0
                                AND             
                                        mardh_1_ini.insme = 0.0
                                AND             
                                        mardh_1_ini.speme = 0.0
                                )
                                                                
                        ) AS mardh
        ON              mard.matnr = mardh.matnr
        AND             mard.werks = mardh.werks
        AND             mard.lgort = mardh.lgort
        -------------------------------------------------------------------------------
        --MSKA
        -------------------------------------------------------------------------------        
        LEFT JOIN       (
                        SELECT          mskah_1_ini.matnr,
                                        mskah_1_ini.werks,
                                        mskah_1_ini.lgort,
                                        mskah_1_ini.lfgja       AS ini_lfgja,
                                        mskah_1_ini.lfmon       AS ini_lfmon,
                                        mskah_1_ini.kalab       AS ini_kalab, --LIBRE
                                        mskah_1_ini.kains       AS ini_kains, --CALIDAD
                                        mskah_1_ini.kaspe       AS ini_kaspe, --BLOQUEADO
                                        mskah_1_ter.lfgja       AS ter_lfgja,
                                        mskah_1_ter.lfmon       AS ter_lfmon,
                                        mskah_1_ter.kalab       AS ter_kalab, --LIBRE
                                        mskah_1_ter.kains       AS ter_kains, --CALIDAD
                                        mskah_1_ter.kaspe       AS ter_kaspe, --BLOQUEADO
                                        mskah_1_count.count_mskah
                                                                AS count_mskah
                        FROM            (
                                        (
                                        SELECT          mska_1_1.matnr,
                                                        mska_1_1.werks,
                                                        mska_1_1.lgort 
                                        FROM            MSKA mska_1_1
                                        GROUP BY        mska_1_1.matnr,
                                                        mska_1_1.werks,
                                                        mska_1_1.lgort
                                        )
                                        UNION
                                        (
                                        SELECT          mskah_1_1.matnr,
                                                        mskah_1_1.werks,
                                                        mskah_1_1.lgort 
                                        FROM            MSKAH mskah_1_1
                                        GROUP BY        mskah_1_1.matnr,
                                                        mskah_1_1.werks,
                                                        mskah_1_1.lgort
                                        )
                                        ) AS mska_1
                        LEFT JOIN       (
                                        SELECT
                                        DISTINCT ON     (mskah_1_1.matnr, mskah_1_1.werks, mskah_1_1.lgort)
                                                        mskah_1_1.matnr,
                                                        mskah_1_1.werks,
                                                        mskah_1_1.lgort,
                                                        mskah_1_1.lfgja,
                                                        mskah_1_1.lfmon,
                                                        mskah_1_1.kalab, --LIBRE
                                                        mskah_1_1.kains, --CALIDAD
                                                        mskah_1_1.kaspe  --BLOQUEADO
                                        
                                        FROM            (
                                                        (
                                                        SELECT          mskah_1_1_1.matnr,
                                                                        mskah_1_1_1.werks,
                                                                        mskah_1_1_1.lgort,
                                                                        mskah_1_1_1.lfgja,
                                                                        mskah_1_1_1.lfmon,
                                                                        SUM(mskah_1_1_1.kalab) AS kalab, --LIBRE
                                                                        SUM(mskah_1_1_1.kains) AS kains, --CALIDAD
                                                                        SUM(mskah_1_1_1.kaspe) AS kaspe --BLOQUEADO
                                                        FROM            MSKAH mskah_1_1_1
                                                        WHERE           to_date(mskah_1_1_1.lfgja::text || LPAD(mskah_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                        
                                                        GROUP BY        mskah_1_1_1.matnr,
                                                                        mskah_1_1_1.werks,
                                                                        mskah_1_1_1.lgort,
                                                                        mskah_1_1_1.lfgja,
                                                                        mskah_1_1_1.lfmon
                                                        )
                                                        UNION ALL
                                                        (
                                                        SELECT          mska_1_1_1.matnr,
                                                                        mska_1_1_1.werks,
                                                                        mska_1_1_1.lgort,
                                                                        mska_1_1_1.lfgja,
                                                                        mska_1_1_1.lfmon,
                                                                        SUM(mska_1_1_1.kalab) AS kalab, --LIBRE
                                                                        SUM(mska_1_1_1.kains) AS kains, --CALIDAD
                                                                        SUM(mska_1_1_1.kaspe) AS kaspe --BLOQUEADO
                                                        FROM            MSKA mska_1_1_1
                                                        WHERE           to_date(mska_1_1_1.lfgja::text || LPAD(mska_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') - INTERVAL '1 month'
                                        
                                                        GROUP BY        mska_1_1_1.matnr,
                                                                        mska_1_1_1.werks,
                                                                        mska_1_1_1.lgort,
                                                                        mska_1_1_1.lfgja,
                                                                        mska_1_1_1.lfmon                                                                       
                                                        )
                                                        ) AS mskah_1_1
                                        ORDER BY        mskah_1_1.matnr, 
                                                        mskah_1_1.werks, 
                                                        mskah_1_1.lgort,
                                                        mskah_1_1.lfgja DESC,
                                                        mskah_1_1.lfmon DESC
                                        ) AS mskah_1_ini
                        ON              mska_1.matnr = mskah_1_ini.matnr
                        AND             mska_1.werks = mskah_1_ini.werks
                        AND             mska_1.lgort = mskah_1_ini.lgort
                        LEFT JOIN       (
                                        SELECT
                                        DISTINCT ON     (mskah_1_1.matnr, mskah_1_1.werks, mskah_1_1.lgort)
                                                        mskah_1_1.matnr,
                                                        mskah_1_1.werks,
                                                        mskah_1_1.lgort,
                                                        mskah_1_1.lfgja,
                                                        mskah_1_1.lfmon,
                                                        mskah_1_1.kalab, --LIBRE
                                                        mskah_1_1.kains, --CALIDAD
                                                        mskah_1_1.kaspe  --BLOQUEADO
                                        
                                        FROM            (
                                                        (
                                                        SELECT          mskah_1_1_1.matnr,
                                                                        mskah_1_1_1.werks,
                                                                        mskah_1_1_1.lgort,
                                                                        mskah_1_1_1.lfgja,
                                                                        mskah_1_1_1.lfmon,
                                                                        SUM(mskah_1_1_1.kalab) AS kalab, --LIBRE
                                                                        SUM(mskah_1_1_1.kains) AS kains, --CALIDAD
                                                                        SUM(mskah_1_1_1.kaspe) AS kaspe --BLOQUEADO
                                                        FROM            MSKAH mskah_1_1_1
                                                        WHERE           to_date(mskah_1_1_1.lfgja::text || LPAD(mskah_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')                                                
                                                        GROUP BY        mskah_1_1_1.matnr,
                                                                        mskah_1_1_1.werks,
                                                                        mskah_1_1_1.lgort,
                                                                        mskah_1_1_1.lfgja,
                                                                        mskah_1_1_1.lfmon
                                                        )
                                                        UNION ALL
                                                        (
                                                        SELECT          mska_1_1_1.matnr,
                                                                        mska_1_1_1.werks,
                                                                        mska_1_1_1.lgort,
                                                                        mska_1_1_1.lfgja,
                                                                        mska_1_1_1.lfmon,
                                                                        SUM(mska_1_1_1.kalab) AS kalab, --LIBRE
                                                                        SUM(mska_1_1_1.kains) AS kains, --CALIDAD
                                                                        SUM(mska_1_1_1.kaspe) AS kaspe --BLOQUEADO
                                                        FROM            MSKA mska_1_1_1
                                                        WHERE           to_date(mska_1_1_1.lfgja::text || LPAD(mska_1_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  >= 
                                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')                                                
                                                        GROUP BY        mska_1_1_1.matnr,
                                                                        mska_1_1_1.werks,
                                                                        mska_1_1_1.lgort,
                                                                        mska_1_1_1.lfgja,
                                                                        mska_1_1_1.lfmon                                                                       
                                                        )
                                                        ) AS mskah_1_1

                                        ORDER BY        mskah_1_1.matnr, 
                                                        mskah_1_1.werks,
                                                        mskah_1_1.lgort, 
                                                        mskah_1_1.lfgja ASC,
                                                        mskah_1_1.lfmon ASC
                                        ) AS mskah_1_ter
                        ON              mska_1.matnr = mskah_1_ter.matnr
                        AND             mska_1.werks = mskah_1_ter.werks
                        AND             mska_1.lgort = mskah_1_ter.lgort
                        
                        --PARA HACER UN COUNT DE CUANTOS REGISTROS TIENE EL MATERIAL
                        LEFT JOIN      (
                                        SELECT
                                                        mskah_1_1.matnr,
                                                        mskah_1_1.werks,
                                                        mskah_1_1.lgort,
                                                        COUNT(*) AS count_mskah
                                        FROM            MSKAH mskah_1_1
                                        WHERE           to_date(mskah_1_1.lfgja::text || LPAD(mskah_1_1.lfmon::text, 2, '0') || '01', 'YYYYMMDD')  <= 
                                                                to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD')
                                        GROUP BY        mskah_1_1.matnr,
                                                        mskah_1_1.werks,
                                                        mskah_1_1.lgort
                                        ) AS mskah_1_count
                        ON              mska_1.matnr = mskah_1_count.matnr
                        AND             mska_1.werks = mskah_1_count.werks
                        AND             mska_1.lgort = mskah_1_count.lgort
                        
                        WHERE           

                                        mskah_1_ini.lfgja IS NOT NULL
                                                
                        --ELIMINA REGISTROS VIEJOS
                        --Y QUE SOLO TIENEN EL HITO DE CREACION
                        --VERSION 2
                        AND NOT             
                                (
                                        mskah_1_ini.lfgja < 2015
                                AND
                                        mskah_1_ter.lfgja IS NULL
                                AND             
                                        mskah_1_ini.kalab = 0.0
                                AND             
                                        mskah_1_ini.kains = 0.0
                                AND             
                                        mskah_1_ini.kaspe = 0.0

                                )                                 
                        ) AS mskah
        ON              mard.matnr = mskah.matnr
        AND             mard.werks = mskah.werks
        AND             mard.lgort = mskah.lgort
                
        LEFT JOIN       MSEG mseg
        ON              mard.matnr = mseg.matnr
        AND             mard.werks = mseg.werks
        AND             mard.lgort = mseg.lgort
        AND             mseg.budat_mkpf::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
        AND             mseg.budat_mkpf::bigint < 
                                to_char(
                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                        + INTERVAL '1 month', 'YYYYMMDD')::bigint
        --ELIMINAR DUPLICACIONES DE MOVIMIENTOS EN UB
        AND             mseg.lgort IS NOT NULL
        
        --------------------------------------------------------------------------------------------
        -- STATS DE URGENCIA
        --------------------------------------------------------------------------------------------
        --NORMAL
        LEFT JOIN       (
                        --NORMAL
                        SELECT          qry.matnr,
                                        qry.werks,
                                        qry.lgort,
                                        SUM(qry.tot_norm) AS tot_norm
                        FROM            (
                                        --MUNDO UB
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort,
                                                        SUM(CASE WHEN likp_1.vsbed NOT IN ('02', '05', '06') THEN 1 ELSE 0 END) AS tot_norm
                                                        
                                        FROM            LIKP likp_1
                                        INNER JOIN      LIPS lips_1
                                        ON              lips_1.vbeln = likp_1.vbeln
                                        AND             lips_1.kzbew = 'L'
                                        AND             lips_1.mandt = likp_1.mandt
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
                                        AND             (ekpo_1.loekz IS NULL OR ekpo_1.loekz = '')
                                        AND             likp_1.mandt = 300
                                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort
                                        UNION ALL
                                        --MUNDO STOCK         
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort,
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
                                        AND             (ekpo_1.loekz IS NULL OR ekpo_1.loekz = '')
                                        AND             ekpo_1.mandt = 300
                                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort
                                        ) AS qry
                        GROUP BY        qry.matnr,
                                        qry.werks,
                                        qry.lgort
                        ) AS oc_stat_norm
        ON              mard.matnr = oc_stat_norm.matnr
        AND             mard.werks = oc_stat_norm.werks
        AND             mard.lgort = oc_stat_norm.lgort
        
        --URGENCIA
        LEFT JOIN       (
                        --URGENCIA
                        SELECT          qry.matnr,
                                        qry.werks,
                                        qry.lgort,
                                        SUM(qry.tot_urg) AS tot_urg
                        FROM            (
                                        --MUNDO UB
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort,
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
                                        AND             (ekpo_1.loekz IS NULL OR ekpo_1.loekz = '')
                                        AND             likp_1.mandt = 300                                       
                                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort
                                        UNION ALL
                                        --MUNDO STOCK         
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort,
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
                                        AND             (ekpo_1.loekz IS NULL OR ekpo_1.loekz = '')
                                        AND             ekpo_1.mandt = 300
                                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort
                                        ) AS qry
                        GROUP BY        qry.matnr,
                                        qry.werks,
                                        qry.lgort
                        ) AS oc_stat_urg
        ON              mard.matnr = oc_stat_urg.matnr
        AND             mard.werks = oc_stat_urg.werks
        AND             mard.lgort = oc_stat_urg.lgort
        
        --------------------------------------------------------------------------------------------
        -- STATS ENTRADAS MEDIDAS EN TERMINOS DE LINEA
        --------------------------------------------------------------------------------------------
        LEFT JOIN       (
                        SELECT          qry.matnr,
                                        qry.werks,
                                        qry.lgort,
                                        SUM(qry.tot_tras)               AS tot_tras,
                                        SUM(qry.tot_stock_nac)          AS tot_stock_nac,
                                        SUM(qry.tot_stock_fab)          AS tot_stock_fab
                        FROM            (
                                        --MUNDO UB
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort,
                                                        COUNT(*)                AS tot_tras,
                                                        0                       AS tot_stock_nac,
                                                        0                       AS tot_stock_fab
                                        FROM            EKPO ekpo_1
                                        INNER JOIN      EKKO ekko_1
                                        ON              ekko_1.ebeln = ekpo_1.ebeln
                                        AND             ekko_1.mandt = ekpo_1.mandt
                                        WHERE           ekko_1.bsart IN ('UB')
                                        AND             ekko_1.aedat::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
                                        AND             ekko_1.aedat::bigint < 
                                                                to_char(
                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                                        AND             (ekpo_1.loekz IS NULL OR ekpo_1.loekz = '')
                                        AND             ekpo_1.mandt = 300
                                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort
                                        UNION ALL
                                        
                                        --MUNDO STOCK NACIONAL        
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort,
                                                        0                       AS tot_tras,
                                                        COUNT(*)                AS tot_stock_nac,
                                                        0                       AS tot_stock_fab
                                        FROM            EKPO ekpo_1
                                        INNER JOIN      EKKO ekko_1
                                        ON              ekko_1.ebeln = ekpo_1.ebeln
                                        AND             ekko_1.mandt = ekpo_1.mandt
                                        WHERE           ekko_1.bsart NOT IN ('UB', 'Z120')
                                        AND             ekko_1.aedat::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
                                        AND             ekko_1.aedat::bigint < 
                                                                to_char(
                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                                        AND             (ekpo_1.loekz IS NULL OR ekpo_1.loekz = '')
                                        AND             ekpo_1.mandt = 300
                                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort
                                        UNION ALL
                                        
                                        --MUNDO STOCK FAB        
                                        SELECT          ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort,
                                                        0                       AS tot_tras,
                                                        0                       AS tot_stock_nac,
                                                        COUNT(*)                AS tot_stock_fab
                                                        
                                        FROM            EKPO ekpo_1
                                        INNER JOIN      EKKO ekko_1
                                        ON              ekko_1.ebeln = ekpo_1.ebeln
                                        AND             ekko_1.mandt = ekpo_1.mandt
                                        WHERE           ekko_1.bsart IN ('Z120')
                                        AND             ekko_1.aedat::bigint >= CONCAT(IN_ANNO_PROCESO, IN_MES_PROCESO, '01')::bigint
                                        AND             ekko_1.aedat::bigint < 
                                                                to_char(
                                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                                        AND             (ekpo_1.loekz IS NULL OR ekpo_1.loekz = '')
                                        AND             ekpo_1.mandt = 300
                                        
                                        GROUP BY        ekpo_1.matnr,
                                                        ekpo_1.werks,
                                                        ekpo_1.lgort
                                        ) AS qry
                        GROUP BY        qry.matnr,
                                        qry.werks,
                                        qry.lgort
                        ) AS oc_stat_tras_stock
        ON              mard.matnr = oc_stat_tras_stock.matnr
        AND             mard.werks = oc_stat_tras_stock.werks
        AND             mard.lgort = oc_stat_tras_stock.lgort
        
        --------------------------------------------------------------------------------------------
        -- STATS FECHA ULTIMA ENTRADA 101
        --------------------------------------------------------------------------------------------
        LEFT JOIN       (
                        SELECT 
                        DISTINCT ON     (TRIM(mseg_1.matnr), mseg_1.werks, mseg_1.lgort)          
                                        mseg_1.mblnr,
                                        TRIM(mseg_1.matnr)      AS matnr, 
                                        mseg_1.werks, 
                                        mseg_1.lgort,
                                        mkpf_1.budat
                                        
                        FROM            MSEG mseg_1
                        
                        INNER JOIN      MKPF mkpf_1
                        ON              mkpf_1.mblnr = mseg_1.mblnr
                        AND             mkpf_1.mjahr = mseg_1.mjahr    
                        
                        WHERE           mseg_1.shkzg = 'S' 
                        AND             (mseg_1.sobkz = ' ' OR mseg_1.sobkz IS NULL)

                        AND             (mseg_1.insmk IS NULL OR (mseg_1.insmk = 'F' AND mseg_1.werks IN ('0149', '0152', '0154', '0157', '0151', '0156')))
                        AND             (mseg_1.bestq NOT IN ('Q', 'S') OR mseg_1.bestq IS NULL)
                                        --101 EM MATERIAL
                        AND             mseg_1.bwart IN ('101')
                        
                        AND             mkpf_1.budat::bigint < 
                        
                                                to_char(
                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                        ORDER BY        TRIM(mseg_1.matnr), 
                                        mseg_1.werks, 
                                        mseg_1.lgort,
                                        
                                        --20190903 INCORPORA MKPF                
                                        mkpf_1.budat::bigint DESC      
                        ) AS mseg_ult_em_libre
        ON              mard.matnr = mseg_ult_em_libre.matnr
        AND             mard.werks = mseg_ult_em_libre.werks
        AND             mard.lgort = mseg_ult_em_libre.lgort
                
        --------------------------------------------------------------------------------------------
        -- STATS FECHA ULTIMO CONSUMO
        --------------------------------------------------------------------------------------------
        LEFT JOIN       (
                        SELECT 
                        DISTINCT ON     (TRIM(mseg_1.matnr), mseg_1.werks, mseg_1.lgort)          
                                        mseg_1.mblnr,
                                        TRIM(mseg_1.matnr)      AS matnr, 
                                        mseg_1.werks, 
                                        mseg_1.lgort,
                                        mkpf_1.budat
                                        
                        FROM            MSEG mseg_1
                        
                        --20190903 INCORPORA MKPF
                        INNER JOIN      MKPF mkpf_1
                        ON              mkpf_1.mblnr = mseg_1.mblnr
                        AND             mkpf_1.mjahr = mseg_1.mjahr    
                        
                        WHERE           mseg_1.shkzg = 'H' 
                        AND             (mseg_1.sobkz = ' ' OR mseg_1.sobkz IS NULL)
                        AND             mseg_1.insmk IS NULL
                        AND             (mseg_1.bestq NOT IN ('Q', 'S') OR mseg_1.bestq IS NULL)
                                        --201 SALIDA A CECO
                                        --221 PROYECTO
                                        --412 TRASPASO A STOCK ESPECIAL
                                        --601 VENTA
                                        --631 CONSIGNACION
                                        --ZPE 
                                        --Z27 -> OS
                                        --Z21 -> OS
                                        --Z15 -> OS
                                        --Z17 -> OS REL
                                        --261 -> SM ORDEN
                                        --ZPF -> OS
                                        --Z85 -> MANT                                         
                                                 
                        AND             mseg_1.bwart IN ('201', '221', '412', '601', '631', 'ZPE', 'Z27', 'Z21', 'Z15', 'Z17', 
                                                '261', 'ZPF', 'Z85')
                        --20190903 INCORPORA MKPF
                        AND             mkpf_1.budat::bigint < 
                        
                                                to_char(
                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                        ORDER BY        TRIM(mseg_1.matnr), 
                                        mseg_1.werks, 
                                        mseg_1.lgort,
                                        
                                        --20190903 INCORPORA MKPF                
                                        mkpf_1.budat::bigint DESC       
                        ) AS mseg_ult_sm_consumo_libre
        ON              mard.matnr = mseg_ult_sm_consumo_libre.matnr
        AND             mard.werks = mseg_ult_sm_consumo_libre.werks
        AND             mard.lgort = mseg_ult_sm_consumo_libre.lgort
        
        --------------------------------------------------------------------------------------------
        -- STATS FECHA ULTIMO TRASLADO
        --------------------------------------------------------------------------------------------
        LEFT JOIN       (
                        SELECT 
                        DISTINCT ON     (TRIM(mseg_1.matnr), mseg_1.werks, mseg_1.lgort)          
                                        mseg_1.mblnr,
                                        TRIM(mseg_1.matnr)      AS matnr, 
                                        mseg_1.werks, 
                                        mseg_1.lgort,                                        
                                        mkpf_1.budat
                                        
                        FROM            MSEG mseg_1
                        
                        INNER JOIN      MKPF mkpf_1
                        ON              mkpf_1.mblnr = mseg_1.mblnr
                        AND             mkpf_1.mjahr = mseg_1.mjahr 
                        
                        WHERE           mseg_1.shkzg = 'H' 
                        AND             (mseg_1.sobkz = ' ' OR mseg_1.sobkz IS NULL)
                        AND             mseg_1.insmk IS NULL
                        AND             (mseg_1.bestq NOT IN ('Q', 'S') OR mseg_1.bestq IS NULL)
                                        --301 TRASLADO INTERNO
                                        --641 TRASLADO ENTRE CENTROS
                        AND             mseg_1.bwart IN ('301', '641')

                        AND             mkpf_1.budat::bigint < 
                        
                                                to_char(
                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint
                        ORDER BY        TRIM(mseg_1.matnr), 
                                        mseg_1.werks, 
                                        mseg_1.lgort,              
                                        mkpf_1.budat::bigint DESC         
                        ) AS mseg_ult_sm_traslado_libre
        ON              mard.matnr = mseg_ult_sm_traslado_libre.matnr
        AND             mard.werks = mseg_ult_sm_traslado_libre.werks
        AND             mard.lgort = mseg_ult_sm_traslado_libre.lgort
        
        --------------------------------------------------------------------------------------------
        -- STATS FECHA ULTIMO TRASPASO
        --------------------------------------------------------------------------------------------        
        LEFT JOIN       (
                        SELECT 
                        DISTINCT ON     (TRIM(mseg_1.matnr), mseg_1.werks, mseg_1.lgort)          
                                        mseg_1.mblnr,
                                        TRIM(mseg_1.matnr)      AS matnr, 
                                        mseg_1.werks, 
                                        mseg_1.lgort,                                        
                                        mkpf_1.budat
                                        
                        FROM            MSEG mseg_1
                        
                        INNER JOIN      MKPF mkpf_1
                        ON              mkpf_1.mblnr = mseg_1.mblnr
                        AND             mkpf_1.mjahr = mseg_1.mjahr 
                        
                        WHERE           mseg_1.shkzg = 'H' 
                        AND             (mseg_1.sobkz = ' ' OR mseg_1.sobkz IS NULL)
                        AND             mseg_1.insmk IS NULL
                        AND             (mseg_1.bestq NOT IN ('Q', 'S') OR mseg_1.bestq IS NULL)
                                        
                                    
                                        --322 AN TRASP CALIDAD A LIBRE (O TRASPASO LIBRE A CALIDAD)
                                        --344 AN TRASP BLOQ A LIBRE (O TRASPASO LIBRE A BLOQ)
                                        --551 DESGUACE
                                        --Z25 DESPIECE      
                                        --717 SM POR AJUSTE INVENTARIO
                                        --Z03 SM POR RECLAMO A FABRICA
                                        --Z07 SM POR SINIESTRO
                                        --241 SM POR ACTIVO FIJO
                                        --ZYG SM POR RECLAMO A TRANSPORTISTA                                
                        
                        AND             mkpf_1.budat::bigint < 
                        
                                                to_char(
                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint            
                        AND             mseg_1.bwart IN ('322', '344', '551', 'Z25', '717', 'Z03', 'Z07', '241', 'ZYG')
                        ORDER BY        TRIM(mseg_1.matnr), 
                                        mseg_1.werks, 
                                        mseg_1.lgort,
                                        
                                        --20190903 INCORPORA MKPF                
                                        mkpf_1.budat::bigint DESC     
                        ) AS mseg_ult_sm_traspaso_libre
        ON              mard.matnr = mseg_ult_sm_traspaso_libre.matnr
        AND             mard.werks = mseg_ult_sm_traspaso_libre.werks
        AND             mard.lgort = mseg_ult_sm_traspaso_libre.lgort
        
        --------------------------------------------------------------------------------------------
        -- STATS FECHA ULTIMA SALIDA (EN GENERAL)
        --------------------------------------------------------------------------------------------
        LEFT JOIN       (
                        SELECT 
                        DISTINCT ON     (TRIM(mseg_1.matnr), mseg_1.werks, mseg_1.lgort)          
                                        mseg_1.mblnr,
                                        TRIM(mseg_1.matnr)      AS matnr, 
                                        mseg_1.werks, 
                                        mseg_1.lgort,                                        
                                        mkpf_1.budat
                                        
                        FROM            MSEG mseg_1
                        
                        INNER JOIN      MKPF mkpf_1
                        ON              mkpf_1.mblnr = mseg_1.mblnr
                        AND             mkpf_1.mjahr = mseg_1.mjahr 
                        
                        WHERE           mseg_1.shkzg = 'H' 
                        AND             (mseg_1.sobkz = ' ' OR mseg_1.sobkz IS NULL)
                        AND             (mseg_1.insmk IS NULL OR mseg_1.insmk = 'F')
                        AND             (mseg_1.bestq NOT IN ('Q', 'S') OR mseg_1.bestq IS NULL)
                                        
                        AND             mkpf_1.budat::bigint <                         
                                                to_char(
                                                        to_date(IN_ANNO_PROCESO || IN_MES_PROCESO || '01' , 'YYYYMMDD') 
                                                                + INTERVAL '1 month', 'YYYYMMDD')::bigint            
                        AND             mseg_1.bwart NOT IN ('321','343', '506', '717', '713')
                        ORDER BY        TRIM(mseg_1.matnr), 
                                        mseg_1.werks, 
                                        mseg_1.lgort,
                                        
          
                                        mkpf_1.budat::bigint DESC     
                        ) AS mseg_ult_sm_libre
        ON              mard.matnr = mseg_ult_sm_libre.matnr
        AND             mard.werks = mseg_ult_sm_libre.werks
        AND             mard.lgort = mseg_ult_sm_libre.lgort
               
       
        --SOLO CENTROS VIGENTES
        WHERE           mard.werks IN ('0000','0001','0002','0003','0005','0006','0007','0008','0009','0010','0011','0012')

        AND             (
                        mardh.matnr IS NOT NULL
                        OR
                        mskah.matnr IS NOT NULL
                        )
    
        --RESTRICCION DE DATOS A PROCESAR
        AND             t001k.bukrs IN ('SAP1', 'SAP2')   
    
        GROUP BY        t001k.bukrs,
                        mard.werks,
                        mard.lgort,
                        mard.matnr,
                        mara.matkl,
                        
                        --E4
                        marc.prctr,
        
                        mardh.ini_lfgja,
                        mardh.ini_lfmon,
                        mardh.ter_lfgja,
                        mardh.ter_lfmon,
                        
                        mskah.ini_lfgja,
                        mskah.ini_lfmon,
                        mskah.ter_lfgja,
                        mskah.ter_lfmon
        ;


END;
$func$
LANGUAGE plpgsql@