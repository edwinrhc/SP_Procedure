-- DROP PROCEDURE  FONAVI_IUD.SP_RN_ACTUALIZA_FECHA_FALLECIMIENTO;

CREATE OR REPLACE PROCEDURE FONAVI_IUD.SP_UPDDEATHDT(
    p_numero_documento IN VARCHAR2,
    p_fecha_fallecimiento IN VARCHAR2,
    p_usuario_sesion IN VARCHAR2
) AS
BEGIN
--     Procedimiento para Actualizar Fecha de Fallecimiento en FON_RENIEC

    -- Establecer formato de fecha para la sesión
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''DD/MM/YYYY''';

-- Eliminar la restricción de solo lectura (si existe)
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE FONAVI.FON_RENIEC DROP CONSTRAINT FON_RENIEC_READ_ONLY';
    EXCEPTION
        WHEN OTHERS THEN
            -- Manejar excepción si la restricción no existe
            NULL;
    END;

    -- Comprobación inicial (opcional)
    DBMS_OUTPUT.PUT_LINE('Comprobación inicial:');
    FOR r IN (SELECT tipo_documento, numero_documento, estado_reniec, fecha_defuncion, fecha_modificacion
              FROM FONAVI.FON_RENIEC
              WHERE numero_documento = p_numero_documento) LOOP
            DBMS_OUTPUT.PUT_LINE('Tipo Documento: ' || r.tipo_documento);
            DBMS_OUTPUT.PUT_LINE('Número Documento: ' || r.numero_documento);
            DBMS_OUTPUT.PUT_LINE('Estado Reniec: ' || r.estado_reniec);
            DBMS_OUTPUT.PUT_LINE('Fecha Defunción: ' || r.fecha_defuncion);
            DBMS_OUTPUT.PUT_LINE('Fecha Modificación: ' || r.fecha_modificacion);
        END LOOP;

    -- Actualización de la fecha de defunción
    UPDATE FONAVI.FON_RENIEC
    SET fecha_defuncion = TO_DATE(p_fecha_fallecimiento, 'YYYY-MM-DD'),
        fecha_validado_reniec = SYSDATE,
        fecha_modificacion = SYSDATE,
        estado_reniec = 'FALLECIMIENTO',
        fuente_st = 'RENIEC - CONSULTA EN LINEA',
--         usuario_modificacion = 'DBASIFONAVI'
        usuario_modificacion = p_usuario_sesion
    WHERE numero_documento = p_numero_documento;

-- Comprobación final (opcional)
    DBMS_OUTPUT.PUT_LINE('Comprobación final:');
    FOR r IN (SELECT tipo_documento, numero_documento, estado_reniec, fecha_defuncion, fecha_modificacion
              FROM FONAVI.FON_RENIEC
              WHERE numero_documento = p_numero_documento) LOOP
            DBMS_OUTPUT.PUT_LINE('Tipo Documento: ' || r.tipo_documento);
            DBMS_OUTPUT.PUT_LINE('Número Documento: ' || r.numero_documento);
            DBMS_OUTPUT.PUT_LINE('Estado Reniec: ' || r.estado_reniec);
            DBMS_OUTPUT.PUT_LINE('Fecha Defunción: ' || r.fecha_defuncion);
            DBMS_OUTPUT.PUT_LINE('Fecha Modificación: ' || r.fecha_modificacion);
        END LOOP;

    -- Confirmar los cambios
    COMMIT;
END SP_UPDDEATHDT;

--2. Procedimiento para Insertar en GPET_PERSONA_VALIDA
 --DROP PROCEDURE FONAVI.SP_GPE_INSERT_PERSONA_VALIDA;

CREATE OR REPLACE PROCEDURE FONAVI.SP_INSERTVALIDPERSON(
    p_personas_str IN VARCHAR2
) AS
BEGIN
    -- Procedimiento para Insertar en GPET_PERSONA_VALIDA

    -- Eliminar todos los registros de FONAVI.GPET_PERSONA_VALIDA
    DELETE FROM FONAVI.GPET_PERSONA_VALIDA;

-- Insertar nuevos registros en FONAVI.GPET_PERSONA_VALIDA
    FOR rec IN (
        SELECT REGEXP_SUBSTR(p_personas_str, '[^|]+', 1, LEVEL) AS persona_str
        FROM DUAL
        CONNECT BY REGEXP_SUBSTR(p_personas_str, '[^|]+', 1, LEVEL) IS NOT NULL
        ) LOOP
            INSERT INTO FONAVI.GPET_PERSONA_VALIDA (PNT_C_TIPDOC_RENIEC, PNT_N_NRODOC)
            VALUES (
                       REGEXP_SUBSTR(rec.persona_str, '[^,]+', 1, 1),
                       REGEXP_SUBSTR(rec.persona_str, '[^,]+', 1, 2)
                   );
        END LOOP;

    -- Confirmar los cambios
    COMMIT;
END SP_INSERTVALIDPERSON;


ALTER PROCEDURE FONAVI_IUD.SP_UPDDEATHDT COMPILE;

SELECT object_name, status FROM all_objects WHERE object_name = 'SP_UPDDEATHDT' AND object_type = 'PROCEDURE';

SELECT * FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME = 'FON_RENIEC_HIST_F'
  AND OWNER = 'FONAVI';





