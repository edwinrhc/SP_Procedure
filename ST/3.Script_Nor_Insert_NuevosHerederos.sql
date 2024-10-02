-- Paso 1 Crea la tabla
-- DROP TABLE FONAVI_IUD.DHE_T_NUEVOS_HEREDEROS_TMP
CREATE TABLE FONAVI_IUD.DHE_T_HERED_TMP
(
    DOC_C_TIPO VARCHAR2(2),
    DOC_C_NUMERO  VARCHAR2(10),
    PER_D_APELLIDO_PATERNO VARCHAR2(50),
    PER_D_APELLIDO_MATERNO VARCHAR2(50),
    PER_D_NOMBRES VARCHAR2(100),
    PER_F_NACIMIENTO DATE
);

-- Paso 2: Insertar registro en la tabla
--  DROP PROCEDURE FONAVI_IUD.SP_DHE_INSERTAR_Y_PROCESAR_HEREDEROS
CREATE OR REPLACE PROCEDURE FONAVI_IUD.SP_INSER_PROC_HERED(
    p_herederos_str IN VARCHAR2
) AS
BEGIN
    -- Paso 1: Truncar la tabla temporal para eliminar todos los registros
    EXECUTE IMMEDIATE 'TRUNCATE TABLE FONAVI_IUD.DHE_T_HERED_TMP';

    -- Paso 2: Insertar los nuevos registros en la tabla temporal
    FOR rec IN (
        SELECT REGEXP_SUBSTR(p_herederos_str, '[^|]+', 1, LEVEL) AS heredero_str
        FROM DUAL
        CONNECT BY LEVEL <= LENGTH(p_herederos_str) - LENGTH(REPLACE(p_herederos_str, '|', '')) + 1
        ) LOOP
            INSERT INTO FONAVI_IUD.DHE_T_HERED_TMP (
                DOC_C_TIPO, DOC_C_NUMERO, PER_D_NOMBRES, PER_D_APELLIDO_PATERNO, PER_D_APELLIDO_MATERNO, PER_F_NACIMIENTO
            ) VALUES (
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 1),
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 2),
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 3),
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 4),
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 5),
                         TO_DATE(REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 6), 'YYYY-MM-DD')
                     );
        END LOOP;

    -- Commit para asegurar que los registros sean insertados
    COMMIT;

    -- Paso 3: Llamar al procedimiento para comparar e insertar herederos
    FONAVI_IUD.SP_INSERT_H_MASIVO(p_herederos_str);

    -- Mensaje de éxito
    DBMS_OUTPUT.PUT_LINE('Proceso de inserción y comparación completado exitosamente.');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        ROLLBACK;
END SP_INSER_PROC_HERED;
/


-- Paso 3: Insertar masivamente
--  DROP PROCEDURE FONAVI_IUD.SP_DHE_INSERT_NUEVO_HEREDEROS_MASIVO

CREATE OR REPLACE PROCEDURE FONAVI_IUD.SP_INSERT_N_HERED_MAS IS
    CURSOR cur_padron IS
        SELECT * FROM FONAVI_IUD.DHE_T_HERED_TMP;

    v_fuente_str VARCHAR2(50) := 'RENIEC - CONSULTA EN LINEA';
    v_usc_c_usumod NUMBER(10) := 1089190;
    v_f_usuario_modifica DATE := SYSDATE;
    v_ip_terminal_modifica VARCHAR2(40) := SYS_CONTEXT('USERENV', 'IP_ADDRESS');
    n_reg NUMBER(10) := 1;
    n_fon NUMBER(10);
    v_pnc_c_persona FONAVI.GPEC_PERSONA.PNC_C_PERSONA%TYPE;
    v_estado_restriccion VARCHAR2(50) := 'SIN RESTRICCION';

BEGIN
    FOR det_padron IN cur_padron LOOP
            -- Verificar si existe en FON_RENIEC
            SELECT COUNT(1) INTO n_fon
            FROM FONAVI.FON_RENIEC
            WHERE TIPO_DOCUMENTO = det_padron.DOC_C_TIPO
              AND NUMERO_DOCUMENTO = det_padron.DOC_C_NUMERO;

            IF n_fon = 0 THEN
                INSERT INTO FONAVI.FON_RENIEC (
                    TIPO_DOCUMENTO,
                    NUMERO_DOCUMENTO,
                    APELLIDO_PATERNO,
                    APELLIDO_MATERNO,
                    NOMBRES,
                    FECHA_NACIMIENTO,
                    USUARIO_CREACION,
                    FECHA_CREACION,
                    FUENTE_ST,
                    FECHA_VALIDADO_RENIEC,
                    ESTADO_RENIEC
                ) VALUES (
                             det_padron.DOC_C_TIPO,
                             det_padron.DOC_C_NUMERO,
                             det_padron.PER_D_APELLIDO_PATERNO,
                             det_padron.PER_D_APELLIDO_MATERNO,
                             det_padron.PER_D_NOMBRES,
                             det_padron.PER_F_NACIMIENTO,
                             v_usc_c_usumod,
                             v_f_usuario_modifica,
                             v_fuente_str,
                             v_f_usuario_modifica,
                             v_estado_restriccion
                         );
            ELSE
                DBMS_OUTPUT.PUT_LINE('REGISTRO FON_RENIEC EXISTE NUMDOC: ' || det_padron.DOC_C_NUMERO);
            END IF;

            -- Verificar si existe en GPEC_PERSONA
            SELECT COUNT(1) INTO n_fon
            FROM FONAVI.GPEC_PERSONA
            WHERE PDD_C_TIPDOC = det_padron.DOC_C_TIPO
              AND PNC_N_NRODOC = det_padron.DOC_C_NUMERO;

            IF n_fon = 0 THEN
                SELECT NVL(MAX(PNC_C_PERSONA), 0) + 1 INTO v_pnc_c_persona
                FROM FONAVI.GPEC_PERSONA;

                INSERT INTO FONAVI.GPEC_PERSONA (
                    PNC_C_PERSONA,
                    PDD_C_TIPDOC,
                    PNC_N_NRODOC,
                    PNC_D_APEPAT,
                    PNC_D_APEMAT,
                    PNC_D_NOMBRES,
                    PNC_F_NACIMIEN,
                    USC_C_USUCRE,
                    F_USUARIO_CREA,
                    IP_TERMINAL_CREA,
                    PNC_E_VALIDADO_RENIEC,
                    PNC_E_REGISTRO
                ) VALUES (
                             v_pnc_c_persona,
                             det_padron.DOC_C_TIPO,
                             det_padron.DOC_C_NUMERO,
                             det_padron.PER_D_APELLIDO_PATERNO,
                             det_padron.PER_D_APELLIDO_MATERNO,
                             det_padron.PER_D_NOMBRES,
                             det_padron.PER_F_NACIMIENTO,
                             v_usc_c_usumod,
                             v_f_usuario_modifica,
                             v_ip_terminal_modifica,
                             1,
                             1
                         );
            ELSE
                DBMS_OUTPUT.PUT_LINE('REGISTRO GPEC_PERSONA EXISTE NUMDOC: ' || det_padron.DOC_C_NUMERO);
            END IF;

            IF MOD(n_reg, 10) = 0 THEN
                DBMS_OUTPUT.PUT_LINE('REGISTRO: ' || n_reg);
            END IF;

            n_reg := n_reg + 1;

        END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Proceso completado exitosamente. Total de registros procesados: ' || n_reg);

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        ROLLBACK;
END SP_INSERT_N_HERED_MAS;
/


SELECT object_name, status FROM all_objects WHERE object_name = 'SP_INSER_PROC_HERED' AND object_type = 'PROCEDURE';