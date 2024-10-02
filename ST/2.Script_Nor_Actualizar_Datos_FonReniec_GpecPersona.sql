-- Scritp Normalizado ACTUALIZAR DATOS FONRENIEC_GPEC_PERSONA

-- Paso 1: Crear la Tabla Temporal para Insertar Datos

-- DROP TABLE FONAVI_IUD.DHE_T_HEREDEROS_TMP;
CREATE TABLE FONAVI_IUD.DHE_TMPHEIRS (
                                         DOC_C_TIPO VARCHAR2(2),
                                         DOC_C_NUMERO VARCHAR2(10),
                                         PER_D_NOMBRES VARCHAR2(100),
                                         PER_D_APELLIDO_PATERNO VARCHAR2(50),
                                         PER_D_APELLIDO_MATERNO VARCHAR2(50)
);

--Paso 2: Crear la Tabla para Actualizar Datos de RENIEC
-- DROP TABLE FONAVI_IUD.RRN_C_ACT_RENIEC;
CREATE TABLE FONAVI_IUD.RRN_ACT_RENIEC (
                                           DOC_C_TIPO VARCHAR2(20),
                                           DOC_C_NUMERO VARCHAR2(20),
                                           PER_D_APELLIDO_PATERNO VARCHAR2(50),
                                           PER_D_APELLIDO_MATERNO VARCHAR2(50),
                                           PER_D_NOMBRES VARCHAR2(100),
                                           RENIEC_D_ESTADO VARCHAR2(20),
                                           HER_D_TIP_DOC VARCHAR2(20),
                                           HER_C_NUNDOC VARCHAR2(20),
                                           HER_D_APELLIDO_PATERNO VARCHAR2(50),
                                           HER_D_APELLIDO_MATERNO VARCHAR2(50),
                                           HER_D_NOMBRE VARCHAR2(100),
                                           IND_PROCESO NUMBER(1)
);

--Paso 3: Crear la Tabla para Actualizar Datos en GPEC Persona
-- DROP TABLE FONAVI_IUD.DHE_C_HEREDEROS_GPEC_PERSONA;
CREATE TABLE FONAVI_IUD.DHE_HERED_PERSONA (
                                              DOC_C_TIPO VARCHAR2(20),
                                              DOC_C_NUMERO VARCHAR2(20),
                                              PER_D_APELLIDO_PATERNO VARCHAR2(50),
                                              PER_D_APELLIDO_MATERNO VARCHAR2(50),
                                              PER_D_NOMBRES VARCHAR2(100),
                                              GPE_DOC_C_TIPO VARCHAR2(20),
                                              GPE_DOC_C_NUMERO VARCHAR2(20),
                                              GPE_PER_D_APELLIDO_PATERNO VARCHAR2(50),
                                              GPE_PER_D_APELLIDO_MATERNO VARCHAR2(50),
                                              GPE_PER_D_NOMBRES VARCHAR2(100),
                                              IND_PROCESO NUMBER(1)
);

--Paso 4: Procedimiento para Insertar y Comparar Herederos
--  DROP PROCEDURE FONAVI_IUD.SP_DHE_INSERT_COMP_HEREDEROS_MASIVO
CREATE OR REPLACE PROCEDURE FONAVI_IUD.SP_INSERT_H_MASIVO(
    p_herederos_str IN VARCHAR2
) AS
BEGIN
    --Procedimiento para Insertar y Comparar Herederos
    -- Eliminar todos los registros de la tabla temporal antes de insertar nuevos registros
    DELETE FROM FONAVI_IUD.DHE_TMPHEIRS;

    -- Insertar nuevos registros en la tabla temporal
    FOR rec IN (
        SELECT REGEXP_SUBSTR(p_herederos_str, '[^|]+', 1, LEVEL) AS heredero_str
        FROM DUAL
        CONNECT BY LEVEL <= LENGTH(p_herederos_str) - LENGTH(REPLACE(p_herederos_str, '|', '')) + 1
        ) LOOP
            INSERT INTO FONAVI_IUD.DHE_TMPHEIRS (
                DOC_C_TIPO, DOC_C_NUMERO, PER_D_NOMBRES, PER_D_APELLIDO_PATERNO, PER_D_APELLIDO_MATERNO
            ) VALUES (
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 1),
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 2),
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 3),
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 4),
                         REGEXP_SUBSTR(rec.heredero_str, '[^,]+', 1, 5)
                     );
        END LOOP;

    -- Commit para asegurar que los registros sean insertados
    COMMIT;

    -- Eliminar registros de la tabla de actualización de RENIEC
    DELETE FROM FONAVI_IUD.RRN_ACT_RENIEC;

    -- Insertar nuevos registros en la tabla de actualización de RENIEC basada en los nuevos registros
    INSERT INTO FONAVI_IUD.RRN_ACT_RENIEC (
        DOC_C_TIPO, DOC_C_NUMERO, PER_D_APELLIDO_PATERNO, PER_D_APELLIDO_MATERNO, PER_D_NOMBRES,
        RENIEC_D_ESTADO, HER_D_TIP_DOC, HER_C_NUNDOC, HER_D_APELLIDO_PATERNO,
        HER_D_APELLIDO_MATERNO, HER_D_NOMBRE, IND_PROCESO
    )
    SELECT f.tipo_documento, f.numero_documento, f.apellido_paterno, f.apellido_materno,
           f.nombres, f.estado_reniec, t.DOC_C_TIPO, t.DOC_C_NUMERO,
           t.PER_D_APELLIDO_PATERNO, t.PER_D_APELLIDO_MATERNO, t.PER_D_NOMBRES, 0 AS IND_PROCESO
    FROM FONAVI_IUD.DHE_TMPHEIRS t
             INNER JOIN FONAVI.Fon_Reniec f ON t.DOC_C_NUMERO = f.numero_documento AND f.tipo_documento = t.DOC_C_TIPO
    WHERE NVL(f.estado_reniec, '*') <> 'CANCELADO'
      AND (
        UPPER(NVL(TRIM(t.PER_D_APELLIDO_PATERNO), '*')) <> UPPER(NVL(TRIM(f.apellido_paterno), '*')) OR
        UPPER(NVL(TRIM(t.PER_D_APELLIDO_MATERNO), '*')) <> UPPER(NVL(TRIM(f.apellido_materno), '*')) OR
        UPPER(NVL(TRIM(t.PER_D_NOMBRES), '*')) <> UPPER(NVL(TRIM(f.nombres), '*'))
        );

    -- Commit para asegurar que los registros sean insertados
    COMMIT;

    -- Eliminar registros de la tabla de actualización en GPEC Persona
    DELETE FROM FONAVI_IUD.DHE_HERED_PERSONA;

    -- Insertar nuevos registros en la tabla de actualización en GPEC Persona basada en los nuevos registros
    INSERT INTO FONAVI_IUD.DHE_HERED_PERSONA (
        DOC_C_TIPO, DOC_C_NUMERO, PER_D_APELLIDO_PATERNO, PER_D_APELLIDO_MATERNO, PER_D_NOMBRES,
        GPE_DOC_C_TIPO, GPE_DOC_C_NUMERO, GPE_PER_D_APELLIDO_PATERNO, GPE_PER_D_APELLIDO_MATERNO,
        GPE_PER_D_NOMBRES, IND_PROCESO
    )
    SELECT t.DOC_C_TIPO, t.DOC_C_NUMERO, t.PER_D_APELLIDO_PATERNO, t.PER_D_APELLIDO_MATERNO, t.PER_D_NOMBRES,
           gp.pdd_c_tipdoc, gp.pnc_n_nrodoc, gp.pnc_d_apepat, gp.pnc_d_apemat, gp.pnc_d_nombres, 0 AS IND_PROCESO
    FROM FONAVI_IUD.DHE_TMPHEIRS t
             INNER JOIN fonavi.gpec_persona gp ON gp.pnc_n_nrodoc = t.DOC_C_NUMERO AND gp.pdd_c_tipdoc = t.DOC_C_TIPO
    WHERE (
        UPPER(NVL(TRIM(t.PER_D_APELLIDO_PATERNO), '*')) <> UPPER(NVL(TRIM(gp.pnc_d_apepat), '*')) OR
        UPPER(NVL(TRIM(t.PER_D_APELLIDO_MATERNO), '*')) <> UPPER(NVL(TRIM(gp.pnc_d_apemat), '*')) OR
        UPPER(NVL(TRIM(t.PER_D_NOMBRES), '*')) <> UPPER(NVL(TRIM(gp.pnc_d_nombres), '*'))
        ) AND gp.pnc_e_registro = 1;

    -- Commit final para asegurar que los registros sean insertados correctamente
    COMMIT;
END SP_INSERT_H_MASIVO;

-- Paso 5: Procedimiento para Actualizar Herederos en RENIEC
-- DROP PROCEDURE FONAVI_IUD.SP_RN_ACTUALIZA_HEREDEROS_RENIEC

CREATE OR REPLACE PROCEDURE FONAVI_IUD.SP_ACT_HERED_RENIEC IS
    CURSOR cur_padron IS
        SELECT *
        FROM FONAVI_IUD.RRN_ACT_RENIEC
        WHERE IND_PROCESO = 0;

    v_fuente_st                VARCHAR2(50) := 'RENIEC - CONSULTA EN LINEA';
    v_usc_c_usumod             NUMBER(10) := 1089190;
    v_f_usuario_modifica       DATE := SYSDATE;
    v_ip_terminal_modifica     VARCHAR2(40) := SYS_CONTEXT('USERENV', 'IP_ADDRESS');
    n_reg                      NUMBER(10) := 1;
BEGIN
    FOR det_padron IN cur_padron LOOP
            UPDATE fonavi.fon_reniec
            SET apellido_paterno = det_padron.HER_D_APELLIDO_PATERNO,
                apellido_materno = det_padron.HER_D_APELLIDO_MATERNO,
                nombres = det_padron.HER_D_NOMBRE,
                usuario_modificacion  = 'ASULCA',
                fuente_st = v_fuente_st,
                fecha_modificacion    = v_f_usuario_modifica
            WHERE tipo_documento = det_padron.DOC_C_TIPO
              AND numero_documento = det_padron.DOC_C_NUMERO;

            UPDATE FONAVI_IUD.RRN_ACT_RENIEC
            SET IND_PROCESO = 1
            WHERE DOC_C_TIPO = det_padron.DOC_C_TIPO
              AND DOC_C_NUMERO = det_padron.DOC_C_NUMERO;

            IF MOD(n_reg, 10) = 0 THEN
                DBMS_OUTPUT.PUT_LINE('REGISTRO ' || n_reg);
            END IF;

            n_reg := n_reg + 1;
        END LOOP;

    DBMS_OUTPUT.PUT_LINE('FIN ' || n_reg);
END SP_ACT_HERED_RENIEC;

-- Paso 6: Procedimiento para Actualizar Datos en GPEC Persona
-- DROP PROCEDURE FONAVI_IUD.SP_GPE_ACTUALIZA_HEREDEROS_PERSONA

CREATE OR REPLACE PROCEDURE FONAVI_IUD.SP_GPE_ACT_HERED_PER IS
    CURSOR cur_padron IS
        SELECT *
        FROM FONAVI_IUD.DHE_HERED_PERSONA
        WHERE IND_PROCESO = 0;

    v_usc_c_usumod NUMBER(10) := 1089190;
    v_f_usuario_modifica DATE := SYSDATE;
    v_ip_terminal_modifica VARCHAR2(40) := SYS_CONTEXT('USERENV', 'IP_ADDRESS');
    n_reg NUMBER(10) := 1;
BEGIN
    FOR det_padron IN cur_padron LOOP
            UPDATE fonavi.gpec_persona gp
            SET gp.pnc_d_apepat = det_padron.PER_D_APELLIDO_PATERNO,
                gp.pnc_d_apemat = det_padron.PER_D_APELLIDO_MATERNO,
                gp.pnc_d_nombres = det_padron.PER_D_NOMBRES,
                gp.f_usuario_modifica = v_f_usuario_modifica,
                gp.ip_terminal_modifica = v_ip_terminal_modifica,
                gp.usc_c_usumod = v_usc_c_usumod
            WHERE gp.pdd_c_tipdoc = det_padron.DOC_C_TIPO
              AND gp.pnc_n_nrodoc = det_padron.DOC_C_NUMERO;

            UPDATE FONAVI_IUD.DHE_HERED_PERSONA
            SET IND_PROCESO = 1
            WHERE DOC_C_TIPO = det_padron.DOC_C_TIPO
              AND DOC_C_NUMERO = det_padron.DOC_C_NUMERO;

            IF MOD(n_reg, 10) = 0 THEN
                DBMS_OUTPUT.PUT_LINE('REGISTRO ' || n_reg);
            END IF;

            n_reg := n_reg + 1;
        END LOOP;

    DBMS_OUTPUT.PUT_LINE('FIN ' || n_reg);
END SP_GPE_ACT_HERED_PER;



SELECT object_name, status FROM all_objects WHERE object_name = 'SP_GPE_ACT_HERED_PER' AND object_type = 'PROCEDURE';

