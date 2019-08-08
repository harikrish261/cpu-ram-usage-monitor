/* Formatted on 8/6/2019 6:07:22 PM (QP5 v5.336) */
DECLARE
    out_file                        UTL_FILE.file_type;
    app_srvr_dir_name               VARCHAR2 (2000);
    app_srvr_tmp_dir                VARCHAR2 (2000);
    app_srvr_tmp_dir_file           VARCHAR2 (2000);
    v_session                       BOOLEAN;
    v_delete                        BOOLEAN;
    out_file_name                   VARCHAR2 (2000); /* stores output file name */
                                                                        -- 10G
    buf                             VARCHAR2 (1000);       /* output buffer */
    rec_num                         NUMBER := 0;
    strt_time                       VARCHAR2 (8);
    end_time                        VARCHAR2 (8);
    l_stts_slctn                    VARCHAR2 (255);
    l_bsshrt_name                   VARCHAR2 (255);
    text_buf                        VARCHAR2 (2000);
    line_buf                        VARCHAR2 (4000);
    c_dir                           VARCHAR2 (500)
                                        := '/specialty_prod/appl/nadb/utl_file';

    CURSOR main_cur IS
          SELECT /*+ index (term_effctv_date) */
                 DISTINCT
                 o.orgnztn_name
                     AS accnt_name,
                 o.orgnztn_id,
                 cr.sbmttng_agnt_prsn_id,
                 cr.cnsmr_rqst_id,
                 REPLACE (o.oprtns_dscrptn, CHR (10), ' ')
                     AS oprtns_dscrptn,
                 cr.term_effctv_date
                     AS effctv_date,
                 s.stts_name,
                 cr.sbmttng_prdcr_orgnztn_id,
                 po.orgnztn_name
                     AS producer,
                 cre.sccssfl_crrr_id,
                 --REPLACE(cre.rsn_text, chr(10), ' ')   AS  rsn_text,
                 REPLACE (REPLACE (cre.rsn_text, CHR (10), ' '), ',', ' ')
                     AS rsn_text, --Added as part of PPM1258916 (Production defect identified during UT)
                 ro.orgnztn_name
                     AS rgnl_offc_name,
                 ro.orgnztn_shrt_name
                     AS rgnl_offc_nmbr,
                 DECODE (p.last_name || p.frst_name,
                         NULL, NULL,
                         p.last_name || ',' || p.frst_name)
                     AS prdcd_by,               ----Added as part of PPM835468
                 crt.cnsmr_rqst_dscrptn
                     AS cnsmr_rqst_dscrptn       --Added as part of PPM1258916
            FROM orgnztn        o,
                 orgnztn        po,
                 orgnztn        ro,
                 stts           s,
                 cnsmr_rqst     cr,
                 bsnss_sgmnt    bs,
                 cnsmr_rqst_eis cre,
                 prsn           p,
                 cnsmr_rqst_type crt
           WHERE     po.orgnztn_id = cr.sbmttng_prdcr_orgnztn_id
                 AND cr.ro_orgnztn_id = ro.orgnztn_id(+)
                 AND o.orgnztn_id = cr.cnsmr_orgnztn_id
                 AND s.stts_id = cr.stts_id
                 AND cr.assgnd_to_prsn_id = p.prsn_id(+)
                 AND cr.cnsmr_rqst_type_id = crt.cnsmr_rqst_type_id
                 --    AND s.stts_name   NOT IN  ('TEST REQUEST') --for PatM special request 4/15/05
                 AND (   s.stts_name = p_stts_slctn
                      OR (NVL (p_stts_slctn, 'ALL') = 'ALL'))
                 AND cr.term_effctv_date BETWEEN p_str_date AND p_end_date
                 AND cr.bsnss_sgmnt_id = bs.bsnss_sgmnt_id
                 --   AND bs.bsnss_sgmnt_shrt_name    IN ('SAID','MAJAC','SRF','STAFF')
                 AND (   INSTR (p_bsshrt_name, bs.bsnss_sgmnt_shrt_name) > 0
                      OR NVL (p_bsshrt_name, 'ALL') = 'ALL')
                 AND cr.cnsmr_rqst_id = cre.cnsmr_rqst_id
                 AND cr.cnsmr_orgnztn_id <> 487020 -- remove BILLS BAR AND GRILL account
                 AND o.orgnztn_name NOT LIKE '{DNU%'    -- remove DNU accounts
                 AND o.orgnztn_name NOT LIKE '%{%DNU%}%'
                 AND o.orgnztn_name NOT LIKE '%*%DNU%*%'
                 AND o.orgnztn_name NOT LIKE '%(%DNU%)%'
                 AND o.orgnztn_name NOT LIKE '%DNU-%'
                 AND o.orgnztn_name NOT LIKE '%-DNU%'
                 AND o.orgnztn_name NOT LIKE '%- DNU'
                 AND o.orgnztn_name NOT LIKE '%DNU- %'
        ORDER BY o.orgnztn_name, cr.term_effctv_date;

    CURSOR cur_crrnt_crrr_orgnztn (
        l_cnsmr_rqst_id                cnsmr_rqst.cnsmr_rqst_id%TYPE,
        l_accnt_prcng_wrksht_optn_id   accnt_prcng_wrksht.accnt_prcng_wrksht_id%TYPE,
        l_accnt_prcng_smmry_optn_id    accnt_prcng_smmry_optn.accnt_prcng_smmry_optn_id%TYPE)
    IS
        SELECT crrnt_crrr_orgnztn_name
          FROM (  SELECT lro.crrnt_crrr_orgnztn_name
                    FROM loss_rtng              lr,
                         loss_rtng_optn_dtl_vw  lro,
                         loss_pick_optn         lpo,
                         io_prdct               iop,
                         prdct                  p,
                         insrnc_optn            io,
                         accnt_prcng_wrksht     apw,
                         accnt_prcng_wrksht_optn apwo,
                         accnt_prcng_smmry_optn apso
                   WHERE     lr.loss_rtng_id = lro.loss_rtng_id
                         AND lro.loss_rtng_optn_id = lpo.loss_rtng_optn_id
                         AND lpo.loss_pick_optn_id = iop.loss_pick_optn_id
                         AND iop.prdct_id = p.prdct_id
                         AND iop.insrnc_optn_id = io.insrnc_optn_id
                         AND io.insrnc_optn_id = apw.insrnc_optn_id
                         AND apw.accnt_prcng_wrksht_id =
                             apwo.accnt_prcng_wrksht_id
                         AND apwo.accnt_prcng_wrksht_optn_id =
                             apso.accnt_prcng_wrksht_optn_id
                         AND lr.cnsmr_rqst_id = l_cnsmr_rqst_id
                         AND apwo.accnt_prcng_wrksht_optn_id =
                             l_accnt_prcng_wrksht_optn_id
                         AND apso.accnt_prcng_smmry_optn_id =
                             l_accnt_prcng_smmry_optn_id
                ORDER BY DECODE (p.prdct_shrt_name,
                                 'WC', 1,
                                 'AL', 2,
                                 'PREM', 3,
                                 'PROD', 4,
                                 5),
                         lro.plan_prd_effctv_date DESC)
         WHERE ROWNUM < 2;

    cr_main                         main_cur%ROWTYPE;
    ln_cnsmr_rqst_smmry_w_id        NUMBER;
    hq_addrss_rec                   orgnztn_addrss_vw%ROWTYPE;
    lc_hq_main_ph                   VARCHAR2 (25);

    lc_accnt_cntct_name             VARCHAR2 (50);
    lc_accnt_cntct_ph               VARCHAR2 (25);
    lc_prdcr_code                   orgnztn_prdcr_code.prdcr_code%TYPE;
    l_crrnt_crrr                    orgnztn.orgnztn_name%TYPE; --Added as part of PPM835468
    lc_prdcr_cntct_name             VARCHAR2 (50);
    lc_prdcr_cntct_ph               VARCHAR2 (25);
    lc_credit_rating                VARCHAR2 (6);

    ln_accnt_prcng_wrksht_optn_id   NUMBER;
    ln_accnt_prcng_smmry_optn_id    NUMBER;

    lc_prdcts_submttd               VARCHAR2 (250);
    lc_sccssful_crrr                orgnztn.orgnztn_name%TYPE;

    l_ca_expsr                      NUMBER;
    l_tot_expsr                     NUMBER;
    l_expsr_prcnt                   NUMBER;

    ln_ttl_loss_pick_at_250         NUMBER;
    ln_wc_claim_admin               VARCHAR2 (250); --Added as part of PPM966222
    ln_al_claim_admin               VARCHAR2 (250); --Added as part of PPM966222
    ln_gl_claim_admin               VARCHAR2 (250); --Added as part of PPM966222
    ln_frst_$_eqvlnt_prmm           NUMBER;
    ln_bkd_prmm                     NUMBER;
    ln_net_wrttn_prmm               NUMBER;
    lc_heading                      VARCHAR2 (4000);
    lc_data                         VARCHAR2 (4000);
    lc_sqlerrm                      VARCHAR2 (250);
    l_filehandle                    UTL_FILE.FILE_TYPE;

    ln_actl_lc                      NUMBER;
    ln_rqrd_lc                      NUMBER;
    ln_net_at_incm                  NUMBER;
    ln_comb_roe                     NUMBER;
    ln_stg_prcnt                    NUMBER;

    ln_ho_undrwrtr_lst_name         prsn.last_name%TYPE;
    ln_lss_cntrl_cnsltnt_lst_name   prsn.last_name%TYPE;
    ln_lss_cntrl_acc_mgr_lst_name   prsn.last_name%TYPE;
    ln_lss_cntrl_acc_exe_lst_name   prsn.last_name%TYPE;

    -- Get Sold Products Listing
    FUNCTION get_sold_prdcts (p_apwo_id IN NUMBER)
        RETURN VARCHAR2
    IS
        CURSOR prdcts_cur IS
              SELECT DISTINCT (apwppl.prdct_shrt_name)     prdct_shrt_name
                FROM apw_pp_lyr_vw apwppl, accnt_prcng_wrksht_optn apwo
               WHERE     apwo.accnt_prcng_wrksht_id =
                         apwppl.accnt_prcng_wrksht_id
                     AND apwo.accnt_prcng_wrksht_optn_id = p_apwo_id
            /*Start - OUP changes*/
            ORDER BY apwppl.prdct_shrt_name;

        /*End - OUP changes*/
        l_prdcts_string   VARCHAR2 (250) := NULL;
    BEGIN
        IF p_apwo_id IS NULL
        THEN
            RETURN NULL;
        END IF;

        FOR prdcts IN prdcts_cur
        LOOP
            IF prdcts.prdct_shrt_name IS NOT NULL
            THEN
                IF l_prdcts_string IS NULL
                THEN
                    l_prdcts_string := prdcts.prdct_shrt_name;
                ELSE
                    l_prdcts_string :=
                        l_prdcts_string || ',' || prdcts.prdct_shrt_name;
                END IF;
            END IF;
        END LOOP;

        RETURN l_prdcts_string;
    EXCEPTION
        WHEN OTHERS
        THEN
            RETURN NULL;
    END;

    -- Get HQ City and State Code
    PROCEDURE get_hq_addrss (p_orgnztn_id   IN     NUMBER,
                             p_hq_main_ph      OUT VARCHAR2)
    IS
        CURSOR hq_addrss_cur IS
            SELECT *
              FROM orgnztn_addrss_vw
             WHERE orgnztn_id = p_orgnztn_id AND city IS NOT NULL;

        CURSOR c_hq_main_ph IS
            SELECT    phn_area_code
                   || ' '
                   || phn_exchng
                   || ' '
                   || phn_nmbr
                   || ' '
                   || DECODE (phn_extnsn, NULL, NULL, 'x' || phn_extnsn)    phone
              FROM orgnztn_phones
             WHERE     addrss_user_id = p_orgnztn_id
                   AND phn_usg_ctgry_name = 'MAIN';
    BEGIN
        OPEN hq_addrss_cur;

        FETCH hq_addrss_cur INTO hq_addrss_rec;

        CLOSE hq_addrss_cur;

        OPEN c_hq_main_ph;

        FETCH c_hq_main_ph INTO p_hq_main_ph;

        CLOSE c_hq_main_ph;
    EXCEPTION
        WHEN OTHERS
        THEN
            NULL;
    END;

    -- Get Producer Contact Person Name and Contact Phone Info.
    PROCEDURE get_prdcr_cntct (p_prsn_id       IN     NUMBER,
                               p_cntct_name       OUT VARCHAR2,
                               p_cntct_phone      OUT VARCHAR2)
    IS
        CURSOR prsn_cur IS
            SELECT p.frst_name || ' ' || p.last_name     contact_name
              FROM prsn p
             WHERE p.prsn_id = p_prsn_id;

        CURSOR phn_cur IS
            SELECT                                       --phn_usg_ctgry_name,
                      phn_area_code
                   || ' '
                   || phn_exchng
                   || ' '
                   || phn_nmbr
                   || ' '
                   || DECODE (phn_extnsn, NULL, NULL, 'x' || phn_extnsn)    phone
              FROM phones_view
             WHERE phn_usg_ctgry_name = 'MAIN'                -- 'WORK', 'FAX'
                                               AND addrss_user_id = p_prsn_id;
    BEGIN
        IF p_prsn_id IS NULL
        THEN
            RETURN;
        END IF;

        p_cntct_name := NULL;
        p_cntct_phone := NULL;

        OPEN prsn_cur;

        FETCH prsn_cur INTO p_cntct_name;

        CLOSE prsn_cur;
    /*
    OPEN  phn_cur;
    FETCH phn_cur INTO p_cntct_phone;
    CLOSE phn_cur;
    */
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line ('Error in get_prdcr_cntct Proc');
    END;

    -- Get Successful Carrier Name and Reason for Unsccuessful
    PROCEDURE get_sccssfl_crrr (p_cnsmr_rqst_id   IN     NUMBER,
                                p_sccssfl_crrr       OUT VARCHAR2,
                                p_rsn_name           OUT VARCHAR2)
    IS
        CURSOR sccssfl_crrr_cur IS
            SELECT o.orgnztn_name, r.rsn_name
              FROM orgnztn o, rsn r, cnsmr_rqst_eis cre
             WHERE     o.orgnztn_id = cre.sccssfl_crrr_id
                   AND r.rsn_id = cre.rsn_id
                   AND cre.cnsmr_rqst_id = p_cnsmr_rqst_id;
    BEGIN
        IF p_cnsmr_rqst_id IS NULL
        THEN
            RETURN;
        END IF;

        p_sccssfl_crrr := NULL;
        p_rsn_name := NULL;

        OPEN sccssfl_crrr_cur;

        FETCH sccssfl_crrr_cur INTO p_sccssfl_crrr, p_rsn_name;

        CLOSE sccssfl_crrr_cur;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line ('Error in get_sccssfl_crrr Proc ');
    END;

    -- Get Account Contact Details
    PROCEDURE get_accnt_cntct (p_org_id        IN     NUMBER,
                               p_cntct_name       OUT VARCHAR2,
                               p_cntct_phone      OUT VARCHAR2)
    IS
        CURSOR accnt_cntct_cur IS
              SELECT ppv.frst_name || ', ' || ppv.last_name                   cntct_name,
                     ppv.role_name,
                     ppv.prty_id,
                        phn_area_code
                     || ' '
                     || phn_exchng
                     || ' '
                     || phn_nmbr
                     || ' '
                     || DECODE (phn_extnsn, NULL, NULL, 'x' || phn_extnsn)    phone
                FROM prty_prsn_vw ppv, phones_view pv
               WHERE     ppv.prty_id = pv.addrss_user_id
                     AND dpndnt_prty_id = p_org_id
                     AND emplyr_prty_id = p_org_id
                     AND role_name IN ('CONTACT', 'INVOICE RECIPIENT')
                     AND pv.phn_usg_ctgry_name = 'WORK'
            ORDER BY role_name DESC;

        cr_accnt_cntct   accnt_cntct_cur%ROWTYPE;
    -- Get Account Contact Information
    BEGIN
        IF p_org_id IS NULL
        THEN
            RETURN;
        END IF;

        OPEN accnt_cntct_cur;

        FETCH accnt_cntct_cur INTO cr_accnt_cntct;

        CLOSE accnt_cntct_cur;

        p_cntct_name := cr_accnt_cntct.cntct_name;
        p_cntct_phone := cr_accnt_cntct.phone;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line ('Error in get_accnt_cntct proc ');
    END;

    -- Get Producer Code
    PROCEDURE get_prdcr_code (p_cnsmr_rqst_id   IN     NUMBER, -- Added as a part of the PPM1429004
                              /* Commented for PPM1429004
                             --p_prdcr_org_id IN NUMBER, */
                              p_prdcr_code         OUT VARCHAR2)
    IS
        /* Below code added for PPM1429004*/
        CURSOR c_cnsmr_prdcr_code IS
            SELECT prdcr_code
              FROM cnsmr_rqst_eis
             WHERE cnsmr_rqst_id = p_cnsmr_rqst_id;
    /* Below code Commented for PPM1429004
     CURSOR c_prdcr_code IS
       SELECT prdcr_code
         FROM orgnztn_prdcr_code
         WHERE orgnztn_id =  p_prdcr_org_id
        ORDER BY orgnztn_prdcr_code_id;
     */

    BEGIN
        /* Below code Commented for PPM1429004
          IF p_prdcr_org_id IS NULL THEN
              RETURN;
           END IF;   */

        OPEN c_cnsmr_prdcr_code;

        FETCH c_cnsmr_prdcr_code INTO p_prdcr_code;

        CLOSE c_cnsmr_prdcr_code;
    END;

    -- Get ROP Values
    PROCEDURE get_rop_values (p_apso_id                IN     NUMBER,
                              p_ttl_loss_pick_at_250      OUT NUMBER,
                              p_wc_claim_admin            OUT VARCHAR2, --Added as part of PPM966222
                              p_gl_claim_admin            OUT VARCHAR2, --Added as part of PPM966222
                              p_al_claim_admin            OUT VARCHAR2, --Added as part of PPM966222
                              p_frst_$_eqvlnt_prmm        OUT NUMBER,
                              p_bkd_prmm                  OUT NUMBER,
                              p_net_wrttn_prmm            OUT NUMBER,
                              p_actl_lc                   OUT NUMBER, --Added as part of PPM1258916
                              p_rqrd_lc                   OUT NUMBER, --Added as part of PPM1258916
                              p_net_at_incm               OUT NUMBER, --Added as part of PPM1258916
                              p_comb_roe                  OUT NUMBER, --Added as part of PPM1258916
                              p_stg_prcnt                 OUT NUMBER) --Added as part of PPM1258916
    IS
        ln_rop_accnt_infrmtn_id   NUMBER;
        --Added as part of PPM1258916
        ln_actl_trty              NUMBER;
        ln_net_prem               NUMBER;
        ln_trgt_prem              NUMBER;
        --Added as part of PPM1258916

        -- Added as part of PPM1355215
        ln_retro_xcss_loss        NUMBER;
    BEGIN
        IF p_apso_id IS NULL
        THEN
            RETURN;
        END IF;

        SELECT rop_accnt_infrmtn_id
          INTO ln_rop_accnt_infrmtn_id
          FROM rop_accnt_infrmtn
         WHERE rop_accnt_prcng_smmry_optn_id = p_apso_id;

        IF ln_rop_accnt_infrmtn_id IS NOT NULL
        THEN
            SELECT SUM (a.rop_otpt_nmbr_vl)
              INTO p_bkd_prmm
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     b.rop_otpt_elmnt_nmbr = 125
                   AND a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id;

            SELECT SUM (a.rop_otpt_nmbr_vl)
              INTO p_net_wrttn_prmm
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     b.rop_otpt_elmnt_nmbr = 126
                   AND a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id;

            SELECT SUM (a.rop_otpt_nmbr_vl)
              INTO p_frst_$_eqvlnt_prmm
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     b.rop_otpt_elmnt_nmbr = 127
                   AND a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id;

            SELECT SUM (NVL (a.rop_otpt_nmbr_vl, 0))
              INTO p_ttl_loss_pick_at_250
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     b.rop_otpt_elmnt_nmbr BETWEEN 87 AND 102
                   AND a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id;

            --Below code is added as part of the PPM966222
            BEGIN
                SELECT DECODE (a.rop_otpt_char_vl,
                               NULL, TO_CHAR (a.rop_otpt_nmbr_vl),
                               a.rop_otpt_char_vl)
                  INTO p_wc_claim_admin
                  FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
                 WHERE     b.rop_otpt_elmnt_nmbr = 189
                       AND a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                       AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id;
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                           'No data found for ROP : '
                        || p_apso_id
                        || ' lnId : '
                        || ln_rop_accnt_infrmtn_id);
                    NULL;
            END;

            BEGIN
                SELECT DECODE (a.rop_otpt_char_vl,
                               NULL, TO_CHAR (a.rop_otpt_nmbr_vl),
                               a.rop_otpt_char_vl)
                  INTO p_gl_claim_admin
                  FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
                 WHERE     b.rop_otpt_elmnt_nmbr = 433
                       AND a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                       AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id;
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                           'No data found for ROP : '
                        || p_apso_id
                        || ' lnId : '
                        || ln_rop_accnt_infrmtn_id);
                    NULL;
            END;

            BEGIN
                SELECT DECODE (a.rop_otpt_char_vl,
                               NULL, TO_CHAR (a.rop_otpt_nmbr_vl),
                               a.rop_otpt_char_vl)
                  INTO p_al_claim_admin
                  FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
                 WHERE     b.rop_otpt_elmnt_nmbr = 434
                       AND a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                       AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id;
            --End of code for PPM966222
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                           'No data found for ROP : '
                        || p_apso_id
                        || ' lnId : '
                        || ln_rop_accnt_infrmtn_id);
                    NULL;
            END;


            -- added for PPM1258916

            -- Actual LC
            SELECT SUM (NVL (a.rop_otpt_nmbr_vl, 0))
              INTO p_actl_lc
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id
                   AND b.rop_otpt_elmnt_nmbr = 42;


            -- Required LC
            SELECT SUM (NVL (a.rop_otpt_nmbr_vl, 0))
              INTO p_rqrd_lc
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id
                   AND b.rop_otpt_elmnt_nmbr = 43;

            -- Net AT Income
            SELECT ROUND (SUM (NVL (a.rop_otpt_nmbr_vl, 0)))
              INTO p_net_at_incm
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id
                   AND b.rop_otpt_elmnt_nmbr = 128;


            -- Combined ROE
            SELECT ROUND (SUM (NVL (a.rop_otpt_nmbr_vl, 0)), 1)
              INTO p_comb_roe
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id
                   AND b.rop_otpt_elmnt_nmbr = 147;

            -- Actual Treaty
            SELECT SUM (NVL (a.rop_otpt_nmbr_vl, 0))
              INTO ln_actl_trty
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id
                   AND b.rop_otpt_elmnt_nmbr = 35;


            -- Net Premium
            SELECT SUM (NVL (a.rop_otpt_nmbr_vl, 0))
              INTO ln_net_prem
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id
                   AND b.rop_otpt_elmnt_nmbr = 126;

            -- Target Premium incl Prefund and Loss Deposit
            SELECT SUM (NVL (a.rop_otpt_nmbr_vl, 0))
              INTO ln_trgt_prem
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id
                   AND b.rop_otpt_elmnt_nmbr = 242;

            -- Below code commented and added as part of PPM1355215

            -- WC retro exp losses      = 71
            -- Prod Retro Exp losses    = 75
            -- Prem retro exp losses    = 79
            -- AL retro expected losses = 83

            SELECT SUM (a.rop_otpt_nmbr_vl)
              INTO ln_retro_xcss_loss
              FROM rop_otpt_elmnt_vls a, rop_otpt_elmnts b
             WHERE     a.rop_otpt_elmnt_id = b.rop_otpt_elmnt_id
                   AND a.rop_accnt_infrmtn_id = ln_rop_accnt_infrmtn_id
                   AND b.rop_otpt_elmnt_nmbr IN (71,
                                                 75,
                                                 79,
                                                 83);

            IF (ln_trgt_prem - ln_retro_xcss_loss) <> 0
            THEN
                p_stg_prcnt :=
                    ROUND (
                          (ln_actl_trty + ln_net_prem - ln_retro_xcss_loss)
                        * 100
                        / (ln_trgt_prem - ln_retro_xcss_loss),
                        1);
            ELSE
                p_stg_prcnt := NULL;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (
                   'No data found for ROP : '
                || p_apso_id
                || ' lnId : '
                || ln_rop_accnt_infrmtn_id);
            NULL;
    END get_rop_values;

    -- Get Credit Rating
    PROCEDURE get_crdt_rtng (p_cnsmr_rqst_id   IN     NUMBER,
                             p_credit_rating      OUT VARCHAR2)
    IS
        CURSOR c_cr IS
              SELECT /*+ index (cnsmr_rqst_id) */
                     frsv.crdt_risk_rtng_code     AS credit_rating
                FROM fnncl_rvw_rqst frr, fnncl_rvw_stts_vw frsv
               WHERE     frsv.fnncl_rvw_rqst_id = frr.fnncl_rvw_rqst_id
                     AND frr.cnsmr_rqst_id = p_cnsmr_rqst_id
                     AND frsv.fnncl_rvw_stts_type_name = 'COMPLETE'
            ORDER BY fnncl_rvw_stts_id DESC;
    BEGIN
        OPEN c_cr;

        FETCH c_cr INTO p_credit_rating;

        CLOSE c_cr;
    EXCEPTION
        WHEN OTHERS
        THEN
            NULL;
    END;

    --GET EXPOSURE AMOUNT
    PROCEDURE get_expsr_amnt (p_orgnztn_id   IN     NUMBER,
                              p_eff_date     IN     DATE,
                              p_ca_expsr        OUT NUMBER,
                              p_tot_expsr       OUT NUMBER)
    IS
        CURSOR expsr_amnt IS
            SELECT /*+ index (expsr_as_of_date) */
                   SUM (
                       DECODE (ose.stt_abbrvtn,
                               'CA', NVL (ose.expsr_amnt, 0),
                               0))                  CA_Expsr,
                   SUM (NVL (ose.expsr_amnt, 0))    tot_expsr
              FROM ou_stt_expsr_VW ose
             WHERE     ose.orgnztn_id = p_orgnztn_id
                   AND ose.expsr_type_code = 'AWCP'
                   AND ose.expsr_as_of_date =
                       (SELECT MAX (expsr_as_of_date)
                          FROM ou_stt_expsr_vw ose
                         WHERE     ose.orgnztn_id = p_orgnztn_id
                               AND ose.expsr_as_of_date < p_eff_date
                               AND ose.expsr_type_code = 'AWCP');
    BEGIN
        p_ca_expsr := NULL;
        p_tot_expsr := NULL;

        OPEN expsr_amnt;

        FETCH expsr_amnt INTO p_ca_expsr, p_tot_expsr;

        CLOSE expsr_amnt;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line ('Error in get_expsr_amnt Proc');
            NULL;
    END;

    --Get APSO, CAPSO Ids
    PROCEDURE get_apso_capso_ids (
        p_cnsmr_rqst_id                IN     NUMBER,
        p_stts_name                    IN     VARCHAR2,
        p_accnt_prcng_wrksht_optn_id      OUT NUMBER,
        p_accnt_prcng_smmry_optn_id       OUT NUMBER)
    IS
        CURSOR sold_cur IS
            SELECT /*+ index (cnsmr_rqst_id) */
                   apso.accnt_prcng_wrksht_optn_id,
                   apso.accnt_prcng_smmry_optn_id
              FROM accnt_prcng_smmry_optn      apso,
                   cr_apprvd_prcng_smmry_optn  capso
             WHERE     apso.accnt_prcng_smmry_optn_id =
                       capso.accnt_prcng_smmry_optn_id
                   AND capso.cnsmr_rqst_id = p_cnsmr_rqst_id
                   AND capso.cr_apprvd_prcng_smmry_optn_id =
                       (SELECT MAX (capso2.cr_apprvd_prcng_smmry_optn_id)
                          FROM cr_apprvd_prcng_smmry_optn capso2
                         WHERE     capso2.cnsmr_rqst_id = p_cnsmr_rqst_id
                               AND p_stts_name = 'SUCCESSFUL'
                               AND capso2.sold_prgrm_indctr = 'Y');

        CURSOR apprvd_cur IS
            SELECT /*+ index (cnsmr_rqst_id) */
                   apso.accnt_prcng_wrksht_optn_id,
                   apso.accnt_prcng_smmry_optn_id
              FROM accnt_prcng_smmry_optn      apso,
                   cr_apprvd_prcng_smmry_optn  capso
             WHERE     apso.accnt_prcng_smmry_optn_id =
                       capso.accnt_prcng_smmry_optn_id
                   AND capso.cnsmr_rqst_id = p_cnsmr_rqst_id
                   AND capso.cr_apprvd_prcng_smmry_optn_id =
                       (SELECT MAX (capso2.cr_apprvd_prcng_smmry_optn_id)
                          FROM cr_apprvd_prcng_smmry_optn capso2
                         WHERE     capso2.cnsmr_rqst_id = p_cnsmr_rqst_id
                               AND p_stts_name != 'SUCCESSFUL'
                               AND capso2.apprvd_indctr = 'Y');

        CURSOR notapprvd_cur IS
            SELECT /*+ index (cnsmr_rqst_id) */
                   apso.accnt_prcng_wrksht_optn_id,
                   apso.accnt_prcng_smmry_optn_id
              FROM accnt_prcng_smmry_optn apso
             WHERE     apso.cnsmr_rqst_id = p_cnsmr_rqst_id
                   AND apso.accnt_prcng_smmry_optn_id =
                       (SELECT MAX (apso2.accnt_prcng_smmry_optn_id)
                          FROM accnt_prcng_smmry_optn apso2
                         WHERE apso2.cnsmr_rqst_id = p_cnsmr_rqst_id);
    BEGIN
        p_accnt_prcng_wrksht_optn_id := NULL;
        p_accnt_prcng_smmry_optn_id := NULL;

        OPEN sold_cur;

        FETCH sold_cur
            INTO p_accnt_prcng_wrksht_optn_id, p_accnt_prcng_smmry_optn_id;

        IF sold_cur%NOTFOUND
        THEN
            p_accnt_prcng_wrksht_optn_id := NULL;
            p_accnt_prcng_smmry_optn_id := NULL;
        END IF;

        CLOSE sold_cur;

        IF p_accnt_prcng_smmry_optn_id IS NULL
        THEN
            OPEN apprvd_cur;

            FETCH apprvd_cur
                INTO p_accnt_prcng_wrksht_optn_id,
                     p_accnt_prcng_smmry_optn_id;

            IF apprvd_cur%NOTFOUND
            THEN
                p_accnt_prcng_wrksht_optn_id := NULL;
                p_accnt_prcng_smmry_optn_id := NULL;
            END IF;

            CLOSE apprvd_cur;

            IF p_accnt_prcng_smmry_optn_id IS NULL
            THEN
                OPEN notapprvd_cur;

                FETCH notapprvd_cur
                    INTO p_accnt_prcng_wrksht_optn_id,
                         p_accnt_prcng_smmry_optn_id;

                IF notapprvd_cur%NOTFOUND
                THEN
                    p_accnt_prcng_wrksht_optn_id := NULL;
                    p_accnt_prcng_smmry_optn_id := NULL;
                END IF;

                CLOSE notapprvd_cur;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line ('Error in get_expsr_amnt Proc');
            NULL;
    END get_apso_capso_ids;

    -- Get Current Account Last Names Information
    PROCEDURE get_crrnt_accnt_lst_name (
        p_org_id                       IN     NUMBER,
        p_ho_undrwrtr_lst_name            OUT VARCHAR2,
        p_lss_cntrl_cnsltnt_lst_name      OUT VARCHAR2,
        p_lss_cntrl_acc_mgr_lst_name      OUT VARCHAR2,
        p_lss_cntrl_acc_exe_lst_name      OUT VARCHAR2)
    IS
        CURSOR crrnt_accnt_lst_name_cur IS
            SELECT MIN (ho_undrwrtr)      ho_undrwrtr,
                   MIN (lc_cnsltnt)       lc_cnsltnt,
                   MIN (lc_accnt_mgr)     lc_accnt_mgr,
                   MIN (lc_accnt_exe)     lc_accnt_exe
              FROM (  SELECT CASE
                                 WHEN role_name = 'H/O UNDERWRITER'
                                 THEN
                                     last_name
                                 ELSE
                                     NULL
                             END    ho_undrwrtr,
                             CASE
                                 WHEN role_name = 'LOSS CONTROL CONSULTANT'
                                 THEN
                                     last_name
                                 ELSE
                                     NULL
                             END    lc_cnsltnt,
                             CASE
                                 WHEN role_name =
                                      'LOSS CONTROL ACCOUNT MANAGER'
                                 THEN
                                     last_name
                                 ELSE
                                     NULL
                             END    lc_accnt_mgr,
                             CASE
                                 WHEN role_name =
                                      'LOSS CONTROL ACCOUNT EXECUTIVE'
                                 THEN
                                     last_name
                                 ELSE
                                     NULL
                             END    lc_accnt_exe
                        FROM role_prsn_phn_vw rpp
                       WHERE     dpndnt_prty_id = p_org_id
                             AND EXISTS
                                     (SELECT 1
                                        FROM role_grp rg
                                       WHERE     rg.role_grp_id =
                                                 rpp.role_grp_id
                                             AND rg.role_grp_name =
                                                 'HIG ACCOUNT TEAM') -- This group is used in the Current Account Team screen
                             AND rpp.role_name IN
                                     ('H/O UNDERWRITER',
                                      'LOSS CONTROL CONSULTANT',
                                      'LOSS CONTROL ACCOUNT MANAGER',
                                      'LOSS CONTROL ACCOUNT EXECUTIVE')
                    ORDER BY role_name, last_name, frst_name) -- This order by used as the same ordering is used in the Current Account Team screen
                                                             /*PIVOT (MIN(last_name)
                                                             FOR    role_name
                                                             IN ('H/O UNDERWRITER'                AS HO_UNDRWRTR,
                                                               'LOSS CONTROL CONSULTANT'        AS LC_CNSLTNT,
                                                               'LOSS CONTROL ACCOUNT MANAGER'   AS LC_ACCNT_MGR,
                                                               'LOSS CONTROL ACCOUNT EXECUTIVE' AS LC_ACCNT_EXE)*/
                                                             ;

        crrnt_accnt_lst_name   crrnt_accnt_lst_name_cur%ROWTYPE;
    -- Get Current Account Last Names Information
    BEGIN
        OPEN crrnt_accnt_lst_name_cur;

        FETCH crrnt_accnt_lst_name_cur INTO crrnt_accnt_lst_name;

        CLOSE crrnt_accnt_lst_name_cur;

        p_ho_undrwrtr_lst_name := crrnt_accnt_lst_name.ho_undrwrtr;
        p_lss_cntrl_cnsltnt_lst_name := crrnt_accnt_lst_name.lc_cnsltnt;
        p_lss_cntrl_acc_mgr_lst_name := crrnt_accnt_lst_name.lc_accnt_mgr;
        p_lss_cntrl_acc_exe_lst_name := crrnt_accnt_lst_name.lc_accnt_exe;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line ('Error in get_crrnt_accnt_lst_name proc ');
    END;
BEGIN
    --  MESSAGEBOX('JUST ENTERED ');
    SELECT TO_CHAR (SYSDATE, 'HH24:MI:SS') INTO strt_time FROM DUAL;

    IF p_stts_slctn IS NULL
    THEN
        l_stts_slctn := 'ALL';
    ELSE
        l_stts_slctn := p_stts_slctn;
    END IF;

    IF p_bsshrt_name IS NULL
    THEN
        l_bsshrt_name := 'ALL';
    ELSE
        l_bsshrt_name := p_bsshrt_name;
    END IF;

    out_file :=
        UTL_FILE.fopen (
            c_dir,
            'RMD Lead Generation/Declination_report_08062019.txt',
            'A');
    text_buf :=
           'Eff.Date'
        || CHR (9)
        || 'Address 1'
        || CHR (9)
        || 'Address 2'
        || CHR (9)
        || 'City'
        || CHR (9)
        || 'State'
        || CHR (9)
        || 'Zip'
        || CHR (9)
        || 'Extn'
        || CHR (9)
        || 'Main Phone'
        || CHR (9)
        || 'Account Contact Name'
        || CHR (9)
        || 'Account Contact Phone'
        || CHR (9)
        || 'Operation Description'
        || CHR (9)
        || 'Producer'
        || CHR (9)
        || 'Producer Code'
        || CHR (9)
        || 'Producer Contact Name'
        || CHR (9)
        || 'Produced By'
        || CHR (9)
        || 'Status'
        || CHR (9)
        || 'Reason'
        || CHR (9)
        || 'Successful Carrier'
        || CHR (9)
        || 'Current Carrier'
        || CHR (9)
        || 'CRAU Credit Rating'
        || CHR (9)
        || 'Products'
        || CHR (9)
        || 'Total Loss Pick @250'
        || CHR (9)
        || 'WC Claim Admin'
        || CHR (9)
        || 'GL Claim Admin'
        || CHR (9)
        || 'AL Claim Admin'
        || CHR (9)
        || 'First $ Eq. Premium'
        || CHR (9)
        || 'Booked Premium'
        || CHR (9)
        || 'Net Written Premium'
        || CHR (9)
        || 'Total Exposure'
        || CHR (9)
        || 'CA Exposure'
        || CHR (9)
        || 'CA Exposure Percent'
        || CHR (9)
        || 'RO Code'
        || CHR (9)
        || 'RO Name'
        || CHR (9)
        || 'New/Renewal'
        || CHR (9)
        || 'H/O Underwriter'
        || CHR (9)
        || 'LC Consultant'
        || CHR (9)
        || 'LC  Account Manager'
        || CHR (9)
        || 'LC Account Executive'
        || CHR (9)
        || 'Actual LC'
        || CHR (9)
        || 'Required LC'
        || CHR (9)
        || 'Net AT Income'
        || CHR (9)
        || 'Combined ROE'
        || CHR (9)
        || 'Sold to Guide (STG) %';
    UTL_FILE.put_line (out_file, text_buf);

    OPEN main_cur;

    LOOP
        FETCH main_cur INTO cr_main;

        EXIT WHEN main_cur%NOTFOUND;

        lc_hq_main_ph := NULL;
        lc_accnt_cntct_name := NULL;
        lc_accnt_cntct_ph := NULL;
        lc_prdcr_code := NULL;
        lc_prdcr_cntct_name := NULL;
        lc_prdcr_cntct_ph := NULL;
        lc_prdcts_submttd := NULL;
        ln_ttl_loss_pick_at_250 := NULL;
        ln_wc_claim_admin := NULL;                --Added as part of PPM966222
        ln_al_claim_admin := NULL;                --Added as part of PPM966222
        ln_gl_claim_admin := NULL;                --Added as part of PPM966222
        ln_frst_$_eqvlnt_prmm := NULL;
        ln_bkd_prmm := NULL;
        ln_net_wrttn_prmm := NULL;
        lc_sccssful_crrr := NULL;
        l_crrnt_crrr := NULL;                     --Added as part of PPM835468
        lc_credit_rating := NULL;
        ln_accnt_prcng_wrksht_optn_id := NULL;
        ln_accnt_prcng_smmry_optn_id := NULL;
        l_ca_expsr := NULL;
        l_tot_expsr := NULL;
        l_expsr_prcnt := NULL;

        ln_actl_lc := NULL;                     -- Added as part of PPM1258916
        ln_rqrd_lc := NULL;                     -- Added as part of PPM1258916
        ln_net_at_incm := NULL;                 -- Added as part of PPM1258916
        ln_comb_roe := NULL;                    -- Added as part of PPM1258916

        ln_ho_undrwrtr_lst_name := NULL;        -- Added as part of PPM1258916
        ln_lss_cntrl_cnsltnt_lst_name := NULL;  -- Added as part of PPM1258916
        ln_lss_cntrl_acc_mgr_lst_name := NULL;  -- Added as part of PPM1258916
        ln_lss_cntrl_acc_exe_lst_name := NULL;  -- Added as part of PPM1258916


        IF cr_main.sccssfl_crrr_id IS NOT NULL
        THEN
            BEGIN
                SELECT orgnztn_name
                  INTO lc_sccssful_crrr
                  FROM orgnztn
                 WHERE orgnztn_id = cr_main.sccssfl_crrr_id;
            EXCEPTION
                WHEN OTHERS
                THEN
                    NULL;
            END;
        END IF;


        get_apso_capso_ids (cr_main.cnsmr_rqst_id,
                            cr_main.stts_name,
                            ln_accnt_prcng_wrksht_optn_id,
                            ln_accnt_prcng_smmry_optn_id);

        get_rop_values (ln_accnt_prcng_smmry_optn_id,
                        ln_ttl_loss_pick_at_250,
                        ln_wc_claim_admin,        --Added as part of PPM966222
                        ln_gl_claim_admin,        --Added as part of PPM966222
                        ln_al_claim_admin,        --Added as part of PPM966222
                        ln_frst_$_eqvlnt_prmm,
                        ln_bkd_prmm,
                        ln_net_wrttn_prmm,
                        ln_actl_lc,             -- Added as part of PPM1258916
                        ln_rqrd_lc,             -- Added as part of PPM1258916
                        ln_net_at_incm,         -- Added as part of PPM1258916
                        ln_comb_roe,            -- Added as part of PPM1258916
                        ln_stg_prcnt);          -- Added as part of PPM1258916

        OPEN cur_crrnt_crrr_orgnztn (cr_main.cnsmr_rqst_id,
                                     ln_accnt_prcng_wrksht_optn_id,
                                     ln_accnt_prcng_smmry_optn_id);

        FETCH cur_crrnt_crrr_orgnztn INTO l_crrnt_crrr;

        IF cur_crrnt_crrr_orgnztn%NOTFOUND
        THEN
            l_crrnt_crrr := NULL;
        END IF;

        CLOSE cur_crrnt_crrr_orgnztn;


        lc_prdcts_submttd := get_sold_prdcts (ln_accnt_prcng_wrksht_optn_id);
        get_hq_addrss (cr_main.orgnztn_id, lc_hq_main_ph);
        get_accnt_cntct (cr_main.orgnztn_id,
                         lc_accnt_cntct_name,
                         lc_accnt_cntct_ph);
        get_prdcr_code ( /* Below code Commented for PPM1429004
                 cr_main.sbmttng_prdcr_orgnztn_id,*/
                        /*Below code added for PPM1429004*/
                        cr_main.cnsmr_rqst_id, lc_prdcr_code);
        get_prdcr_cntct (cr_main.sbmttng_agnt_prsn_id,
                         lc_prdcr_cntct_name,
                         lc_prdcr_cntct_ph);
        get_crdt_rtng (cr_main.cnsmr_rqst_id, lc_credit_rating);
        get_expsr_amnt (cr_main.orgnztn_id,
                        cr_main.effctv_date,
                        l_ca_expsr,
                        l_tot_expsr);

        BEGIN
            l_expsr_prcnt :=
                ROUND ((NVL (l_ca_expsr, 0) * 100) / NVL (l_tot_expsr, 0), 2);
        EXCEPTION
            WHEN OTHERS
            THEN
                l_expsr_prcnt := NULL;
        END;

        IF l_tot_expsr = 0
        THEN
            l_tot_expsr := NULL;
        END IF;

        IF l_ca_expsr = 0
        THEN
            l_ca_expsr := NULL;
        END IF;

        -- added for PPM1258916
        IF cr_main.orgnztn_id IS NOT NULL
        THEN
            get_crrnt_accnt_lst_name (
                p_org_id                 => cr_main.orgnztn_id,
                p_ho_undrwrtr_lst_name   => ln_ho_undrwrtr_lst_name,
                p_lss_cntrl_cnsltnt_lst_name   =>
                    ln_lss_cntrl_cnsltnt_lst_name,
                p_lss_cntrl_acc_mgr_lst_name   =>
                    ln_lss_cntrl_acc_mgr_lst_name,
                p_lss_cntrl_acc_exe_lst_name   =>
                    ln_lss_cntrl_acc_exe_lst_name);
        END IF;

        text_buf :=
               RTRIM (cr_main.accnt_name)
            || CHR (9)
            || RTRIM (TO_CHAR (cr_main.effctv_date, 'MM/DD/YYYY'))
            || CHR (9)
            || RTRIM (hq_addrss_rec.addrss_ln_1)
            || CHR (9)
            || RTRIM (hq_addrss_rec.addrss_ln_2)
            || CHR (9)
            || RTRIM (hq_addrss_rec.city)
            || CHR (9)
            || RTRIM (hq_addrss_rec.stt_cd)
            || CHR (9)
            || RTRIM (hq_addrss_rec.zip_cd)
            || CHR (9)
            || RTRIM (hq_addrss_rec.zip_extn)
            || CHR (9)
            || RTRIM (lc_hq_main_ph)
            || CHR (9)
            || RTRIM (lc_accnt_cntct_name)
            || CHR (9)
            || RTRIM (lc_accnt_cntct_ph)
            || CHR (9)
            || RTRIM (cr_main.oprtns_dscrptn)
            || CHR (9)
            || RTRIM (cr_main.producer)
            || CHR (9)
            || RTRIM (lc_prdcr_code)
            || CHR (9)
            || RTRIM (lc_prdcr_cntct_name)
            || CHR (9)
            || RTRIM (cr_main.prdcd_by)
            || CHR (9)
            || RTRIM (cr_main.stts_name)
            || CHR (9)
            || RTRIM (cr_main.rsn_text)
            || CHR (9)
            || RTRIM (lc_sccssful_crrr)
            || CHR (9)
            || RTRIM (l_crrnt_crrr)
            || CHR (9)
            || RTRIM (lc_credit_rating)
            || CHR (9)
            || RTRIM (lc_prdcts_submttd)
            || CHR (9)
            || RTRIM (TO_CHAR (ln_ttl_loss_pick_at_250))
            || CHR (9)
            || RTRIM (ln_wc_claim_admin)
            || CHR (9)
            || RTRIM (ln_gl_claim_admin)
            || CHR (9)
            || RTRIM (ln_al_claim_admin)
            || CHR (9)
            || RTRIM (TO_CHAR (ln_frst_$_eqvlnt_prmm))
            || CHR (9)
            || RTRIM (TO_CHAR (ln_bkd_prmm))
            || CHR (9)
            || RTRIM (TO_CHAR (ln_net_wrttn_prmm))
            || CHR (9)
            || RTRIM (TO_CHAR (l_tot_expsr))
            || CHR (9)
            || RTRIM (TO_CHAR (l_ca_expsr))
            || CHR (9)
            || RTRIM (TO_CHAR (l_expsr_prcnt))
            || CHR (9)
            || RTRIM (cr_main.rgnl_offc_nmbr)
            || CHR (9)
            || RTRIM (cr_main.rgnl_offc_name)
            || CHR (9)
            || RTRIM (cr_main.cnsmr_rqst_dscrptn)
            || CHR (9)
            || RTRIM (ln_ho_undrwrtr_lst_name)
            || CHR (9)
            || RTRIM (ln_lss_cntrl_cnsltnt_lst_name)
            || CHR (9)
            || RTRIM (ln_lss_cntrl_acc_mgr_lst_name)
            || CHR (9)
            || RTRIM (ln_lss_cntrl_acc_exe_lst_name)
            || CHR (9)
            || RTRIM (TO_CHAR (ln_actl_lc))
            || CHR (9)
            || RTRIM (TO_CHAR (ln_rqrd_lc))
            || CHR (9)
            || RTRIM (TO_CHAR (ln_net_at_incm))
            || CHR (9)
            || RTRIM (TO_CHAR (ln_comb_roe))
            || CHR (9)
            || RTRIM (TO_CHAR (ln_stg_prcnt));
        UTL_FILE.put_line (out_file, text_buf);
    END LOOP;

    UTL_FILE.fclose (out_file);
EXCEPTION
    WHEN OTHERS
    THEN
        DBMS_OUTPUT.put_line ('Error ' || SQLERRM);
END;