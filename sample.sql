-------------------------------------------------------------------------------
--Pull in policy information from ARE, including liability premium-------------
-------------------------------------------------------------------------------
DROP TABLE TH76428.ARE_AUTO_POLICIES;
CREATE TABLE TH76428.ARE_AUTO_POLICIES 
AS 
WITH RANKING_TRANS_ROWS 
AS (
    SELECT 
        item.POL_PK_ID,
        item.POLICY_NBR,
        item.REG_PK_ID,
        item.ITM_PK_ID,
        item.AGMT_GID,
        item.AGMT_TRANS_GID,
        item.TRANS_SASN,
        item.VEH_SEQ_NBR,
        item.VEH_NO,
        item.NUM_IDENT_VEH,
        item.RISK_STATE_ABBR,
        item.POLICY_EFFECTIVE_DT,
        item.TRANS_EFF_DT,
        item.ITEM_TYPE,
        item.AUTO_CLASS_TYPE_DESC,
        item.final_man_liab_prm,
        item.final_man_coll_prm,    
        item.final_man_otc_prm,
        item.final_man_med_prm,
        item.final_man_nf_prem,
        item.final_man_udm_prm,
        item.final_man_um_prm,
        item.final_man_umpd_prm,
        pol.POLICY_EXPIRATION_DT,
        pol.BUS_SEG_GRP_DESC,
        pol.POLICY_TYPE,
        pol.WRITING_COMPANY_CD,
        pol.WRITING_COMPANY_DESC,
        pol.AUTO_ARE_FULL_SUPP_CD,
        pol.FINAL_MAN_DOC_PRM,
        pol.FINAL_MAN_ENOL_PRM,
        pol.FINAL_MAN_HA_PRM,
        pol.FINAL_MAN_PLT_PRM,
        region.FINAL_MAN_PNOL_PRM,
        CASE WHEN item.ITEM_TYPE = 'VEHICLE' THEN 1 ELSE 0 END AS HNO_IND,
        CASE WHEN pol.AUTO_ARE_FULL_SUPP_CD = 'F' THEN 0 ELSE 1 END AS FULL_IND,
        ROW_NUMBER () OVER (
            PARTITION BY 
                item.POLICY_NBR,
                TO_DATE (item.POLICY_EFFECTIVE_DT,'MM/DD/YYYY'),
                item.VEH_NO,
                item.RISK_STATE_ABBR
            ORDER BY TO_DATE (item.TRANS_EFF_DT, 'MM/DD/YYYY') DESC)
            AS TRANS_ROW_RANK
            
    FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_ITM_AT_BATCH_OP_VW@EDACTPRD item
    LEFT JOIN CLACT_AUTO_ARE_DATA.AUTO_ARE_POL_AT_BATCH_OP_VW@EDACTPRD pol
    ON item.POL_PK_ID = pol.POL_PK_ID
    LEFT JOIN CLACT_AUTO_ARE_DATA.AUTO_ARE_REG_AT_BATCH_OP_VW@EDACTPRD region
    ON item.REG_PK_ID = region.REG_PK_ID
    WHERE item.ITERATION_NBR IN
    (2000000645
    )
    AND pol.ITERATION_NBR IN
    (2000000645
    )
    AND region.ITERATION_NBR IN
    (2000000645
    )
    AND item.AUTO_CLASS_TYPE_DESC <> 'PUB'    --Per Reuben Houser 1/31/2020 we're not using the date filters below anymore
    --AND TO_DATE(pol.POLICY_EXPIRATION_DT, 'MM/DD/YYYY') >= TO_DATE('04/01/2020', 'MM/DD/YYYY')
    --AND TO_DATE(pol.POLICY_EXPIRATION_DT, 'MM/DD/YYYY') <= TO_DATE('06/30/2020', 'MM/DD/YYYY')
 )
 SELECT ranked.* FROM RANKING_TRANS_ROWS ranked
    WHERE TRANS_ROW_RANK = 1
;

SELECT * FROM TH76428.ARE_AUTO_POLICIES;
--Check grain
SELECT COUNT(*) FROM TH76428.ARE_AUTO_POLICIES;
WITH TEMP AS (
    SELECT DISTINCT 
        A.POLICY_NBR,
        A.POLICY_EFFECTIVE_DT,
        A.VEH_NO,
        A.RISK_STATE_ABBR
    FROM TH76428.ARE_AUTO_POLICIES A
) SELECT COUNT(*) FROM TEMP;

SELECT A.POLICY_NBR, A.POLICY_EFFECTIVE_DT, A.VEH_NO, A.RISK_STATE_ABBR, COUNT(*)
--Identify duplicates
FROM TH76428.ARE_AUTO_POLICIES A
GROUP BY A.POLICY_NBR, A.POLICY_EFFECTIVE_DT, A.VEH_NO, A.RISK_STATE_ABBR
HAVING COUNT(*) > 1;

SELECT * FROM TH76428.ARE_AUTO_POLICIES A
WHERE A.POLICY_NBR = '08UE QO8062'
AND A.POLICY_EFFECTIVE_DT = '08/31/2013'
AND VEH_NO = '00000';

-------------------------------------------------------------------------------
--Select Middle Market only (only score MM, SC only used for model build)------
--Sum to policy level----------------------------------------------------------
-------------------------------------------------------------------------------
DROP TABLE TH76428.ARE_AUTO_POLICIES_2;
CREATE TABLE TH76428.ARE_AUTO_POLICIES_2
AS
SELECT
    A.AGMT_GID,
    CASE WHEN B.ACCT_ID IS NULL THEN 
            CONCAT(TO_CHAR(ROW_NUMBER() OVER (ORDER BY A.POLICY_NBR)),'AUTOGEN')
        ELSE B.ACCT_ID END AS ACCT_ID, 
    B.POL_ID,
    A.POLICY_NBR,
    A.POLICY_EFFECTIVE_DT,
    A.POLICY_EXPIRATION_DT,
    A.BUS_SEG_GRP_DESC,
    A.WRITING_COMPANY_CD,
    A.WRITING_COMPANY_DESC,
    SUM(A.final_man_liab_prm * A.HNO_IND) AS final_man_liab_prm,
    SUM(A.final_man_coll_prm * A.HNO_IND) AS final_man_coll_prm,    
    SUM(A.final_man_otc_prm * A.HNO_IND) AS final_man_otc_prm,
    SUM(A.final_man_med_prm * A.HNO_IND) AS final_man_med_prm,
    SUM(A.final_man_nf_prem * A.HNO_IND) AS final_man_nf_prem,
    SUM(A.final_man_udm_prm * A.HNO_IND) AS final_man_udm_prm,
    SUM(A.final_man_um_prm * A.HNO_IND) AS final_man_um_prm,
    SUM(A.final_man_umpd_prm * A.HNO_IND) AS final_man_umpd_prm,
    MAX(A.FINAL_MAN_DOC_PRM) AS FINAL_MAN_DOC_PRM,
    MAX(A.FINAL_MAN_ENOL_PRM) AS FINAL_MAN_ENOL_PRM,
    MAX(A.FINAL_MAN_HA_PRM) AS FINAL_MAN_HA_PRM,
    MAX(A.FINAL_MAN_PLT_PRM) AS FINAL_MAN_PLT_PRM,
    MAX(A.FINAL_MAN_PNOL_PRM) AS FINAL_MAN_PNOL_PRM,
    SUM(A.HNO_IND) AS HNO_IND,
    SUM(A.FULL_IND) AS FULL_IND,
    LEAST(TO_DATE(A.POLICY_EXPIRATION_DT,'MM/DD/YY') - TO_DATE(A.POLICY_EFFECTIVE_DT,'MM/DD/YY'), 365) 
        AS POLICY_TERM,
    CONCAT(
        CONCAT(CEIL(EXTRACT (MONTH FROM TO_DATE(A.POLICY_EXPIRATION_DT,'MM/DD/YY')) / 3),'Q'),
        EXTRACT (YEAR FROM TO_DATE(A.POLICY_EXPIRATION_DT,'MM/DD/YY')))
        AS EXPIRATION_QUARTER
FROM TH76428.ARE_AUTO_POLICIES A
LEFT JOIN cldw_adm_dlv.adm_policy_dim@edaprd B
    ON A.AGMT_GID = B.AGMT_GID
WHERE A.BUS_SEG_GRP_DESC = 'MIDDLE MARKET'
GROUP BY
    A.AGMT_GID,
    B.ACCT_ID,
    B.POL_ID,
    A.POLICY_NBR,
    A.POLICY_EFFECTIVE_DT,
    A.POLICY_EXPIRATION_DT,
    A.BUS_SEG_GRP_DESC,
    A.WRITING_COMPANY_DESC,
    A.WRITING_COMPANY_CD
;

SELECT * FROM TH76428.ARE_AUTO_POLICIES_2;
--Check grain
SELECT COUNT(*) FROM TH76428.ARE_AUTO_POLICIES_2;
WITH TEMP AS (
    SELECT DISTINCT 
        A.POLICY_NBR,
        A.POLICY_EFFECTIVE_DT
    FROM TH76428.ARE_AUTO_POLICIES_2 A
) SELECT COUNT(*) FROM TEMP;

-------------------------------------------------------------------------------
--Low touch scoring------------------------------------------------------------
-------------------------------------------------------------------------------
DROP TABLE TH76428.ARE_AUTO_POLICIES_3;
CREATE TABLE TH76428.ARE_AUTO_POLICIES_3
AS
SELECT
    A.ACCT_ID,
    A.POL_ID,
    A.POLICY_NBR,
    A.POLICY_EFFECTIVE_DT,
    A.POLICY_EXPIRATION_DT,
    A.EXPIRATION_QUARTER,
    CASE WHEN A.HNO_IND > 0 THEN 0 ELSE 1 END AS HNO_IND,
    CASE WHEN A.FULL_IND > 0 THEN 0 ELSE 1 END AS FULL_IND,
    (A.final_man_liab_prm * (365 / A.POLICY_TERM)) AS final_man_liab_prm,
    ((A.FINAL_MAN_DOC_PRM +
        A.FINAL_MAN_ENOL_PRM +
        A.FINAL_MAN_HA_PRM +
        A.FINAL_MAN_PLT_PRM +
        A.FINAL_MAN_PNOL_PRM) * (365 / A.POLICY_TERM)) AS total_hno_prm,
    CASE WHEN (A.WRITING_COMPANY_CD = 'H' OR A.WRITING_COMPANY_CD = '5' OR A.HNO_IND = 0) THEN (
        CASE WHEN A.FULL_IND > 0 THEN 'NOT LOW' ELSE (
            CASE WHEN A.HNO_IND > 0 THEN (
                CASE WHEN (A.final_man_liab_prm * (365 / A.POLICY_TERM)) < 30000 
                    THEN 'LOW' ELSE 'NOT LOW' END 
                ) 
            ELSE (
                CASE WHEN ((
                    A.FINAL_MAN_DOC_PRM +
                    A.FINAL_MAN_ENOL_PRM +
                    A.FINAL_MAN_HA_PRM +
                    A.FINAL_MAN_PLT_PRM +
                    A.FINAL_MAN_PNOL_PRM) * (365 / A.POLICY_TERM)) < 3000 
                        THEN 'LOW' ELSE 'NOT LOW' END
            ) END 
        ) END
    ) ELSE 'NOT APPLICABLE' END AS TOUCH,
    CASE WHEN (A.WRITING_COMPANY_CD = 'H' OR A.WRITING_COMPANY_CD = '5' OR A.HNO_IND = 0) THEN (
        CASE WHEN A.FULL_IND > 0 THEN 'Partially supported' ELSE (
            CASE WHEN A.HNO_IND > 0 THEN (
                CASE WHEN (A.final_man_liab_prm * (365 / A.POLICY_TERM)) < 30000 
                    THEN 'Less than $30K liability premium' ELSE 'Greater than $30K liability premium' END 
                ) 
            ELSE (
                CASE WHEN ((
                    A.FINAL_MAN_DOC_PRM +
                    A.FINAL_MAN_ENOL_PRM +
                    A.FINAL_MAN_HA_PRM +
                    A.FINAL_MAN_PLT_PRM +
                    A.FINAL_MAN_PNOL_PRM) * (365 / A.POLICY_TERM)) < 3000 
                        THEN 'HNO only, less than $3K total premium' ELSE 'HNO only, greater than $3K total premium' END
            ) END 
        ) END
    ) ELSE 'Sentinel or Legacy writing company' END AS TOUCH_REASON,
    CASE WHEN (A.WRITING_COMPANY_CD = 'H' OR A.WRITING_COMPANY_CD = '5' OR A.HNO_IND = 0) THEN (
        CASE WHEN A.FULL_IND > 0 THEN '2' ELSE (
            CASE WHEN A.HNO_IND > 0 THEN (
                CASE WHEN (A.final_man_liab_prm * (365 / A.POLICY_TERM)) < 30000 
                    THEN '1' ELSE '2' END 
                ) 
            ELSE (
                CASE WHEN ((
                    A.FINAL_MAN_DOC_PRM +
                    A.FINAL_MAN_ENOL_PRM +
                    A.FINAL_MAN_HA_PRM +
                    A.FINAL_MAN_PLT_PRM +
                    A.FINAL_MAN_PNOL_PRM) * (365 / A.POLICY_TERM)) < 3000 
                        THEN '1' ELSE '2' END
            ) END 
        ) END
    ) ELSE '3' END AS TOUCH_RANK
FROM TH76428.ARE_AUTO_POLICIES_2 A
;

SELECT * FROM TH76428.ARE_AUTO_POLICIES_3;
SELECT COUNT(*) FROM TH76428.ARE_AUTO_POLICIES_3;

-------------------------------------------------------------------------------
--Assign worst designation to account level------------------------------------
-------------------------------------------------------------------------------
DROP TABLE TH76428.ARE_AUTO_POLICIES_4;
CREATE TABLE TH76428.ARE_AUTO_POLICIES_4
AS 
WITH TEMP
AS (
    SELECT
        A.ACCT_ID,
        A.EXPIRATION_QUARTER,
        MAX(A.TOUCH_RANK) AS MAX_TOUCH_RANK
    FROM TH76428.ARE_AUTO_POLICIES_3 A
    GROUP BY
        A.ACCT_ID,
        A.EXPIRATION_QUARTER
)
SELECT
    A.ACCT_ID,
    A.POL_ID,
    A.POLICY_NBR,
    A.POLICY_EFFECTIVE_DT,
    A.POLICY_EXPIRATION_DT,
    A.EXPIRATION_QUARTER,
    A.HNO_IND,
    A.FULL_IND,
    A.FINAL_MAN_LIAB_PRM,
    A.TOTAL_HNO_PRM,
    CASE WHEN B.MAX_TOUCH_RANK = 1 THEN 'LOW'
        ELSE ( CASE WHEN B.MAX_TOUCH_RANK = 2 THEN 'NOT LOW' 
            ELSE 'NOT APPLICABLE' END) END AS TOUCH,
    A.TOUCH_REASON
FROM TH76428.ARE_AUTO_POLICIES_3 A
LEFT JOIN TEMP B
    ON A.ACCT_ID = B.ACCT_ID
    AND A.EXPIRATION_QUARTER = B.EXPIRATION_QUARTER
;

SELECT * FROM TH76428.ARE_AUTO_POLICIES_4;
SELECT COUNT(*) FROM TH76428.ARE_AUTO_POLICIES_4;

-------------------------------------------------------------------------------
--Check for zero premiums------------------------------------------------------
-------------------------------------------------------------------------------
SELECT COUNT(*) FROM TH76428.ARE_AUTO_POLICIES_2;
SELECT COUNT(*) FROM TH76428.ARE_AUTO_POLICIES_3;
SELECT COUNT(*) FROM TH76428.ARE_AUTO_POLICIES_4;

SELECT 
    A.*,
    B.TOUCH,
    B.TOUCH_REASON
FROM TH76428.ARE_AUTO_POLICIES_2 A
LEFT JOIN TH76428.ARE_AUTO_POLICIES_3 B
ON A.POLICY_NBR = B.POLICY_NBR AND A.POLICY_EFFECTIVE_DT = B.POLICY_EFFECTIVE_DT
WHERE A.FINAL_MAN_LIAB_PRM = 0
AND A.FINAL_MAN_COLL_PRM = 0
AND A.FINAL_MAN_OTC_PRM = 0
AND A.FINAL_MAN_MED_PRM	= 0
AND A.FINAL_MAN_NF_PREM = 0
AND A.FINAL_MAN_UDM_PRM = 0
AND A.FINAL_MAN_UM_PRM = 0
AND A.FINAL_MAN_UMPD_PRM = 0
AND A.FINAL_MAN_DOC_PRM = 0
AND A.FINAL_MAN_ENOL_PRM = 0
AND A.FINAL_MAN_HA_PRM = 0
AND A.FINAL_MAN_PLT_PRM	= 0
AND A.FINAL_MAN_PNOL_PRM = 0
AND B.TOUCH = 'LOW'
;
--These policies need to be run through ARE as single policies
--Otherwise, they will be tagged as low for having $0 premium
--when actually they just weren't rated in ARE for some reason

-------------------------------------------------------------------------------
--Zero premium single policy runs----------------------------------------------
-------------------------------------------------------------------------------
DROP TABLE TH76428.ARE_SINGLE_POLICY_RUNS;
CREATE TABLE TH76428.ARE_SINGLE_POLICY_RUNS 
AS 
WITH RANKING_TRANS_ROWS 
AS (
    SELECT 
        item.POL_PK_ID,
        item.POLICY_NBR,
        item.REG_PK_ID,
        item.ITM_PK_ID,
        item.AGMT_GID,
        item.AGMT_TRANS_GID,
        item.TRANS_SASN,
        item.VEH_SEQ_NBR,
        item.VEH_NO,
        item.NUM_IDENT_VEH,
        item.RISK_STATE_ABBR,
        item.POLICY_EFFECTIVE_DT,
        item.TRANS_EFF_DT,
        item.AUTO_CLASS_TYPE_DESC,
        item.ITEM_TYPE,
        item.final_man_liab_prm,
        item.final_man_coll_prm,    
        item.final_man_otc_prm,
        item.final_man_med_prm,
        item.final_man_nf_prem,
        item.final_man_udm_prm,
        item.final_man_um_prm,
        item.final_man_umpd_prm,
        pol.POLICY_EXPIRATION_DT,
        pol.BUS_SEG_GRP_DESC,
        pol.POLICY_TYPE,
        pol.WRITING_COMPANY_CD,
        pol.WRITING_COMPANY_DESC,
        pol.AUTO_ARE_FULL_SUPP_CD,
        pol.FINAL_MAN_DOC_PRM,
        pol.FINAL_MAN_ENOL_PRM,
        pol.FINAL_MAN_HA_PRM,
        pol.FINAL_MAN_PLT_PRM,
        region.FINAL_MAN_PNOL_PRM,
        CASE WHEN item.ITEM_TYPE = 'VEHICLE' THEN 1 ELSE 0 END AS HNO_IND,
        CASE WHEN pol.AUTO_ARE_FULL_SUPP_CD = 'F' THEN 0 ELSE 1 END AS FULL_IND,
        ROW_NUMBER () OVER (
            PARTITION BY 
                item.POLICY_NBR,
                TO_DATE (item.POLICY_EFFECTIVE_DT,'MM/DD/YYYY'),
                item.VEH_NO,
                item.RISK_STATE_ABBR
            ORDER BY TO_DATE (item.TRANS_EFF_DT, 'MM/DD/YYYY') DESC)
            AS TRANS_ROW_RANK
    FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_ITM_AT_BATCH_OP_VW@EDACTPRD item
    LEFT JOIN CLACT_AUTO_ARE_DATA.AUTO_ARE_POL_AT_BATCH_OP_VW@EDACTPRD pol
    ON item.POL_PK_ID = pol.POL_PK_ID
    LEFT JOIN CLACT_AUTO_ARE_DATA.AUTO_ARE_REG_AT_BATCH_OP_VW@EDACTPRD region
    ON item.REG_PK_ID = region.REG_PK_ID
    WHERE item.ITERATION_NBR IN
    (1900006254,
    1900006255,
    1900006256,
    1900006257,
    1900006258,
    1900006259,
    1900006260,
    1900006261,
    1900006262,
    1900006263,
    1900006264,
    1900006265,
    1900006266,
    1900006267,
    1900006268,
    1900006269,
    1900006270,
    1900006271,
    1900006272,
    1900006273,
    1900006274,
    1900006275,
    1900006276,
    1900006277,
    1900006278,
    1900006279,
    1900006280,
    1900006281,
    1900006282,
    1900006283,
    1900006284,
    1900006285,
    1900006286,
    1900006287,
    1900006288,
    1900006289,
    1900006290,
    1900006291,
    1900006292,
    1900006293,
    1900006294,
    1900006295,
    1900006296,
    1900006297,
    1900006298,
    1900006299,
    1900006300,
    1900006301,
    1900006302,
    1900006303,
    1900006304,
    1900006305,
    1900006306,
    1900006307,
    1900006308,
    1900006309,
    1900006310,
    1900006311,
    1900006312,
    1900006313,
    1900006314,
    1900006315,
    1900006316,
    1900006317,
    1900006318,
    1900006319,
    1900006320,
    1900006321,
    1900006322,
    1900006323,
    1900006324,
    1900006325,
    1900006326,
    1900006327,
    1900006328,
    1900006329,
    1900006330,
    1900006331,
    1900006332,
    1900006333,
    1900006334,
    1900006335,
    1900006336,
    1900006337,
    1900006338,
    1900006339
    )
    AND pol.ITERATION_NBR IN
    (1900006254,
    1900006255,
    1900006256,
    1900006257,
    1900006258,
    1900006259,
    1900006260,
    1900006261,
    1900006262,
    1900006263,
    1900006264,
    1900006265,
    1900006266,
    1900006267,
    1900006268,
    1900006269,
    1900006270,
    1900006271,
    1900006272,
    1900006273,
    1900006274,
    1900006275,
    1900006276,
    1900006277,
    1900006278,
    1900006279,
    1900006280,
    1900006281,
    1900006282,
    1900006283,
    1900006284,
    1900006285,
    1900006286,
    1900006287,
    1900006288,
    1900006289,
    1900006290,
    1900006291,
    1900006292,
    1900006293,
    1900006294,
    1900006295,
    1900006296,
    1900006297,
    1900006298,
    1900006299,
    1900006300,
    1900006301,
    1900006302,
    1900006303,
    1900006304,
    1900006305,
    1900006306,
    1900006307,
    1900006308,
    1900006309,
    1900006310,
    1900006311,
    1900006312,
    1900006313,
    1900006314,
    1900006315,
    1900006316,
    1900006317,
    1900006318,
    1900006319,
    1900006320,
    1900006321,
    1900006322,
    1900006323,
    1900006324,
    1900006325,
    1900006326,
    1900006327,
    1900006328,
    1900006329,
    1900006330,
    1900006331,
    1900006332,
    1900006333,
    1900006334,
    1900006335,
    1900006336,
    1900006337,
    1900006338,
    1900006339
    )
    AND region.ITERATION_NBR IN
    (1900006254,
    1900006255,
    1900006256,
    1900006257,
    1900006258,
    1900006259,
    1900006260,
    1900006261,
    1900006262,
    1900006263,
    1900006264,
    1900006265,
    1900006266,
    1900006267,
    1900006268,
    1900006269,
    1900006270,
    1900006271,
    1900006272,
    1900006273,
    1900006274,
    1900006275,
    1900006276,
    1900006277,
    1900006278,
    1900006279,
    1900006280,
    1900006281,
    1900006282,
    1900006283,
    1900006284,
    1900006285,
    1900006286,
    1900006287,
    1900006288,
    1900006289,
    1900006290,
    1900006291,
    1900006292,
    1900006293,
    1900006294,
    1900006295,
    1900006296,
    1900006297,
    1900006298,
    1900006299,
    1900006300,
    1900006301,
    1900006302,
    1900006303,
    1900006304,
    1900006305,
    1900006306,
    1900006307,
    1900006308,
    1900006309,
    1900006310,
    1900006311,
    1900006312,
    1900006313,
    1900006314,
    1900006315,
    1900006316,
    1900006317,
    1900006318,
    1900006319,
    1900006320,
    1900006321,
    1900006322,
    1900006323,
    1900006324,
    1900006325,
    1900006326,
    1900006327,
    1900006328,
    1900006329,
    1900006330,
    1900006331,
    1900006332,
    1900006333,
    1900006334,
    1900006335,
    1900006336,
    1900006337,
    1900006338,
    1900006339
    )
    AND item.AUTO_CLASS_TYPE_DESC <> 'PUB'
    --AND TO_DATE(pol.POLICY_EXPIRATION_DT, 'MM/DD/YYYY') >= TO_DATE('04/01/2020', 'MM/DD/YYYY')
    --AND TO_DATE(pol.POLICY_EXPIRATION_DT, 'MM/DD/YYYY') <= TO_DATE('06/30/2020', 'MM/DD/YYYY')
 )
 SELECT ranked.* FROM RANKING_TRANS_ROWS ranked
    WHERE TRANS_ROW_RANK = 1
;

SELECT COUNT(*) FROM TH76428.ARE_SINGLE_POLICY_RUNS;
SELECT * FROM TH76428.ARE_SINGLE_POLICY_RUNS;

DROP TABLE TH76428.ARE_SINGLE_POLICY_RUNS_2;
CREATE TABLE TH76428.ARE_SINGLE_POLICY_RUNS_2
AS
SELECT
    A.AGMT_GID,
    CASE WHEN B.ACCT_ID IS NULL THEN 
            CONCAT(TO_CHAR(ROW_NUMBER() OVER (ORDER BY A.POLICY_NBR)),'AUTOGEN')
        ELSE B.ACCT_ID END AS ACCT_ID, 
    B.POL_ID,
    A.POLICY_NBR,
    A.POLICY_EFFECTIVE_DT,
    A.POLICY_EXPIRATION_DT,
    A.BUS_SEG_GRP_DESC,
    A.WRITING_COMPANY_CD,
    A.WRITING_COMPANY_DESC,
    SUM(A.final_man_liab_prm * A.HNO_IND) AS final_man_liab_prm,
    SUM(A.final_man_coll_prm * A.HNO_IND) AS final_man_coll_prm,    
    SUM(A.final_man_otc_prm * A.HNO_IND) AS final_man_otc_prm,
    SUM(A.final_man_med_prm * A.HNO_IND) AS final_man_med_prm,
    SUM(A.final_man_nf_prem * A.HNO_IND) AS final_man_nf_prem,
    SUM(A.final_man_udm_prm * A.HNO_IND) AS final_man_udm_prm,
    SUM(A.final_man_um_prm * A.HNO_IND) AS final_man_um_prm,
    SUM(A.final_man_umpd_prm * A.HNO_IND) AS final_man_umpd_prm,
    MAX(A.FINAL_MAN_DOC_PRM) AS FINAL_MAN_DOC_PRM,
    MAX(A.FINAL_MAN_ENOL_PRM) AS FINAL_MAN_ENOL_PRM,
    MAX(A.FINAL_MAN_HA_PRM) AS FINAL_MAN_HA_PRM,
    MAX(A.FINAL_MAN_PLT_PRM) AS FINAL_MAN_PLT_PRM,
    MAX(A.FINAL_MAN_PNOL_PRM) AS FINAL_MAN_PNOL_PRM,
    SUM(A.HNO_IND) AS HNO_IND,
    SUM(A.FULL_IND) AS FULL_IND,
    LEAST(TO_DATE(A.POLICY_EXPIRATION_DT,'MM/DD/YY') - TO_DATE(A.POLICY_EFFECTIVE_DT,'MM/DD/YY'), 365) 
        AS POLICY_TERM,
    CONCAT(
        CONCAT(CEIL(EXTRACT (MONTH FROM TO_DATE(A.POLICY_EXPIRATION_DT,'MM/DD/YY')) / 3),'Q'),
        EXTRACT (YEAR FROM TO_DATE(A.POLICY_EXPIRATION_DT,'MM/DD/YY')))
        AS EXPIRATION_QUARTER
FROM TH76428.ARE_SINGLE_POLICY_RUNS A
LEFT JOIN cldw_adm_dlv.adm_policy_dim@edaprd B
    ON A.AGMT_GID = B.AGMT_GID
WHERE A.BUS_SEG_GRP_DESC = 'MIDDLE MARKET'
GROUP BY
    A.AGMT_GID,
    B.ACCT_ID,
    B.POL_ID,
    A.POLICY_NBR,
    A.POLICY_EFFECTIVE_DT,
    A.POLICY_EXPIRATION_DT,
    A.BUS_SEG_GRP_DESC,
    A.WRITING_COMPANY_DESC,
    A.WRITING_COMPANY_CD
;

SELECT * FROM TH76428.ARE_SINGLE_POLICY_RUNS_2;
SELECT COUNT(*) FROM TH76428.ARE_SINGLE_POLICY_RUNS_2;

DROP TABLE TH76428.ARE_SINGLE_POLICY_RUNS_3;
CREATE TABLE TH76428.ARE_SINGLE_POLICY_RUNS_3
AS
SELECT
    A.ACCT_ID,
    A.POL_ID,
    A.POLICY_NBR,
    A.POLICY_EFFECTIVE_DT,
    A.POLICY_EXPIRATION_DT,
    A.EXPIRATION_QUARTER,
    CASE WHEN A.HNO_IND > 0 THEN 0 ELSE 1 END AS HNO_IND,
    CASE WHEN A.FULL_IND > 0 THEN 0 ELSE 1 END AS FULL_IND,
    (A.final_man_liab_prm * (365 / A.POLICY_TERM)) AS final_man_liab_prm,
    ((A.FINAL_MAN_DOC_PRM +
        A.FINAL_MAN_ENOL_PRM +
        A.FINAL_MAN_HA_PRM +
        A.FINAL_MAN_PLT_PRM +
        A.FINAL_MAN_PNOL_PRM) * (365 / A.POLICY_TERM)) AS total_hno_prm,
    CASE WHEN (A.WRITING_COMPANY_CD = 'H' OR A.WRITING_COMPANY_CD = '5' OR A.HNO_IND = 0) THEN (
        CASE WHEN A.FULL_IND > 0 THEN 'NOT LOW' ELSE (
            CASE WHEN A.HNO_IND > 0 THEN (
                CASE WHEN (A.final_man_liab_prm * (365 / A.POLICY_TERM)) < 30000 
                    THEN 'LOW' ELSE 'NOT LOW' END 
                ) 
            ELSE (
                CASE WHEN ((
                    A.FINAL_MAN_DOC_PRM +
                    A.FINAL_MAN_ENOL_PRM +
                    A.FINAL_MAN_HA_PRM +
                    A.FINAL_MAN_PLT_PRM +
                    A.FINAL_MAN_PNOL_PRM) * (365 / A.POLICY_TERM)) < 3000 
                        THEN 'LOW' ELSE 'NOT LOW' END
            ) END 
        ) END
    ) ELSE 'NOT APPLICABLE' END AS TOUCH,
    CASE WHEN (A.WRITING_COMPANY_CD = 'H' OR A.WRITING_COMPANY_CD = '5' OR A.HNO_IND = 0) THEN (
        CASE WHEN A.FULL_IND > 0 THEN 'Partially supported' ELSE (
            CASE WHEN A.HNO_IND > 0 THEN (
                CASE WHEN (A.final_man_liab_prm * (365 / A.POLICY_TERM)) < 30000 THEN 'Less than $30K liability premium' 
                    ELSE 'Greater than $30K liability premium' END 
                ) 
            ELSE (
                CASE WHEN ((
                    A.FINAL_MAN_DOC_PRM +
                    A.FINAL_MAN_ENOL_PRM +
                    A.FINAL_MAN_HA_PRM +
                    A.FINAL_MAN_PLT_PRM +
                    A.FINAL_MAN_PNOL_PRM) * (365 / A.POLICY_TERM)) < 3000 THEN 'HNO only, less than $3K total premium' 
                        ELSE 'HNO only, greater than $3K total premium'  END
            ) END 
        ) END
    ) ELSE 'Sentinel or Legacy writing company' END AS TOUCH_REASON,
    CASE WHEN (A.WRITING_COMPANY_CD = 'H' OR A.WRITING_COMPANY_CD = '5' OR A.HNO_IND = 0) THEN (
        CASE WHEN A.FULL_IND > 0 THEN '2' ELSE (
            CASE WHEN A.HNO_IND > 0 THEN (
                CASE WHEN (A.final_man_liab_prm * (365 / A.POLICY_TERM)) < 30000 
                    THEN '1' ELSE '2' END 
                ) 
            ELSE (
                CASE WHEN ((
                    A.FINAL_MAN_DOC_PRM +
                    A.FINAL_MAN_ENOL_PRM +
                    A.FINAL_MAN_HA_PRM +
                    A.FINAL_MAN_PLT_PRM +
                    A.FINAL_MAN_PNOL_PRM) * (365 / A.POLICY_TERM)) < 3000 
                        THEN '1' ELSE '2' END
            ) END 
        ) END
    ) ELSE '3' END AS TOUCH_RANK
FROM TH76428.ARE_SINGLE_POLICY_RUNS_2 A
;

SELECT * FROM TH76428.ARE_SINGLE_POLICY_RUNS_3;
SELECT COUNT(*) FROM TH76428.ARE_SINGLE_POLICY_RUNS_3;

DROP TABLE TH76428.ARE_SINGLE_POLICY_RUNS_4;
CREATE TABLE TH76428.ARE_SINGLE_POLICY_RUNS_4
AS 
WITH TEMP
AS (
    SELECT
        A.ACCT_ID,
        A.EXPIRATION_QUARTER,
        MAX(A.TOUCH_RANK) AS MAX_TOUCH_RANK
    FROM TH76428.ARE_SINGLE_POLICY_RUNS_3 A
    GROUP BY
        A.ACCT_ID,
        A.EXPIRATION_QUARTER
)
SELECT
    A.ACCT_ID,
    A.POL_ID,
    A.POLICY_NBR,
    A.POLICY_EFFECTIVE_DT,
    A.POLICY_EXPIRATION_DT,
    A.EXPIRATION_QUARTER,
    A.HNO_IND,
    A.FULL_IND,
    A.FINAL_MAN_LIAB_PRM,
    A.TOTAL_HNO_PRM,
    CASE WHEN B.MAX_TOUCH_RANK = 1 THEN 'LOW'
        ELSE ( CASE WHEN B.MAX_TOUCH_RANK = 2 THEN 'NOT LOW' 
            ELSE 'NOT APPLICABLE' END) END AS TOUCH,
    A.TOUCH_REASON
FROM TH76428.ARE_SINGLE_POLICY_RUNS_3 A
LEFT JOIN TEMP B
    ON A.ACCT_ID = B.ACCT_ID
    AND A.EXPIRATION_QUARTER = B.EXPIRATION_QUARTER
;

SELECT * FROM TH76428.ARE_SINGLE_POLICY_RUNS_4;
SELECT COUNT(*) FROM TH76428.ARE_SINGLE_POLICY_RUNS_4;

--Manually check for account-level designations

-------------------------------------------------------------------------------
--Validation-------------------------------------------------------------------
-------------------------------------------------------------------------------

SELECT * FROM TH76428.AUTO_POLICIES_TO_SCORE_2;
SELECT * FROM TH76428.ARE_AUTO_POLICIES_3;

SELECT * FROM TH76428.ARE_AUTO_POLICIES A
WHERE A.POLICY_NBR	= '13UE BM2628'
ORDER BY A.POLICY_EFFECTIVE_DT	
;

SELECT * FROM TH76428.ARE_AUTO_POLICIES_2 A
WHERE A.POLICY_NBR	= '44UU TI3312'
ORDER BY A.POLICY_EFFECTIVE_DT	
;

SELECT * FROM TH76428.ARE_AUTO_POLICIES_3 A
WHERE A.POLICY_NBR	= '10UU ZP6045'
ORDER BY A.POLICY_EFFECTIVE_DT	
;

SELECT * FROM TH76428.AUTO_POLICIES_SCORED_FINAL_2 A
WHERE A.AUTO_POL = '013AB  UK0737'
ORDER BY A.AUTO_POL_EFF	
;

SELECT MAX(A.POLICY_EFFECTIVE_DT) FROM TH76428.ARE_AUTO_POLICIES A;
SELECT MAX(A.POLICY_EXPIRATION_DT) FROM TH76428.ARE_AUTO_POLICIES A;

SELECT DISTINCT A.WRITING_COMPANY_DESC FROM TH76428.ARE_AUTO_POLICIES_2 A;

SELECT DISTINCT A.WRITING_COMPANY_CD FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_POL_AT_BATCH_OP_VW@EDACTPRD A;

SELECT DISTINCT A.AUTO_CLASS_TYPE_DESC FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_ITM_AT_BATCH_OP_VW@EDACTPRD A;

SELECT * FROM CLDW_ADM_DLV.adm_pol_mstr_dim@edaprd B;

SELECT 
    B.POL_ID,
    B.POL_NUM,
    B.POL_EFF_DT,
    B.POL_EXP_DT
FROM CLDW_ADM_DLV.adm_pol_mstr_dim@edaprd B
WHERE B.POL_ID = '21UUNOZ6305'
--WHERE B.POL_NUM = 'QZ5589'
ORDER BY B.POL_EFF_DT
;

SELECT DISTINCT B.POL_SYM_CD FROM cldw_adm_dlv.adm_policy_dim@edaprd B
ORDER BY B.POL_SYM_CD
;

SELECT * FROM cldw_adm_dlv.adm_policy_dim@edaprd B
WHERE B.POL_ID = '21UUNOZ6305'
;

SELECT * FROM CLDW_ADM_DLV.adm_pol_mstr_dim@edaprd B
WHERE B.POL_ID = '42UENNA0953'
;

SELECT 
    item.ITERATION_NBR,
    item.POL_PK_ID,
    item.POLICY_NBR,
    item.REG_PK_ID,
    item.ITM_PK_ID,
    item.AGMT_GID,
    item.AGMT_TRANS_GID,
    item.TRANS_SASN,
    item.VEH_SEQ_NBR,
    item.VEH_NO,
    item.NUM_IDENT_VEH,
    item.RISK_STATE_ABBR,
    item.POLICY_EFFECTIVE_DT,
    item.TRANS_EFF_DT,
    item.AUTO_CLASS_TYPE_DESC,
    item.ITEM_TYPE,
    item.final_man_liab_prm,
    item.final_man_coll_prm,    
    item.final_man_otc_prm,
    item.final_man_med_prm,
    item.final_man_nf_prem,
    item.final_man_udm_prm,
    item.final_man_um_prm,
    item.final_man_umpd_prm,
    pol.POLICY_EXPIRATION_DT,
    pol.BUS_SEG_GRP_DESC,
    pol.POLICY_TYPE,
    pol.WRITING_COMPANY_CD,
    pol.WRITING_COMPANY_DESC,
    pol.AUTO_ARE_FULL_SUPP_CD,
    pol.FINAL_MAN_DOC_PRM,
    pol.FINAL_MAN_ENOL_PRM,
    pol.FINAL_MAN_HA_PRM,
    pol.FINAL_MAN_PLT_PRM,
    region.FINAL_MAN_PNOL_PRM,
    CASE WHEN item.ITEM_TYPE = 'VEHICLE' THEN 1 ELSE 0 END AS HNO_IND,
    CASE WHEN pol.AUTO_ARE_FULL_SUPP_CD = 'F' THEN 0 ELSE 1 END AS FULL_IND,
    ROW_NUMBER () OVER (
        PARTITION BY 
            item.POLICY_NBR,
            TO_DATE (item.POLICY_EFFECTIVE_DT,'MM/DD/YYYY'),
            item.VEH_NO,
            item.RISK_STATE_ABBR
        ORDER BY TO_DATE (item.TRANS_EFF_DT, 'MM/DD/YYYY') DESC)
        AS TRANS_ROW_RANK
FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_ITM_AT_BATCH_OP_VW@EDACTPRD item
LEFT JOIN CLACT_AUTO_ARE_DATA.AUTO_ARE_POL_AT_BATCH_OP_VW@EDACTPRD pol
ON item.POL_PK_ID = pol.POL_PK_ID
LEFT JOIN CLACT_AUTO_ARE_DATA.AUTO_ARE_REG_AT_BATCH_OP_VW@EDACTPRD region
ON item.REG_PK_ID = region.REG_PK_ID
WHERE item.POLICY_NBR = '42UE NA0953';

SELECT * FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_ITM_AT_BATCH_OP_VW@EDACTPRD item;
SELECT * FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_POL_AT_BATCH_OP_VW@EDACTPRD pol;
SELECT * FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_REG_AT_BATCH_OP_VW@EDACTPRD region;

SELECT * FROM CLACT_AUTO_ARE_DATA.AUTO_ARE_ITM_AT_BATCH_OP_VW@EDACTPRD item
WHERE item.POLICY_NBR = '13UE BM2628'
AND item.ITERATION_NBR = 1900003994
;

SELECT C.CANC_DT FROM cldw_adm_dlv.adm_policy_dim@edaprd C
WHERE C.POL_ID = '13UENBM2628'
;
