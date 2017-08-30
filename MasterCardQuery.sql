/* See bottom for information on the main query used for this correlated query */
/* The main purpose of this query is to gather the three most recene master    */
/* card transactions of a group of customers. For the sake of simplicity, I    */
/* used inner joins to get only the people who made consecutive payments in the*/
/* three months used.*/

SELECT 
	T1.POL_NBR
	,T1.CHRG_1
	,T1.DT_1
	,T2.CHRG_2
	,T2.DT_2
	,T3.CHRG_3
	,T3.DT_3
	,P.POL_STOP_DT
	,P.CNCL_DT
	--,DAYS(P.POL_STOP_DT)-DAYS(T1.TRAN_DT) DAY_DIFF
	,CASE
		WHEN DAYS(P.CNCL_DT)-DAYS(T1.TRAN_DT) > 30 THEN 1
		WHEN DAYS(P.POL_STOP_DT)-DAYS(T1.TRAN_DT) > 30 THEN 1
		ELSE 0
	 END AS IN_FORCE
FROM (SELECT 
	A.Rank
	,A.POL_NBR
	,A.CHRG_AMT CHRG_1
	,A.SYS_TMSTMP DT_1
	,A.TRAN_DT

FROM (SELECT 
		POL_NBR
		,SYS_TMSTMP
		,TRAN_DT
		,CHRG_AMT
		,ROW_NUMBER()
	OVER ( Partition BY POL_NBR
		ORDER BY TRAN_DT DESC) AS Rank
	FROM PBW.CARD_DETAIL
	
	WHERE
	TRAN_DT BETWEEN '03/01/2017' AND '05/31/2017'
	AND POL_NBR != ''
	AND POL_NBR != '99999999'
	AND RPLY_CARD_ATH_STAT = '100'
) A

WHERE Rank =1
) T1	

INNER JOIN 

(SELECT 
	B.Rank
	,B.POL_NBR
	,B.CHRG_AMT CHRG_2
	,B.SYS_TMSTMP DT_2

FROM (SELECT 
		POL_NBR
		,SYS_TMSTMP
		,CHRG_AMT
		,ROW_NUMBER()
	OVER ( Partition BY POL_NBR
		ORDER BY TRAN_DT DESC) AS Rank
	FROM PBW.CARD_DETAIL
	
	WHERE
	TRAN_DT BETWEEN '03/01/2017' AND '05/31/2017'
	AND POL_NBR != ''
	AND POL_NBR != '99999999'
	AND RPLY_CARD_ATH_STAT = '100'
) B

WHERE Rank =2
) T2

ON T1.POL_NBR = T2.POL_NBR

INNER JOIN 

(SELECT 
	C.Rank
	,C.POL_NBR
	,C.CHRG_AMT CHRG_3
	,C.SYS_TMSTMP DT_3

FROM (SELECT 
		POL_NBR
		,SYS_TMSTMP
		,CHRG_AMT
		,ROW_NUMBER()
	OVER ( Partition BY POL_NBR
		ORDER BY TRAN_DT DESC) AS Rank
	FROM PBW.CARD_DETAIL
	
	WHERE
	TRAN_DT BETWEEN '03/01/2017' AND '05/31/2017'
	AND POL_NBR != ''
	AND POL_NBR != '99999999'
	AND RPLY_CARD_ATH_STAT = '100'
) C

WHERE Rank = 3
) T3

ON T1.POL_NBR = T3.POL_NBR

INNER JOIN M20.POLICY P ON T1.POL_NBR = P.POL_ID_NBR 

WHERE P.POL_STOP_DT = (SELECT 
							  MAX(POL_STOP_DT)
							  FROM M20.POLICY
							  WHERE P.POL_ID_NBR = POL_ID_NBR )



/*
   The query below gathers information on every mastercard payment made by on behalf of 
   each policy over the past several months, and assigns them a number 1,2,or 3 based on the date
   they were made in descending order-- i.e., from most recent to least recent. This is accomplished by
   using the ROW_NUMBER() OVER (Partition BY POL_NBR ORDER BY TRAN_DT DESC) AS Rank.
   This query is used as a master table for several correlated queries where I join the same table to itself
   multiple times conditioned on Rank = j where j = 1,2, or 3. This allows me to get my data in the format
   POL_NBR|CHRG_1|DT_1|CHRG_2|DT_2|CHRG_3|DT_3|. . .  The inner join keeps only the records where at least three
   payments were received.. Fortunately, this produces ~ 1 million rows, which should be enough volume to start.

SELECT *
FROM
(SELECT 
	Rank
	,POL_NBR
	,CHRG_AMT CHRG_AMT
	,SYS_TMSTMP DT

FROM (SELECT 
		POL_NBR
		,SYS_TMSTMP
		,CHRG_AMT
		,ROW_NUMBER()
	OVER ( Partition BY POL_NBR
		ORDER BY TRAN_DT DESC) AS Rank
	FROM PBW.CARD_DETAIL
	
	WHERE
	TRAN_DT BETWEEN '05/01/2017' AND '05/01/2017'
	AND POL_NBR != ''
	AND POL_NBR != '99999999'
	AND RPLY_CARD_ATH_STAT = '100'
) B

WHERE Rank <=3
) 




*/