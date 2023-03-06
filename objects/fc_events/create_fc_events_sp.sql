CREATE PROCEDURE CL.InsertFitnessConsultationEvent
@logTaskControlFlowKey	BIGINT

AS
BEGIN		
DECLARE		@rowsFromSource			INT				= 0
			,@rowsInserted			INT				= 0
			,@rowsFailed				INT				= 0
			,@ErrorMessage			NVARCHAR(1023)	= ''
			,@ErrorSeverity			INT
			,@ErrorState				INT
			,@DataLoadStatus			BIT				= 1
            ,@Today                  DATE            = GETDATE()
            ,@MaxFirstDayOfRange     DATE                                          
            ,@MaxLastDayOfRange      DATE                                                

	BEGIN TRY

		BEGIN TRAN


            -- Incremental Load
            SELECT
                @MaxFirstDayOfRange = MAX(FirstDayOfMonth)
                ,@MaxLastDayOfRange = MAX(LastDayOfMonth)
            FROM Master.DateDetail AS DD
            WHERE FirstDayOfMonth >= '2019-01-01'
                AND FirstDayOfMonth <= @Today
                AND DATEDIFF(DAY, LastDayOfMonth, @Today) >= 10

			DELETE
			FROM CL.FitnessConsultationEvent
			WHERE FirstDayOfRange = @MaxFirstDayOfRange


            -- Dimensions and Logs
            INSERT INTO CL.FitnessConsultationEvent (
                LocationKey
                ,AFNumber
                ,FirstDayOfRange
                ,LastDayOfRange
                ,CreatedBy
                ,CreatedDate
            )

            SELECT DISTINCT
                LocationKey
                ,LocationId
                ,FirstDayOfRange
                ,LastDayOfRange
                ,@logTaskControlFlowKey
                ,@Today
            FROM DW.LOCATION AS L 
            CROSS JOIN (
                    SELECT DISTINCT
                        FirstDayOfMonth AS FirstDayOfRange
                        ,LastDayOfMonth AS LastDayOfRange
                        ,DD.Month
                        ,DD.Year  
                    FROM Master.DateDetail AS DD  
                    WHERE FirstDayOfMonth >= '2019-01-01' AND FirstDayOfMonth <= @Today
                ) AS DD  
                WHERE (Country = 'CYM' OR IsDomestic = 1)  
                        AND Brand = 'Anytime Fitness'   
                        AND IsTestClub = 0              
                        AND FirstDayOfRange <= (ISNULL(CloseDate, @Today))
                        AND DATEDIFF(DAY, LastDayOfRange, @Today) >= 10
                        AND FirstDayOfRange = @MaxFirstDayOfRange                                                   


            ------------------
            -- PPV EXCLUDED --
            ------------------
			
            -- FC Events
            SELECT DISTINCT 
	            L.LocationId
	            ,L.LocationKey
	            ,A.CustomerKey
	            ,E.startTimeKey
	            ,E.EventKey
				,A.CurrentStatusKey
				,ISNULL(E.deletedAt,'2999-12-31') AS DeletedAt
            INTO #TempFCCustomers
            FROM DW.Event AS E
                INNER JOIN DW.Activity AS A
                    ON E.Eventkey = A.Eventkey 
			    INNER JOIN DW.Customer AS CS
				    ON CS.CustomerKey = A.CustomerKey
                    AND ISNULL(CS.IsTestMember,0) <> 1 -- Exclude Test Members
                    AND CS.CurrentTypeKey <> 5-- Exclude PPV Members
                INNER JOIN DW.Location AS L
                    ON E.LocationKey = L.LocationKey
            WHERE E.eventTypeKey = 8 -- Fitness Consultation Events


            -- PT Cancellation and Payment Date 
            SELECT
                AgreementID
                ,MIN(CAST(PaymentAttemptResponseDateTime AS DATETIME)) AS CancellationDate
            INTO #PTCancellation
            FROM DW.PTInvoiceDetail
            WHERE InvoiceType = 'Cancellation'
            GROUP BY AgreementID

            SELECT
                AgreementID
                ,MAX(CAST(PaymentAttemptResponseDateTime AS DATETIME)) AS MaxPaymentDate
            INTO #MaxPaymentDate
            FROM DW.PTInvoiceDetail AS TC  
            WHERE TC.InvoiceStatus = 1
            GROUP BY AgreementID


            -- PT Sales
            SELECT DISTINCT 
                AG.AgreementId
                ,AG.SaleDateKey
                ,RL.CustomerKey
                ,L.LocationID
                ,L.LocationKey
                ,CAST(CAST(AG.StartDateKey AS NVARCHAR) AS DATE) StartDate
                ,CAST(CAST(AG.EndDateKey AS NVARCHAR) AS DATE) EndDate 
                ,CAST(CAST(AG.SaleDateKey AS NVARCHAR) AS DATE) SaleDate  
                ,CASE
                    WHEN PTC.CancellationDate IS NOT NULL THEN PTC.CancellationDate
                    WHEN AG.RenewType = 0 THEN CAST(CAST(AG.EndDateKey AS NVARCHAR) AS DATE)
                    WHEN AG.RenewType = 1 THEN DATEADD(DAY, AG.RenewalCycle, CAST(CAST(AG.EndDateKey AS NVARCHAR) AS DATE))
                    WHEN AG.RenewType = 2 THEN DATEADD(DAY, BillingCycle, ISNULL(MPD.MaxPaymentDate,CAST(CAST(AG.EndDateKey AS NVARCHAR) AS DATE)))
                END AS ExpirationDate
                ,AG.RenewType
            INTO #TempPTSales
            FROM DW.Relationship AS RL
                INNER JOIN DW.Agreement AS AG 
                    ON RL.AgreementKey = AG.AgreementKey
                    AND RL.RelationshipValue > 0.00
                INNER JOIN Master.AgreementType AS AGT  
                    ON AGT.AgreementTypeKey = AG.AgreementTypeKey  
                    AND  AGT.AgreementType = 'Training Contract'
                INNER JOIN master.AgreementStatus AS MAS
                    ON MAS.AgreementStatusKey=AG.CurrentStatusKey
                    AND MAS.AgreementType = 'Training Contract'
                JOIN Master.DateDetail AS D 
                    ON  D.DateId = AG.SaleDATeKey 
                INNER JOIN DW.LOCATION AS L 
                    ON L.LocationKey = RL.LocationKey
                    AND L.Country IN ('USA','CAN')
                INNER JOIN DW.Customer AS CS
                    ON CS.CustomerKey = RL.CustomerKey
                    AND ISNULL(CS.IsTestMember,0)<>1 -- Exclude Test Members
                    AND CS.CurrentTypeKey <> 5-- Exclude PPV Members
                LEFT JOIN #PTCancellation AS PTC  
                    ON AG.AgreementId = PTC.AgreementId  
                LEFT JOIN #MaxPaymentDate AS MPD
                    ON MPD.AgreementId  = AG.AgreementID
            WHERE DATE <= @Today 


            -- New PT Member and Agreement From FC
            SELECT
                DT.FirstDayOfMonth
                ,FC.LocationID
                ,FC.LocationKey
                ,COUNT(DISTINCT CASE WHEN CurrentStatusKey = 7 AND DT.LastDayOfMonth<= deletedAt THEN FC.CustomerKey END) AS NewPTMembersFromFC
                ,COUNT(DISTINCT CASE WHEN CurrentStatusKey = 7 AND DT.LastDayOfMonth<= deletedAt THEN CONCAT(FC.CustomerKey, '-', PT.AgreementID) END) AS NewPTAgreementFC
            INTO #NewPTAgreementFC
            FROM #TempFCCustomers AS FC
                INNER JOIN #TempPTSales  AS PT 
                    ON FC.CustomerKey = PT.CustomerKey
                    AND FC.LocationID = PT.LocationID
                    AND FC.startTimeKey = PT.SaleDateKey
                INNER JOIN Master.DateDetail AS DT 
                    ON DT.DateId = FC.startTimeKey
            GROUP BY DT.FirstDayOfMonth, FC.LocationID, FC.LocationKey


            UPDATE VW 
            SET VW.NewPTAgreementsFromFC = FC.NewPTAgreementFC
				,VW.NewPTMembersFromFC = FC.NewPTMembersFromFC
            FROM CL.FitnessConsultationEvent AS VW
                INNER JOIN #NewPTAgreementFC AS FC
                    ON FC.LocationKey = VW.LocationKey 
                    AND FC.FirstDayOfMonth = VW.FirstDayOfRange
            

            -- New PT Agreements
            SELECT
                DT.FirstDayOfMonth
                ,FC.LocationID
                ,FC.LocationKey
                ,COUNT( DISTINCT CONCAT(FC.CustomerKey,'-',PT.AgreementID)) AS NewPTAgreement
            INTO #NewPTAgreement
            FROM #TempFCCustomers AS FC
                INNER JOIN #TempPTSales AS PT 
                    ON FC.CustomerKey = PT.CustomerKey
                    AND FC.LocationID = PT.LocationID
                    AND FC.startTimeKey <> PT.SaleDateKey
                INNER JOIN Master.DateDetail AS DT 
                    ON DT.DateId = PT.SaleDateKey
            GROUP BY DT.FirstDayOfMonth, FC.LocationID,FC.LocationKey


            UPDATE VW 
            SET VW.NewPTAgreementsNotFromFC = FC.NewPTAgreement
            FROM CL.FitnessConsultationEvent AS VW
                INNER JOIN #NewPTAgreement AS FC
                    ON	FC.LocationKey = VW.LocationKey 
                    AND FC.FirstDayOfMonth = VW.FirstDayOfRange


            -- FC Scheduled and Showed
            SELECT
                DT.FirstDayOfMonth
                ,L.LocationID
                ,L.LocationKey
                ,COUNT (DISTINCT (CASE WHEN DT.LastDayOfMonth <= ISNULL(E.deletedAt, '2999-12-31') THEN eventId END)) AS FCSchedule
                ,COUNT(DISTINCT (CASE WHEN A.CurrentStatusKey = 7   AND DT.LastDayOfMonth <= ISNULL(E.deletedAt, '2999-12-31') THEN eventId  END)) AS FCShowed 
            INTO #TempFCScheduled_Showed
            FROM DW.Event AS E
                INNER JOIN DW.Activity AS A 
                    ON E.EventKey = A.EventKey 
                INNER JOIN DW.Location AS L
                    ON E.LocationKey = L.LocationKey
                INNER JOIN MASTER.DateDetail AS DT 
                    ON DT.DateId = E.startTimeKey
                INNER JOIN DW.Customer AS CS
                    ON CS.CustomerKey = A.CustomerKey
                    AND ISNULL(CS.IsTestMember,0) <> 1 -- Exclude Test Members
                    AND CS.CurrentTypeKey <> 5-- Exclude PPV Members
            WHERE E.eventTypeKey = 8 -- Fitness Consultation Events
                AND DT.FirstDayOfMonth > = '2019-01-01'
            GROUP BY DT.FirstDayOfMonth, L.LocationID, L.LocationKey


            UPDATE VW 
            SET VW.FitnessConsultationScheduled = FC.FCSchedule
                ,VW.FitnessConsultationShowed = FC.FCShowed 
            FROM CL.FitnessConsultationEvent AS VW
                INNER JOIN #TempFCScheduled_Showed AS FC
                    ON FC.LocationKey = VW.LocationKey 
                    AND FC.FirstDayOfMonth = VW.FirstDayOfRange


            -- Renewed PT Agreements
            ;WITH CTE1 AS (
                SELECT *, LAG(ExpirationDate) OVER (PARTITION BY CustoMerKey ORDER BY StartDate) AS PreExpirationDate
                FROM #TempPTSales
            )

            SELECT
                D.FirstDayOfMonth
                ,LocationKey
                ,COUNT(DISTINCT AgreementId) AS RenewedPTAgreements
            INTO #TempRenewedPTAgreements
            FROM CTE1 AS C 
                INNER JOIN Master.DateDetail AS D 
                    ON C.SaleDateKey = D.DateId
            WHERE SaleDate <= EOMONTH(DATEADD(MONTH, 2, PreExpirationDate))
            GROUP BY D.FirstDayOfMonth, LocationKey


            UPDATE VW 
            SET VW.RenewedPTAgreements = FC.RenewedPTAgreements           
            FROM CL.FitnessConsultationEvent AS VW
                INNER JOIN #TempRenewedPTAgreements AS FC
                    ON FC.LocationKey = VW.LocationKey 
                    AND FC.FirstDayOfMonth = VW.FirstDayOfRange


            -- Total PT Agreements
            UPDATE VW 
            SET VW.TotalPTAgreements = NewPTAgreementsFromFC + NewPTAgreementsNotFromFC + RenewedPTAgreements     
            FROM CL.FitnessConsultationEvent AS VW


            --------------
            -- PPV ONLY --
            --------------


            -- FC Events
            SELECT DISTINCT 
	            L.LocationId
	            ,L.LocationKey
	            ,A.CustomerKey
	            ,E.startTimeKey
	            ,E.EventKey
				,A.CurrentStatusKey
				,ISNULL(E.deletedAt, '2999-12-31') AS DeletedAt
            INTO #TempFCCustomersPPV
            FROM DW.Event AS E 
                INNER JOIN DW.Activity AS A 
                    ON E.Eventkey = A.Eventkey 
			    INNER JOIN DW.Customer AS CS
                    ON CS.CustomerKey = A.CustomerKey
                    AND ISNULL(CS.IsTestMember,0) <> 1 -- Exclude Test Members
                    AND CS.CurrentTypeKey = 5 -- PPV Members Only
                INNER JOIN DW.Location AS L
                    ON E.LocationKey = L.LocationKey
            WHERE E.eventTypeKey = 8 -- Fitness Consultation Events


            -- PT Sales
            SELECT DISTINCT
                AG.AgreementId
                ,AG.SaleDateKey
                ,RL.CustomerKey
                ,L.LocationID
                ,L.LocationKey
                ,CAST(CAST(AG.StartDateKey AS NVARCHAR) AS DATE) StartDate
                ,CAST(CAST(AG.EndDateKey AS NVARCHAR) AS DATE) EndDate 
                ,CAST(CAST(AG.SaleDateKey AS NVARCHAR) AS DATE) SaleDate 
                ,CASE
                    WHEN PTC.CancellationDate IS NOT NULL THEN PTC.CancellationDate
                    WHEN AG.RenewType = 0 THEN CAST(CAST(AG.EndDateKey AS NVARCHAR) AS DATE)
                    WHEN AG.RenewType = 1 THEN DATEADD(DAY, AG.RenewalCycle, CAST(CAST(AG.EndDateKey AS NVARCHAR) AS DATE))
                    WHEN AG.RenewType = 2 THEN DATEADD(DAY, BillingCycle, ISNULL(MPD.MaxPaymentDate, CAST(CAST(AG.EndDateKey AS NVARCHAR) AS DATE)))
                END AS ExpirationDate
                ,AG.RenewType
                INTO #TempPTSalesPPV
                FROM DW.Relationship AS RL
                    INNER JOIN DW.Agreement AS AG 
                        ON RL.AgreementKey = AG.AgreementKey
                        AND RL.RelationshipValue > 0.00
                    INNER JOIN [Master].AgreementType AS AGT  
                        ON AGT.AgreementTypeKey = AG.AgreementTypeKey  
                        AND  AGT.AgreementType = 'Training Contract'
                    INNER JOIN Master.AgreementStatus AS MAS
                        ON MAS.AgreementStatusKey = AG.CurrentStatusKey
                        AND MAS.AgreementType = 'Training Contract'
                    INNER JOIN Master.DateDetail AS D 
                        ON D.DATEID = AG.SaleDATeKey 
                    INNER JOIN DW.LOCATION AS L 
                        ON L.LocationKey = RL.LocationKey
                        AND L.Country IN ('USA','CAN')
                    INNER JOIN DW.Customer AS CS
                            ON CS.CustomerKey = RL.CustomerKey
                            AND ISNULL(CS.IsTestMember,0) <> 1 -- Exclude Test Members
                            AND CS.CurrentTypeKey = 5 -- PPV Members Only
                    LEFT JOIN #PTCancellation PTC  
                        ON AG.AgreementId = PTC.AgreementId  
                    LEFT JOIN #MaxPaymentDate AS MPD
                        ON MPD.AgreementId  = AG.AgreementID
                WHERE DATE < = @Today 


            -- New PT Member and Agreement From FC
            SELECT
                DT.FirstDayOfMonth
                ,FC.LocationID
                ,FC.LocationKey
                ,COUNT(DISTINCT CASE WHEN CurrentStatusKey = 7 AND DT.LastDayOfMonth <= deletedAt THEN CONCAT(FC.CustomerKey, '-', PT.AgreementID) END) AS NewPTAgreementFCPPV
            INTO #NewPTAgreementFCPPV
            FROM #TempFCCustomersPPV AS FC
                INNER JOIN #TempPTSalesPPV  AS PT 
                    ON FC.CustomerKey = PT.CustomerKey
                    AND FC.LocationID = PT.LocationID
                    AND FC.startTimeKey = PT.SaleDateKey
                INNER JOIN Master.DateDetail AS DT 
                    ON DT.DateId = FC.startTimeKey
            GROUP BY DT.FirstDayOfMonth, FC.LocationID, FC.LocationKey


            UPDATE VW 
            SET VW.NewPTAgreementsFromFCPPV = FC.NewPTAgreementFCPPV
            FROM CL.FitnessConsultationEvent AS VW
                INNER JOIN #NewPTAgreementFCPPV AS FC
                    ON FC.LocationKey = VW.LocationKey 
                    AND FC.FirstDayOfMonth = VW.FirstDayOfRange


            -- New PT Agreements
            SELECT
                DT.FirstDayOfMonth
                ,FC.LocationID
                ,FC.LocationKey
                ,COUNT( DISTINCT CONCAT(FC.CustomerKey, '-', PT.AgreementID)) AS NewPTAgreementPPV
            INTO #NewPTAgreementPPV
            FROM #TempFCCustomersPPV AS FC
                INNER JOIN #TempPTSalesPPV  AS PT 
                    ON FC.CustomerKey = PT.CustomerKey
                    AND FC.LocationID = PT.LocationID
                    AND FC.startTimeKey <> PT.SaleDateKey
                INNER JOIN Master.DateDetail AS DT 
                    ON DT.DateId = PT.SaleDateKey
            GROUP BY DT.FirstDayOfMonth, FC.LocationID,FC.LocationKey


            UPDATE VW 
            SET VW.NewPTAgreementsNotFromFCPPV = FC.NewPTAgreementPPV
            FROM CL.FitnessConsultationEvent AS VW
                INNER JOIN #NewPTAgreementPPV AS FC
                    ON FC.LocationKey = VW.LocationKey 
                    AND FC.FirstDayOfMonth = VW.FirstDayOfRange


            -- FC Scheduled and Showed
            SELECT
                DT.FirstDayOfMonth
                ,L.LocationID
                ,L.LocationKey
                ,COUNT (DISTINCT (CASE WHEN DT.LastDayOfMonth<= ISNULL(E.deletedAt,'2999-12-31') THEN eventId END)) AS FCSchedulePPV
                ,COUNT(DISTINCT (CASE WHEN A.CurrentStatusKey = 7   AND DT.LastDayOfMonth<= ISNULL(E.deletedAt,'2999-12-31') THEN eventId  END)) AS FCShowedPPV 
            INTO #TempFCScheduled_ShowedPPV
            FROM DW.Event AS E
                INNER JOIN DW.Activity AS A 
                    ON E.EventKey = A.EventKey 
                INNER JOIN DW.Location AS L
                    ON E.LocationKey = L.LocationKey
                INNER JOIN MASTER.DateDetail AS DT 
                    ON  DT.DateId = E.startTimeKey
                INNER JOIN DW.Customer AS CS
                    ON CS.CustomerKey = A.CustomerKey
                    AND ISNULL(CS.IsTestMember,0) <> 1 -- Exclude Test Members
                    AND CS.CurrentTypeKey = 5-- PPV Members Only
            WHERE E.eventTypeKey = 8 -- Fitness Consultation Events 
                AND DT.FirstDayOfMonth >= '2019-01-01'
            GROUP BY DT.FirstDayOfMonth, L.LocationID, L.LocationKey


            UPDATE VW 
            SET VW.FitnessConsultationScheduledPPV = FC.FCSchedulePPV
                ,VW.FitnessConsultationShowedPPV = FC.FCShowedPPV 
            FROM CL.FitnessConsultationEvent AS VW
                INNER JOIN #TempFCScheduled_ShowedPPV AS FC
                    ON FC.LocationKey = VW.LocationKey 
                    AND FC.FirstDayOfMonth = VW.FirstDayOfRange


            -- Renewed PT Agreements
            ;WITH CTE1 AS (
                SELECT *, LAG(ExpirationDate) OVER (PARTITION BY CustoMerKey ORDER BY StartDate) AS PreExpirationDate
                FROM #TempPTSalesPPV
            )

            SELECT
                D.FirstDayOfMonth
                ,LocationKey
                ,COUNT(DISTINCT AgreementId) AS RenewedPTAgreementsPPV
            INTO #TempRenewedPTAgreementsPPV
            FROM CTE1 AS C 
                INNER JOIN Master.DateDetail AS D 
                    ON C.SaleDateKey = D.DateId
            WHERE  SaleDate <= EOMONTH(DATEADD(MONTH, 2, PreExpirationDate))
            GROUP BY D.FirstDayOfMonth, LocationKey


            UPDATE VW 
            SET VW.RenewedPTAgreementsPPV = FC.RenewedPTAgreementsPPV           
            FROM CL.FitnessConsultationEvent AS VW
                INNER JOIN #TempRenewedPTAgreementsPPV AS FC
                    ON FC.LocationKey = VW.LocationKey 
                    AND FC.FirstDayOfMonth = VW.FirstDayOfRange


            -- Total PT Agreements
            UPDATE VW 
            SET VW.TotalPTAgreementsPPV = NewPTAgreementsFromFCPPV + NewPTAgreementsNotFromFCPPV + RenewedPTAgreementsPPV     
            FROM CL.FitnessConsultationEvent AS VW


		COMMIT TRAN    
    
    END TRY
    BEGIN CATCH    
    
        IF @@TRANCOUNT >= 1    
            ROLLBACK TRAN    
    
        SELECT @ErrorMessage = ISNULL(ERROR_MESSAGE(), ''),    
               @DataLoadStatus = 0    
    
        INSERT INTO Process.LogError    
        (    
            ErrorNumber,    
            ErrorSeverity,    
            ErrorState,    
            ErrorProcedure,    
            ErrorLine,    
            ErrorMessage    
        )    
        SELECT ERROR_NUMBER() AS ErrorNumber,    
               ERROR_SEVERITY() AS ErrorSeverity,    
               ERROR_STATE() AS ErrorState,    
               ERROR_PROCEDURE() AS ErrorProcedure,    
               ERROR_LINE() AS ErrorLine,    
               N'For LogTaskControlFlowKey: ' + CAST(@logTaskControlFlowKey AS NVARCHAR(15)) + N' - ' + @ErrorMessage AS ErrorMessage    

		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState)
    
    END CATCH 

    SELECT @rowsInserted = COUNT(1) FROM CL.FitnessConsultationEvent
    
    UPDATE Process.LogTaskControlFlow    
    SET TotalRowsFromSource = @rowsFromSource,    
        TotalRowsInserted = @rowsInserted,    
        TotalRowsFailed = @rowsFailed,    
        EndTime = GETDATE(),    
        DataLoadStatus = CASE    
                            WHEN @DataLoadStatus = 1 THEN 'Completed'    
                            ELSE 'Procedure Failed'    
                        END,    
        ErrorMessage = CONCAT(ErrorMessage, '|', @ErrorMessage)    
    WHERE LogTaskControlFlowKey = @logTaskControlFlowKey    
    
    
END