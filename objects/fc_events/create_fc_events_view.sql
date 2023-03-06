DROP VIEW IF EXISTS CL.VW_FitnessConsultationEvent
GO


CREATE VIEW CL.VW_FitnessConsultationEvent
AS
    SELECT 
        AFNumber						
        ,FirstDayOfRange				
        ,LastDayOfRange				
        ,FitnessConsultationScheduled	
        ,FitnessConsultationShowed    
        ,NewPTAgreementsNotFromFC	
        ,NewPTAgreementsFromFC	
        ,NewPTMembersFromFC
        ,RenewedPTAgreements			
        ,TotalPTAgreements	
        ,FitnessConsultationScheduledPPV
        ,FitnessConsultationShowedPPV   
        ,NewPTAgreementsNotFromFCPPV	
        ,NewPTAgreementsFromFCPPV	
        ,RenewedPTAgreementsPPV			
        ,TotalPTAgreementsPPV
    FROM CL.FitnessConsultationEvent;