DROP TABLE IF EXISTS CL.FitnessConsultationEvent
GO


CREATE TABLE CL.FitnessConsultationEvent (
   FitnessConsultationEventKey      INT IDENTITY (1,1)
   ,LocationKey						   BIGINT
   ,AFNumber						      NVARCHAR (263)
   ,FirstDayOfRange					   DATE 
   ,LastDayOfRange					   DATE
   ,FitnessConsultationScheduled	   INT DEFAULT (0)
   ,FitnessConsultationShowed       INT DEFAULT (0)
   ,NewPTAgreementsNotFromFC		   INT DEFAULT (0)
   ,NewPTAgreementsFromFC			   INT DEFAULT (0)
   ,NewPTMembersFromFC				   INT DEFAULT (0)
   ,RenewedPTAgreements				   INT DEFAULT (0)
   ,TotalPTAgreements				   INT DEFAULT (0)
   ,FitnessConsultationScheduledPPV INT DEFAULT (0)
   ,FitnessConsultationShowedPPV    INT DEFAULT (0)
   ,NewPTAgreementsNotFromFCPPV		INT DEFAULT (0)
   ,NewPTAgreementsFromFCPPV		   INT DEFAULT (0)
   ,RenewedPTAgreementsPPV			   INT DEFAULT (0)
   ,TotalPTAgreementsPPV			   INT DEFAULT (0)
   ,CreatedDate						   DATETIME
   ,CreatedBy                       BIGINT

   CONSTRAINT PK_FitnessConsultationEvent_FitnessConsultationEventKey PRIMARY KEY CLUSTERED (FitnessConsultationEventKey ASC),
   CONSTRAINT FK_FitnessConsultationEvent_LocationKey FOREIGN KEY (LocationKey) REFERENCES DW.Location(LocationKey)
); 