
select * from ProcessingFiles where file_path='LAB'  order by id desc
--This query retrieves all entries from the ProcessingFiles table where the file_path is 'LAB'. 
--By ordering the results in descending order by id, it ensures that the most recently processed files appear first.

  
Declare		 
			@FileName varchar(200),   
			@FileID int, 
			@uploadedDate DateTime
   SET @FileName = 'EHC_VIVANT_LAB_01012025_04302025_MEDICARE.txt'
 --Set up variables to use later.`@FileName` is the name of the lab data file you're importing.
  
  

IF NOT EXISTS (SELECT '*' FROM ProcessingFiles where File_Name=@FileName and File_Path='LAB')
BEGIN
		Insert into ProcessingFiles(File_Name,File_Path,File_Uploaded_By,File_Uploaded_Date,status,Received_Date,file_year,Display_Name)
		select @FileName,'LAB','test',GETDATE(),'Pending',GETDATE(),2025,'Lab_Rivercity_Elica_01012023_12312023'
END
--Registers the new file in the `ProcessingFiles` table.
--* Prevents duplicate entries for the same file.
--* Marks the file as `"Pending"

  
IF OBJECT_ID('tempdb..#DumpLABRxFile1') IS NOT NULL
			DROP TABLE #DumpLABRxFile1;
--Ensures the temp table is clean before loading new data.


CREATE TABLE #DumpLABRxFile1 (
    [Line of Business] varchar(50),
    [Member ID] varchar(50),
    [Medicare HIC] varchar(50),
    [Medicaid ID] varchar(50),
    [Member Last Name] varchar(50),
    [Member First Name] varchar(50),
    [Gender] varchar(10),
    [DOB] varchar(50),
    [Provider_Group] varchar(50),
    [Lab Provider Name] varchar(150),
    [Date of Service] varchar(50),
    [LOINC] varchar(50),
    [CPT ] varchar(50),
    [Result_Value] varchar(1000),
    [Units] varchar(50),
    [Test_Name ] varchar(500),
    [Ordering Provider NPI] varchar(50),
    [Miscellaneous] varchar(50),
    [Miscellaneous2] varchar(50)
)
-- creating DumpLABRxFile1 table 

  
BULK
			INSERT #DumpLABRxFile1			
			--FROM 'E:\Projects\HEDIS\Latest\EMR_Data_Ram\Jan-May 2017RCMGEMR REP.txt'
			FROM 'G:\Captivateteam\Upload Files\Lab Data\EHC_VIVANT_LAB_01012025_04302025_MEDICARE.txt'
			WITH
			(
				FIRSTROW=2,
				FIELDTERMINATOR = '|',
				ROWTERMINATOR = '\n'
			);
- Loads pipe-delimited data from the file into `#DumpLABRxFile1`.
-- Skips the header row

---->Loads data from a pipe-delimited .txt file into the temporary table #DumpLABRxFile1.
---->FIRSTROW = 2: Skips the header row (assumes the first row contains column names).
---->FIELDTERMINATOR = '|': Specifies that fields are separated by the pipe (|) character.
---->ROWTERMINATOR = '\n': Indicates that each record ends with a newline characte


			
			select * from #DumpLABRxFile1 --where Gender='NB'
      --retriving data from DumpLABRxFile1
			 where Gender NOT IN ('F', 'M') 
			select *  from #DumpLABRxFile1 where Gender NOT IN ('F', 'M') 
--* Identify invalid gender values (like `NB`, empty, etc.)
--Fix Invalid Gender Using `Members` Table
			select * from #DumpLABRxFile1 where len([Result_Value])=254
   --- retriving data from DumpLABRxFile1 of len([Result_Value])=254 

  
--Update LAB
	SET LAB.[Gender] = M.Gender
FROM #DumpLABRxFile1 LAB(NOLOCK)
JOIN Members M(NOLOCK) ON M.MemberID = LAB.[Member ID]
where LAB.[Gender] NOT IN ('F', 'M') 
-- Match directly with member table 
  -- (NOLOCK) it alows dirty reads
  -- herev we update m.gender as lab.gender by joining members 
  -- where member id match with the lab of member id 
 -- and gender of not f ,m 
  
--Update LAB
	SET LAB.[Gender] = M.Gender
FROM #DumpLABRxFile1 LAB(NOLOCK)
JOIN Members M(NOLOCK) ON M.MemberID = REPLACE(LAB.[Member ID],substring(LAB.[Member ID],1,3),'')
where LAB.[Gender] NOT IN ('F', 'M')
	and (ISNUMERIC(substring(LAB.[Member ID],1,1))=0 and ISNUMERIC(substring(LAB.[Member ID],2,2))=0 
	and ISNUMERIC(substring(LAB.[Member ID],3,3))=0)

--REPLACE(LAB.[Member ID],substring(LAB.[Member ID],1,3),'') here we replace the member id with removing first 3 digits 
--Extract the numeric part of the [Member ID] by skipping the first 3 characters.
--Ensure the prefix is non-numeric (i.e., you're dealing with something like 'ABC123456').


			-- Update #DumpLABRxFile1
			set Gender='F'
			where Gender='NB'
     -- here we update  Gender from nb to f
			select Gender From Members where memberid='93308399A'
  -- here we retrive the gender from members table of memberid='93308399A'

			Insert into LabFileData(FileId, [LineofBusiness], Patient_ID, [MedicareHIC], [MedicaidID], PT_LastName,PT_FirstName,
			 PT_Gender,PT_DOB,Provider_Location_Name,Prov_Name,DateofService,LOINC_Code,[CPT_Code],
			 Result_Value,Result_Units,TestName,Prov_NPI,[Miscellaneous1],[Miscellaneous2],
			 UploadDate,IsProcessed,CreatedBy,CreatedDateTime,ModifiedBy,ModifiedDateTime
			 )


			 select 5390, Ltrim(Rtrim([Line of Business])),Ltrim(Rtrim([Member ID])), Ltrim(Rtrim([Medicare HIC])),Ltrim(Rtrim([Medicaid ID])),Ltrim(Rtrim([Member Last Name])),
			 Ltrim(Rtrim([Member First Name])), Ltrim(Rtrim([Gender])), Ltrim(Rtrim([DOB])), Ltrim(Rtrim([Provider_Group])), Ltrim(Rtrim([Lab Provider Name])), Ltrim(Rtrim([Date of Service])),
			 Ltrim(Rtrim([LOINC])), Ltrim(Rtrim([CPT ])), Ltrim(Rtrim([Result_Value])), Ltrim(Rtrim([Units])), Ltrim(Rtrim([Test_Name ])), Ltrim(Rtrim([Ordering Provider NPI])), Ltrim(Rtrim([Miscellaneous])),
			 Ltrim(Rtrim([Miscellaneous2])), Getdate(), 1, 'Test', Getdate(), 'Test', Getdate()
			 from #DumpLABRxFile1

        -- here we insert data into the main table after removing all unwanted 
         -- ltrim and rtrim are used to trim the spaces of the coloums 
          --Transfers cleaned data from temp table to `LabFileData`.
          --Trims all values to remove extra spaces.

			 
--Update LAB
	SET Patient_ID = RTRIM(LTRIM(Patient_ID))
FROM LabFileData LAB(NOLOCK)
where FileID = 5390

--Sets `FileId = 5390`.
--Transfers cleaned and validated data from the temp table #DumpLABRxFile1 to the permanent table LabFileData.
--Sets FileId = 5390 to identify this specific import batch.
--Applies LTRIM(RTRIM(...)) to remove any accidental leading/trailing spaces from text fields.
--Optionally uses TRY_CAST to ensure safe type conversion, especially for date fields.


  
DECLARE @FileId INT = ?

--Update LAB
	SET MemberID = M.MemberID
FROM LabFileData LAB(NOLOCK)
JOIN Members M(NOLOCK) ON M.MemberID = REPLACE(Patient_ID,substring(Patient_ID,1,3),'')
where FileID = 5390
	and (ISNUMERIC(substring(Patient_ID,1,1))=0 and ISNUMERIC(substring(Patient_ID,2,2))=0 
	and ISNUMERIC(substring(Patient_ID,3,3))=0)
	
	DECLARE @FileId INT = ?

--Update LAB
	SET MemberID = M.MemberID
FROM LabFileData LAB(NOLOCK)
JOIN Members M(NOLOCK) ON M.MemberID = Patient_ID
where FileID = 5390 and LAB.Memberid is null

DECLARE @FileId INT = ?

--update LAB  
	set Memberid=(
					Select top 1 MemberID 
					from Members(NOLOCK) 
					where LastName = LAB.PT_LastName AND FirstName = LAB.PT_FirstName AND CAST(DateOfBirth as Date) = CAST(LAB.PT_DOB as Date) 
					order by ISNULL(PCPToDate,GETDATE()) desc
				)
 FROM  LabFileData LAB(NOLOCK)
 where FileID = 5390 and LAB.Memberid is null
 
 DECLARE @FileId INT = ?

--Update LAB 
	set LAB.memberid = M.MemberID
from LabFileData LAB
JOIN Members M ON M.MemberID = substring(Patient_ID,1,9)
where fileid = 5390 AND len(Patient_ID) > 9 AND LEFT(Patient_ID,1) = '9' AND LAB.MEMBERID IS NULL

DECLARE @FileId INT = ?

--Update LAB 
	set MemberID = Patient_ID 
FROM LabFileData LAB(NOLOCK)
Where MemberID is null
	AND FileID=5390



DECLARE @FileId INT = ?

--update LAB 
	set LAB.Healthplancode = M.HealthPlanCode
from LabFileData LAB (nolock)
inner join Members M (nolock) on LAB.MemberID = M.MemberID  
where LAB.MemberID IS NOT NULL  
	AND LAB.HealthPlanCode is null 
	and M.OPTHDate is null
	and FileId= 5390
	
	DECLARE @FileId INT = ?

--update LAB 
	set LAB.Healthplancode = (SELECT top 1 HealthPlanCode FROM Members M (NOLOCK) 
								WHERE LAB.MemberID=M.MemberID and M.OPTHDate is not null
								order by  M.OPTHDate desc)
from LabFileData LAB (nolock) 
where LAB.MemberID IS NOT NULL  
	AND LAB.HealthPlanCode is null
	and FileId= 5390
	
	DECLARE @FileId INT = ?
	
--Update LAB 
	set ClaimNo = (
					SELECT MAX(ClaimNo) ClaimNo 
					FROM ClaimS CM (NOLOCK) 
					WHERE  LAB.[CPT_Code]= LTRIM(rtrim(CM.ProcCode)) AND
						cast(LAB.DateofService as Date) = cast(CM.ServiceDateFrom as Date) AND 
						LAB.MemberID=CM.MemberID
				)
FROM LabFileData LAB 
WHERE FileID = 5390 
	and (LAB.ClaimNo is null or ISNULL(LAB.ClaimNo,'')='')
	
	DECLARE @FileId INT = ?

--Update LAB 
	set ClaimNo = (
					SELECT MAX(ClaimNo) ClaimNo 
					FROM ClaimS CM (NOLOCK) 
					WHERE cast(LAB.DateofService as Date) = cast(CM.ServiceDateFrom as Date) AND 
					LAB.MemberID=CM.MemberID
				)
FROM LabFileData LAB 
WHERE FileID=5390
	and (LAB.ClaimNo is null or ISNULL(LAB.ClaimNo,'')='')

	
--UPDATE ProcessingFiles 
SET status = 'Completed' 
where id = 5390


--update ConfigurationParams 
set ConfigValue = CAST(GETDATE() as varchar)

--select * from ConfigurationParams
where ConfigID = 14
