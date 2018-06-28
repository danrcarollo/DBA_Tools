USE Master
GO


IF (select object_id('sp_WhatNow')) IS NULL BEGIN EXEC('CREATE PROC sp_WhatNow AS select getdate()') END
GO

ALTER PROCEDURE [dbo].[sp_WhatNow]   
  @spid varchar(8) = null,  
  @status sysname = null,  
  @loginame sysname = null,  
  @command sysname = null,  
  @dbname sysname = null,   
  @hostname sysname = null,  
  @waittime int = null, 
  @lastwaittype varchar(48) = null, 
  @lastbatch datetime = null,  
  @program sysname = null,  
  @opentran int = null,  
  @blocked bit = null,  
  @verbose bit = null,
  @showall bit = 0, 
  @datacollect bit =0,
  @CollectActiveTran bit=0,
  @OpenTranDate datetime=null, --passed from spGetOpenTranInfo
  @excludereplication bit=null,
  @debug bit=0
as  
  
/***********************************************************************************************************   
**  PROC:		sp_WhatNow
**	PURPOSE:	Shows active processes on SQL Server, including actual query statements and query plans. Also displays blocking information.
**	EXAMPLES:	
		
		--Show ALL actively-running processes. 
			exec [sp_WhatNow] 

		--Show ALL actively-running processes AND lock details. NOTE: This is more intensive. 
			exec [sp_WhatNow] @verbose=1

		--Show details for a specific, acttive session_id (@spid)
			exec sp_WhatNow @spid=782

		--Show blocking processes
			exec [sp_WhatNow] @blocked=1
	
		--Show ALL Connections, active or not.
			exec [sp_WhatNow] @showall=1

		--Filter by an attribute, such as status, login, dbname, hostname.   CAn pass multiple parameters.
			exec [sp_WhatNow] @status=running
			exec [sp_WhatNow] @hostname='MyComputerName'
			exec [sp_WhatNow] @loginame='Bilbo'

**	
 Author: Richard Ding  
**   Creation Date: 10/10/2004  
**   Suppoorted for SQL versions 2008 and later. 

**   Version: 1.0.0  
**   MODIFIED: 04/15/2005 Dan Carollo -- Added DBCC INPUTBUFFER for blocking spids  
**   MODIFIED: 04/28/2005 Dan Carollo -- Removed bogus check on whether login exists in syslogins.  
**   MODIFIED: 05/11/2005 Dan Carollo -- Added @verbose mode, DBCC INPUTBUFFER, Lock Info and SQL Handle when passing a @spid  
**   MODIFIED: 05/23/2005 Dan Carollo -- Increased lenghth of DBCC INPUTBUFFER EventInfo to varchar(1000)  
**             Also:  Added CountBlocked column to display how many spids a Blocker is blocking.  
**	 MODIFIED: 05/04/2007 Dan Carollo -- More work. Compiled for PASUPDB  
**   MODIFIED: 05/07/2007 Dan Carollo -- Commented out code to translate waitresource names.  
**	 MODIFIED: 04/09/2008 Dan Carollo -- Added @Verbose when you add @spid (whether blocking or not).
**   MODIFIED: 11/29/2009 Dan Carollo -- Added @lastwaittype search parameter.  Useful for tracking CPU Parallelism, for example:  EXEC AMOPS..sp_WhatNow @lastwaittype='CXPACKET'
**	 MODIFIED: 03/29/2011 Dan Carollo -- Added @active=1 to display TOP active queries...
**	 MODIFIED: 05/02/2011 Dan Carollo -- Added TEMPDB space Usage to @active=1 query
**	 MODIFIED: 12/01/2011 Dan Carollo -- Using OUTER JOIN to TEMPDB users to show ALL actively running
**	 MODIFIED: 11/27/2012 Dan Carollo -- Fixed bug where dbname was longer than 64 characters
**	 MODIFIED: 12/07/2012 Dan Carollo -- Fixed bug with #LOCKS and null DBNAME.
**   MODIFIED: 01/17/2014 Dan Carollo -- Removed extra output for blocking
**	 MODIFIED: 04/17/2014 Dan Carollo -- Added option to pass @SPID with @ACTIVE=1, so we can see TEMPDB usage information per selected SPID
**	 MODIFIED: 05/22/2014 Dan Carollo -- Addition additional info to @ACTIVE=1.  Also added @datacollect parameter to allow us to collect to [tblServerAccessAuditQueryDetail]
**	 MODIFIED: 07/28/2014 Dan Carollo -- Added optional parameter @excludereplication, to exlude active spids doing replication.
**	 MODIFIED: 08/06/2014 Dan Carollo -- Fixed DataCollect to acoomodate new column
**	 MODIFIED: 08/28/2014 Dan Carollo -- Changed this to OUTER APPLY sys.dm_exec_sql_text(er.sql_handle)
**	 MODIFIED: 09/03/2014 Yossi Mihailovici -- Changed query plans to be stored in a seperate table
**	 MODIFIED: 09/08/2014 Dan Carollo - Fixed a bug to eliminate duplicate values from being attempted to be inserted into tblQueriesPlans
**	 MODIFIED: 09/29/2014 Dan Carollo - Fixed bug to show ONLY blocking where waittime > 1000 by default. Otherwise shows confusing results
**	 MODIFIED: 11/09/2014 Dan Carollo - Added @CollectActiveTran switch to collect active transactions in OPEN TRAN
**	 MODIFIED: 04/25/2016 Dan Carollo - Changed varchar(max) to nvarchar(max) when collecting query plans:  Conversion of one or more characters from XML to target collation impossible
**   MODIFIED: 01/20/2017 Dan Carollo - Converted program_name field to display the JOB Name (instead of GUID) for SQLAgent jobs.
**	 MODIFIED: 11/09/2017 Dan Carollo - Re-write for Azure - BETA 1
**	 MODIFIED: 06/19/2018 Dan Carollo - Removed legacy syntax prior to 2008. 
**	 MODIFIED: 06/20/2018 Dan Carollo - Added Count of CountParallelThreads to the active results
**	 MODIFIED: 06/28/2018 Dan Carollo - Greatly simplified the search argument logic and removed the dynamic SQL. 
***************************************************************************************************************************/  
  
set nocount on  
  

--This is just to check if we have ANY Params passed.
DECLARE @SARG_all varchar(8000)
SET @SARG_all=COALESCE(
	CAST(@spid as varchar),  
	CAST(@status as varchar),  
	CAST(@loginame as varchar),  
	CAST(@command as varchar),  
	CAST(@dbname as varchar),   
	CAST(@hostname as varchar),  
	CAST(@waittime as varchar), 
	CAST(@lastwaittype as varchar), 
	CAST(@lastbatch as varchar),  
	CAST(@program as varchar),  
	CAST(@opentran as varchar),  
	CAST(@blocked as varchar)
	)



--FIRST, COLLECT ALL THE PROCESSES, ACTIVE AND NON...

	 SELECT  
	 er.session_id,   
	 db_name(er.database_id) as DBName,  
	 es.host_name,  
	 es.status,  
	 es.program_name,  
	 es.login_name,  
	 es.login_time,
	 er.start_time, 
	 er.wait_time, 
	 er.last_wait_type,
	 er.blocking_session_id,
	 er.cpu_time,  
	 er.total_elapsed_time,  
	 er.open_transaction_count,
	 er.reads as physical_reads,  
	 er.writes, 
	 er.logical_reads, 
	 er.granted_query_memory,
	 ThreadsCTE.CountThreads as CountParallelThreads,
	 (R1.request_internal_objects_alloc_page_count*8)/1024 as TEMPDBSpace_UsageMB,   
	 er.command,  
	  cast((      
	   SELECT TOP 1 SUBSTRING(st.text,  statement_start_offset / 2, ( (CASE WHEN statement_end_offset = -1 THEN (LEN(CONVERT(nvarchar(max),st.text)) * 2) ELSE statement_end_offset END)  - statement_start_offset) / 2)        
	  ) as nvarchar(max))  AS sql_statement ,   
		object_name(st.objectid,st.dbid) AS ObjectName,
		qp.query_plan,
		 CHECKSUM (CONVERT(nvarchar(max),query_plan))  AS hashedPlan
	INTO #TEMPExecSessions
	  FROM sys.dm_exec_requests as er    
	  OUTER APPLY sys.dm_exec_sql_text(er.sql_handle) as st   
	  OUTER APPLY sys.dm_exec_query_plan (er.plan_handle) as qp  
	 JOIN sys.dm_exec_sessions es  
	 ON er.session_id=es.session_id  
	 LEFT OUTER JOIN 		(  
			SELECT TOP 100 PERCENT session_id, request_id, 
		  SUM(internal_objects_alloc_page_count+user_objects_alloc_page_count) AS request_internal_objects_alloc_page_count
		  FROM sys.dm_db_task_space_usage 
		  GROUP BY session_id, request_id
		  HAVING SUM(internal_objects_alloc_page_count+user_objects_alloc_page_count) >0
		  ) as R1 
	 ON er.session_id=R1.session_id
	 AND er.request_id=R1.request_id
	 LEFT OUTER JOIN   (
		 SELECT session_id,count(*) as CountThreads 
		 FROM sys.dm_os_tasks 
		-- where  task_state='RUNNING'
		 GROUP BY session_id
		 ) as ThreadsCTE
	 ON er.session_id=ThreadsCTE.session_id
	 WHERE er.session_id <> @@SPID
	 --AND (@excludereplication is NULL or es.client_interface_name<>'OLEDB'); --Exlude replication activity if @excludereplication=1



--SHOW BLOCKING, IF @blocked IS NOT NULL
DECLARE @block_flag BIT  
SET @block_flag = 0  
IF @blocked =1
	BEGIN  --BEGIN BLOCKED INFO
   

			SELECT 'LEAD BLOCKER(S)...'

			SELECT
					es.session_id
					,es.status
					,login_name   = SUBSTRING(login_name, 1, 12)
					,host_name   = SUBSTRING(host_name, 1, 12)
					,blockedby        =  blocking_session_id
					,dbname     = SUBSTRING(DB_NAME(er.database_id),1,64)
					,er.command
					,wait_type
					,wait_time
					,start_time
					,SQLStatement       =
						SUBSTRING
						(
							qt.text,
							er.statement_start_offset/2,
							(CASE WHEN er.statement_end_offset = -1
								THEN LEN(CONVERT(nvarchar(MAX), qt.text)) * 2
								ELSE er.statement_end_offset
								END - er.statement_start_offset)/2
						)
			FROM sys.dm_exec_sessions es
			LEFT JOIN sys.dm_exec_requests er
					ON er.session_id = es.session_id
			OUTER APPLY sys.dm_exec_sql_text(er.sql_handle) AS qt
			WHERE  es.session_id IN (SELECT blocking_session_id FROM sys.dm_exec_requests WHERE blocking_session_id >0) -- AND wait_time>=ISNULL(@waittime,1000)) 
			AND blocking_session_id = 0
			ORDER BY er.start_time asc

			----FULL LIST...
			SELECT 'DETAILED LIST...'

			SELECT
			 er.session_id as Waiting_Sessionid
			 , es.login_time
			 , er.start_time
			 , db_name(er.database_id) as DBName
			 , es.host_name
			 , es.login_name
			 , er.command
			 , er.status
			 , br.session_id AS Blocking_SessionID
			 , br.blocking_session_id AS [Blocking_SessionID_Blocker]
			 , ISNULL(cb.CountBlockedSessions,0) as CountBlockedSessions
			 , er.wait_time
			 , er.total_elapsed_time
			 , REPLACE(REPLACE(REPLACE(substring(wait_text.text,1,128),'CREATE  PROCEDURE ',''),'CREATE PROCEDURE ',''),'CREATE PROC','') as [WaitingBatch] 
			 , REPLACE(REPLACE(REPLACE(substring(block_text.text,1,128),'CREATE  PROCEDURE ',''),'CREATE PROCEDURE ',''),'CREATE PROC','') as [BlockingBatch]
			 --Waiting statement
			 , SUBSTRING(wait_text.text, er.statement_start_offset/2 +1, 
			(CASE WHEN er.statement_end_offset = -1 
				THEN LEN(CONVERT(nvarchar(max),wait_text.text)) * 2
				ELSE er.statement_end_offset END
			- er.statement_start_offset) / 2+1) as [WaitingStatement]

			--Blocking statement
			,SUBSTRING(block_text.text,
			br.statement_start_offset/2 +1, 
			(CASE WHEN br.statement_end_offset = -1 
				THEN LEN(CONVERT(nvarchar(max),block_text.text)) * 2
				ELSE br.statement_end_offset END
			- br.statement_start_offset) / 2+1) as [BlockingStatement]

			 , er.wait_type
			 , er.wait_resource
			 , es.program_name
			 , er.cpu_time
			 , er.reads
			 , er.writes
			 , er.logical_reads
			 , er.granted_query_memory
			 , er.open_transaction_count
		   --  , qp.query_plan as Blocking_Query_plan

		  FROM sys.dm_exec_requests AS er (NOLOCK) --blocked
		  JOIN sys.dm_exec_sessions AS es (NOLOCK)
			ON er.session_id=es.session_id
		  LEFT JOIN sys.dm_exec_requests AS br (NOLOCK) --blocking
			ON  er.blocking_session_id = br.session_id 
		--CROSS APPLY
		  --     sys.dm_exec_query_plan(br.plan_handle) AS qp 
		CROSS APPLY 
			sys.dm_exec_sql_text(br.sql_handle) AS block_text 
		OUTER APPLY 
			sys.dm_exec_sql_text(er.sql_handle) AS wait_text 
		LEFT JOIN (SELECT blocking_session_id
						, CountBlockedSessions = count(*)
					 FROM sys.dm_exec_requests (NOLOCK)
					WHERE blocking_session_id>0 
				 GROUP BY blocking_session_id)        cb
			ON er.session_id=cb.blocking_session_id
		 WHERE (ISNULL(er.blocking_session_id,0)>0 OR ISNULL(cb.CountBlockedSessions,0)>0)
		AND er.wait_time>=ISNULL(@waittime,0)
		ORDER BY er.start_time asc


	RETURN

	END  --END BLOCKED INFO

  


--IF NO Search params passed, OR if @showall=0, then show only ACTIVE SPIDS
IF (@showall=0 AND @SARG_all is NULL) --we only show ACTIVE connections by default.
		BEGIN
			--ACTIVE ONLY...
			SELECT 	*
			FROM #TEMPExecSessions
			WHERE status<> 'sleeping'
			ORDER BY cpu_time desc, physical_reads desc
	
		END
ELSE IF (@showall=1 OR @SARG_all is NOT NULL)
		BEGIN

			SELECT 	*
			FROM #TEMPExecSessions
			WHERE (@spid is NULL or session_id=@spid)
			AND (@status is NULL or status=@status)
			AND (@loginame IS NULL OR login_name=@loginame)
			AND (@command IS NULL or command=@command)
			AND (@dbname is NULL or DBName=@dbname)
			AND (@program IS NULL OR program_name=@program)
			AND (@hostname IS NULL OR host_name=@hostname  )
			AND (@waittime IS NULL OR wait_time>=@waittime )
			AND (@lastwaittype IS NULL OR last_wait_type=@lastwaittype )
			AND (@lastbatch IS NULL OR start_time >=@lastbatch  )
			AND (@opentran IS NULL OR open_transaction_count>=@opentran )
			AND (@blocked IS NULL OR blocking_session_id>=@blocked )
			ORDER BY cpu_time desc, physical_reads desc

		END
	

  
-- ALSO DISPLAY DBCC INPUTBUFFER, SQL HANDLE and LOCK INFO For SINGLE Spids when @VERBOSE=1  
IF (@verbose=1 OR @spid IS NOT NULL)
   
 BEGIN --Verbose  
 
	--LOCKS
	SELECT 
	  request_session_id as spid,  
	  db_name(resource_database_id) as DBName,
	  Object_name(resource_associated_entity_id,resource_database_id) as ObjectName,
	request_type,
	request_mode,
	request_status,
	COUNT_BIG(*) as CountLocks
	FROM sys.dm_tran_locks (nolock)
	WHERE resource_type='OBJECT'
	AND (@spid IS NULL OR request_session_id = @spid)
	GROUP BY 
	  request_session_id ,  
	  db_name(resource_database_id),
	  Object_name(resource_associated_entity_id,resource_database_id),
	request_type,
	request_mode,
	request_status
   --ORDER BY 1,2 --DBName, ObjectName  
  

 END  --Verbose  
  
  
return (0)  
  
DROP TABLE #TEMPExecSessions


GO

