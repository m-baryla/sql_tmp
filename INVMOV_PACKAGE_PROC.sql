


-- =============================================
-- Author: Mateusz Bary³a
-- Create date: 2023-07-10
-- Edit date:   
-- Description:	procedure for creating packages automatically based on data from IMS
-- Change Log
-- 
-- =============================================
CREATE PROCEDURE [IN_CRUD].[CreatePackageFromIMS]
AS


/* DECLARE */
BEGIN
	declare @INVMOVBaseTbl table
	(
		ProdDate datetime,
		MOID varchar(50),
		BulkCode varchar(50),
		WorkCenter varchar(50),
		MOLineKey int
	)
	declare @groupDataTbl table
	(
		ProdDate datetime,
		BulkCode varchar(50),
		MOLineKey int
	)
	declare @groupMergeDataTbl table
	(
		PreviousProdDate datetime,
		PreviousMOLineKey int,
		NextProdDate datetime,
		NextMOLineKey int,
		DiffHourProdDate int,
		Seq int
	)
	declare @groupResultDataTbl table
	(
		BulkCode varchar(50),
		PreviousProdDate datetime,
		PreviousMOLineKey int,
		NextProdDate datetime,
		NextMOLineKey int,
		DiffHourProdDate int,
		CanBeMerge bit,
		Seq int
	)
	declare @resultDataTbl table
	(
		BulkCode varchar(50),
		PreviousProdDate datetime,
		PreviousMOLineKey int,
		NextProdDate datetime,
		NextMOLineKey int,
		DiffHourProdDate int,
		CanBeMerge bit
	)
	declare @createPackageTbl table
	(
		ProdDate datetime,
		PreviousMOLineKey int,
		NextMOLineKey int,
		PackageDate datetime,
		WorkCenter varchar(50),
		StockRoom varchar(50),
		Bin varchar(50),
		WorkCenterGroupKey int
	)
	declare @sqlStr nvarchar(2000)
	declare @UserMsg varchar(max)
	declare @CountBulkCode int

	declare @DiffHourProdDate int = 24		-- roznica godzinowa pomiedzy paczkami w obrebie 24 h paczki sa laczone w  max 2 szarze
	declare @WorkShiftMode int = 2			-- tryb zmiany 2 lub 3 zmiany

	
END

/* GET DATA FROM IMS external dbo/sys*/
begin
	SET @sqlStr = 
	N' SELECT' +
	 ' convert(varchar(20), DataPlanowanejProdukcji, 120),' +
	 ' EXTVIEW_ProductionOrderAndSpecyficationList.MOID,' +
	 ' EXTVIEW_ProductionOrderAndSpecyficationList.BulkCode,' +
	 ' WorkCenter' +
	 ' FROM "dbo.OrderAndSpecyficationList'

	insert into @INVMOVBaseTbl(ProdDate,MOID,BulkCode,WorkCenter) 
	exec sp_executesql @sqlStr
end

/* PREPARE DATA - pobranie MOLineKey potrzebne do dalszej analizy*/
BEGIN
	update @INVMOVBaseTbl
	set MOLineKey = d.MOLineKey
	from @INVMOVBaseTbl b
	inner join FS.MOLineDetail d on (CONCAT(d.MONumber,'-',d.MOLineNumberString) = b.MOID)

	-- remove all mo where package exist
	delete from @INVMOVBaseTbl where MOLineKey in (select d.MOLineKey from [INVMOV].[PackageDetail_IMS] d)

	--log data
	insert into [INVMOV].[LogBaseData_IMS](ProdDate,MOID,BulkCode,WorkCenter,MOLineKey,LoadDate)
	select 
	t.ProdDate,t.MOID,t.BulkCode,t.WorkCenter,t.MOLineKey,getdate()
	from @INVMOVBaseTbl t

END


-- Start db_cursor1////////////		
declare
	@crs1_BulkCode varchar(50)

DECLARE db_cursor1 CURSOR FOR 
	select DISTINCT 
		b.BulkCode
	from @INVMOVBaseTbl b -- grupowanie po bulku
OPEN db_cursor1  
FETCH NEXT FROM db_cursor1 INTO @crs1_BulkCode
WHILE @@FETCH_STATUS = 0  
	BEGIN  

-- Start db_cursor2////////////		
		declare
			@crs2_grBulkCode varchar(50),
			@crs2_grProdDate datetime,
			@crs2_grMOLineKey int

		DECLARE db_cursor2 CURSOR FOR 
			select DISTINCT 
				r.BulkCode,
				r.ProdDate,
				r.MOLineKey
			from @INVMOVBaseTbl r 
			where r.BulkCode = @crs1_BulkCode 
			order by r.ProdDate asc -- grupowanie po dacie produkcji od najczesniejszej do nastarszej per bulk code
		OPEN db_cursor2  
		FETCH NEXT FROM db_cursor2 INTO @crs2_grBulkCode,@crs2_grProdDate,@crs2_grMOLineKey
		WHILE @@FETCH_STATUS = 0  
			BEGIN  
				insert into @groupDataTbl(BulkCode,ProdDate,MOLineKey)
				select @crs2_grBulkCode,@crs2_grProdDate,@crs2_grMOLineKey
			FETCH NEXT FROM db_cursor2 INTO @crs2_grBulkCode,@crs2_grProdDate,@crs2_grMOLineKey
			END 
		CLOSE db_cursor2  
		DEALLOCATE db_cursor2 
-- End db_cursor2////////////		


		set @CountBulkCode = (select count(g.BulkCode) from @groupDataTbl g where g.BulkCode = @crs1_BulkCode)

		insert into @groupMergeDataTbl (PreviousProdDate,PreviousMOLineKey,NextProdDate,NextMOLineKey,DiffHourProdDate,Seq)
		select 
			t.ProdDate as PreviousProdDate,
			t.MOLineKey as PreviousMOLineKey,
			COALESCE(LEAD(g.ProdDate) over(order by g.ProdDate asc),g.ProdDate) as NextProdDate,
			COALESCE(LEAD(g.MOLineKey) over(order by g.ProdDate asc),g.MOLineKey) as NextMOLineKey,
			DATEDIFF(hour,t.ProdDate,COALESCE(LEAD(g.ProdDate) over(order by g.ProdDate asc),g.ProdDate)) as DiffHourProdDate,
			ROW_NUMBER() OVER( ORDER BY t.MOLineKey asc) 
		from @groupDataTbl g
		inner join @INVMOVBaseTbl t on (g.MOLineKey = t.MOLineKey)


-- Start db_cursor3////////////		
		declare @crs3_Seq int

		DECLARE db_cursor3 CURSOR FOR 
			select 
				g.Seq
			from @groupMergeDataTbl g 
		OPEN db_cursor3  
		FETCH NEXT FROM db_cursor3 INTO @crs3_Seq
		WHILE @@FETCH_STATUS = 0  
			BEGIN  
				if @CountBulkCode <> 1 --z wyjatek dla ilosci bulkow = 1
				begin
					delete from @groupMergeDataTbl where (Seq % 2 = 0)
				end			
			FETCH NEXT FROM db_cursor3 INTO @crs3_Seq
			END 
		CLOSE db_cursor3  
		DEALLOCATE db_cursor3 
-- end db_cursor3////////////	

		insert into @groupResultDataTbl
		(
			BulkCode,
			PreviousProdDate,
			PreviousMOLineKey,
			NextProdDate,
			NextMOLineKey,
			DiffHourProdDate,
			CanBeMerge,
			Seq
		)
		select 
			@crs1_BulkCode,
			g.PreviousProdDate,
			g.PreviousMOLineKey,
			g.NextProdDate,
			g.NextMOLineKey,
			g.DiffHourProdDate,
			CASE
				WHEN g.DiffHourProdDate >= @DiffHourProdDate THEN 0
				ELSE 1 
			END as CanBeMerge,
			g.Seq
		from @groupMergeDataTbl g 
		
		delete from @groupDataTbl
		delete from @groupMergeDataTbl
		FETCH NEXT FROM db_cursor1 INTO @crs1_BulkCode
	END 
CLOSE db_cursor1  
DEALLOCATE db_cursor1 
-- End db_cursor1////////////		



insert into @resultDataTbl (BulkCode,PreviousProdDate,PreviousMOLineKey,NextProdDate,NextMOLineKey,DiffHourProdDate,CanBeMerge)
	select g.BulkCode,g.PreviousProdDate,g.PreviousMOLineKey,g.NextProdDate,g.NextMOLineKey,g.DiffHourProdDate,g.CanBeMerge
	from @groupResultDataTbl g
	where g.CanBeMerge = 1 -- wszystkie mergowane zlecenia max 2 
union all
	select g.BulkCode,g.PreviousProdDate,g.PreviousMOLineKey,g.PreviousProdDate,g.PreviousMOLineKey,0,1 
	from @groupResultDataTbl g
	where g.CanBeMerge = 0 -- rozdzielenie tych zlecen ktore nie moga byc zmergowane: zlecenia nr 1 
union all
	select g.BulkCode,g.NextProdDate,g.NextMOLineKey,g.NextProdDate,g.NextMOLineKey,0,1 
	from @groupResultDataTbl g
	where g.CanBeMerge = 0 -- rozdzielenie tych zlecen ktore nie moga byc zmergowane: zlecenia nr 2



-- System 2 zmianowy
-- Jesli data produkcji jest pomiedzy 06:00 do 10:00 w poniedzialek to data paczki musi byc na piatek do godziny 16:00 max przy 10:00 (-66h).
-- Jesli data produkcji jest pomiedzy 10:00 do 22:00 w poniedzialek to data paczki musi byc tego samego dnia (-4h).
-- Jesli data produkcji jest pomiedzy 06:00 do 10:00 w wtorek,sroda,czwartek,piatek to data paczki musi byc na poprzedni dzien na I zmianie (-12h).
-- Jesli data produkcji jest pomiedzy 10:00 do 22:00 w wtorek,sroda,czwartek,piatek to data paczki musi byc data prod (-4h).

-- System 3 zmianowy
-- Jesli data produkcji jest pomiedzy 6:00 do 10:00 w poniedzialek to data paczki musi byc na piatek do godziny 16:00 max przy 10:00 (-66h).
-- Jesli data produkcji jest >= 10:00 w poniedzialek to data paczki musi byc data prod (-6h).
-- Jesli data produkcji jest w wtorek,sroda,czwartek,piatek to data paczki musi byc data prod (-6h).

insert into @createPackageTbl (ProdDate,PreviousMOLineKey,NextMOLineKey,PackageDate,WorkCenter,StockRoom,Bin,WorkCenterGroupKey)
select 
	--r.BulkCode,
	r.PreviousProdDate,
	r.PreviousMOLineKey,
	--r.NextProdDate,
	r.NextMOLineKey,
	--r.CanBeMerge,
	--r.DiffHourProdDate,
	--DATENAME(DW, r.PreviousProdDate) as ProdDateName,
	CASE
		WHEN -- System 2 zmianowy: Jesli data produkcji jest pomiedzy 06:00 do 10:00 w poniedzialek to data paczki musi byc na piatek do godziny 16:00 max przy prod date 10:00 (-66h).
			(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (1) AND 
			DATEPART(HOUR, r.PreviousProdDate) >= 6 and 
			DATEPART(HOUR, r.PreviousProdDate) <= 10 and 
			@WorkShiftMode = 2 
		THEN DATEADD(HOUR,-66, r.PreviousProdDate)
		------------------------------------------------------------------------------
		WHEN -- System 2 zmianowy: Jesli data produkcji jest pomiedzy 10:00 do 22:00 w poniedzialek to data paczki musi byc tego samego dnia (-4h).
			(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (1) AND 
			DATEPART(HOUR, r.PreviousProdDate) > 10 and 
			DATEPART(HOUR, r.PreviousProdDate) <= 22 and 
			@WorkShiftMode = 2 
		THEN DATEADD(HOUR,-4, r.PreviousProdDate)
		------------------------------------------------------------------------------
		WHEN -- System 2 zmianowy: Jesli data produkcji jest pomiedzy 06:00 do 10:00 w wtorek,sroda,czwartek,piatek to data paczki musi byc na poprzedni dzien na I zmianie (-12h).
			(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (2,3,4,5) AND 
			DATEPART(HOUR, r.PreviousProdDate) >= 6 and 
			DATEPART(HOUR, r.PreviousProdDate) <= 10 and 
			@WorkShiftMode = 2 
		THEN DATEADD(HOUR,-12, r.PreviousProdDate)
		------------------------------------------------------------------------------	
		WHEN -- System 2 zmianowy: Jesli data produkcji jest pomiedzy 10:00 do 22:00 w wtorek,sroda,czwartek,piatek to data paczki musi byc data prod (-4h).
			(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (2,3,4,5) AND 
			DATEPART(HOUR, r.PreviousProdDate) > 10 and 
			DATEPART(HOUR, r.PreviousProdDate) <= 22 and 
			@WorkShiftMode = 2 
		THEN DATEADD(HOUR,-4, r.PreviousProdDate)
		------------------------------------------------------------------------------	
		WHEN -- System 3 zmianowy: Jesli data produkcji jest pomiedzy 6:00 do 10:00 w poniedzialek to data paczki musi byc na piatek do godziny 16:00 max przy prod date 10:00 (-66h).
			(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (1) AND 
			DATEPART(HOUR, r.PreviousProdDate) >= 6 and 
			DATEPART(HOUR, r.PreviousProdDate) <= 10 and 
			@WorkShiftMode = 3 
		THEN DATEADD(HOUR,-66, r.PreviousProdDate)
		------------------------------------------------------------------------------	
		WHEN -- System 3 zmianowy: Jesli data produkcji jest >= 10:00 w poniedzialek to data paczki musi byc data prod (-6h).
			(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (1) AND 
			DATEPART(HOUR, r.PreviousProdDate) >= 10 and 
			@WorkShiftMode = 3 
		THEN DATEADD(HOUR,-6, r.PreviousProdDate)
		------------------------------------------------------------------------------	
		WHEN -- System 3 zmianowy: Jesli data produkcji jest w wtorek,sroda,czwartek,piatek to data paczki musi byc data prod (-6h).
			(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (2,3,4,5) AND 
			@WorkShiftMode = 3 
		THEN DATEADD(HOUR,-6, r.PreviousProdDate)
	END as PackageDate,
	d.WorkCenter,
	l.StockRoom,
	l.Bin,
	rg.WorkCenterGroupKey
from @resultDataTbl r
inner join FS.MOLineDetail d on (d.MOLineKey = r.PreviousMOLineKey)
inner join INVMOV.LocationPerWorkCenter l on (l.WorkCenterKey = d.WorkCenterKey)
inner join DICT.WorkCenter w on (w.WorkCenterKey = l.WorkCenterKey)
inner join DICT.REL_WorkCenterGroup rg on (rg.WorkCenterKey = d.WorkCenterKey)
where l.DictStatus = 1
order by r.BulkCode,r.PreviousProdDate asc 


-- Start db_cursor4////////////		
declare 
		@crs4_ProdDate datetime,
		@crs4_PreviousMOLineKey int,
		@crs4_NextMOLineKey int,
		@crs4_PackageDate datetime,
		@crs4_WorkCenter varchar(50),
		@crs4_StockRoom varchar(50),
		@crs4_Bin varchar(50),
		@crs4_WorkCenterGroupKey int

DECLARE db_cursor4 CURSOR FOR 
	select t.ProdDate,t.PreviousMOLineKey,t.NextMOLineKey,t.PackageDate,t.WorkCenter,t.StockRoom,t.Bin,t.WorkCenterGroupKey from @createPackageTbl t
OPEN db_cursor4  
FETCH NEXT FROM db_cursor4 INTO @crs4_ProdDate,@crs4_PreviousMOLineKey,@crs4_NextMOLineKey,@crs4_PackageDate,@crs4_WorkCenter,@crs4_StockRoom,@crs4_Bin,@crs4_WorkCenterGroupKey
WHILE @@FETCH_STATUS = 0  
	BEGIN  
		--////////////

		declare @lastPackageKey int = null
		--declare @lastPackageDetailKey int = null
		
		if 
			NOT EXISTS (select d.MOLineKey from [INVMOV].[PackageDetail_IMS] d where d.MOLineKey = @crs4_PreviousMOLineKey) --or 
			--NOT EXISTS (select d.MOLineKey from [INVMOV].[PackageDetail_IMS] d where d.MOLineKey = @crs4_NextMOLineKey)
		begin
			insert into [INVMOV].[Package_IMS]
			(
				--PackageKey,
				WorkCenterGroupKey,
				PackageName,
				PackageDescription,
				PackageDate,
				PackageVersion,
				PackageStatusKey,
				Stockroom,
				Bin,
				AddDate,
				AddUserName,
				ModDate,
				ModUserName,
				RNDMOID 
			)
			select 
				--@lastPackageKey,
				@crs4_WorkCenterGroupKey,
				@crs4_WorkCenter,
				'Created by automat.',
				@crs4_PackageDate,
				1,
				10,
				@crs4_StockRoom,
				@crs4_Bin,
				GETDATE(),
				'IMS',
				null,
				null,
				NULL

				 if @@ROWCOUNT = 1
					begin

						set @lastPackageKey = (select max(p.PackageKey) from [INVMOV].[Package_IMS] p)

						if NOT EXISTS (select d.MOLineKey from [INVMOV].[PackageDetail_IMS] d where d.MOLineKey = @crs4_PreviousMOLineKey)
						begin
							--set @lastPackageDetailKey = (select max(p.PackageDetailKey) from [INVMOV].[PackageDetail_IMS] p) + 1

							insert into [INVMOV].[PackageDetail_IMS]
								(
									--PackageDetailKey,
									PackageKey,
									MOLineKey,
									UserName,
									AddDate
								)
								select 
									--@lastPackageDetailKey,
									@lastPackageKey,
									@crs4_PreviousMOLineKey,
									'IMS',
									GETDATE()
						end	

						if NOT EXISTS (select d.MOLineKey from [INVMOV].[PackageDetail_IMS] d where d.MOLineKey = @crs4_NextMOLineKey)
						begin
							--set @lastPackageDetailKey = (select max(p.PackageDetailKey) from [INVMOV].[PackageDetail_IMS] p) + 1

							insert into [INVMOV].[PackageDetail_IMS]
							(
								--PackageDetailKey,
								PackageKey,
								MOLineKey,
								UserName,
								AddDate
							)
							select 
								--@lastPackageDetailKey,
								@lastPackageKey,
								@crs4_NextMOLineKey,
								'IMS',
								GETDATE()
						end
					end
		end
		--////////////
	FETCH NEXT FROM db_cursor4 INTO @crs4_ProdDate,@crs4_PreviousMOLineKey,@crs4_NextMOLineKey,@crs4_PackageDate,@crs4_WorkCenter,@crs4_StockRoom,@crs4_Bin,@crs4_WorkCenterGroupKey
	END 
CLOSE db_cursor4  
DEALLOCATE db_cursor4 
-- end db_cursor4////////////		


-- log load data

	insert into [INVMOV].[LogCreatePackageFromIMS]
	(
		BulkCode,
		PreviousProdDate,
		PreviousMOLineKey,
		NextProdDate,
		NextMOLineKey,
		CanBeMerge,
		DiffHourProdDate,
		ProdDateName,
		PackageDate,
		WorkCenter,
		StockRoom,
		Bin,
		WorkCenterGroupKey,
		LogDate
	)
	select 
		r.BulkCode,
		r.PreviousProdDate,
		r.PreviousMOLineKey,
		r.NextProdDate,
		r.NextMOLineKey,
		r.CanBeMerge,
		r.DiffHourProdDate,
		DATENAME(DW, r.PreviousProdDate) as ProdDateName,
		CASE
			WHEN -- System 2 zmianowy: Jesli data produkcji jest pomiedzy 06:00 do 10:00 w poniedzialek to data paczki musi byc na piatek do godziny 16:00 max przy prod date 10:00 (-66h).
				(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (1) AND 
				DATEPART(HOUR, r.PreviousProdDate) >= 6 and 
				DATEPART(HOUR, r.PreviousProdDate) <= 10 and 
				@WorkShiftMode = 2 
			THEN DATEADD(HOUR,-66, r.PreviousProdDate)
			------------------------------------------------------------------------------
			WHEN -- System 2 zmianowy: Jesli data produkcji jest pomiedzy 10:00 do 22:00 w poniedzialek to data paczki musi byc tego samego dnia (-4h).
				(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (1) AND 
				DATEPART(HOUR, r.PreviousProdDate) > 10 and 
				DATEPART(HOUR, r.PreviousProdDate) <= 22 and 
				@WorkShiftMode = 2 
			THEN DATEADD(HOUR,-4, r.PreviousProdDate)
			------------------------------------------------------------------------------
			WHEN -- System 2 zmianowy: Jesli data produkcji jest pomiedzy 06:00 do 10:00 w wtorek,sroda,czwartek,piatek to data paczki musi byc na poprzedni dzien na I zmianie (-12h).
				(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (2,3,4,5) AND 
				DATEPART(HOUR, r.PreviousProdDate) >= 6 and 
				DATEPART(HOUR, r.PreviousProdDate) <= 10 and 
				@WorkShiftMode = 2 
			THEN DATEADD(HOUR,-12, r.PreviousProdDate)
			------------------------------------------------------------------------------	
			WHEN -- System 2 zmianowy: Jesli data produkcji jest pomiedzy 10:00 do 22:00 w wtorek,sroda,czwartek,piatek to data paczki musi byc data prod (-4h).
				(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (2,3,4,5) AND 
				DATEPART(HOUR, r.PreviousProdDate) > 10 and 
				DATEPART(HOUR, r.PreviousProdDate) <= 22 and 
				@WorkShiftMode = 2 
			THEN DATEADD(HOUR,-4, r.PreviousProdDate)
			------------------------------------------------------------------------------	
			WHEN -- System 3 zmianowy: Jesli data produkcji jest pomiedzy 6:00 do 10:00 w poniedzialek to data paczki musi byc na piatek do godziny 16:00 max przy prod date 10:00 (-66h).
				(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (1) AND 
				DATEPART(HOUR, r.PreviousProdDate) >= 6 and 
				DATEPART(HOUR, r.PreviousProdDate) <= 10 and 
				@WorkShiftMode = 3 
			THEN DATEADD(HOUR,-66, r.PreviousProdDate)
			------------------------------------------------------------------------------	
			WHEN -- System 3 zmianowy: Jesli data produkcji jest >= 10:00 w poniedzialek to data paczki musi byc data prod (-6h).
				(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (1) AND 
				DATEPART(HOUR, r.PreviousProdDate) >= 10 and 
				@WorkShiftMode = 3 
			THEN DATEADD(HOUR,-6, r.PreviousProdDate)
			------------------------------------------------------------------------------	
			WHEN -- System 3 zmianowy: Jesli data produkcji jest w wtorek,sroda,czwartek,piatek to data paczki musi byc data prod (-6h).
				(((DATEPART(DW, r.PreviousProdDate) - 1 ) + @@DATEFIRST ) % 7) IN (2,3,4,5) AND 
				@WorkShiftMode = 3 
			THEN DATEADD(HOUR,-6, r.PreviousProdDate)
		END as PackageDate,
		d.WorkCenter,
		l.StockRoom,
		l.Bin,
		rg.WorkCenterGroupKey,
		getdate()
	from @resultDataTbl r
	inner join FS.MOLineDetail d on (d.MOLineKey = r.PreviousMOLineKey)
	inner join INVMOV.LocationPerWorkCenter l on (l.WorkCenterKey = d.WorkCenterKey)
	inner join DICT.WorkCenter w on (w.WorkCenterKey = l.WorkCenterKey)
	inner join DICT.REL_WorkCenterGroup rg on (rg.WorkCenterKey = d.WorkCenterKey)
	where l.DictStatus = 1 and r.PreviousMOLineKey not in (select s.PreviousMOLineKey from [INVMOV].[LogCreatePackageFromIMS] s)
	order by r.BulkCode,r.PreviousProdDate asc 


GO


