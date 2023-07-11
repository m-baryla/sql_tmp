


-- =============================================
-- Author:		  Mateusz Bary³a
-- Create date: 2022-11-20
-- Edit date:   2022-12-15
-- Description:	From WH to process
-- Change Log
-- 2016-01-21 ?
-- 2019-08-28 added clue where NeedQuantity > 0.0001 in output
-- 2019-09-10 rebuilt of procedure, added filter for remaining percentage
-- 2021-04-28 PackakingType description added
-- 2022-11-22 Rebuilding the procedure into two parts:
--			 1. Handling of raw materials that are part of the RM3P project (downloading allowed lots for a given BUKLU)
--			 2. Handling of raw materials that are not part of the RM3P project (download any lots regardless of BULK)
-- 2022-12-15 wyjatek dla bulku perrigo ktorego receptura jest rozwiajana prze cetes
-- 2023-05-18 added RNDMOID
-- 2023-05-23 ADDED AdditionalLotIndicator is NULL IN WH INVENT TABLE
-- 2023-06-23 added limited mo line status < 5 for RND MO
-- =============================================
CREATE PROCEDURE [SSRS].[INVMOV_GetDataFromWHToProc] (
	@WorkCenterGroupKey int, -- this parameter is not needed, as PackageKey is Primary Key in INVMOV.Package table
	@PackageKey int
)

AS
BEGIN


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	declare @CurrentPackageRequiredDate datetime
	declare @CurrentWorkCenterGroup int
	declare @MinimumRemainingRel float = 0.01 -- =1%
	declare	@MinimumReportedQuantity float = 0.0001
	declare	@StkBinSeparator char(1) = '-'
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	select
		@CurrentPackageRequiredDate = p.PackageDate,
		@CurrentWorkCenterGroup = p.WorkCenterGroupKey
	from INVMOV.Package p
	where p.PackageKey = @PackageKey
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	declare @ResTbl table 
	(
		ItemNumber varchar(50),
		ItemDescription varchar(200),
		RMQuantity float,
		MissingQuantity float,
		QuantityFromLocation float,
		Stockroom varchar(50),
		Bin varchar(50),
		InventoryQuantity float,
		LotNumber varchar(50),
		LotExpirationDate datetime,
		FirstReceiptDate datetime,
		DateFromLot datetime,
		LocationDescription varchar(50),
		LocationBarcode varchar(50)
	)
	declare @RMTblTmp table 
	(
		PackageDate datetime,
		PackageKey int,
		BULKItemKey int,
		RMItemKey int,
		RMQuantity float,
		BULKCustomerItemSubgroupingKey int,
		RM3PScopeCheck bit,
		InventoryQuantity float
	)
	declare @procInven table 
	(
		ItemKey int,
		LotNumber varchar(50),
		AvailabeQuantity float,
		ExpDate datetime
	)
	declare @whInven table 
	(
		ItemKey int,
		LotNumber varchar(50),
		InventoryQuantity float,
		AvailabeQuantity float,
		ExpDate datetime,
		Stockroom varchar(50),
		Bin varchar(50),
		FirstReceiptDate datetime,
		DateFromLot datetime,
		ItemInventoryKey int
	)
	declare @RM_Pack table 
	(
		BULKItemKey int,
		RMItemKey int,
		RMQuantity float,
		InventoryQuantity float,
		Seq int
	)
	declare @ItemDataTbl table 
	(
		ItemKey int,
		ItemNumber varchar(50),
		ItemDescription varchar(200),
		ItemCategory varchar(50),
		LocationInfo nvarchar(200),
		LocationBarcode varchar(50)
	)
	declare @WHInvCrsResult table 
	(
		BULKItemKey int,
		RMItemKey int,
		ItemInventoryKey int,
		QuantityToMove float,
		Customer varchar(50)
	)
	declare @ResTmp table 
	(
		RMItemKey int,
		ItemInventoryKey int,
		QuantityToMove float,
		Customer varchar(2000)
	)
	declare @RMPackageTmp table 
	(
		RMItemKey int,
		PackageKey int,
		PackageDate datetime
	)
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	insert into @RMTblTmp 
	(
		PackageDate,
		PackageKey,
		BULKItemKey,
		RMItemKey,
		RMQuantity
	)
	select
		ph.PackageDate,
		ph.PackageKey,
		m.ItemKey,
		rm.ComponentItemKey,
		rm.OpenQuantity
	from
		INVMOV.Package ph 
		inner join INVMOV.PackageDetail pd on (ph.PackageKey = pd.PackageKey) 
		inner join INVMOV.GetOpenRMQuantityInMOPick(@CurrentWorkCenterGroup, @MinimumRemainingRel) rm on (pd.MOLineKey = rm.MOLineKey)
		inner join FS.MOLineDetail m on (m.MOLineKey = rm.MOLineKey)
	where
		ph.WorkCenterGroupKey = @CurrentWorkCenterGroup and
		(ph.PackageStatusKey in (30, 40, 100) or (ph.PackageStatusKey = 20 and ph.PackageDate < @CurrentPackageRequiredDate)) and
		ph.RNDMOID is NULL

	update @RMTblTmp
	set BULKCustomerItemSubgroupingKey = i.CustomerItemSubgroupingKey
	from @RMTblTmp r
	inner join FS.ItemMaster i ON (i.ItemKey = r.BULKItemKey)

	update @RMTblTmp
	set RM3PScopeCheck = 1
	where BULKCustomerItemSubgroupingKey in 
	(
		select c.CustomerKey from RM3P.GetCustListForPivot() c
	)

	-- rnd moid
	insert into @RMTblTmp 
	(
		PackageDate,
		PackageKey,
		BULKItemKey,
		RMItemKey,
		RMQuantity
	)
	select
		ph.PackageDate,
		ph.PackageKey,
		rw.BulkKey,
		i.ItemKey,
		--rw.RequestQuantity - rw.IssuedQuantity
		CASE 
		WHEN (rw.RequestQuantity - rw.IssuedQuantity) / rw.RequestQuantity < 0.05 THEN 0
		ELSE rw.RequestQuantity - rw.IssuedQuantity 
		END 
	from
		INVMOV.Package ph 
		inner join INVMOV.PackageDetail pd on (ph.PackageKey = pd.PackageKey) 
		INNER JOIN IMS.RNDWeighting rw on (rw.MOID = ph.RNDMOID)
		inner join FS.ItemMaster i on (i.ItemNumber = rw.RMCode)
	where
		ph.WorkCenterGroupKey = @CurrentWorkCenterGroup 
		and ph.PackageStatusKey in (20,30, 40, 100) and rw.MOLineStatus < 5
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	insert into @procInven
	(
		ItemKey,
		LotNumber,
		AvailabeQuantity,
		ExpDate
	)
	select
		ii.ItemKey,
		ii.LotNumber,
		sum(ii.InventoryQuantity),
		lm.LotExpirationDate
	from
		FS.FS_ItemInventory ii 
		inner join INVMOV.ProcessLocation pl on (ii.Stockroom = pl.Stockroom and ii.Bin = pl.Bin)
		left join FS.LotMaster lm on (ii.LotTraceKey = lm.LotTraceKey)
	where
		pl.WorkCenterGroupKey = @CurrentWorkCenterGroup 
		and pl.ProductionAvailable <> 0 
		and ii.InventoryCategory = 'O'
	group by 
		ii.ItemKey,
		ii.LotNumber,
		lm.LotExpirationDate
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	insert into @whInven
	(
		ItemKey,
		LotNumber,
		InventoryQuantity,
		AvailabeQuantity,
		ExpDate,
		Stockroom,
		Bin,
		FirstReceiptDate,
		DateFromLot,
		ItemInventoryKey
	)
	select
		ii.ItemKey,
		ii.LotNumber,
		ii.InventoryQuantity,
		ii.InventoryQuantity,
		l.LotExpirationDate,
		ii.Stockroom,
		ii.Bin,
		l.FirstReceiptDate,
		l.DateFromLot,
		ii.ItemInventoryKey
	from
		(
			select DISTINCT r.RMItemKey from @RMTblTmp r
		) t1 
		inner join FS.FS_ItemInventory ii on (t1.RMItemKey = ii.ItemKey) 
		inner join FS.LotMaster l on (ii.LotTraceKey = l.LotTraceKey) 
		inner join INVMOV.WhLocation whl on (ii.Stockroom = whl.Stockroom) 
		left outer join
		(
			select 
				Stockroom, 
				Bin, 
				ProcessLocKey 
			from INVMOV.ProcessLocation 
			where (IncludedInternal = 0 or ProductionAvailable <> 0) 
			and WorkCenterGroupKey = @CurrentWorkCenterGroup
		) pl on (ii.Stockroom = pl.Stockroom and ii.Bin = pl.Bin)
	where
		ii.InventoryCategory = 'O' 
		and whl.StockroomLevelKey = 1 
		and pl.ProcessLocKey is null 
		and whl.WorkCenterGroupKey = @CurrentWorkCenterGroup
		and l.AdditionalLotIndicator is NULL
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	update @RMTblTmp
	set InventoryQuantity = INVMOV.GetRMInvLotForBulk(BULKItemKey,RMItemKey)
	where RM3PScopeCheck = 1
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	insert into @RM_Pack
	SELECT 
	r.BULKItemKey,r.RMItemKey,r.RMQuantity,r.InventoryQuantity,
	CASE  
		WHEN RM3PScopeCheck is NULL THEN -1
		ELSE ROW_NUMBER() OVER( ORDER BY InventoryQuantity asc) 
	END as Seq
	FROM @RMTblTmp r 
	inner join FS.ItemMaster i on (i.ItemKey = r.RMItemKey)  
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	-- resultCrs var
	declare
		@BULKItemKey int,
		@RMItemKey int,
		@RMQuantity float,
		@Seq int,
		@neededRMQty float

	-- procInvCrs var
	DECLARE 
		@ProcLotNumber varchar(50),
		@ProcAvailabeQuantity float

	-- whInvCrs var
	DECLARE 
		@WhAvailabeQuantity float,
		@qtyFromLoc float,
		@WHItemInventoryKey int


	DECLARE resultCrs CURSOR FOR 
		select r.BULKItemKey,r.RMItemKey,r.RMQuantity from @RM_Pack r where r.Seq <> -1
	OPEN resultCrs  
	FETCH NEXT FROM resultCrs INTO @BULKItemKey,@RMItemKey,@RMQuantity
	WHILE @@FETCH_STATUS = 0  


		BEGIN  
			set @neededRMQty = @RMQuantity

			DECLARE procInvCrs CURSOR FOR 
					select p.LotNumber,p.AvailabeQuantity 
					from @procInven p
					where p.LotNumber in (select g.LotNumber from INVMOV.GetRMLotForBulk(@BULKItemKey,@RMItemKey) g)
					order by p.ExpDate
				OPEN procInvCrs  
				FETCH NEXT FROM procInvCrs INTO @ProcLotNumber,@ProcAvailabeQuantity
				WHILE @@FETCH_STATUS = 0 and @neededRMQty > 0  
					BEGIN 
						if(@ProcAvailabeQuantity > @neededRMQty)
							begin
								set @ProcAvailabeQuantity = @ProcAvailabeQuantity - @neededRMQty
								set @neededRMQty = 0
							end
						else
							begin
								set @neededRMQty = @neededRMQty - @ProcAvailabeQuantity
								set @ProcAvailabeQuantity = 0
							end

						update @procInven
						set AvailabeQuantity = @ProcAvailabeQuantity
						where LotNumber = @ProcLotNumber

						FETCH NEXT FROM procInvCrs INTO @ProcLotNumber,@ProcAvailabeQuantity
					END 
				CLOSE procInvCrs  
				DEALLOCATE procInvCrs 

				if(@neededRMQty > 0)
				begin
					DECLARE whInvCrs CURSOR FOR 
						select p.AvailabeQuantity,p.ItemInventoryKey 
						from @whInven p
						where p.LotNumber in (select g.LotNumber from INVMOV.GetRMLotForBulk(@BULKItemKey,@RMItemKey) g)
						order by p.ExpDate,p.FirstReceiptDate,p.DateFromLot,p.InventoryQuantity,p.Stockroom,p.Bin
					OPEN whInvCrs  
					FETCH NEXT FROM whInvCrs INTO @WhAvailabeQuantity,@WHItemInventoryKey 
					WHILE @@FETCH_STATUS = 0 and @neededRMQty > @MinimumReportedQuantity 
						BEGIN 
							if(@WhAvailabeQuantity >= @neededRMQty)
							begin
								set @WhAvailabeQuantity = @WhAvailabeQuantity - @neededRMQty
								set @qtyFromLoc = @neededRMQty
								set @neededRMQty = 0
							end
							else
							begin
								set @neededRMQty = @neededRMQty - @WhAvailabeQuantity
								set @qtyFromLoc = @WhAvailabeQuantity
								set @WhAvailabeQuantity = 0
							end

							update @whInven
							set AvailabeQuantity = @WhAvailabeQuantity
							where ItemInventoryKey = @WHItemInventoryKey
					
							insert into @WHInvCrsResult
							(
								BULKItemKey,
								RMItemKey,
								ItemInventoryKey,
								QuantityToMove
							)
							values
							(
								@BULKItemKey,
								@RMItemKey,
								@WHItemInventoryKey,
								@qtyFromLoc
							)
							FETCH NEXT FROM whInvCrs INTO @WhAvailabeQuantity,@WHItemInventoryKey 
						END 
					CLOSE whInvCrs  
					DEALLOCATE whInvCrs 
				end	
		FETCH NEXT FROM resultCrs INTO @BULKItemKey,@RMItemKey,@RMQuantity
		END 
	CLOSE resultCrs  
	DEALLOCATE resultCrs 
end
-- koniec analizy surowcow dla klientow in scope - w zakresie projektu rm3p
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	declare @RMTbl table
	(
		RMItemKey int,
		RMQuantity float,
		RMQtyToMove float
		--PackageKey int,
		--PackageDate datetime
	)
	declare @ProdLocRMTbl table 
	(
		RMItemKey int,
		RMQuantity float
	)


	insert into @RMTbl 
	(
		RMItemKey,
		RMQuantity
	)
	select
	RMItemKey,
	sum(RMQuantity)
	from @RMTblTmp
	where RM3PScopeCheck is NULL OR RM3PScopeCheck <> 1
	group by RMItemKey


	update @RMTbl
	set RMQtyToMove = t1.RMQuantity - t2.AvailabeQuantity
	from
		@RMTbl t1 inner join
		@procInven t2 on (t1.RMItemKey = t2.ItemKey)


	insert into @ProdLocRMTbl 
	(
		RMItemKey,
		RMQuantity
	)
	select
		p.ItemKey,
		sum(p.AvailabeQuantity)
	from @procInven p
	where p.AvailabeQuantity > 0
	group by
		p.ItemKey


	update @RMTbl
	set RMQtyToMove = t1.RMQuantity - t2.RMQuantity
	from
		@RMTbl t1 inner join
		@ProdLocRMTbl t2 on (t1.RMItemKey = t2.RMItemKey)

	update @RMTbl
	set RMQtyToMove = RMQuantity
	where RMQtyToMove is null

	delete @RMTbl
	where RMQtyToMove < @MinimumReportedQuantity
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	declare itemCursor cursor LOCAL FORWARD_ONLY FAST_FORWARD READ_ONLY
	for
		select RMItemKey, RMQtyToMove from @RMTbl

	open itemCursor
	fetch next from itemCursor into @RMItemKey, @RMQuantity
	while @@FETCH_STATUS = 0
	begin
		declare whCursor cursor LOCAL FORWARD_ONLY FAST_FORWARD READ_ONLY
		for
			select AvailabeQuantity,ItemInventoryKey
			from @whInven
			where ItemKey = @RMItemKey and AvailabeQuantity > 0
			order by ExpDate, FirstReceiptDate, DateFromLot, InventoryQuantity, Stockroom, Bin
		open whCursor
		fetch next from whCursor into @WhAvailabeQuantity,@WHItemInventoryKey 
		while @@FETCH_STATUS = 0 and @RMQuantity > @MinimumReportedQuantity
		begin

			if @RMQuantity <= @WhAvailabeQuantity
			begin
				set @qtyFromLoc = @RMQuantity
				set @RMQuantity = 0
			end
			else
			begin
				set @qtyFromLoc = @WhAvailabeQuantity
				set @RMQuantity = @RMQuantity - @qtyFromLoc
			end

			insert into @WHInvCrsResult
			(
				RMItemKey,
				ItemInventoryKey,
				QuantityToMove
			)
			values
			(
				@RMItemKey,
				@WHItemInventoryKey,
				@qtyFromLoc
			)
				
			fetch next from whCursor into @WhAvailabeQuantity,@WHItemInventoryKey 
		end
		close whCursor
		deallocate whCursor

		fetch next from itemCursor into @RMItemKey, @RMQuantity
	end
	close itemCursor
	deallocate itemCursor
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	update @WHInvCrsResult
	set Customer = i.CustomerItemSubgrouping
	from @WHInvCrsResult w
	INNER JOIN FS.ItemMaster i on (i.ItemKey = w.BULKItemKey)

	UPDATE @WHInvCrsResult
	SET Customer = 'URI'
	WHERE Customer is null

	insert into @ResTmp
	(
		RMItemKey ,
		ItemInventoryKey ,
		QuantityToMove
		--Customer
	)
	select 
	w.RMItemKey,
	w.ItemInventoryKey,
	sum(w.QuantityToMove)
	from @WHInvCrsResult w
	group by 
	w.RMItemKey,
	w.ItemInventoryKey

	declare @crs_ItemInventoryKey int


	DECLARE customerCrs CURSOR FOR 
		select t.ItemInventoryKey from @ResTmp t
	OPEN customerCrs  
	FETCH NEXT FROM customerCrs INTO @crs_ItemInventoryKey
	WHILE @@FETCH_STATUS = 0  
		BEGIN  
			update @ResTmp
			set Customer = (select REPLACE(REPLACE((SELECT s.Customer FROM @WHInvCrsResult s where s.ItemInventoryKey = @crs_ItemInventoryKey FOR XML path('')),'<Customer>',''),'</Customer>',', '))
			where ItemInventoryKey = @crs_ItemInventoryKey

		FETCH NEXT FROM customerCrs INTO @crs_ItemInventoryKey
		END 
	CLOSE customerCrs  
	DEALLOCATE customerCrs 


	insert into @RMPackageTmp 
	(
		RMItemKey,
		PackageDate
	)
	select
	RMItemKey,
	max(PackageDate)
	from @RMTblTmp
	group by RMItemKey


	update @RMPackageTmp 
	set PackageKey = @PackageKey
	from
		@RMPackageTmp t1 inner join
		@RMTblTmp t2 on t1.RMItemKey = t2.RMItemKey
	where
		t2.PackageKey = @PackageKey

	update @RMPackageTmp 
	set PackageKey = t2.MaxPackageKey
	from
		@RMPackageTmp t1 inner join
		(
			select
				PackageDate,
				max(PackageKey) as MaxPackageKey
			from @RMTblTmp
			where PackageKey <> @PackageKey
			group by PackageDate
		) t2 on (t1.PackageDate = t2.PackageDate)
	where
		t1.PackageKey is null
end
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
	insert into @ItemDataTbl 
	(
		ItemKey,
		ItemNumber,
		ItemDescription,
		ItemCategory,
		LocationInfo,
		LocationBarcode
	)
	select
		i.ItemKey,
		i.ItemNumber,
		i.ItemDescription,
		i.ItemCategory,
		CASE 
			WHEN i.StorageConditionKey IS NULL THEN 'PRODUCTION AIRLOCK' 
			WHEN i.StorageCondition = 'N/A' THEN 'PRODUCTION AIRLOCK'
			WHEN i.StorageCondition = 'FRIDGE' THEN 'PRODUCTION AIRLOCK'
			ELSE ISNULL(sl.Description, 'NOT ASSIGNED')
		END AS LocationDescription, 
		CASE 
			WHEN i.StorageConditionKey IS NULL THEN p.Stockroom + @StkBinSeparator + p.Bin 
			WHEN i.StorageCondition = 'N/A' THEN p.Stockroom + @StkBinSeparator + p.Bin 
			WHEN i.StorageCondition = 'FRIDGE' THEN p.Stockroom + @StkBinSeparator + p.Bin 
			ELSE ISNULL(sl.SpecificLocation, '')
		END AS LocationBarcode
	from @RMPackageTmp t 
	inner join INVMOV.Package p on (t.PackageKey = p.PackageKey)
	inner join FS.ORI_ItemMaster i on (t.RMItemKey = i.ItemKey)
	left outer join
	(
		select StorageConditionKey, Description, Stockroom + @StkBinSeparator + Bin as SpecificLocation
		from INVMOV.SpecificItemLocation
		where WorkCenterGroupKey = @CurrentWorkCenterGroup
	) sl on (i.StorageConditionKey = sl.StorageConditionKey)



	select 
	it.ItemNumber,
	it.ItemDescription,
		case
			when r.QuantityToMove >= 10 then FS.Float2StrSeparator(r.QuantityToMove, 1, ' ')
			when r.QuantityToMove < 10 and r.QuantityToMove > 1 then FS.Float2Str(r.QuantityToMove, 2)
			else FS.Float2Str(r.QuantityToMove, 4)
		end as NeedQuantity,
	ii.Stockroom,
	ii.Bin,
	ii.InventoryQuantity,
	ii.LotNumber,
	wh.FirstReceiptDate,
	it.LocationBarcode,
	it.LocationInfo as SpecificStorageCondition,
	it.LocationBarcode as Localization,
	p.PackagingType,
	r.Customer
	from @ResTmp r
	inner join @ItemDataTbl it on (it.ItemKey = r.RMItemKey)
	inner join FS.FS_ItemInventory ii on (r.ItemInventoryKey = ii.ItemInventoryKey)
	inner join @whInven wh on (wh.ItemInventoryKey = r.ItemInventoryKey)
	left join INVMOV.PackagingType p on (p.LotNumber = ii.LotNumber)
	where QuantityToMove >= @MinimumReportedQuantity
	order by ii.Stockroom, ii.Bin, it.ItemNumber, ii.InventoryQuantity


end
-- koniec analizy surowcow dla klientow out scope - poza zakresem projektu rm3p
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


END


GO


