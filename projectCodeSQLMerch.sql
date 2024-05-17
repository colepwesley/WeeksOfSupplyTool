WITH WHSInv as

(
SELECT case when Country = 'US' then 'USA' when country = 'UK' then 'GBR' when country = 'MX' then 'MEX' when country = 'CA' then 'CAN' else Country end PhysicalCountry
, dp.ProductSKey
, sum(OnHand) OnHand

FROM dbo.wminv war

JOIN (select * from edw.dimproduct dp where skeyiscurrent = 1) dp on war.sku = dp.sku

--WHERE dp.SKU = '20905580'

GROUP BY case when Country = 'US' then 'USA' when country = 'UK' then 'GBR' when country = 'MX' then 'MEX' when country = 'CA' then 'CAN' else Country end
, dp.ProductSKey

--order by 2, 1
),

StoreInv as

(
SELECT
case when physicalcountry = 'PRI' then 'USA' when physicalcountry <> 'PRI' then physicalcountry end PhysicalCountry
--ds.physicalcountry
, dp.ProductSKey
, sum(OnHand) OnHand
, sum(InTransit) InTransit
, sum(Reserved) OnReserveIn
, 0 OnReserveOut
, sum(OnHand + InTransit + Reserved) StoreOnHand
, sum(fi.MinQty) MinQty
, sum(fi.MaxQty) MaxQty

FROM edw.dimproduct dp

join edw.factinventorydetailsnapshotdaily fi on fi.productskey = dp.productskey

join edw.dimcalendar dc on dc.calendardateskey = fi.transactiondateskey
	and cast(calendardate as date) = cast(getdate() - 3 as date)

join edw.dimstore ds on ds.storeskey = fi.storeskey

WHERE 1 = 1
	and OperationsClass <> 'Warehouse'
	--and sku = '20905548'

GROUP BY case when physicalcountry = 'PRI' then 'USA' when physicalcountry <> 'PRI' then physicalcountry end
--, physicalcountry
, dp.ProductSKey
),

HistSales as (select --ds.StoreID
--p.producthierarchyskey --currentproducthierarchyskey <-- connect to dimproduct and also connect physicalcountry
ds.physicalcountry
, ph.LVL30Name
, ph.LVL40Name
, ph.LVL50Name
, ph.LVL60Name
, ph.LVL70Name
--, p.productskey
, sum(netsale) Revenue
, sum(s.Quantity) Units
, sum(s.NetCost) COGS
, sum(s.GrossProfit) GP
, coalesce(sum(s.GrossProfit)/nullif(sum(s.NetSale),0),0) "GP%"
, sum(case when datedifference between -407 and -365 then netsale else 0 end) LYLast6W
, sum(case when datedifference between -364 and -295 then netsale else 0 end) LYNext10W
--, coalesce(sum(case when datedifference between -364 and -295 then netsale else 0 end) / NULLIF(sum(case when datedifference between -407 and -365 then netsale else 0 end),0),0) Build

from edw.dimProduct p

join edw.factsalesdetail s
	on s.productskey = p.productskey
	
join edw.dim_producthierarchy ph on ph.ProductHierarchySKey = p.ProductHierarchySKey

join edw.dimstore ds on ds.storeskey = s.storeskey
	and ds.operationsclass in('In-Store Macys', 'Store', 'DSW - Non POS')

Join Reporting.Calendar c ON c.calendardateskey = s.transactiondateskey
	and c.CalendarDate BETWEEN cast(getdate() - 407 AS date) AND cast(getdate() - 295 AS date)
	--and DayInWeek <= (select DayInWeek from reporting.calendar where calendardate = cast(getdate() - 1 AS DATE) Group by DayInWeek)


where 1 = 1 
	and lvl30Name in  ('apparel', 'headwear', 'fan accessories', 'novelties')

group by --ds.StoreID
--p.producthierarchyskey
ds.physicalcountry
, ph.LVL30Name
, ph.LVL40Name
, ph.LVL50Name
, ph.LVL60Name
, ph.LVL70Name
--, p.productskey
),

RecentSales as (select --ds.StoreID
ph.producthierarchyskey
, ds.physicalcountry
, p.ProductSKey
, sum(Quantity) UnitsSold

from edw.dimProduct p

join edw.factsalesdetail s
	on s.productskey = p.productskey
	
join edw.dim_producthierarchy ph on ph.ProductHierarchySKey = p.CurrentProductHierarchySKey

Join Reporting.Calendar c
	ON c.calendardateskey = s.transactiondateskey
	and c.CalendarDate BETWEEN cast(getdate() - 42 AS date) AND cast(getdate() - 1 AS date)
	--and DayInWeek <= (select DayInWeek from reporting.calendar where calendardate = cast(getdate() - 1 AS DATE) Group by DayInWeek)

join edw.dimstore ds on ds.storeskey = s.storeskey
	and ds.operationsclass in('In-Store Macys', 'Store', 'DSW - Non POS')

WHERE 1 = 1 
	and lvl30Name in  ('apparel', 'headwear', 'fan accessories', 'novelties')
	--and p.productskey = '3629558'

group by --ds.StoreID
ph.producthierarchyskey
, ds.physicalcountry
, p.ProductSKey
),


OnOrder as (SELECT dp.ProductSKey
, ds.PhysicalCountry
, sum(fpo.Orderedqty) OrderQty37
, sum(fpo.Receivedqty) ReceivedQty
, sum(fpo.openorderqty) OpenOrderQty
--, sum(fpo.OrderedCost) OrderCost
--, sum(fpo.ReceivedCost) ReceivedCost
--, sum(fpo.orderedCost - fpo.receivedCost) OpenOrderCost
--, sum(fpo.OrderedRetail) OrderRetail
--, sum(fpo.ReceivedRetail) ReceivedRetail
--, sum(fpo.orderedRetail - fpo.receivedRetail) OpenOrderRetail


FROM dbo.onorder fpo

JOIN (select * from edw.dimproduct where skeyiscurrent = 1) dp ON dp.sku = fpo.sku

JOIN (select * from edw.dimstore where skeyiscurrent = 1) ds on ds.storeid = fpo.storeid

JOIN reporting.Calendar c ON c.CalendarDate = fpo.headerdeliverydate
	AND c.fiscalyearID = 2023
	AND c.weekdifference <=10

WHERE 1=1
	--and fpo.Received = 0
	AND fpo.orderedqty > 0

GROUP BY dp.ProductSKey
, ds.PhysicalCountry
),


--DailyInv as (SELECT cast(calendardate as date) InvDate
--, ds.PhysicalCountry
--, p.ProductSKey
--, ds.StoreSKey
--, sum(OnHand) OnHand

--FROM edw.dimproduct p

--JOIN edw.factInventoryDetailSnapshotDaily i on i.ProductSKey = p.ProductSKey

--JOIN edw.dimstore ds on i.storeskey = ds.storeskey

--JOIN Reporting.Calendar c
    --ON c.calendardateSKey = i.transactiondateskey
    --and c.CalendarDate between cast(getdate() - 43 as date) and cast(getdate() - 1 AS date)

--group by c.calendardate
--, ds.PhysicalCountry
--, p.ProductSKey
--, ds.StoreSKey
--),


DailyInv as (select ds.PhysicalCountry
, c.CalendarDate
, p.ProductSKey
--, p.sku
, sum(OnHand) OnHand

from edw.DimProduct p

join edw.factInventoryDetailSnapshotDaily oh on p.ProductSkey = oh.ProductSKey

JOIN edw.dimStore ds ON ds.StoreSkey = oh.StoreSkey
    AND ds.StoreOpenFlag = 'Open'

join reporting.calendar c on c.CalendarDateSKey = oh.TransactionDateSKey
    and c.CalendarDate >= cast(getdate() - 42 as date)

WHERE 1=1 
    and ds.PhysicalCountry in('USA','PRI','CAN','GBR')
    AND ds.operationsclass in('In-Store Macys', 'Store', 'DSW - Non POS')

group by ds.PhysicalCountry
, c.CalendarDate
, p.ProductSKey
--, p.sku
),

AvlblDays as(select --i.StoreID
case when physicalcountry = 'PRI' then 'USA' when physicalcountry <> 'PRI' then physicalcountry end PhysicalCountry
--, i.SKU
--, i.Size
, i.productskey
--, i.physicalcountry
, count(distinct CalendarDate) AvailDays
--, coalesce(datediff(day,ss.MinDate,cast(getdate() - 1 as date)),43) CalcAvlbl
--, count(transactiondateskey)  AvlblDays

FROM DailyInv i

where 1 = 1
	and OnHand > 0

GROUP BY --i.StoreID
case when physicalcountry = 'PRI' then 'USA' when physicalcountry <> 'PRI' then physicalcountry end
--, i.SKU
, i.productskey
--, i.physicalcountry
),

--select * from avlbldays where sku = '20905548'

Calc as (select a.productskey
, a.PhysicalCountry
, UnitsSold
, Availdays
, 7 * (sum(UnitsSold) / sum(AvailDays))  AvgSalesLast6W

from AvlblDays a

JOIN RecentSales r on a.productskey = r.productskey
		and a.PhysicalCountry = r.PhysicalCountry

Group by a.productskey
, a.PhysicalCountry
, UnitsSold
, AvailDays)

----------------------------------------------------------------------------------------------------------------------------------------------,



--final as(
SELECT case when sto.PhysicalCountry = 'PRI' then 'USA' else sto.PhysicalCountry end PhysicalCountry
--, p.SKU
, p.ProductName
, ph.Lvl30Name Division
, ph.LVL40Name SubDivision
, ph.LVL50Name Department
, ph.LVL60Name SubDepartment
, ph.LVL70Name Class
, p.Team
, sum(war.OnHand) WSInv
, sum(sto.StoreOnHand) StoreInv
, sum(ord.openorderqty) OpenOrderQty
, sum(ad.AvailDays) AvailableDays
, coalesce(sum(LYNext10W / NULLIF(LYLast6W,0)),0) Build
, cal.AvgSalesLast6W
, ((sum(rec.UnitsSold) / sum(ad.AvailDays)) * 7) * (coalesce(sum(LYNext10W / NULLIF(LYLast6W,0)),0)) AvgSalesNext10W
, sum(war.OnHand) / ((NULLIF(AvgSalesLast6W,0)) * NULLIF(coalesce(sum(LYNext10W / NULLIF(LYLast6W,0)),0),0)) WOS
, (sum(sto.StoreOnHand)) / ((NULLIF(AvgSalesLast6W,0)) * NULLIF(coalesce(sum(LYNext10W / NULLIF(LYLast6W,0)),0),0)) StoreWOS
, (sum(ord.openorderqty)) / ((NULLIF(AvgSalesLast6W,0)) * NULLIF(coalesce(sum(LYNext10W / NULLIF(LYLast6W,0)),0),0)) OnOrderWOS
, (sum(ord.openorderqty) + sum(war.OnHand)) / ((NULLIF(AvgSalesLast6W,0)) * NULLIF(coalesce(sum(LYNext10W / NULLIF(LYLast6W,0)),0),0)) OHOOWOS 
, (sum(war.OnHand) + sum(ord.openorderqty) + sum(sto.StoreOnHand)) / ((NULLIF(AvgSalesLast6W,0)) * NULLIF(coalesce(sum(LYNext10W / NULLIF(LYLast6W,0)),0),0)) StoreOHOOWOS

FROM edw.dimproduct p

JOIN StoreInv sto on sto.productskey = p.productskey
	and sto.PhysicalCountry is not null

LEFT JOIN WHSInv war on war.productskey = p.productskey
	AND war.physicalcountry = sto.PhysicalCountry

LEFT JOIN AvlblDays ad on p.ProductSkey = ad.ProductSKey
	and sto.PhysicalCountry = ad.PhysicalCountry

LEFT JOIN RecentSales rec on rec.ProductSKey = p.ProductSKey
	and rec.PhysicalCountry = sto.PhysicalCountry

JOIN edw.dim_producthierarchy ph on ph.ProductHierarchySKey = p.CurrentProductHierarchySKey

LEFT JOIN HistSales his on his.Lvl30Name = ph.Lvl30Name and his.Lvl40Name = ph.Lvl40Name and his.Lvl50Name = ph.Lvl50Name and his.Lvl60Name = ph.Lvl60Name and his.Lvl70Name = ph.Lvl70Name
	--his.currentproducthierarchyskey = p.currentproducthierarchyskey
	and his.PhysicalCountry = case when sto.PhysicalCountry = 'GBR' then 'USA' else sto.PhysicalCountry end

LEFT JOIN OnOrder ord on ord.ProductSKey = sto.ProductSKey
	and sto.PhysicalCountry = ord.PhysicalCountry

LEFT JOIN Calc cal on ord.productskey = cal.productskey
	and cal.physicalcountry = ord.physicalcountry

WHERE 1 = 1
--, ph.Lvl30Name Division
--, ph.LVL40Name SubDivision
--, ph.LVL50Name Department
--, ph.LVL60Name SubDepartment
--, ph.LVL70Name Class

GROUP BY case when sto.PhysicalCountry = 'PRI' then 'USA' else sto.PhysicalCountry end
--, p.SKU
, p.ProductName
, ph.LVL30Name
, ph.LVL40Name
, ph.LVL50Name
, ph.LVL60Name
, ph.LVL70Name
, p.Team
, cal.AvgSalesLast6W

ORDER BY 17 desc
--)

--select SKU
--, ProductName
--, ProductCategory
--, League
--, ProductDepartment
--, ProductSilhouette
--, Gender
--, Team
--, WSInv
--, StoreInv
--, AvailableDays
--, Build
--, AvgSalesLast6W
--, AvgSalesNext10W
--, WOS
--, OpenOrderQty
--, OnOrderWOS
--, OHOOWOS
--, StoreWOS
--, StoreOHOOWOS
--, StoreOHOOWOS

--from Final

--where physicalcountry = 'USA'