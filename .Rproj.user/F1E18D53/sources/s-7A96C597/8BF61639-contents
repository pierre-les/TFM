# Creates spreadsheets used to review forecast and inventory targets in S&OP process.

rm(list=ls())
options(java.parameters = "- Xmx1024m")
#
library(data.table)
library(tseries)
library(tidyr)
library(lubridate)
library(forecast)
library(reshape2)
library(RODBC)
library(doParallel)
library(strucchange)
library(xlsx)

load('Master Tables.Rda')
ch=''

loc600=master_loc[RegionID==6]

load('rentals_Local_Monthly.Rda')
###MinHist is oldest month to add to history
MinHist=max(rentals_Local_Monthly$MonthStartDate)-months(17)

rentals_Monthly=rentals_Local_Monthly[MonthStartDate>=MinHist,.(TotalRentalsAvg=round(mean(TotalRentals),3)),by=.(CatClass,Location=LocationID,MonthStartDate)]
remove(rentals_Local_Monthly)

###Prop table contains proportionality to use to disaggregate District and Metro forecasts
###down to the branch level
###Proportinality based on latest month average rentals
Prop=rentals_Monthly[MonthStartDate>=max(rentals_Monthly$MonthStartDate)]
Prop=merge(Prop,loc600[,.(Location,LocationID,MetroID,DistrictID)],by='Location')
Prop=Prop[,.(TotalRentals=sum(TotalRentalsAvg)),by=.(CatClass,Location,LocationID,MetroID,DistrictID)]
Prop[,`:=`(DistrictRentals=sum(TotalRentals)),by=.(CatClass,DistrictID)]
Prop[,`:=`(MetroRentals=sum(TotalRentals)),by=.(CatClass,MetroID)]
Prop[,`:=`(LocalRentals=sum(TotalRentals)),by=.(CatClass,LocationID)]

Prop[,`:=`(LocalProp=TotalRentals/LocalRentals,MetroProp=TotalRentals/MetroRentals,DistrictProp=TotalRentals/DistrictRentals),by=.(CatClass,DistrictID,MetroID,LocationID,Location)]
Prop[,`:=`(DistrictCount=.N),by=.(CatClass,DistrictID)]
Prop[,`:=`(MetroCount=.N),by=.(CatClass,MetroID)]

Prop[LocalRentals==0,LocalProp:=1]
###if no rentals at local and Metro level use 1/Number of Metros
Prop[LocalRentals==0 & MetroRentals==0,MetroProp:=1/MetroCount]
###if no rentals at local and District level use 1/Number of Metros
Prop[LocalRentals==0 & DistrictRentals==0,DistrictProp:=1/DistrictCount]

Prop=unique(Prop)
Prop[,MetroID:=as.character(MetroID)]
Prop[,DistrictID:=as.character(DistrictID)]
Prop[,LocationID:=as.character(LocationID)]



##District
#Monthly
load('TargetsDistrictMonthly_Avg.Rda')

setnames(InventoryTargetsDistrict_Avg,c('Forecast','Inventory','UncInventory'),c('AvgForecast','AvgInventory','AvgUncInventory'))

InventoryTargetsDistrict=merge(InventoryTargetsDistrict_Avg,Prop,by=c('CatClass','DistrictID'),allow.cartesian = TRUE,all.x=TRUE)

# InventoryTargetsDistrict[,`:=`(LocalProp=TotalRentals/LocalRentals,MetroProp=TotalRentals/MetroRentals,DistrictProp=TotalRentals/DistrictRentals),by=.(CatClass,DistrictID,MetroID,LocationID,Location)]
# InventoryTargetsDistrict[,`:=`(DistrictCount=.N),by=.(CatClass,DistrictID)]
# InventoryTargetsDistrict[,`:=`(MetroCount=.N),by=.(CatClass,MetroID)]
# 
# InventoryTargetsDistrict[LocalRentals==0,LocalProp:=1]
# InventoryTargetsDistrict[LocalRentals==0 & MetroRentals==0,MetroProp:=1/MetroCount]
# InventoryTargetsDistrict[LocalRentals==0 & DistrictRentals==0,DistrictProp:=1/DistrictCount]

InventoryTargetsDistrict[,`:=`(AvgForecast=round(AvgForecast*DistrictProp,3),AvgInventory=round(AvgInventory*DistrictProp,0),
                               AvgUncInventory=round(AvgUncInventory*DistrictProp,0))]

InventoryTargetsDistrict=InventoryTargetsDistrict[,.(CatClass,DistrictID,MonthStartDate,AvgForecast,AvgInventory,AvgUncInventory,
                                                              Location,MetroID,LocationID)]

load('TargetsMetroMonthly_Avg.Rda')

setnames(InventoryTargetsMetro_Avg,c('Forecast','Inventory','UncInventory'),c('AvgForecast','AvgInventory','AvgUncInventory'))

InventoryTargetsMetro=merge(InventoryTargetsMetro_Avg,Prop,by=c('CatClass','MetroID'),allow.cartesian = TRUE,all.x=TRUE)

# InventoryTargetsMetro[,`:=`(LocalProp=TotalRentals/LocalRentals,MetroProp=TotalRentals/MetroRentals,DistrictProp=TotalRentals/DistrictRentals),by=.(CatClass,DistrictID,MetroID,LocationID,Location)]
# InventoryTargetsMetro[,`:=`(DistrictCount=.N),by=.(CatClass,DistrictID)]
# InventoryTargetsMetro[,`:=`(MetroCount=.N),by=.(CatClass,MetroID)]
# 
# InventoryTargetsMetro[LocalRentals==0,LocalProp:=1]
# InventoryTargetsMetro[LocalRentals==0 & MetroRentals==0,MetroProp:=1/MetroCount]
# InventoryTargetsMetro[LocalRentals==0 & DistrictRentals==0,DistrictProp:=1/DistrictCount]

InventoryTargetsMetro[,`:=`(AvgForecast=round(AvgForecast*MetroProp,3),AvgInventory=round(AvgInventory*MetroProp,0),
                            AvgUncInventory=round(AvgUncInventory*MetroProp,0))]

InventoryTargetsMetro=InventoryTargetsMetro[!is.na(MetroID),.(CatClass,MetroID,MonthStartDate,AvgForecast,AvgInventory,AvgUncInventory,
                                                              Location,DistrictID,LocationID)]

#Local

load('TargetsLocalMonthly_Avg.Rda')

setnames(InventoryTargetsLocal_Avg,c('Forecast','Inventory','UncInventory'),c('AvgForecast','AvgInventory','AvgUncInventory'))
InventoryTargetsLocal_Avg[,LocationID:=as.character(LocationID)]

InventoryTargetsLocal=merge(InventoryTargetsLocal_Avg,loc600[,.(LocationID=as.character(LocationID),Location,MetroID=as.character(MetroID),DistrictID=as.character(DistrictID))],by='LocationID')

InventoryTargetsLocal=InventoryTargetsLocal[!is.na(LocationID),.(CatClass,LocationID,MonthStartDate,AvgForecast,AvgInventory,AvgUncInventory,
                                               Location,MetroID,DistrictID)]

#Select history
##Include last 18 months
MaxHist=floor_date(min(InventoryTargetsDistrict_Avg$MonthStartDate),unit='month')-months(1)
MinHist=MaxHist-months(17)
rentals_Monthly=rentals_Monthly[MonthStartDate>=MinHist & MonthStartDate<=MaxHist]

##Bind All

DistrictCCs=unique(InventoryTargetsDistrict$CatClass)
MetroCCs=unique(InventoryTargetsMetro$CatClass)
LocalCCs=unique(InventoryTargetsLocal$CatClass)

MetroCCs[MetroCCs %in% DistrictCCs]
DistrictCCs[DistrictCCs %in% MetroCCs]

InventoryFull=rbind(InventoryTargetsDistrict[!CatClass %in% MetroCCs],InventoryTargetsMetro,InventoryTargetsLocal)
InventoryFull=InventoryFull[!is.na(Location)]
###Cap time ut at 90.9%
InventoryFull[,TimeUt:=pmin(0.909,AvgForecast/AvgInventory)]
InventoryFull[AvgInventory==0 & AvgForecast!=0,TimeUt:=.909]
InventoryFull[AvgInventory==0 & AvgForecast==0,TimeUt:=0]

rentals_Monthly=rentals_Monthly[paste0(CatClass,"_",Location) %in% paste0(InventoryFull$CatClass,"_",InventoryFull$Location)]
rentals_Monthly=merge(rentals_Monthly,loc600[,.(Location,DistrictID=as.character(DistrictID),MetroID=as.character(MetroID),LocationID=as.character(LocationID))],by=c('Location'))

InventoryFull=merge(InventoryFull,rentals_Monthly,by=c('CatClass','Location','LocationID','DistrictID','MetroID','MonthStartDate'),all=TRUE)
InventoryFull=merge(InventoryFull,loc600[,.(DistrictID=as.character(DistrictID),MetroID=as.character(MetroID),DistrictName,MetroName,Location,LocationID=as.character(LocationID))],by=c('MetroID','DistrictID','LocationID','Location'))
InventoryFull=merge(InventoryFull,master_cc[,.(CatClass,CatClassGroup,AvgOEC)],by='CatClass')

InventoryFull=data.table(gather(InventoryFull,DataType,Quantity,AvgForecast:TotalRentalsAvg))

InventoryFull=InventoryFull[,.(CatClass,Location,MetroName,DistrictName,AvgOEC,CatClassGroup,
                               Date=MonthStartDate,`Data Type`=DataType,Quantity)]

InventoryFull[`Data Type`=='AvgForecast',`Data Type`:='F']
InventoryFull[`Data Type`=='AvgInventory',`Data Type`:='CI']
InventoryFull[`Data Type`=='AvgUncInventory',`Data Type`:='UI']

InventoryFull[`Data Type`=='TimeUt',`Data Type`:='T']
InventoryFull[`Data Type`=='TotalRentalsAvg',`Data Type`:='H']

InventoryFull=InventoryFull[!is.na(Quantity)]
InventoryFull=InventoryFull[Location %in% loc600[DistrictID=='R6D16']$Location & Date>=max(rentals_Monthly$MonthStartDate)-months(17)]

###Export TFM data sheet
# write.csv(InventoryFull,file='TFM Data.csv')
save(InventoryFull,file=paste0('TFM Run_',Sys.Date(),'.Rda'))

ExportDate=Sys.Date()

##Create excel workbook and add first sheet
wb=createWorkbook()
sheet1=createSheet(wb,sheetName='TFM Data')
addDataFrame(InventoryFull, sheet1,row.names = FALSE)

#### Create TFM adjusted data table

##District
#Monthly
load('TargetsDistrictMonthly_AvgAdjusted.Rda')

setnames(InventoryTargetsDistrict_Avg,c('Forecast','Inventory','UncInventory'),c('AvgForecast','AvgInventory','AvgUncInventory'))

InventoryTargetsDistrict=merge(InventoryTargetsDistrict_Avg,Prop,by=c('CatClass','DistrictID'),allow.cartesian = TRUE,all.x=TRUE)

# InventoryTargetsDistrict[,`:=`(LocalProp=TotalRentals/LocalRentals,MetroProp=TotalRentals/MetroRentals,DistrictProp=TotalRentals/DistrictRentals),by=.(CatClass,DistrictID,MetroID,LocationID,Location)]
# InventoryTargetsDistrict[,`:=`(DistrictCount=.N),by=.(CatClass,DistrictID)]
# InventoryTargetsDistrict[,`:=`(MetroCount=.N),by=.(CatClass,MetroID)]
# 
# InventoryTargetsDistrict[LocalRentals==0,LocalProp:=1]
# InventoryTargetsDistrict[LocalRentals==0 & MetroRentals==0,MetroProp:=1/MetroCount]
# InventoryTargetsDistrict[LocalRentals==0 & DistrictRentals==0,DistrictProp:=1/DistrictCount]

InventoryTargetsDistrict[,`:=`(AvgForecast=round(AvgForecast*DistrictProp,3),AvgInventory=round(AvgInventory*DistrictProp,0),
                               AvgUncInventory=round(AvgUncInventory*DistrictProp,0))]

InventoryTargetsDistrict=InventoryTargetsDistrict[,.(CatClass,DistrictID,MonthStartDate,AvgForecast,AvgInventory,AvgUncInventory,
                                                     Location,MetroID,LocationID)]

load('TargetsMetroMonthly_AvgAdjusted.Rda')

setnames(InventoryTargetsMetro_Avg,c('Forecast','Inventory','UncInventory'),c('AvgForecast','AvgInventory','AvgUncInventory'))

InventoryTargetsMetro=merge(InventoryTargetsMetro_Avg,Prop,by=c('CatClass','MetroID'),allow.cartesian = TRUE,all.x=TRUE)

# InventoryTargetsMetro[,`:=`(LocalProp=TotalRentals/LocalRentals,MetroProp=TotalRentals/MetroRentals,DistrictProp=TotalRentals/DistrictRentals),by=.(CatClass,DistrictID,MetroID,LocationID,Location)]
# InventoryTargetsMetro[,`:=`(DistrictCount=.N),by=.(CatClass,DistrictID)]
# InventoryTargetsMetro[,`:=`(MetroCount=.N),by=.(CatClass,MetroID)]
# 
# InventoryTargetsMetro[LocalRentals==0,LocalProp:=1]
# InventoryTargetsMetro[LocalRentals==0 & MetroRentals==0,MetroProp:=1/MetroCount]
# InventoryTargetsMetro[LocalRentals==0 & DistrictRentals==0,DistrictProp:=1/DistrictCount]

InventoryTargetsMetro[,`:=`(AvgForecast=round(AvgForecast*MetroProp,3),AvgInventory=round(AvgInventory*MetroProp,0),
                            AvgUncInventory=round(AvgUncInventory*MetroProp,0))]

InventoryTargetsMetro=InventoryTargetsMetro[!is.na(MetroID),.(CatClass,MetroID,MonthStartDate,AvgForecast,AvgInventory,AvgUncInventory,
                                                              Location,DistrictID,LocationID)]

#Local

load('TargetsLocalMonthly_AvgAdjusted.Rda')

setnames(InventoryTargetsLocal_Avg,c('Forecast','Inventory','UncInventory'),c('AvgForecast','AvgInventory','AvgUncInventory'))
InventoryTargetsLocal_Avg[,LocationID:=as.character(LocationID)]

InventoryTargetsLocal=merge(InventoryTargetsLocal_Avg,loc600[,.(LocationID=as.character(LocationID),Location,MetroID=as.character(MetroID),DistrictID=as.character(DistrictID))],by='LocationID')

InventoryTargetsLocal=InventoryTargetsLocal[!is.na(LocationID),.(CatClass,LocationID,MonthStartDate,AvgForecast,AvgInventory,AvgUncInventory,
                                                                 Location,MetroID,DistrictID)]

##Bind All

DistrictCCs=unique(InventoryTargetsDistrict$CatClass)
MetroCCs=unique(InventoryTargetsMetro$CatClass)
LocalCCs=unique(InventoryTargetsLocal$CatClass)

MetroCCs[MetroCCs %in% DistrictCCs]
DistrictCCs[DistrictCCs %in% MetroCCs]

InventoryFull=rbind(InventoryTargetsDistrict[!CatClass %in% MetroCCs],InventoryTargetsMetro,InventoryTargetsLocal)
InventoryFull=InventoryFull[!is.na(Location)]
###Cap time ut at 90.9%
InventoryFull[,TimeUt:=pmin(0.909,AvgForecast/AvgInventory)]
InventoryFull[AvgInventory==0 & AvgForecast!=0,TimeUt:=.909]
InventoryFull[AvgInventory==0 & AvgForecast==0,TimeUt:=0]

InventoryFull=merge(InventoryFull,rentals_Monthly,by=c('CatClass','Location','LocationID','DistrictID','MetroID','MonthStartDate'),all=TRUE)
InventoryFull=merge(InventoryFull,loc600[,.(DistrictID=as.character(DistrictID),MetroID=as.character(MetroID),DistrictName,MetroName,Location,LocationID=as.character(LocationID))],by=c('MetroID','DistrictID','LocationID','Location'))
InventoryFull=merge(InventoryFull,master_cc[,.(CatClass,CatClassGroup,AvgOEC)],by='CatClass')

InventoryFull=data.table(gather(InventoryFull,DataType,Quantity,AvgForecast:TotalRentalsAvg))

InventoryFull=InventoryFull[,.(CatClass,Location,MetroName,DistrictName,AvgOEC,CatClassGroup,
                               Date=MonthStartDate,`Data Type`=DataType,Quantity)]

InventoryFull[`Data Type`=='AvgForecast',`Data Type`:='F']
InventoryFull[`Data Type`=='AvgInventory',`Data Type`:='CI']
InventoryFull[`Data Type`=='AvgUncInventory',`Data Type`:='UI']

InventoryFull[`Data Type`=='TimeUt',`Data Type`:='T']
InventoryFull[`Data Type`=='TotalRentalsAvg',`Data Type`:='H']

InventoryFull=InventoryFull[!is.na(Quantity)]
InventoryFull=InventoryFull[Location %in% loc600[DistrictID=='R6D16']$Location & Date>=max(rentals_Monthly$MonthStartDate)-months(17)]

save(InventoryFull,file=paste0('TFM Run_',Sys.Date(),'Adjusted.Rda'))

##add second sheet to excel workbook
sheet2=createSheet(wb,sheetName='TFM Data Adjusted')
addDataFrame(InventoryFull, sheet2,row.names = FALSE)

###Create CM sheet
load('CMDist.Rda')
load('CMMet.Rda')

InvBaseDist=unique(InventoryTargetsDistrict_Avg[,.(CatClass,DistrictID)])
InvBaseMet=unique(InventoryTargetsMetro_Avg[,.(CatClass,MetroID)])
InvBaseLoc=unique(InventoryTargetsLocal_Avg[,.(CatClass,LocationID)])
InvBaseLoc=merge(InvBaseLoc,unique(master_loc[,.(MetroID,LocationID)]))
InvBaseMet=rbind(InvBaseMet,unique(InvBaseLoc[,.(CatClass,MetroID)]))

CMDist=merge(CMDist,InvBaseDist,by=c('CatClass','DistrictID'),all=TRUE)
CMMet=merge(CMMet,InvBaseMet,by=c('CatClass','MetroID'),all=TRUE)

CMDist[is.na(FillRate),FillRate:=.5]
CMMet[is.na(FillRate),FillRate:=.5]
CMDist[is.na(CM),CM:=0]
CMMet[is.na(CM),CM:=0]

CMDistExp=merge(CMDist[,.(CatClass,DistrictID,DistrictCM=CM,FillRate)],
             unique(master_loc[DistrictID=='R6D16',.(DistrictID,DistrictName)]),by='DistrictID')

CMMetExp=merge(CMMet[CatClass %in% master_cc[CatClassGroup %in% c('Metro/Metro','Metro/Local','Local/Local')]$CatClass,.(CatClass,MetroID,MetroCM=CM,FillRate)],
            unique(master_loc[MetroID %in% master_loc[DistrictID=='R6D16']$MetroID,.(DistrictName,MetroID,MetroName)]),by='MetroID')

CMAll=merge(CMDistExp,CMMetExp,by=c('CatClass','DistrictName'),allow.cartesian = TRUE,all=TRUE)
CMAll=merge(CMAll,master_cc[,.(CatClass,CatClassDesc,CatClassGroup)],by='CatClass')
CMAll[CatClassGroup=='District/Metro',FillRate:=FillRate.x]
CMAll[CatClassGroup!='District/Metro',FillRate:=FillRate.y]

CMAll=CMAll[,.(CatClass=as.character(CatClass),CatClassDesc=as.character(CatClassDesc),CatClassGroup=as.character(CatClassGroup),
               DistrictName,MetroID,MetroName,FillRate,DistrictCM,MetroCM)]



# write.csv(CMAll,'CM and Fill Rates.csv',na="")
sheet3=createSheet(wb,sheetName='CM and Fill Rates')

##column styels for excel sheet
pct <- CellStyle(wb, dataFormat=DataFormat("0.0%"))
dfColIndex <- rep(list(pct),3)
names(dfColIndex) <- 7:9

##create third sheet and save xlsx file
addDataFrame(CMAll, sheet3,row.names = FALSE,colStyle = dfColIndex)
saveWorkbook(wb, file=paste0("TFM Data ",ExportDate,".xlsx"))
