# Calculates constrained and unconstrained inventory targets
# taking into consideration overrides made to forecast and/or time ut values as part of the S&OP process.

rm(list=ls())
#
library(data.table)
library(tseries)
library(lubridate)
library(forecast)
library(reshape2)
library(RODBC)
library(tidyr)
library(doParallel)
library(strucchange)
library(xts)

load('Master Tables.Rda')
ch=''

##Create vectors of active cat-classes and locations
active_Local=unique(master_loc[RegionID==6]$LocationID)
active_Metro=unique(master_loc[RegionID==6]$MetroID)
active_District=unique(master_loc[RegionID==6]$DistrictID)
active_cc=master_cc[as.numeric(substr(CatClass,1,3))>=100]$CatClass
District_cc=master_cc[CatClassGroup=='District-Metro' | CatClassGroup=='District/Metro']$CatClass
Metro_cc=master_cc[CatClassGroup=='Metro-Metro' | CatClassGroup=='Metro/Metro' | 
                     CatClassGroup=='Metro-Local' | CatClassGroup=='Metro/Local']$CatClass
Local_cc=master_cc[CatClassGroup=='Local/Local']$CatClass

District_cc=District_cc[District_cc %in% active_cc]
Metro_cc=Metro_cc[Metro_cc %in% active_cc]
Local_cc=Local_cc[Local_cc %in% active_cc]

source('InvTargetsFunc_TMP.R')

###Parameters
OECNA=1.1

####Get adjusted forecasts from csv file
AdjForecast=data.table(read.csv('FCST Overrides.csv',check.names = FALSE))
AdjForecast=data.table(gather(AdjForecast,key=MonthStartDate,value=AdjForecast,-CC))
AdjForecast[,MonthStartDate:=as.Date(as.character(MonthStartDate),format='%m/%d/%Y')]
AdjForecast[,AdjForecast:=as.numeric(AdjForecast)]
AdjForecast$DistrictID='R6D16'
setnames(AdjForecast,'CC','CatClass')


##Get adjusted UT from csv file
AdjTimeUT=data.table(read.csv('TIME UT Overrides.csv',check.names = FALSE))
AdjTimeUT=data.table(gather(AdjTimeUT,key=MonthStartDate,value=AdjTimeUT,-CC))
AdjTimeUT[,MonthStartDate:=as.Date(as.character(MonthStartDate),format='%m/%d/%Y')]
AdjTimeUT[,AdjTimeUT:=as.numeric(gsub('%','',AdjTimeUT))/100]
AdjTimeUT=AdjTimeUT[!is.na(AdjTimeUT)]
AdjTimeUT$DistrictID='R6D16'
setnames(AdjTimeUT,'CC','CatClass')
AdjTimeUT=merge(AdjTimeUT,master_loc[,.(DistrictID,MetroID,LocationID)],by='DistrictID',allow.cartesian = TRUE)

#Re-write forecast tables with adjusted values
load('MonthlyRentalForecastDistrict_Avg.Rda')
forecast_District_Monthly=merge(AdjForecast,forecast_District_Monthly,
                             by=c('CatClass','DistrictID','MonthStartDate'),all.y=TRUE)

forecast_District_Monthly[!is.na(AdjForecast),Forecast:=AdjForecast]
forecast_District_Monthly[,AdjForecast:=NULL]

save(forecast_District_Monthly,file='MonthlyRentalForecastDistrict_AvgAdjusted.Rda')


#Metro
load('MonthlyRentalForecastMetro_Avg.Rda')
forecast_Metro_Monthly=merge(forecast_Metro_Monthly,unique(master_loc[,.(DistrictID,MetroID)]),by='MetroID')
forecast_Metro_Monthly=merge(AdjForecast,forecast_Metro_Monthly,
                       by=c('CatClass','DistrictID','MonthStartDate'),all.y=TRUE)


forecast_Metro_Monthly[,DistrictForecast:=sum(Forecast),by=.(CatClass,DistrictID)]
forecast_Metro_Monthly[,YearlyForecast:=sum(Forecast),by=.(CatClass,MetroID)]
forecast_Metro_Monthly[,Prop:=YearlyForecast/DistrictForecast]

forecast_Metro_Monthly[!is.na(AdjForecast),Forecast:=AdjForecast*Prop]
forecast_Metro_Monthly[,`:=`(AdjForecast=NULL,DistrictID=NULL,DistrictForecast=NULL,YearlyForecast=NULL,Prop=NULL)]

save(forecast_Metro_Monthly,file='MonthlyRentalForecastMetro_AvgAdjusted.Rda')
  
#Local
load('MonthlyRentalForecastLocal_Avg.Rda')
forecast_Local_Monthly=merge(forecast_Local_Monthly,unique(master_loc[,.(DistrictID,LocationID)]),by='LocationID')
forecast_Local_Monthly=merge(AdjForecast,forecast_Local_Monthly,
                             by=c('CatClass','DistrictID','MonthStartDate'),all.y=TRUE)


forecast_Local_Monthly[,DistrictForecast:=sum(Forecast),by=.(CatClass,DistrictID)]
forecast_Local_Monthly[,YearlyForecast:=sum(Forecast),by=.(CatClass,LocationID)]
forecast_Local_Monthly[,Prop:=YearlyForecast/DistrictForecast]

forecast_Local_Monthly[!is.na(AdjForecast),Forecast:=AdjForecast*Prop]
forecast_Local_Monthly[,`:=`(AdjForecast=NULL,DistrictID=NULL,DistrictForecast=NULL,YearlyForecast=NULL,Prop=NULL)]

save(forecast_Local_Monthly,file='MonthlyRentalForecastLocal_AvgAdjusted.Rda')



###District Metro
###Pull transfer cost values for Region 600
cost<- data.table(read.csv("Transfers-Distance-Cost.csv"))
setnames(cost,'Cost','TransCost')
cost=merge(cost,unique(master_loc[,.(DistrictID,DistrictName)]),by.x='DestinationDistrict',by.y='DistrictName')
setnames(cost,'DistrictID','DestinationID')
cost=merge(cost,unique(master_loc[,.(DistrictID,DistrictName)]),by.x='OriginDistrict',by.y='DistrictName')
setnames(cost,'DistrictID','OriginID')
cost[,OriginID:=as.character(OriginID)]
cost[,DestinationID:=as.character(DestinationID)]

InterDistCost=cost

###Initialize empty tables
TargetsDM=NULL
InvChangeDM=NULL
MoveInvDM=NULL
MoveInvDMSOP=NULL

### Get unconstrained inventory
Forecast=getRegInvData('District',active_District,District_cc,ch,OECNA,ForecastInput='Adjusted')
###Shift owned target one month and write new column 'OwnedPrevMonth'
Forecast[,OwnedPrevMonth:=shift(OwnedTarget,1L),by=.(CatClass,Location)]
#### When OwnedPrevMonth is NA, use value from column CurrentOwned
Forecast[is.na(OwnedPrevMonth),OwnedPrevMonth:=as.numeric(CurrentOwned)]
TargetsTMP=Forecast[,.(CatClass,Location,Year,Month,Date,CurrentOwned)]


##For loop calculating constrained inventory month by month
for (i in unique(Forecast$Date)){
  
  TargetsTMPInit=copy(TargetsTMP)
  ####Get  constrained inventory after region In/Out
  InvChangeFunc=getInvChange(TargetsTMP,Forecast,i,OECNA)
  #####InvChangeTMP contains information of which catClasses are added/removed at each location for month i
  InvChangeTMP=InvChangeFunc[[1]]
  #####TargetsTMP contains inventory after applying add/remove logic
  TargetsTMP=InvChangeFunc[[2]]
  
  ###TargetsTransf contains the recommended inventory after adding/removing regional units, and Owned Inventory at the previous month
  TargetsTransf=merge(TargetsTMP,Forecast[,.(CatClass,Location,Date,OwnedPrevMonth)],by=c('CatClass','Location','Date'))
  TargetsTransf[,CurrentOwned:=OwnedPrevMonth]
  
  ###Get constrained inventory after transfers
  MoveInvFunc=getInvMovesbyUnit(TargetsTMP,i,cost,OECNA)
  
  ####Get SOP recommended transfers
  MoveInvSOPTMP=getSOPMovesUnc(TargetsTransf,i,InterDistCost,'District/Metro',OECNA)
  
  
  ####MoveInvTMP contains information of transfers applied to reach inventory targets
  MoveInvTMP=MoveInvFunc[[1]]
  
  ###Assign inventiry values after transfer to TargetsTMP to be used for next iteration of for loop
  TargetsTMP=MoveInvFunc[[2]]
  
  ###Bind tables at the end of each iteration
  TargetsDM=rbind(TargetsDM,TargetsTMP[Date==i])
  InvChangeDM=rbind(InvChangeDM,InvChangeTMP)
  MoveInvDM=rbind(MoveInvDM,MoveInvTMP)
  MoveInvDMSOP=rbind(MoveInvDMSOP,MoveInvSOPTMP)
  
  remove(InvChangeFunc,MoveInvFunc)
  
  
}

save(MoveInvDMSOP,file='SOPRecommTransfDMAdjusted.Rda')

InventoryTargetsDistrict_Avg=merge(Forecast[,.(CatClass,DistrictID=Location,Forecast,UncInventory=OwnedTarget,Accuracy,Date)],
                                   TargetsDM[,.(CatClass,DistrictID=Location,Date,Inventory=CurrentOwned)],
                                   by=c('CatClass','DistrictID','Date'),all=TRUE,allow.cartesian = TRUE)

InventoryTargetsDistrict_Avg[,MonthStartDate:=as.Date(Date)]
InventoryTargetsDistrict_Avg=InventoryTargetsDistrict_Avg[,.(CatClass,DistrictID,MonthStartDate,Forecast,Accuracy,UncInventory,Inventory)]
#InventoryTargetsDistrict_Avg[Forecast>2 & Inventory<(Forecast*1.1),Inventory:=round(Forecast*1.1,0)]

###Appply UT Override to inventory targets
InventoryTargetsDistrict_Avg=merge(unique(AdjTimeUT[,.(CatClass,DistrictID,MonthStartDate,AdjTimeUT)]),InventoryTargetsDistrict_Avg,
                                by=c('CatClass','DistrictID','MonthStartDate'),all.y=TRUE)

InventoryTargetsDistrict_Avg[!is.na(AdjTimeUT),`:=`(UncInventory=round(Forecast/AdjTimeUT),Inventory=round(Forecast/AdjTimeUT))]
InventoryTargetsDistrict_Avg[,AdjTimeUT:=NULL]

save(InventoryTargetsDistrict_Avg,file='TargetsDistrictMonthly_AvgAdjusted.Rda')
save(InventoryTargetsDistrict_Avg,file=paste0('TargetsDistrictMonthly_AvgAdjusted_',Sys.Date(),'.Rda'))
save.image(file='SourceDestDMAdjusted.Rda')
save(InvChangeDM,file='InvChangeDM_Month_AvgAdjusted.Rda')
save(MoveInvDM,file='MoveInvDM_Month_AvgAdjusted.Rda')

remove(Targets,ForecastDistWeekly,ForecastDistrict,CurrentInv,CurrentInvDist,FRDistrict,HistInv,InvChangeDM,MoveInvDM,TargetsDM,
       TargetsTMP,Transfers,TransfersIn,TransfersOut,TargetsReg,InventoryTargetsDistrict_Avg)

###Metro
###Pull transfer cost values for Region 600
cost<- data.table(read.csv("Transfers-Distance-Cost.csv"))
setnames(cost,'Cost','TransCost')
cost=merge(cost,unique(master_loc[,.(DistrictID,DistrictName)]),by.x='DestinationDistrict',by.y='DistrictName')
setnames(cost,'DistrictID','DestinationID')
cost=merge(cost,unique(master_loc[,.(DistrictID,DistrictName)]),by.x='OriginDistrict',by.y='DistrictName')
setnames(cost,'DistrictID','OriginID')

##Merge with Master Location table to include MetroIDs
metcost=data.table(OriginID=rep(active_Metro,each=length(active_Metro)),DestinationID=rep(active_Metro,length(active_Metro)))
metcost=merge(metcost[,.(OriginID,DestinationID)],unique(master_loc[,.(MetroID,DistrictID)]),by.x='OriginID',by.y='MetroID')
setnames(metcost,'DistrictID','OriginDistrict')
metcost=merge(metcost[,.(OriginID,DestinationID,OriginDistrict)],unique(master_loc[,.(MetroID,DistrictID)]),by.x='DestinationID',by.y='MetroID')
setnames(metcost,'DistrictID','DestinationDistrict')
metcost=merge(metcost,cost[,.(OriginID,DestinationID,TransCost)],by.x=c('OriginDistrict','DestinationDistrict'),by.y=c('OriginID','DestinationID'))
metcost[,OriginID:=as.character(OriginID)]
metcost[,DestinationID:=as.character(DestinationID)]

InterDistCost=metcost[OriginDistrict!=DestinationDistrict]

###Initialize empty tables
TargetsMM=NULL
InvChangeMM=NULL
MoveInvMM=NULL
MoveInvMMSOP=NULL

### Get unconstrained inventory
ForecastMetro=getRegInvData('Metro',active_Metro,Metro_cc,ch,OECNA,ForecastInput='Adjusted')
###Shift owned target one month and write new column 'OwnedPrevMonth'
Forecast=ForecastMetro[CatClass %in% master_cc[CatClassGroup=='Metro/Metro']$CatClass]
Forecast[,OwnedPrevMonth:=shift(OwnedTarget,1L),by=.(CatClass,Location)]
Forecast[is.na(OwnedPrevMonth),OwnedPrevMonth:=as.numeric(CurrentOwned)]
#### When OwnedPrevMonth is NA, use value from column CurrentOwned
TargetsTMP=Forecast[,.(CatClass,Location,Year,Month,Date,CurrentOwned)]

##For loop calculating constrained inventory month by month
for (i in unique(Forecast$Date)){
  
  
  TargetsTMPInit=copy(TargetsTMP)
  ####Get  constrained inventory after region In/Out
  InvChangeFunc=getInvChange(TargetsTMP,Forecast,i,OECNA)
  #####InvChangeTMP contains information of which catClasses are added/removed at each location for month i
  InvChangeTMP=InvChangeFunc[[1]]
  #####TargetsTMP contains inventory after applying add/remove logic
  TargetsTMP=InvChangeFunc[[2]]
  
  ###TargetsTransf contains the recommended inventory after adding/removing regional units, and Owned Inventory at the previous month
  TargetsTransf=merge(TargetsTMP,Forecast[,.(CatClass,Location,Date,OwnedPrevMonth)],by=c('CatClass','Location','Date'))
  TargetsTransf[,CurrentOwned:=OwnedPrevMonth]
  
  ###Get constrained inventory after transfers
  MoveInvFunc=getInvMovesbyUnit(TargetsTMP,i,metcost,OECNA)
  ####Get SOP recommended transfers
  MoveInvSOPTMP=getSOPMovesUnc(TargetsTransf,i,InterDistCost,'Metro/Metro',OECNA)
  
  ####MoveInvTMP contains information of transfers applied to reach inventory targets
  MoveInvTMP=MoveInvFunc[[1]]
  ###Assign inventiry values after transfer to TargetsTMP to be used for next iteration of for loop
  TargetsTMP=MoveInvFunc[[2]]
  
  ###Bind tables at the end of each iteration
  TargetsMM=rbind(TargetsMM,TargetsTMP[Date==i])
  InvChangeMM=rbind(InvChangeMM,InvChangeTMP)
  MoveInvMM=rbind(MoveInvMM,MoveInvTMP)
  MoveInvMMSOP=rbind(MoveInvMMSOP,MoveInvSOPTMP)
  
  remove(InvChangeFunc,MoveInvFunc)
  
}

save(MoveInvMMSOP,file='SOPRecommTransfMMAdjusted.Rda')

###Metro/Local
##Initialize empty tables
TargetsML=NULL
InvChangeML=NULL
MoveInvML=NULL
MoveInvDetML=NULL
MoveInvMLSOP=NULL

### Get unconstrained inventory
Forecast=ForecastMetro[CatClass %in% master_cc[CatClassGroup=='Metro/Local']$CatClass]
###Shift owned target one month and write new column 'OwnedPrevMonth'
Forecast[,OwnedPrevMonth:=shift(OwnedTarget,1L),by=.(CatClass,Location)]
#### When OwnedPrevMonth is NA, use value from column CurrentOwned
Forecast[is.na(OwnedPrevMonth),OwnedPrevMonth:=as.numeric(CurrentOwned)]
TargetsTMP=Forecast[,.(CatClass,Location,Year,Month,Date,CurrentOwned)]


##For loop calculating constrained inventory month by month
for (i in unique(Forecast$Date)){
  
  TargetsTMPInit=copy(TargetsTMP)
  ####Get  constrained inventory after region In/Out
  InvChangeFunc=getInvChange(TargetsTMP,Forecast,i,OECNA)
  #####InvChangeTMP contains information of which catClasses are added/removed at each location for month i
  InvChangeTMP=InvChangeFunc[[1]]
  #####TargetsTMP contains inventory after applying add/remove logic
  TargetsTMP=InvChangeFunc[[2]]
  
  ###TargetsTransf contains the recommended inventory after adding/removing regional units, and Owned Inventory at the previous month
  TargetsTransf=merge(TargetsTMP,Forecast[,.(CatClass,Location,Date,OwnedPrevMonth)],by=c('CatClass','Location','Date'))
  TargetsTransf[,CurrentOwned:=OwnedPrevMonth]
  
  ###Get constrained inventory after transfers
  ##Uses different function than Metro/Metro
  ###For Metro/Local, model considers that different cat-classes can be moved from Origin to Destination in one single transfer
  MoveInvFunc=getInvMovesbyOD(TargetsTMP,i,metcost,OECNA)
  ####Get SOP recommended transfers
  MoveInvSOPTMP=getSOPMovesUnc(TargetsTransf,i,InterDistCost,'Metro/Local',OECNA)
  
  ####MoveInvTMP contains information of transfers applied to reach inventory targets
  MoveInvTMP=MoveInvFunc[[1]]
  ###Assign inventory values after transfer to TargetsTMP to be used for next iteration of for loop
  TargetsTMP=MoveInvFunc[[2]]
  ###MoveInvDetTMP contains more detailed info of transfers applied to reach inventory targets
  MoveInvDetTMP=MoveInvFunc[[3]]
  
  ###Bind tables at the end of each iteration
  TargetsML=rbind(TargetsML,TargetsTMP[Date==i])
  InvChangeML=rbind(InvChangeML,InvChangeTMP)
  MoveInvML=rbind(MoveInvML,MoveInvTMP)
  MoveInvDetML=rbind(MoveInvDetML,MoveInvDetTMP)
  MoveInvMLSOP=rbind(MoveInvMLSOP,MoveInvSOPTMP)
  
  
  remove(InvChangeFunc,MoveInvFunc)
  
}

save(MoveInvMLSOP,file='SOPRecommTransfMLAdjusted.Rda')

###Bind Metro/Metro and Metro/Local inventory targets
TargetsMetro=rbind(TargetsMM,TargetsML)

InventoryTargetsMetro_Avg=merge(ForecastMetro[,.(CatClass,MetroID=Location,Forecast,UncInventory=OwnedTarget,Accuracy,Date)],
                                TargetsMetro[,.(CatClass,MetroID=Location,Date,Inventory=CurrentOwned)],
                                by=c('CatClass','MetroID','Date'),all=TRUE,allow.cartesian = TRUE)

InventoryTargetsMetro_Avg[,MonthStartDate:=as.Date(Date)]
InventoryTargetsMetro_Avg=InventoryTargetsMetro_Avg[,.(CatClass,MetroID,MonthStartDate,Forecast,Accuracy,UncInventory,Inventory)]
#InventoryTargetsMetro_Avg[Forecast>2 & Inventory<(Forecast*1.1),Inventory:=round(Forecast*1.1,0)]

###Appply UT Override
InventoryTargetsMetro_Avg=merge(unique(AdjTimeUT[,.(CatClass,MetroID,MonthStartDate,AdjTimeUT)]),InventoryTargetsMetro_Avg,
                                   by=c('CatClass','MetroID','MonthStartDate'),all.y=TRUE)

InventoryTargetsMetro_Avg[!is.na(AdjTimeUT),`:=`(UncInventory=round(Forecast/AdjTimeUT),Inventory=round(Forecast/AdjTimeUT))]
InventoryTargetsMetro_Avg[,AdjTimeUT:=NULL]


save(InventoryTargetsMetro_Avg,file='TargetsMetroMonthly_AvgAdjusted.Rda')
save(InventoryTargetsMetro_Avg,file=paste0('TargetsMetroMonthly_AvgAdjusted_',Sys.Date(),'.Rda'))
save(InvChangeMM,file='InvChangeMM_Month_AvgAdjusted.Rda')
save(InvChangeML,file='InvChangeML_Month_AvgAdjusted.Rda')
save(MoveInvMM,file='MoveInvMM_Month_AvgAdjusted.Rda')
save(MoveInvML,file='MoveInvML_Month_AvgAdjusted.Rda')
save(MoveInvDetML,file='MoveInvDetML_Month_AvgAdjusted.Rda')


remove(Targets,ForecastMetWeekly,ForecastMetro,CurrentInv,CurrentInvMet,FRMetro,HistInv,InvChangeMM,MoveInvMM,TargetsMM,
       InvChangeML,MoveInvML,TargetsML,TargetsTMP,Transfers,TransfersIn,TransfersOut,TargetsReg,TargetsMetro,InventoryTargetsMetro_Avg)


##Local/Local
##No constrained inventory for Local-Local, export only unconstrained inventory targets
Forecast=getRegInvData('Local',active_Local,Local_cc,ch,OECNA,ForecastInput='Adjusted')
TargetsTMP=Forecast[,.(CatClass,Location,Year,Month,Date,CurrentOwned)]
#MaxWeekFcst=as.Date('2018-05-13')


InventoryTargetsLocal_Avg=merge(Forecast[,.(CatClass,LocationID=Location,Forecast,UncInventory=OwnedTarget,Inventory=OwnedTarget,Accuracy,Date)],
                                TargetsTMP[,.(CatClass,LocationID=Location,Date)],
                                by=c('CatClass','LocationID','Date'),all=TRUE,allow.cartesian = TRUE)

InventoryTargetsLocal_Avg[,MonthStartDate:=as.Date(Date)]
InventoryTargetsLocal_Avg=InventoryTargetsLocal_Avg[,.(CatClass,LocationID,MonthStartDate,Forecast,Accuracy,UncInventory,Inventory)]
#InventoryTargetsLocal_Avg[Forecast>2 & Inventory<(Forecast*1.1),Inventory:=round(Forecast*1.1,0)]

###Appply UT Override
InventoryTargetsLocal_Avg=merge(unique(AdjTimeUT[,.(CatClass,LocationID,MonthStartDate,AdjTimeUT)]),InventoryTargetsLocal_Avg,
                                by=c('CatClass','LocationID','MonthStartDate'),all.y=TRUE)

InventoryTargetsLocal_Avg[!is.na(AdjTimeUT),`:=`(UncInventory=round(Forecast/AdjTimeUT),Inventory=round(Forecast/AdjTimeUT))]
InventoryTargetsLocal_Avg[,AdjTimeUT:=NULL]


save(InventoryTargetsLocal_Avg,file='TargetsLocalMonthly_AvgAdjusted.Rda')
save(InventoryTargetsLocal_Avg,file=paste0('TargetsLocalMonthly_AvgAdjusted_',Sys.Date(),'.Rda'))
