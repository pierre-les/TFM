# Calculates ensemble forecast weights for each algorithm using backcast data.
# Also calculates historical forecast accuracy to be used later as an input for inventory targets calculations.

rm(list=ls())
list.of.packages <- c("data.table", "forecast", "RODBC", "strucchange", "lubridate")

#
library(data.table)
library(tseries)

library(lubridate)
library(forecast)
library(reshape2)
library(RODBC)
library(doParallel)
library(strucchange)

load('Master Tables.Rda')


##Define Ensemble forecast function
##Function returns weights to be used for each algorithm
##Inputs are:
##MAPEs: forecast error of each algorithm
##Perc: Percentage of lower MAPE, used to determine cap to include algorithm in forecast
## i.e. if lowest MAPE is 10% and Perc is 0.5, then cap is 10%*(1+0.5)=15%
##Methods: maximum number of methods to include 

Ensemble=function(MAPEs,Perc,Methods){
  
  MAPEs[is.na(MAPEs)]=2
  Limit=min(MAPEs)*(1+Perc)+.0001
  Weights=Limit-MAPEs
  Limit2=Weights[order(-Weights)][Methods]
  Weights[Weights<Limit2]=0
  Weights[Weights<0]=0
  Weights=Weights/sum(Weights)
  return(Weights)
  
}

####Parameters
###Forecast Accuracy Cap
AccuracyCap=90
###Maximum number of algorthms to include in ensemble forecast
MaxAlgos=3
###Cap MAPE to include algorithms in ensemble forecast
CapMonthlyMAPE=.5
CapWeeklyMAPE=.2
CapDailyMAPE=.2


##District

###Load Backcast Data
load('backcast_District_Monthly.Rda')

###Include last 12 months if present
backcast_val_rentals_District=backcast_val_rentals_District[MonthStartDate>max(backcast_val_rentals_District$MonthStartDate)-months(12)]

###convert backcast table from wide to long format
FcstAcc=melt(backcast_val_rentals_District,id.vars=c('CatClass','DistrictID','MonthStartDate','actuals'),
             variable.name = 'Method',value.name = 'Backcast')

FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])

###compute accuracy for each method
FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
FcstAcc[actuals==0 & Backcast==0,APE:=0]
FcstAcc[actuals==0 & Backcast!=0,APE:=1]
FcstAcc[APE>1,APE:=1]
FcstAcc[,AE:=abs(actuals-Backcast)]
FcstAcc=FcstAcc[!Method %in% c('naive','glmnetDec','randomForest')]
Accuracy_District=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,DistrictID,CC_Loc=paste0(CatClass,"_",DistrictID),Method)]
Accuracy_District[,MAPE:=MAE/MeanActuals]
Accuracy_District[MeanActuals==0 & MAE==0,MAPE:=0]
Accuracy_District[MeanActuals==0 & MAE!=0,MAPE:=1]

##Generate ensemble forecast with Perc=50% and three methods maximum
EnsembleWeights=Accuracy_District[,Weight:=Ensemble(MAPE,CapMonthlyMAPE,MaxAlgos),by=.(CC_Loc)]
EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])

FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','DistrictID','Method'))
FcstAcc[,WeightBcst:=Backcast*Weight]


##Aggregate backcast and floor at zero
FcstAcc=FcstAcc[,.(Backcast=pmax(sum(WeightBcst),0)),by=.(CatClass,DistrictID,Lag,MonthStartDate,actuals)]

#Cap APE at 100%, set APE to 0% when actuals and Backcast are 0, set APe to 100% when actuals is zero and backcast>0
FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
FcstAcc[actuals==0 & Backcast==0,APE:=0]
FcstAcc[actuals==0 & Backcast!=0,APE:=1]
FcstAcc[APE>1,APE:=1]

#Compute ensemble forecast accuracy
FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,DistrictID)]

###Cap accuracy at 90%
FcstAccAgg[,Accuracy:=pmin(Accuracy,AccuracyCap)]

remove(FcstAcc)

save(EnsembleWeights,file='EnsembleWeightsDistrictMonthly.Rda')
save(FcstAccAgg,file='ForecastAccuracyDistrictMonthly.Rda')


###Metro

load('backcast_Metro_Monthly.Rda')

###Include last 12 months if present
backcast_val_rentals_Metro=backcast_val_rentals_Metro[MonthStartDate>max(backcast_val_rentals_Metro$MonthStartDate)-months(12)]

FcstAcc=melt(backcast_val_rentals_Metro,id.vars=c('CatClass','MetroID','MonthStartDate','actuals'),
             variable.name = 'Method',value.name = 'Backcast')

FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])

FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
FcstAcc[actuals==0 & Backcast==0,APE:=0]
FcstAcc[actuals==0 & Backcast!=0,APE:=1]
FcstAcc[APE>1,APE:=1]
FcstAcc[,AE:=abs(actuals-Backcast)]
FcstAcc=FcstAcc[!Method %in% c('naive','glmnetDec','randomForest')]
Accuracy_Metro=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,MetroID,CC_Loc=paste0(CatClass,"_",MetroID),Method)]
Accuracy_Metro[,MAPE:=MAE/MeanActuals]
Accuracy_Metro[MeanActuals==0 & MAE==0,MAPE:=0]
Accuracy_Metro[MeanActuals==0 & MAE!=0,MAPE:=1]


##Generate ensemble forecast with Perc=50% and three methods maximum
EnsembleWeights=Accuracy_Metro[,Weight:=Ensemble(MAPE,CapMonthlyMAPE,MaxAlgos),by=.(CC_Loc)]
EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])

FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','MetroID','Method'))
FcstAcc[,WeightBcst:=Backcast*Weight]

##Aggregate backcast and floor at zero
FcstAcc=FcstAcc[,.(Backcast=pmax(sum(WeightBcst),0)),by=.(CatClass,MetroID,Lag,MonthStartDate,actuals)]

#Cap APE at 100%, set APE to 0% when actuals and Backcast are 0, set APe to 100% when actuals is zero and backcast>0
FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
FcstAcc[actuals==0 & Backcast==0,APE:=0]
FcstAcc[actuals==0 & Backcast!=0,APE:=1]
FcstAcc[APE>1,APE:=1]

#Compute ensemble forecast accuracy
FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,MetroID)]

###Cap accuracy at 90%
FcstAccAgg[,Accuracy:=pmin(Accuracy,AccuracyCap)]

remove(FcstAcc)


save(EnsembleWeights,file='EnsembleWeightsMetroMonthly.Rda')
save(FcstAccAgg,file='ForecastAccuracyMetroMonthly.Rda')


###Local


load('backcast_Local_Monthly.Rda')

###Include last 12 months if present
backcast_val_rentals_Local=backcast_val_rentals_Local[MonthStartDate>max(backcast_val_rentals_Local$MonthStartDate)-months(12)]

FcstAcc=melt(backcast_val_rentals_Local,id.vars=c('CatClass','LocationID','MonthStartDate','actuals'),
             variable.name = 'Method',value.name = 'Backcast')

FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])

FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
FcstAcc[actuals==0 & Backcast==0,APE:=0]
FcstAcc[actuals==0 & Backcast!=0,APE:=1]
FcstAcc[APE>1,APE:=1]
FcstAcc[,AE:=abs(actuals-Backcast)]
FcstAcc=FcstAcc[!Method %in% c('naive','glmnetDec','randomForest')]
Accuracy_Local=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,LocationID,CC_Loc=paste0(CatClass,"_",LocationID),Method)]
Accuracy_Local[,MAPE:=MAE/MeanActuals]
Accuracy_Local[MeanActuals==0 & MAE==0,MAPE:=0]
Accuracy_Local[MeanActuals==0 & MAE!=0,MAPE:=1]


##Generate ensemble forecast with Perc=50% and three methods maximum
EnsembleWeights=Accuracy_Local[,Weight:=Ensemble(MAPE,CapMonthlyMAPE,MaxAlgos),by=.(CC_Loc)]
EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])

FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','LocationID','Method'))
FcstAcc[,WeightBcst:=Backcast*Weight]


##Aggregate backcast and floor at zero
FcstAcc=FcstAcc[,.(Backcast=pmax(sum(WeightBcst),0)),by=.(CatClass,LocationID,Lag,MonthStartDate,actuals)]

#Cap APE at 100%, set APE to 0% when actuals and Backcast are 0, set APe to 100% when actuals is zero and backcast>0
FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
FcstAcc[actuals==0 & Backcast==0,APE:=0]
FcstAcc[actuals==0 & Backcast!=0,APE:=1]
FcstAcc[APE>1,APE:=1]

#Compute ensemble forecast accuracy
FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,LocationID)]

###Cap accuracy at 90%
FcstAccAgg[,Accuracy:=pmin(Accuracy,AccuracyCap)]

remove(FcstAcc)

save(EnsembleWeights,file='EnsembleWeightsLocalMonthly.Rda')
save(FcstAccAgg,file='ForecastAccuracyLocalMonthly.Rda')



###Comment out Weekly and Daily Forecasts 

# 
# 
# 
# ####Weekly
# 
# # load('Accuracy_District_Weekly.Rda')
# # Accuracy_District[,MAPE_BestFit:=NULL]
# # Accuracy_District[,Method_BestFit:=NULL]
# # Accuracy_District=melt(Accuracy_District,id.vars=c('CatClass','DistrictID'),
# #                        variable.name = 'Method',value.name='MAPE')
# # Accuracy_District[,CC_Loc:=paste0(CatClass,"_",DistrictID)]
# 
# load('backcast_District_Weekly.Rda')
# 
# ###Include last 12 months if present
# backcast_val_rentals_District=backcast_val_rentals_District[WeekStartDate>max(backcast_val_rentals_District$WeekStartDate)-months(12)]
# 
# FcstAcc=melt(backcast_val_rentals_District,id.vars=c('CatClass','DistrictID','WeekStartDate','actuals'),
#              variable.name = 'Method',value.name = 'Backcast')
# 
# FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
# FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# FcstAcc[,AE:=abs(actuals-Backcast)]
# FcstAcc=FcstAcc[!Method %in% c('glmnetDec')]
# Accuracy_District=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,DistrictID,CC_Loc=paste0(CatClass,"_",DistrictID),Method)]
# Accuracy_District[,MAPE:=MAE/MeanActuals]
# Accuracy_District[MeanActuals==0 & MAE==0,MAPE:=0]
# Accuracy_District[MeanActuals==0 & MAE!=0,MAPE:=1]
# 
# 
# 
# ##Generate ensemble forecast with Perc=20% and three methods
# EnsembleWeights=Accuracy_District[,Weight:=Ensemble(MAE,CapWeeklyMAPE,MaxAlgos),by=.(CC_Loc)]
# EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','DistrictID','Method'))
# FcstAcc[,WeightBcst:=Backcast*Weight]
# 
# FcstAcc=FcstAcc[,.(Backcast=pmax(sum(WeightBcst),0)),by=.(CatClass,DistrictID,Lag,WeekStartDate,actuals)]
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# 
# FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,DistrictID)]
# 
# remove(FcstAcc)
# 
# 
# save(EnsembleWeights,file='EnsembleWeightsDistrictWeekly.Rda')
# save(FcstAccAgg,file='ForecastAccuracyDistrictWeekly.Rda')
# 
# 
# 
# 
# # load('Accuracy_Metro_Weekly.Rda')
# # Accuracy_Metro[,MAPE_BestFit:=NULL]
# # Accuracy_Metro[,Method_BestFit:=NULL]
# # Accuracy_Metro=melt(Accuracy_Metro,id.vars=c('CatClass','MetroID'),
# #                     variable.name = 'Method',value.name='MAPE')
# # Accuracy_Metro[,CC_Loc:=paste0(CatClass,"_",MetroID)]
# 
# load('backcast_Metro_Weekly.Rda')
# 
# ###Include last 12 months if present
# backcast_val_rentals_Metro=backcast_val_rentals_Metro[WeekStartDate>max(backcast_val_rentals_District$WeekStartDate)-months(12)]
# 
# FcstAcc=melt(backcast_val_rentals_Metro,id.vars=c('CatClass','MetroID','WeekStartDate','actuals'),
#              variable.name = 'Method',value.name = 'Backcast')
# 
# FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
# FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# FcstAcc[,AE:=abs(actuals-Backcast)]
# FcstAcc=FcstAcc[!Method %in% c('glmnetDec')]
# Accuracy_Metro=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,MetroID,CC_Loc=paste0(CatClass,"_",MetroID),Method)]
# Accuracy_Metro[,MAPE:=MAE/MeanActuals]
# Accuracy_Metro[MeanActuals==0 & MAE==0,MAPE:=0]
# Accuracy_Metro[MeanActuals==0 & MAE!=0,MAPE:=1]
# 
# 
# 
# 
# ##Generate ensemble forecast with Perc=20% and three methods
# EnsembleWeights=Accuracy_Metro[,Weight:=Ensemble(MAPE,CapWeeklyMAPE,MaxAlgos),by=.(CC_Loc)]
# EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','MetroID','Method'))
# FcstAcc[,WeightBcst:=Backcast*Weight]
# 
# FcstAcc=FcstAcc[,.(Backcast=pmax(sum(WeightBcst),0)),by=.(CatClass,MetroID,Lag,WeekStartDate,actuals)]
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# 
# FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,MetroID)]
# remove(FcstAcc)
# 
# 
# save(EnsembleWeights,file='EnsembleWeightsMetroWeekly.Rda')
# save(FcstAccAgg,file='ForecastAccuracyMetroWeekly.Rda')
# 
# 
# 
# # 
# # load('Accuracy_Local_Weekly.Rda')
# # Accuracy_Local[,MAPE_BestFit:=NULL]
# # Accuracy_Local[,Method_BestFit:=NULL]
# # Accuracy_Local=melt(Accuracy_Local,id.vars=c('CatClass','LocationID'),
# #                     variable.name = 'Method',value.name='MAPE')
# # Accuracy_Local[,CC_Loc:=paste0(CatClass,"_",LocationID)]
# 
# load('backcast_Local_Weekly.Rda')
# 
# ###Include last 12 months if present
# backcast_val_rentals_Local=backcast_val_rentals_Local[WeekStartDate>max(backcast_val_rentals_Local$WeekStartDate)-months(12)]
# 
# FcstAcc=melt(backcast_val_rentals_Local,id.vars=c('CatClass','LocationID','WeekStartDate','actuals'),
#              variable.name = 'Method',value.name = 'Backcast')
# 
# FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
# FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# FcstAcc[,AE:=abs(actuals-Backcast)]
# 
# FcstAcc=FcstAcc[!Method %in% c('glmnetDec')]
# Accuracy_Local=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,LocationID,CC_Loc=paste0(CatClass,"_",LocationID),Method)]
# Accuracy_Local[,MAPE:=MAE/MeanActuals]
# Accuracy_Local[MeanActuals==0 & MAE==0,MAPE:=0]
# Accuracy_Local[MeanActuals==0 & MAE!=0,MAPE:=1]
# 
# 
# ##Generate ensemble forecast with Perc=20% and three methods
# EnsembleWeights=Accuracy_Local[,Weight:=Ensemble(MAPE,CapWeeklyMAPE,MaxAlgos),by=.(CC_Loc)]
# EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','LocationID','Method'))
# FcstAcc[,WeightBcst:=Backcast*Weight]
# 
# FcstAcc=FcstAcc[,.(Backcast=pmax(sum(WeightBcst),0)),by=.(CatClass,LocationID,Lag,WeekStartDate,actuals)]
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# 
# FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,LocationID)]
# remove(FcstAcc)
# 
# save(EnsembleWeights,file='EnsembleWeightsLocalWeekly.Rda')
# save(FcstAccAgg,file='ForecastAccuracyLocalWeekly.Rda')
# 
# 
# 
# ###DAILY
# 
# # #active_cc=master_cc$CatClass
# # load('Accuracy_District_Daily.Rda')
# # Accuracy_District[,MAPE_BestFit:=NULL]
# # Accuracy_District[,Method_BestFit:=NULL]
# # Accuracy_District=melt(Accuracy_District,id.vars=c('CatClass','DistrictID'),
# #                        variable.name = 'Method',value.name='MAPE')
# # Accuracy_District[,CC_Loc:=paste0(CatClass,"_",DistrictID)]
# 
# load('backcast_District_Daily.Rda')
# 
# ###Include last 12 months if present
# backcast_val_rentals_District=backcast_val_rentals_District[DayDate>max(backcast_val_rentals_District$DayDate)-months(12)]
# 
# 
# FcstAcc=melt(backcast_val_rentals_District,id.vars=c('CatClass','DistrictID','DayDate','actuals'),
#              variable.name = 'Method',value.name = 'Backcast')
# 
# FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
# FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# FcstAcc[,AE:=abs(actuals-Backcast)]
# 
# 
# Accuracy_District=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,DistrictID,CC_Loc=paste0(CatClass,"_",DistrictID),Method)]
# Accuracy_District[,MAPE:=MAE/MeanActuals]
# Accuracy_District[MeanActuals==0 & MAE==0,MAPE:=0]
# Accuracy_District[MeanActuals==0 & MAE!=0,MAPE:=1]
# 
# ##Generate ensemble forecast with Perc=20% and three methods
# EnsembleWeights=Accuracy_District[,Weight:=Ensemble(MAPE,CapDailyMAPE,MaxAlgos),by=.(CC_Loc)]
# EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# EnsembleWeights[is.na(MAPE),MAPE:=100]
# 
# FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','DistrictID','Method'))
# FcstAcc[,WeightBcst:=Backcast*Weight]
# 
# FcstAcc=FcstAcc[,.(Backcast=pmax(round(sum(WeightBcst)),0,0)),by=.(CatClass,DistrictID,Lag,DayDate,actuals)]
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# 
# FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,DistrictID)]
# remove(FcstAcc)
# 
# 
# save(EnsembleWeights,file='EnsembleWeightsDistrictDaily.Rda')
# save(FcstAccAgg,file='ForecastAccuracyDistrictDaily.Rda')
# 
# 
# # 
# # 
# # load('Accuracy_Metro_Daily.Rda')
# # Accuracy_Metro[,MAPE_BestFit:=NULL]
# # Accuracy_Metro[,Method_BestFit:=NULL]
# # Accuracy_Metro=melt(Accuracy_Metro,id.vars=c('CatClass','MetroID'),
# #                     variable.name = 'Method',value.name='MAPE')
# # Accuracy_Metro[,CC_Loc:=paste0(CatClass,"_",MetroID)]
# 
# load('backcast_Metro_Daily.Rda')
# 
# ###Include last 12 months if present
# backcast_val_rentals_Metro=backcast_val_rentals_Metro[DayDate>max(backcast_val_rentals_Metro$DayDate)-months(12)]
# 
# FcstAcc=melt(backcast_val_rentals_Metro,id.vars=c('CatClass','MetroID','DayDate','actuals'),
#              variable.name = 'Method',value.name = 'Backcast')
# 
# FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
# FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# FcstAcc[,AE:=abs(actuals-Backcast)]
# 
# 
# Accuracy_Metro=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,MetroID,CC_Loc=paste0(CatClass,"_",MetroID),Method)]
# Accuracy_Metro[,MAPE:=MAE/MeanActuals]
# Accuracy_Metro[MeanActuals==0 & MAE==0,MAPE:=0]
# Accuracy_Metro[MeanActuals==0 & MAE!=0,MAPE:=1]
# 
# ##Generate ensemble forecast with Perc=20% and three methods
# EnsembleWeights=Accuracy_Metro[,Weight:=Ensemble(MAPE,CapDailyMAPE,MaxAlgos),by=.(CC_Loc)]
# EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# EnsembleWeights[is.na(MAPE),MAPE:=100]
# 
# FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','MetroID','Method'))
# FcstAcc[,WeightBcst:=Backcast*Weight]
# 
# FcstAcc=FcstAcc[,.(Backcast=pmax(round(sum(WeightBcst)),0,0)),by=.(CatClass,MetroID,Lag,DayDate,actuals)]
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# 
# FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,MetroID)]
# remove(FcstAcc)
# 
# 
# save(EnsembleWeights,file='EnsembleWeightsMetroDaily.Rda')
# save(FcstAccAgg,file='ForecastAccuracyMetroDaily.Rda')
# 
# # 
# # 
# # load('Accuracy_Local_Daily.Rda')
# # Accuracy_Local[,MAPE_BestFit:=NULL]
# # Accuracy_Local[,Method_BestFit:=NULL]
# # Accuracy_Local=melt(Accuracy_Local,id.vars=c('CatClass','LocationID'),
# #                     variable.name = 'Method',value.name='MAPE')
# # Accuracy_Local[,CC_Loc:=paste0(CatClass,"_",LocationID)]
# 
# load('backcast_Local_Daily.Rda')
# 
# 
# ###Include last 12 months if present
# backcast_val_rentals_Local=backcast_val_rentals_Local[DayDate>max(backcast_val_rentals_Local$DayDate)-months(12)]
# 
# 
# FcstAcc=melt(backcast_val_rentals_Local,id.vars=c('CatClass','LocationID','DayDate','actuals'),
#              variable.name = 'Method',value.name = 'Backcast')
# 
# FcstAcc$Lag=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][2])
# FcstAcc$Method=apply(FcstAcc[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# FcstAcc[,AE:=abs(actuals-Backcast)]
# 
# 
# Accuracy_Local=FcstAcc[,.(MAE=mean(AE),MeanActuals=mean(actuals)),by=.(CatClass,LocationID,CC_Loc=paste0(CatClass,"_",LocationID),Method)]
# Accuracy_Local[,MAPE:=MAE/MeanActuals]
# Accuracy_Local[MeanActuals==0 & MAE==0,MAPE:=0]
# Accuracy_Local[MeanActuals==0 & MAE!=0,MAPE:=1]
# 
# 
# ##Generate ensemble forecast with Perc=20% and three methods
# EnsembleWeights=Accuracy_Local[,Weight:=Ensemble(MAPE,CapDailyMAPE,MaxAlgos),by=.(CC_Loc)]
# EnsembleWeights$Method=apply(EnsembleWeights[,.(Method)],1,function(x) strsplit(x,"_")[[1]][1])
# EnsembleWeights[is.na(MAPE),MAPE:=100]
# 
# FcstAcc=merge(FcstAcc,EnsembleWeights,by=c('CatClass','LocationID','Method'))
# FcstAcc[,WeightBcst:=Backcast*Weight]
# 
# FcstAcc=FcstAcc[,.(Backcast=pmax(round(sum(WeightBcst)),0,0)),by=.(CatClass,LocationID,Lag,DayDate,actuals)]
# 
# FcstAcc[,APE:=abs(actuals-Backcast)/actuals]
# FcstAcc[actuals==0 & Backcast==0,APE:=0]
# FcstAcc[actuals==0 & Backcast!=0,APE:=1]
# FcstAcc[APE>1,APE:=1]
# 
# FcstAccAgg=FcstAcc[,.(Accuracy=100-100*mean(APE)),by=.(CatClass,LocationID)]
# remove(FcstAcc)
# 
# save(EnsembleWeights,file='EnsembleWeightsLocalDaily.Rda')
# save(FcstAccAgg,file='ForecastAccuracyLocalDaily.Rda')
