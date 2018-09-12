# Generates the monthly on rent forecast using ensemble forecast for 
# active cat-classes at District, Metro and Local levels. 

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

####Parameters
MarketGrowth=.18
ForecastHorizon=15


##############################################################
####District Forecast

#read historical rentals Rda file, ensemble forecast weights, and historical forecast accuracy
load('rentals_District_Monthly.Rda')
load('EnsembleWeightsDistrictMonthly.Rda')
load('ForecastAccuracyDistrictMonthly.Rda')

active_cc=unique(FcstAccAgg[CatClass %in% master_cc[CatClassGroup=='District/Metro']$CatClass]$CatClass)
active_loc=unique(FcstAccAgg$DistrictID)

rentals_District=rentals_District_Monthly[CatClass %in% active_cc]
remove(rentals_District_Monthly)
rentals_District[,CC_Loc:=paste(CatClass, DistrictID, sep = "_")]
cc_loc = unique(rentals_District[,CC_Loc])

MaxMonth=max(rentals_District$MonthStartDate)
##Order rentals_District table
setkey(rentals_District,CC_Loc,MonthStartDate)

##Remove catclass-locations with zero rentals in the last 365 days
rentals_District[,`:=`(CatClass=NULL,DistrictID=NULL)]
rentals_District[MonthStartDate>=(MaxMonth-365),SumRentals:=sum(TotalRentals),by=CC_Loc]
rentals_District[,SumRentals:=max(SumRentals,na.rm=TRUE),by=CC_Loc]
rentals_District=rentals_District[SumRentals>0]
rentals_District[,`:=`(SumRentals=NULL)]

cc_loc_null=cc_loc[!cc_loc %in% rentals_District$CC_Loc]
cc_loc = unique(rentals_District[,CC_Loc])

###Parallel processing to generate forecast

source("bestfit_20180506_UR.R")
clus = makeCluster(detectCores() - 1)
#clusterExport(cl = clus, list("fcast_UR"))
registerDoParallel(clus)
start_time = Sys.time()
forecast_District_Monthly =foreach(i = 1:length(cc_loc),.combine=rbind,.packages = list.of.packages,.inorder=FALSE,.errorhandling = 'stop') %dopar% {
  
  ### x is time series history for item i
  x=ts(rentals_District[CC_Loc==cc_loc[i]]$TotalRentals,frequency=12)
  ##xreg: total rentals for region 600, used as an input for the thetaDec model
  xreg=rentals_District[substr(CC_Loc,1,8)==substr(cc_loc[i],1,8),.(TotalRentals=sum(TotalRentals)),by=.(substr(CC_Loc,1,8),MonthStartDate)]$TotalRentals
  
  ##ensemble forecast methods used
  EnsMethods=EnsembleWeights[CC_Loc==cc_loc[i]]
  
  ##Forecast with 18% market growth input, 15-month horizon forecast
  Forecast=ensemble_forecast(x,xreg,EnsMethods,MarketGrowth,ForecastHorizon)
  
    
    ##Add historical accuracy to output
  Acc=ifelse(length(FcstAccAgg[paste0(CatClass,"_",DistrictID)==cc_loc[i]]$Accuracy)==0,100,
             FcstAccAgg[paste0(CatClass,"_",DistrictID)==cc_loc[i]]$Accuracy)
  
  fcast=data.table(CatClass = strsplit(cc_loc[i], "_")[[1]][1],
                   DistrictID = strsplit(cc_loc[i], "_")[[1]][2],
                   MonthStartDate = MaxMonth + months(1:ForecastHorizon),
                   Forecast = Forecast,
                   Accuracy=Acc,
                   Bias = 0,
                   Fcst_Method = 'Ens') 
  
}


Sys.time() - start_time
#
closeAllConnections()
#

remove(rentals_District)


DateTime = strftime(Sys.time(),'%Y %b-%d %H:%m:%S')

forecast_District_Monthly = merge(master_calendar,forecast_District_Monthly)
forecast_District_Monthly = merge(master_cc,forecast_District_Monthly,by='CatClass')


forecast_District_Monthly[,':='(DataID = 1:.N,
                                DataDate = DateTime)]


setkey(forecast_District_Monthly,CatClass,DistrictID,MonthStartDate)

save(forecast_District_Monthly,file = "MonthlyRentalForecastDistrict_Avg.Rda")


remove(forecast_District_Monthly)
################################################
###########################################################

#Get Metro Forecast

load('rentals_Metro_Monthly.Rda')
load('EnsembleWeightsMetroMonthly.Rda')
load('ForecastAccuracyMetroMonthly.Rda')

active_cc=unique(FcstAccAgg[CatClass %in% master_cc[CatClassGroup %in% c('Metro/Metro','Metro/Local')]$CatClass]$CatClass)
active_loc=unique(FcstAccAgg$MetroID)

rentals_Metro=rentals_Metro_Monthly[CatClass %in% active_cc]
remove(rentals_Metro_Monthly)
rentals_Metro[,CC_Loc:=paste(CatClass, MetroID, sep = "_")]
cc_loc = unique(rentals_Metro[,CC_Loc])

MaxMonth=max(rentals_Metro$MonthStartDate)
setkey(rentals_Metro,CC_Loc,MonthStartDate)

##Remove catclass-locations with zero rentals in the last 365 days
rentals_Metro[,`:=`(CatClass=NULL,MetroID=NULL)]
rentals_Metro[MonthStartDate>=(MaxMonth-365),SumRentals:=sum(TotalRentals),by=CC_Loc]
rentals_Metro[,SumRentals:=max(SumRentals,na.rm=TRUE),by=CC_Loc]
rentals_Metro=rentals_Metro[SumRentals>0]
rentals_Metro[,`:=`(SumRentals=NULL)]

cc_loc_null=cc_loc[!cc_loc %in% rentals_Metro$CC_Loc]
cc_loc = unique(rentals_Metro[,CC_Loc])


source("bestfit_20180506_UR.R")
clus = makeCluster(detectCores() - 1)
#clusterExport(cl = clus, list("fcast_UR"))
registerDoParallel(clus)
start_time = Sys.time()
forecast_Metro_Monthly =foreach(i = 1:length(cc_loc),.combine=rbind,.packages = list.of.packages,.inorder=FALSE,.errorhandling = 'stop') %dopar% {
  
  ### x is time series history for item i
  x=ts(rentals_Metro[CC_Loc==cc_loc[i]]$TotalRentals,frequency=12)
  ##xreg: total rentals for region 600, used as an input for the thetaDec model
  xreg=rentals_Metro[substr(CC_Loc,1,8)==substr(cc_loc[i],1,8),.(TotalRentals=sum(TotalRentals)),by=.(substr(CC_Loc,1,8),MonthStartDate)]$TotalRentals
  
  ##ensemble forecast methods used
  EnsMethods=EnsembleWeights[CC_Loc==cc_loc[i]]
  
  ##Forecast with 18% market growth input, 15-month horizon forecast
  Forecast=ensemble_forecast(x,xreg,EnsMethods,MarketGrowth,ForecastHorizon)
  
  
  ##Add historical accuracy to output
  Acc=ifelse(length(FcstAccAgg[paste0(CatClass,"_",MetroID)==cc_loc[i]]$Accuracy)==0,100,
             FcstAccAgg[paste0(CatClass,"_",MetroID)==cc_loc[i]]$Accuracy)
  
  fcast=data.table(CatClass = strsplit(cc_loc[i], "_")[[1]][1],
                   MetroID = strsplit(cc_loc[i], "_")[[1]][2],
                   MonthStartDate = MaxMonth + months(1:ForecastHorizon),
                   Forecast = Forecast,
                   Accuracy=Acc,
                   Bias = 0,
                   Fcst_Method = 'Ens') 
  
}


Sys.time() - start_time
#
closeAllConnections()
#

remove(rentals_Metro)


DateTime = strftime(Sys.time(),'%Y %b-%d %H:%m:%S')

forecast_Metro_Monthly = merge(master_calendar,forecast_Metro_Monthly)
forecast_Metro_Monthly = merge(master_cc,forecast_Metro_Monthly,by='CatClass')


forecast_Metro_Monthly[,':='(DataID = 1:.N,
                             DataDate = DateTime)]


setkey(forecast_Metro_Monthly,CatClass,MetroID,MonthStartDate)

save(forecast_Metro_Monthly,file = "MonthlyRentalForecastMetro_Avg.Rda")




remove(forecast_Metro_Monthly)


################################################
###########################################################

#Get Local Forecast

load('rentals_Local_Monthly.Rda')
load('EnsembleWeightsLocalMonthly.Rda')
load('ForecastAccuracyLocalMonthly.Rda')

active_cc=unique(FcstAccAgg[CatClass %in% master_cc[CatClassGroup %in% c('Local/Local','Local/Local')]$CatClass]$CatClass)
active_loc=unique(FcstAccAgg$LocationID)

rentals_Local=rentals_Local_Monthly[CatClass %in% active_cc]
remove(rentals_Local_Monthly)
rentals_Local[,CC_Loc:=paste(CatClass, LocationID, sep = "_")]
cc_loc = unique(rentals_Local[,CC_Loc])

MaxMonth=max(rentals_Local$MonthStartDate)
setkey(rentals_Local,CC_Loc,MonthStartDate)

##Remove catclass-locations with zero rentals in the last 365 days
rentals_Local[,`:=`(CatClass=NULL,LocationID=NULL)]
rentals_Local[MonthStartDate>=(MaxMonth-365),SumRentals:=sum(TotalRentals),by=CC_Loc]
rentals_Local[,SumRentals:=max(SumRentals,na.rm=TRUE),by=CC_Loc]
rentals_Local=rentals_Local[SumRentals>0]
rentals_Local[,`:=`(SumRentals=NULL)]

cc_loc_null=cc_loc[!cc_loc %in% rentals_Local$CC_Loc]
cc_loc = unique(rentals_Local[,CC_Loc])

source("bestfit_20180506_UR.R")
clus = makeCluster(detectCores() - 1)
#clusterExport(cl = clus, list("fcast_UR"))
registerDoParallel(clus)
start_time = Sys.time()
forecast_Local_Monthly =foreach(i = 1:length(cc_loc),.combine=rbind,.packages = list.of.packages,.inorder=FALSE,.errorhandling = 'stop') %dopar% {
  
  ### x is time series history for item i
  x=ts(rentals_Local[CC_Loc==cc_loc[i]]$TotalRentals,frequency=12)
  ##xreg: total rentals for region 600, used as an input for the thetaDec model
  xreg=rentals_Local[substr(CC_Loc,1,8)==substr(cc_loc[i],1,8),.(TotalRentals=sum(TotalRentals)),by=.(substr(CC_Loc,1,8),MonthStartDate)]$TotalRentals
  
  ##ensemble forecast methods used
  EnsMethods=EnsembleWeights[CC_Loc==cc_loc[i]]
  
  ##Forecast with 18% market growth input, 15-month horizon forecast
  Forecast=ensemble_forecast(x,xreg,EnsMethods,MarketGrowth,ForecastHorizon)
  
  
  ##Add historical accuracy to output
  Acc=ifelse(length(FcstAccAgg[paste0(CatClass,"_",LocationID)==cc_loc[i]]$Accuracy)==0,100,
             FcstAccAgg[paste0(CatClass,"_",LocationID)==cc_loc[i]]$Accuracy)
  
  fcast=data.table(CatClass = strsplit(cc_loc[i], "_")[[1]][1],
                   LocationID = strsplit(cc_loc[i], "_")[[1]][2],
                   MonthStartDate = MaxMonth + months(1:ForecastHorizon),
                   Forecast = Forecast,
                   Accuracy=Acc,
                   Bias = 0,
                   Fcst_Method = 'Ens') 
}


Sys.time() - start_time
#
closeAllConnections()
#

remove(rentals_Local)


DateTime = strftime(Sys.time(),'%Y %b-%d %H:%m:%S')

forecast_Local_Monthly = merge(master_calendar,forecast_Local_Monthly)
forecast_Local_Monthly = merge(master_cc,forecast_Local_Monthly,by='CatClass')


forecast_Local_Monthly[,':='(DataID = 1:.N,
                             DataDate = DateTime)]


setkey(forecast_Local_Monthly,CatClass,LocationID,MonthStartDate)

save(forecast_Local_Monthly,file = "MonthlyRentalForecastLocal_Avg.Rda")

remove(forecast_Local_Monthly)
