rm(list=ls())
setwd("C:/Sri/ForecastandTargets")

list.of.packages <- c("data.table", "forecast", "RODBC", "doParallel", "polynom", "strucchange", "lubridate", "reshape2")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages, repos="http://cran.rstudio.com/")
#
library(data.table)
library(tseries)
library(xlsx)
library(lubridate)
library(forecast)
library(reshape2)
library(RODBC)
library(doParallel)
library(strucchange)
### ODBC Connection
load('Master Tables.Rda')

# subset to front range district
loc600=master_loc[DistrictID=='R6D16']

####Choose month to evaluate
FcstEvalDate='2018-07-01'
Cutoff='2018-04-30'

##District
load('rentals_District_Monthly.Rda')
load(paste0('MonthlyRentalForecastDistrict_Avg',Cutoff,'.Rda'))
forecast_District_MarketAdj=forecast_District_Monthly
load(paste0('MonthlyRentalForecastDistrict_NonAdj',Cutoff,'.Rda'))
forecast_District_NonAdj=forecast_District_Monthly
rentals_District_Monthly[MonthStartDate<=FcstEvalDate & MonthStartDate>as.Date(FcstEvalDate)-months(12),
                         YearlyRentals:=mean(TotalRentals),by=.(CatClass,DistrictID)]
rentals_District_Monthly=rentals_District_Monthly[MonthStartDate==FcstEvalDate & DistrictID %in% loc600$DistrictID 
                                                  & CatClass %in% master_cc[CatClassGroup=='District/Metro']$CatClass]

FcstAccDistrict=merge(rentals_District_Monthly[,.(CatClass,DistrictID,MonthStartDate,TotalRentals,YearlyRentals)],
                      forecast_District_MarketAdj[,.(CatClass,DistrictID,MonthStartDate,Forecast,CatClassGroup)],by=c('CatClass','DistrictID','MonthStartDate'))

FcstAccDistrict=merge(FcstAccDistrict,
                      forecast_District_NonAdj[,.(CatClass,DistrictID,MonthStartDate,Forecast_Unadjusted=Forecast,CatClassGroup)],by=c('CatClass','CatClassGroup','DistrictID','MonthStartDate'))

##Metro
load('rentals_Metro_Monthly.Rda')
load(paste0('MonthlyRentalForecastMetro_Avg',Cutoff,'.Rda'))
forecast_Metro_MarketAdj=forecast_Metro_Monthly
load(paste0('MonthlyRentalForecastMetro_NonAdj',Cutoff,'.Rda'))
forecast_Metro_NonAdj=forecast_Metro_Monthly
rentals_Metro_Monthly[MonthStartDate<=FcstEvalDate & MonthStartDate>as.Date(FcstEvalDate)-months(12),
                         YearlyRentals:=mean(TotalRentals),by=.(CatClass,MetroID)]
rentals_Metro_Monthly=rentals_Metro_Monthly[MonthStartDate==FcstEvalDate & MetroID %in% loc600$MetroID 
                                                  & CatClass %in% master_cc[CatClassGroup %in% c('Metro/Metro','Metro/Local')]$CatClass]


FcstAccMetro=merge(rentals_Metro_Monthly[,.(CatClass,MetroID,MonthStartDate,TotalRentals,YearlyRentals)],
                      forecast_Metro_MarketAdj[,.(CatClass,MetroID,MonthStartDate,Forecast,CatClassGroup)],by=c('CatClass','MetroID','MonthStartDate'))

FcstAccMetro=merge(FcstAccMetro,
                      forecast_Metro_NonAdj[,.(CatClass,MetroID,MonthStartDate,Forecast_Unadjusted=Forecast,CatClassGroup)],by=c('CatClass','CatClassGroup','MetroID','MonthStartDate'))


##Local
load('rentals_Local_Monthly.Rda')
load(paste0('MonthlyRentalForecastLocal_Avg',Cutoff,'.Rda'))
forecast_Local_MarketAdj=forecast_Local_Monthly
load(paste0('MonthlyRentalForecastLocal_NonAdj',Cutoff,'.Rda'))
forecast_Local_NonAdj=forecast_Local_Monthly
rentals_Local_Monthly[MonthStartDate<=FcstEvalDate & MonthStartDate>as.Date(FcstEvalDate)-months(12),
                         YearlyRentals:=mean(TotalRentals),by=.(CatClass,LocationID)]
rentals_Local_Monthly=rentals_Local_Monthly[MonthStartDate==FcstEvalDate & LocationID %in% loc600$LocationID 
                                            & CatClass %in% master_cc[CatClassGroup %in% c('Local/Local')]$CatClass]


FcstAccLocal=merge(rentals_Local_Monthly[,.(CatClass,LocationID,MonthStartDate,TotalRentals,YearlyRentals)],
                   forecast_Local_MarketAdj[,.(CatClass,LocationID,MonthStartDate,Forecast,CatClassGroup)],by=c('CatClass','LocationID','MonthStartDate'))

FcstAccLocal=merge(FcstAccLocal,
                   forecast_Local_NonAdj[,.(CatClass,LocationID,MonthStartDate,Forecast_Unadjusted=Forecast,CatClassGroup)],by=c('CatClass','CatClassGroup','LocationID','MonthStartDate'))

FcstAcc=rbind(FcstAccDistrict,FcstAccMetro,FcstAccLocal,fill=TRUE)

FcstAcc[,APE:=abs(TotalRentals-Forecast)/TotalRentals]
FcstAcc[TotalRentals==0 & Forecast==0,APE:=0]
FcstAcc[TotalRentals==0 & Forecast!=0,APE:=1]
FcstAcc[,APE:=pmin(APE,1)]

FcstAcc[,MarketBias:=(Forecast-Forecast_Unadjusted)/Forecast_Unadjusted]
FcstAcc[Forecast==0 & Forecast_Unadjusted==0,MarketBias:=0]
FcstAcc[Forecast!=0 & Forecast_Unadjusted==0,MarketBias:=1]
FcstAcc[,MarketBias:=pmin(MarketBias,1)]
FcstAcc[,MarketBias:=pmax(MarketBias,-1)]


FcstAccAll=FcstAcc[,.(CatClass='All',ForecastAccuracy=1-weighted.mean(APE,YearlyRentals),MarketForecastBias=weighted.mean(MarketBias,YearlyRentals),
                      AdjustedForecastBias=weighted.mean(MarketBias,YearlyRentals))]

FcstAccbyCatClass=FcstAcc[,.(ForecastAccuracy=1-weighted.mean(APE,YearlyRentals),MarketForecastBias=weighted.mean(MarketBias,YearlyRentals),
                             AdjustedForecastBias=weighted.mean(MarketBias,YearlyRentals)),
                          by=.(CatClass)]

FcstAccbySC=FcstAcc[,.(ForecastAccuracy=1-weighted.mean(APE,YearlyRentals),MarketForecastBias=weighted.mean(MarketBias,YearlyRentals),
                       AdjustedForecastBias=weighted.mean(MarketBias,YearlyRentals)),
                          by=.(CatClass=CatClassGroup)]


wb=createWorkbook()
pct <- CellStyle(wb, dataFormat=DataFormat("0.0%"))
dfColIndex <- rep(list(pct),3)
names(dfColIndex) <- 2:4

sheet1=createSheet(wb,sheetName='Metrics All')
addDataFrame(FcstAccAll, sheet1,row.names = FALSE,colStyle = dfColIndex)

sheet2=createSheet(wb,sheetName='Metrics by CatClass')
addDataFrame(FcstAccbyCatClass, sheet2,row.names = FALSE,colStyle = dfColIndex)

sheet3=createSheet(wb,sheetName='Metrics by Supply Chain')
addDataFrame(FcstAccbySC, sheet3,row.names = FALSE,colStyle = dfColIndex)


saveWorkbook(wb, file=paste0("TFM Metrics ",FcstEvalDate,".xlsx"))