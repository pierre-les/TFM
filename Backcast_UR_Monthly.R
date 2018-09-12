# Generates a backcast for the newest months present in rental history
# and not yet included in backcast history.


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
##Read master tables:
## master_cc: list of cat-classes
## master_loc: list of all locations
## master_calendar: fiscal calendar
load('Master Tables.Rda')
#
###########################################################

#Get District Forecast
### Select active cat-classes and locations
active_cc=master_cc[CatClassGroup=='District/Metro']$CatClass
#active_cc='300-2000'
active_loc=unique(loc600$DistrictID)

#read historical rentals Rda file
load('rentals_District_Monthly.Rda')

rentals_District=rentals_District_Monthly[CatClass %in% active_cc & DistrictID %in% active_loc]
remove(rentals_District_Monthly)
rentals_District[,CC_Loc:=paste0(CatClass,"_",DistrictID)]

MaxMonth=max(rentals_District$MonthStartDate)
setkey(rentals_District,CC_Loc,MonthStartDate)

#Remove all cat-class/location pairs with zero rentals in past 365 days
rentals_District[,`:=`(CatClass=NULL,DistrictID=NULL)]
rentals_District[MonthStartDate>=(MaxMonth-365),SumRentals:=sum(TotalRentals),by=CC_Loc]
rentals_District[,SumRentals:=max(SumRentals,na.rm=TRUE),by=CC_Loc]
rentals_District=rentals_District[SumRentals>0]
rentals_District[,`:=`(SumRentals=NULL)]

# cc_loc_null=cc_loc[!cc_loc %in% rentals_District$CC_Loc]
cc_loc = unique(rentals_District[,CC_Loc])

###Read Backcast file to see number of months needed
load('backcast_District_Monthly.Rda')
backcast=backcast_val_rentals_District
remove(backcast_val_rentals_District)

### Parallel processing of backcast logic

source("bestfit_20180506_UR.R")
clus = makeCluster(detectCores() - 1)
#clusterExport(cl = clus, list("fcast_UR"))
registerDoParallel(clus)
start_time = Sys.time()
backcast_val_rentals_District =foreach(i = 1:length(cc_loc),.combine=rbind,.packages = list.of.packages,.inorder=FALSE,.errorhandling = 'stop') %dopar% {
  
  x=ts(rentals_District[CC_Loc==cc_loc[i]]$TotalRentals,frequency=12)
  
  ##xreg: total rentals for region 600, used as an input for the thetaDec model
  xreg=rentals_District[substr(CC_Loc,1,8)==substr(cc_loc[i],1,8),.(TotalRentals=sum(TotalRentals)),by=.(substr(CC_Loc,1,8),MonthStartDate)]$TotalRentals
  
  ####number of periods
  RentalMonths=unique(rentals_District[CC_Loc==cc_loc[i]]$MonthStartDate)
  backcastPeriods=min(6,length(RentalMonths[RentalMonths>max(backcast$MonthStartDate)]))
  
  if (backcastPeriods!=0){
  ResBcast=bestfit(x,test_periods = backcastPeriods,lead_time=3:4,methods=c('tbatsnotrend','arima',
                                                              'thetaDec','stlm'),outliers=TRUE,xreg=xreg)
  
  
  Backcast=data.table(CatClass = strsplit(cc_loc[i],"_")[[1]][1],
                      DistrictID = strsplit(cc_loc[i],"_")[[1]][2],
                      MonthStartDate = seq.Date(MaxMonth- months(ResBcast$Backcast[,.N] - 1),
                                               MaxMonth, by = "month"),
                      ResBcast$Backcast[,c(2:length(ResBcast$Backcast)), with = F],
                      ResBcast$Accuracy[horizon=='All'])
  } else {
    
    Backcast=NULL
  }

  
}


Sys.time() - start_time
#
closeAllConnections()
#

remove(rentals_District)

Index=which(colnames(backcast_val_rentals_District)=='horizon')
# Accuracy_District=unique(backcast_val_rentals_District[,c(1,2,(Index+1):ncol(backcast_val_rentals_District)), with = F])
if (!is.null(backcast_val_rentals_District)) backcast_val_rentals_District=backcast_val_rentals_District[,1:(Index-1)]

backcast_val_rentals_District=rbind(backcast,backcast_val_rentals_District)

###save backcast data to Rda file
save(backcast_val_rentals_District,file='backcast_District_Monthly.Rda')
# save(Accuracy_District,file='Accuracy_District_Monthly.Rda')

remove(backcast_val_rentals_District)
remove(Accuracy_District)

#########################################################################################################################################################
#

#Get Metro Forecast
### Select active cat-classes and locations
active_cc=master_cc[CatClassGroup %in% c('Metro/Metro','Metro/Local')]$CatClass
#active_cc='300-2000'
active_loc=unique(loc600$MetroID)

#read historical rentals Rda file
load('rentals_Metro_Monthly.Rda')

rentals_Metro=rentals_Metro_Monthly[CatClass %in% active_cc & MetroID %in% active_loc]
remove(rentals_Metro_Monthly)
rentals_Metro[,CC_Loc:=paste0(CatClass,"_",MetroID)]

MaxMonth=max(rentals_Metro$MonthStartDate)
setkey(rentals_Metro,CC_Loc,MonthStartDate)

#Remove all cat-class/location pairs with zero rentals in past 365 days
rentals_Metro[,`:=`(CatClass=NULL,MetroID=NULL)]
rentals_Metro[MonthStartDate>=(MaxMonth-365),SumRentals:=sum(TotalRentals),by=CC_Loc]
rentals_Metro[,SumRentals:=max(SumRentals,na.rm=TRUE),by=CC_Loc]
rentals_Metro=rentals_Metro[SumRentals>0]
rentals_Metro[,`:=`(SumRentals=NULL)]

# cc_loc_null=cc_loc[!cc_loc %in% rentals_Metro$CC_Loc]
cc_loc = unique(rentals_Metro[,CC_Loc])

###Read Backcast file to see number of months needed
load('backcast_Metro_Monthly.Rda')
backcast=backcast_val_rentals_Metro
remove(backcast_val_rentals_Metro)

### Parallel processing of backcast logic

source("bestfit_20180506_UR.R")
clus = makeCluster(detectCores() - 1)
#clusterExport(cl = clus, list("fcast_UR"))
registerDoParallel(clus)
start_time = Sys.time()
backcast_val_rentals_Metro =foreach(i = 1:length(cc_loc),.combine=rbind,.packages = list.of.packages,.inorder=FALSE,.errorhandling = 'stop') %dopar% {
  
  x=ts(rentals_Metro[CC_Loc==cc_loc[i]]$TotalRentals,frequency=12)
  ##xreg: total rentals for region 600, used as an input for the thetaDec model
  xreg=rentals_Metro[substr(CC_Loc,1,8)==substr(cc_loc[i],1,8),.(TotalRentals=sum(TotalRentals)),by=.(substr(CC_Loc,1,8),MonthStartDate)]$TotalRentals
  
  ####number of periods
  RentalMonths=unique(rentals_Metro[CC_Loc==cc_loc[i]]$MonthStartDate)
  backcastPeriods=min(6,length(RentalMonths[RentalMonths>max(backcast$MonthStartDate)]))
  
  if (backcastPeriods!=0){
    
  ResBcast=bestfit(x,test_periods = backcastPeriods,lead_time=3:4,methods=c('tbatsnotrend','arima',
                                                              'thetaDec','stlm'),outliers=TRUE,xreg=xreg)
  
  Backcast=data.table(CatClass = strsplit(cc_loc[i],"_")[[1]][1],
                      MetroID = strsplit(cc_loc[i],"_")[[1]][2],
                      MonthStartDate = seq.Date(MaxMonth- months(ResBcast$Backcast[,.N] - 1),
                                                MaxMonth, by = "month"),
                      ResBcast$Backcast[,c(2:length(ResBcast$Backcast)), with = F],
                      ResBcast$Accuracy[horizon=='All'])
  
  } else {
    
    Backcast=NULL
    
  }
  
  
}


Sys.time() - start_time
#
closeAllConnections()
#

remove(rentals_Metro)

Index=which(colnames(backcast_val_rentals_Metro)=='horizon')
# Accuracy_Metro=unique(backcast_val_rentals_Metro[,c(1,2,(Index+1):ncol(backcast_val_rentals_Metro)), with = F])
if (!is.null(backcast_val_rentals_Metro)) backcast_val_rentals_Metro=backcast_val_rentals_Metro[,1:(Index-1)]

backcast_val_rentals_Metro=rbind(backcast,backcast_val_rentals_Metro)

###save backcast data to Rda file
save(backcast_val_rentals_Metro,file='backcast_Metro_Monthly.Rda')
# save(Accuracy_Metro,file='Accuracy_Metro_Monthly.Rda')

remove(backcast_val_rentals_Metro)
remove(Accuracy_Metro)


############################

#Get Local Forecast
### Select active cat-classes and locations
active_cc=master_cc[CatClassGroup %in% c('Local/Local')]$CatClass
#active_cc='300-2000'
active_loc=unique(loc600$LocationID)

#read historical rentals Rda file
load('rentals_Local_Monthly.Rda')

rentals_Local=rentals_Local_Monthly[CatClass %in% active_cc & LocationID %in% active_loc]
remove(rentals_Local_Monthly)
rentals_Local[,CC_Loc:=paste0(CatClass,"_",LocationID)]

MaxMonth=max(rentals_Local$MonthStartDate)
setkey(rentals_Local,CC_Loc,MonthStartDate)

#Remove all cat-class/location pairs with zero rentals in past 365 days
rentals_Local[,`:=`(CatClass=NULL,LocationID=NULL)]
rentals_Local[MonthStartDate>=(MaxMonth-365),SumRentals:=sum(TotalRentals),by=CC_Loc]
rentals_Local[,SumRentals:=max(SumRentals,na.rm=TRUE),by=CC_Loc]
rentals_Local=rentals_Local[SumRentals>0]
rentals_Local[,`:=`(SumRentals=NULL)]

# cc_loc_null=cc_loc[!cc_loc %in% rentals_Local$CC_Loc]
cc_loc = unique(rentals_Local[,CC_Loc])

###Read Backcast file to see number of months needed
load('backcast_Local_Monthly.Rda')
backcast=backcast_val_rentals_Local
remove(backcast_val_rentals_Local)

###
### Parallel processing of backcast logic

source("bestfit_20180506_UR.R")
clus = makeCluster(detectCores() - 1)
#clusterExport(cl = clus, list("fcast_UR"))
registerDoParallel(clus)
start_time = Sys.time()
backcast_val_rentals_Local =foreach(i = 1:length(cc_loc),.combine=rbind,.packages = list.of.packages,.inorder=FALSE,.errorhandling = 'stop') %dopar% {
  
  x=ts(rentals_Local[CC_Loc==cc_loc[i]]$TotalRentals,frequency=12)
  ##xreg: total rentals for region 600, used as an input for the thetaDec model
  xreg=rentals_Local[substr(CC_Loc,1,8)==substr(cc_loc[i],1,8),.(TotalRentals=sum(TotalRentals)),by=.(substr(CC_Loc,1,8),MonthStartDate)]$TotalRentals
  
  ####number of periods
  RentalMonths=unique(rentals_Local[CC_Loc==cc_loc[i]]$MonthStartDate)
  backcastPeriods=min(6,length(RentalMonths[RentalMonths>max(backcast$MonthStartDate)]))
  
  if (backcastPeriods!=0){
    
  ResBcast=bestfit(x,test_periods = backcastPeriods,lead_time=3:4,methods=c('tbatsnotrend','arima',
                                                              'thetaDec','stlm'),outliers=TRUE,xreg=xreg)
  
  Backcast=data.table(CatClass = strsplit(cc_loc[i],"_")[[1]][1],
                      LocationID = strsplit(cc_loc[i],"_")[[1]][2],
                      MonthStartDate = seq.Date(MaxMonth- months(ResBcast$Backcast[,.N] - 1),
                                                MaxMonth, by = "month"),
                      ResBcast$Backcast[,c(2:length(ResBcast$Backcast)), with = F],
                      ResBcast$Accuracy[horizon=='All'])
  
  } else {
    
    Backcast=NULL
    
  }
  
  
}


Sys.time() - start_time
#
closeAllConnections()
#

remove(rentals_Local)

Index=which(colnames(backcast_val_rentals_Local)=='horizon')
# Accuracy_Local=unique(backcast_val_rentals_Local[,c(1,2,(Index+1):ncol(backcast_val_rentals_Local)), with = F])
if (!is.null(backcast_val_rentals_Local)) backcast_val_rentals_Local=backcast_val_rentals_Local[,1:(Index-1)]

backcast_val_rentals_Local=rbind(backcast,backcast_val_rentals_Local)

###save backcast data to Rda file
save(backcast_val_rentals_Local,file='backcast_Local_Monthly.Rda')
# save(Accuracy_Local,file='Accuracy_Local_Monthly.Rda')

remove(backcast_val_rentals_Local)
remove(Accuracy_Local)