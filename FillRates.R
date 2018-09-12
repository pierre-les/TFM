# Calculates target fill rates for all active cat-classes, using historical variable revenue and variable cost
# from TVA table, and cost of capital from Joe Pledger csv file.

rm(list=ls())

library(data.table)
library(RODBC)
library(tidyr)

##Pull Master tables
load('Master Tables.Rda')

active_cc=master_cc[CatClassGroup!='Out Of Scope']$CatClass
active_loc=master_loc$Location

###Create object with active cat-classes in the format of CM table (i.e. 907-75 instead of 907-0075)
active_cc_CMtable=paste0(master_cc$Category,"-",master_cc$Class)

###Pull CM data from SQL database
###Pull only contracts from last 365 days
load('CMTable.Rda')
load('ContractDetails.Rda')

MasCatClass2 <- master_cc[,.(CatClass,Category,Class,AvgOEC)]

CM <- data.table(merge(CM,MasCatClass2, by.x=c("CategoryCode","ClassCode"), by.y=c("Category","Class"),allow.cartesian = TRUE))

###Data for number of units rented in each contract is contained in ContractDetails table
###Merge with CM in order to add number of units to CM table
CM=merge(CM,ContractDetails,by=c('ContractId','CatClass'),allow.cartesian = TRUE)

CM=CM[CatClass %in% active_cc]

#CM=CM[as.Date(ReportingDate)>=('2016-04-01') & as.Date(ReportingDate)<('2016-10-01') & CategoryCode>=100]

###
setnames(CM,'LocationCode','Location')
CM=merge(CM,master_loc[,c('Location','MetroID','RegionID','DistrictID')],by='Location')

###Convert Rental Dates to Date format
CM$RentalStartDate = as.Date(CM$RentalStartDate)
CM$RentalEndDate = as.Date(CM$RentalEndDate)

###Get contract duration
CM[,duration:=RentalEndDate-RentalStartDate]
CM <- CM[complete.cases(duration),]
###Floor contract duration at 1 day and cap at 28 days
CM[,duration:=max(pmin(28,pmax(duration,1))),by=.(Location,CatClass,ReportingDate,ContractId)]

####When ProfitRuleLabelTxt starts with 'C' set as cost, starts with 'R' set as revenue
CM[,type:=substr(CM$ProfitRuleLabelTxt,1,1)]
##set these two categories as Fixed Costs
CM[(ProfitRuleLabelTxt %in% c("CEQD__1D", "CEQDTOOL")),type:='FC']


###Calculate Region Cost by RateCodeUsed
CM2 <- CM[,.(ContractId,ReportingDate,RateUsedCode,CatClass,RegionID,Location,TrgObjRuleAmt, duration,Qty, type)]

CMunique=unique(CM2[,.(CatClass,RegionID,Location,ContractId,RateUsedCode,ReportingDate,Qty,duration)])
##Total Days equals Contract Duration*Contract Qty
TotalDays=CMunique[,.(TotalDays=as.numeric(sum(Qty*duration))),by=.(CatClass,RegionID,RateUsedCode)]

###Get total costs by catclass, contract
CMagg=CM2[,.(TrgObjRuleAmt=sum(TrgObjRuleAmt),duration=mean(duration)),by=.(CatClass,RateUsedCode,ContractId,ReportingDate,RegionID,Location,type)]
CMsp=data.table(spread(CMagg,type,TrgObjRuleAmt,fill=0))
####Get total yearly costs by CatClass,Region,Contract Type (monthly, weekly, daily)
CostRevbyCont=CMsp[,.(Cost=sum(C),FixedCost=sum(FC)),by=.(CatClass,RegionID,RateUsedCode)]

####Get average daily costs CatClass, Regio, Contract Type
CostReg=merge(TotalDays,CostRevbyCont,by=c('CatClass','RegionID','RateUsedCode'))
CostReg=CostReg[,.(Cost=Cost/TotalDays,FixedCost=FixedCost/TotalDays),by=.(CatClass,RegionID,RateUsedCode)]
CostReg=merge(CostReg,MasCatClass2[,.(CatClass,AvgOEC)],by='CatClass')


###Compute Metro Revenue
CM2 <- CM[,.(ContractId,ReportingDate,CatClass,MetroID,Location,TrgObjRuleAmt, duration,Qty, type)]

CMunique=unique(CM2[,.(CatClass,MetroID,Location,ContractId,ReportingDate,Qty,duration)])
TotalDays=CMunique[,.(TotalDays=as.numeric(sum(Qty*duration))),by=.(CatClass,MetroID)]

###Get total revenue by catclass, contract
CMagg=CM2[,.(TrgObjRuleAmt=sum(TrgObjRuleAmt),duration=mean(duration)),by=.(CatClass,ContractId,ReportingDate,MetroID,Location,type)]
CMsp=data.table(spread(CMagg,type,TrgObjRuleAmt,fill=0))
##Get total revenue by catclass, metro
CostRevbyCont=CMsp[,.(Revenue=sum(R)),by=.(CatClass,MetroID)]

###Get average daily revenue by CatClass, metro
MetroRev=merge(TotalDays,CostRevbyCont,by=c('CatClass','MetroID'))
MetroRev=MetroRev[,.(Revenue=Revenue/TotalDays),by=.(CatClass,MetroID)]


###Compute Metro Cost
CM2 <- CM[,.(ContractId,RateUsedCode,ReportingDate,CatClass,RegionID,MetroID,Location,TrgObjRuleAmt, duration,Qty, type)]

Count=CM2[,.(Count=sum(Qty)),by=.(CatClass,MetroID,RateUsedCode)]

CMunique=unique(CM2[,.(CatClass,RateUsedCode,RegionID,MetroID,Location,ContractId,ReportingDate,Qty,duration)])
TotalDays=CMunique[,.(TotalDays=as.numeric(sum(Qty*duration))),by=.(CatClass,RegionID,MetroID,RateUsedCode)]

###Metro Cost is weighted average of average daily cost by CatClass,MetroID, Contract Type
CostMetbyRate=merge(CostReg,TotalDays,by=c('CatClass','RegionID','RateUsedCode'))
MetroCost=CostMetbyRate[,.(Cost=weighted.mean(Cost,TotalDays),FixedCost=weighted.mean(FixedCost,TotalDays)),by=.(CatClass,MetroID)]

CMMet=merge(MetroRev,MetroCost,by=c('CatClass','MetroID'))
CMMet=merge(CMMet,MasCatClass2[,.(CatClass,AvgOEC)],by='CatClass')



###District calculated the same as Metro, aggregated at District level

### Revenue
CM2 <- CM[,.(ContractId,ReportingDate,CatClass,DistrictID,Location,TrgObjRuleAmt, duration,Qty, type)]

CMunique=unique(CM2[,.(CatClass,DistrictID,Location,ContractId,ReportingDate,Qty,duration)])
TotalDays=CMunique[,.(TotalDays=as.numeric(sum(Qty*duration))),by=.(CatClass,DistrictID)]

CMagg=CM2[,.(TrgObjRuleAmt=sum(TrgObjRuleAmt),duration=mean(duration)),by=.(CatClass,ContractId,ReportingDate,DistrictID,Location,type)]
CMsp=data.table(spread(CMagg,type,TrgObjRuleAmt,fill=0))
CostRevbyCont=CMsp[,.(Revenue=sum(R)),by=.(CatClass,DistrictID)]

DistrictRev=merge(TotalDays,CostRevbyCont,by=c('CatClass','DistrictID'))
DistrictRev=DistrictRev[,.(Revenue=Revenue/TotalDays),by=.(CatClass,DistrictID)]


###Cost

CM2 <- CM[,.(ContractId,RateUsedCode,ReportingDate,CatClass,RegionID,DistrictID,Location,TrgObjRuleAmt, duration,Qty, type)]

CMunique=unique(CM2[,.(CatClass,RateUsedCode,RegionID,DistrictID,Location,ContractId,ReportingDate,Qty,duration)])
TotalDays=CMunique[,.(TotalDays=as.numeric(sum(Qty*duration))),by=.(CatClass,RegionID,DistrictID,RateUsedCode)]

CostDistbyRate=merge(CostReg,TotalDays,by=c('CatClass','RegionID','RateUsedCode'))
DistrictCost=CostDistbyRate[,.(Cost=weighted.mean(Cost,TotalDays),FixedCost=weighted.mean(FixedCost,TotalDays)),by=.(CatClass,DistrictID)]

CMDist=merge(DistrictRev,DistrictCost,by=c('CatClass','DistrictID'))
CMDist=merge(CMDist,MasCatClass2[,.(CatClass,AvgOEC)],by='CatClass')


###Fill Rates

###Pull Joe Pledger Cost of Capital values
TVAPay=data.table(read.csv('TVA - Annualized Payment Percent - Using OEC instead of ACV.csv'))
TVAPay[,CatClass:=paste0(substr(Cat_Class,1,3),"-",substr(Cat_Class,4,7))]

CMDist=merge(CMDist,TVAPay[,.(CatClass,AnnPay)],by='CatClass',all.x=TRUE)
CMMet=merge(CMMet,TVAPay[,.(CatClass,AnnPay)],by='CatClass',all.x=TRUE)

##If AnnPay is missing use 17%
CMDist[is.na(AnnPay),AnnPay:=.17]
CMMet[is.na(AnnPay),AnnPay:=.17]

###Use Joe Pledger values minus 6%
CMDist[,AnnPay:=AnnPay-.06]
CMMet[,AnnPay:=AnnPay-.06]

###use 10% OECNA
OECNA=1.1

###Apply fill rate formula
CMDist[,FillRate:=(Revenue-Cost - AvgOEC*OECNA*AnnPay/365)/(Revenue-Cost)]
CMDist[Revenue==0,FillRate:=0.5]
CMDist[AvgOEC==0,FillRate:=.5]
CMDist[FillRate>1,FillRate:=.5]
CMDist[FillRate>.99 & FillRate<=1,FillRate:=.99]
CMDist[FillRate<0.5,FillRate:=.5]
CMDist$Scenario=999

###Apply fill rate formula
CMMet[,FillRate:=(Revenue-Cost - AvgOEC*OECNA*AnnPay/365)/(Revenue-Cost)]
CMMet[Revenue==0,FillRate:=0.5]
CMMet[AvgOEC==0,FillRate:=.5]
CMMet[FillRate>1,FillRate:=.5]
CMMet[FillRate>.99 & FillRate<=1,FillRate:=.99]
CMMet[FillRate<0.5,FillRate:=.5]
CMMet$Scenario=999

# CMDist[,CM:=(Revenue-Cost-FixedCost)/Revenue]
# CMMet[,CM:=(Revenue-Cost-FixedCost)/Revenue]
# CMDist[Revenue==0,CM:=0]
# CMMet[Revenue==0,CM:=0]


####Get Historical Contribution Margin for all cat-classes
CM2 <- CM[,.(ContractId,ReportingDate,CatClass,DistrictID,Location,TrgObjRuleAmt, duration,Qty, type)]

CMunique=unique(CM2[,.(CatClass,DistrictID,Location,ContractId,ReportingDate,Qty,duration)])
TotalDays=CMunique[,.(TotalDays=as.numeric(sum(Qty*duration))),by=.(CatClass,DistrictID)]

CMagg=CM2[,.(TrgObjRuleAmt=sum(TrgObjRuleAmt),duration=mean(duration)),by=.(CatClass,ContractId,ReportingDate,DistrictID,Location,type)]
CMsp=data.table(spread(CMagg,type,TrgObjRuleAmt,fill=0))
CostRevbyCont=CMsp[,.(Cost=sum(C),FixedCost=sum(FC),Revenue=sum(R)),by=.(CatClass,DistrictID)]

ContribMargDist=CostRevbyCont[,CM:=(Revenue-Cost-FixedCost)/Revenue]


CM2 <- CM[,.(ContractId,ReportingDate,CatClass,MetroID,Location,TrgObjRuleAmt, duration,Qty, type)]

CMunique=unique(CM2[,.(CatClass,MetroID,Location,ContractId,ReportingDate,Qty,duration)])
TotalDays=CMunique[,.(TotalDays=as.numeric(sum(Qty*duration))),by=.(CatClass,MetroID)]

CMagg=CM2[,.(TrgObjRuleAmt=sum(TrgObjRuleAmt),duration=mean(duration)),by=.(CatClass,ContractId,ReportingDate,MetroID,Location,type)]
CMsp=data.table(spread(CMagg,type,TrgObjRuleAmt,fill=0))
CostRevbyCont=CMsp[,.(Cost=sum(C),FixedCost=sum(FC),Revenue=sum(R)),by=.(CatClass,MetroID)]

ContribMargMet=CostRevbyCont[,CM:=(Revenue-Cost-FixedCost)/Revenue]

CMDist=merge(CMDist,ContribMargDist[,.(CatClass,DistrictID,CM)],by=c('CatClass','DistrictID'))
CMMet=merge(CMMet,ContribMargMet[,.(CatClass,MetroID,CM)],by=c('CatClass','MetroID'))

save(CMDist,file='CMDist.Rda')
save(CMMet,file='CMMet.Rda')



