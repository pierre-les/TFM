rm(list=ls())
setwd('C:/Sri/ForecastandTargets')

library(data.table)
library(RODBC)
library(reshape)
library(tidyr)

load('Master Tables.Rda')

master_cc[is.na(CatClassGroup),Active:='No']
master_cc[!is.na(CatClassGroup),Active:='Yes']

#For all catclasses not actively managed, the classification should be based on the below rules; 
#Local/Local = OEC < $6k, Metro/Local = $6K to <$20K, Metro/Metro = OEC $20K to <$60K, District/Metro = OEC >=$60k
master_cc[is.na(CatClassGroup) & AvgOEC<6000,CatClassGroup:='Local/Local']
master_cc[is.na(CatClassGroup) & AvgOEC>=6000 & AvgOEC<20000,CatClassGroup:='Metro/Local']
master_cc[is.na(CatClassGroup) & AvgOEC>=20000 & AvgOEC<60000,CatClassGroup:='Metro/Metro']
master_cc[is.na(CatClassGroup) & AvgOEC>=60000,CatClassGroup:='District/Metro']


##FR District
active_loc=master_loc[DistrictID=='R6D16']$Location

###Pull CM data from SQL database
load('CMTable.Rda')

##Pull Contract Details from SQL database
# ContractDetails <- data.table(sqlQuery(ch, paste0("select ContractId, Qty, CatClass
#                                               from [United].[dbo].[DATA_ContractDetails] where 
#                                               Location in (",paste0("'",active_loc,"'",collapse=","),")")))

CM <- data.table(merge(CM,master_cc[,.(CatClass,Category,Class,CatClassGroup,CatClassDesc,AvgOEC,Active)], by.x=c("CategoryCode","ClassCode"), by.y=c("Category","Class"),allow.cartesian = TRUE))

###Data for number ofunits rented in each contract is contained in ContractDetails table
###Merge with CM in order to add number of units to CM table
# CM=merge(CM,ContractDetails,by=c('ContractId','CatClass'),allow.cartesian = TRUE)

#CM=CM[as.Date(ReportingDate)>=('2016-04-01') & as.Date(ReportingDate)<('2016-10-01') & CategoryCode>=100]

###Pull owned inventory table from SQL database
MaxDate=sqlQuery(ch,"SELECT MAX(DayDate) FROM DATA_RentalDetailByDay
                 WHERE OwnedInventory IS NOT NULL")
MaxDate=MaxDate[1,1]
# MaxDate='2018-07-31'
OwnedInv<- data.table(sqlQuery(ch,paste0("SELECT CatClass,Location,DayDate,OwnedInventory FROM  DATA_RentalDetailByDay
                                                   WHERE (TotalRentals>0 OR OwnedInventory>0)
                                                   AND Location IN  (",paste0("'",active_loc,"'",collapse=","),
                                                   ") AND DayDate = '",as.Date(as.character(MaxDate)),"'"),believeNRows=FALSE))

CM=CM[CatClass!='000-0000']
###
setnames(CM,'LocationCode','Location')
CM=merge(CM,master_loc[,c('Location','MetroID','RegionID','DistrictID')],by='Location')

####When ProfitRuleLabelTxt starts with 'C' set as cost, starts with 'R' set as revenue
CM[,type:=substr(CM$ProfitRuleLabelTxt,1,1)]

Contracts=unique(CM[,.(CatClass,ContractId)])
Contracts=Contracts[,Contracts:=.N,by=.(CatClass)]
Contracts$PercContracts=Contracts$Contracts/length(unique(Contracts$ContractId))
Contracts=unique(Contracts[,.(CatClass,Contracts,PercContracts)])

Revenue=CM[type=='R',.(Revenue=sum(TrgObjRuleAmt)),by=.(CatClass)]
Revenue[Revenue<0,Revenue:=0]
Revenue[,PercRevenue:=Revenue/sum(Revenue)]
Revenue=Revenue[order(-Revenue),]
Revenue[,RevRank:=1:.N]
Revenue[RevRank>250,RevRank:=NA]

OwnedInv=OwnedInv[as.numeric(substr(CatClass,1,3))>=100,.(OwnedInventory=sum(OwnedInventory)),by=.(CatClass)]
OwnedInv=merge(OwnedInv,master_cc[,.(CatClass,AvgOEC)],by='CatClass')
OwnedInv[is.na(OwnedInventory),OwnedInventory:=0]
OwnedInv[,TotalOEC:=OwnedInventory*AvgOEC]
OwnedInv[,OECPerc:=TotalOEC/sum(OwnedInv$TotalOEC)]

Report=merge(Revenue[,.(CatClass,RevRank,PercRevenue)],OwnedInv[,.(CatClass,OECPerc)],by='CatClass',all=TRUE)

Report=merge(Report,Contracts[,.(CatClass,PercContracts)],by='CatClass',all=TRUE)

Report=merge(master_cc[,.(CatClass,CatClassDesc,CatClassGroup,AvgOEC,Active)],
             Report,by='CatClass',all.y=TRUE)

Report[is.na(OECPerc),OECPerc:=0]
Report[is.na(PercRevenue),PercRevenue:=0]
Report[is.na(PercContracts),PercContracts:=0]

Report=Report[,.(CatClass,`CatClass Description`=CatClassDesc,`Supply Chain`=CatClassGroup,
                 `Avg OEC`=AvgOEC,`Revenue Rank`=RevRank,`% of Total OEC`=OECPerc,
                 `% of Total Rental Days`=PercContracts,`% of Total Revenue`=PercRevenue,
                 `Actively Managed`=Active)]


wb=createWorkbook()
sheet1=createSheet(wb,sheetName='TFM CatClass Report')

##ColStyles
pct <- CellStyle(wb, dataFormat=DataFormat("0%"))
curr=CellStyle(wb, dataFormat=DataFormat("$0.00"))
dfColIndex <- c(list(curr),rep(list(pct),3))
names(dfColIndex) <- c(4,6:8)

addDataFrame(Report, sheet1,row.names = FALSE,colStyle = dfColIndex)
saveWorkbook(wb, file=paste0("TFM CatClass Report.xlsx"))

