
# Creates spreadsheets with recommended transfers between districts

rm(list=ls())
options(java.parameters = "- Xmx1024m")

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

load('Master Tables.Rda')
ch=''

locFR=master_loc[DistrictID=='R6D16']

load('SOPRecommTransfDM.Rda')
load('SOPRecommTransfMM.Rda')
load('SOPRecommTransfML.Rda')

MoveInvDMSOP=merge(MoveInvDMSOP,unique(master_loc[,.(Location.x=DistrictID,DistrictName)]),by='Location.x')
MoveInvMMSOP=merge(MoveInvMMSOP,unique(master_loc[,.(Location.x=MetroID,MetroName,DistrictName)]),by='Location.x')
MoveInvMLSOP=merge(MoveInvMLSOP,unique(master_loc[,.(Location.x=MetroID,MetroName,DistrictName)]),by='Location.x')
MoveInvDMSOP=merge(MoveInvDMSOP,unique(master_loc[,.(Location.y=DistrictID,DistrictName)]),by='Location.y')
MoveInvMMSOP=merge(MoveInvMMSOP,unique(master_loc[,.(Location.y=MetroID,MetroName,DistrictName)]),by='Location.y')
MoveInvMLSOP=merge(MoveInvMLSOP,unique(master_loc[,.(Location.y=MetroID,MetroName,DistrictName)]),by='Location.y')

MoveInvDetSOP=rbind(MoveInvDMSOP,MoveInvMMSOP,MoveInvMLSOP,fill=TRUE)

MoveInvSummDMSOP=MoveInvDMSOP[,.(TotalTransfers=.N,TransCost=sum(TransCost),TotalProfit=sum(TotalProfit)),
                              by=.(CatClass,Date,DistrictName.x,CurrentOwned.x,DistrictName.y,CurrentOwned.y)]
MoveInvSummMMSOP=MoveInvMMSOP[,.(TotalTransfers=.N,TransCost=sum(TransCost),TotalProfit=sum(TotalProfit)),
                              by=.(CatClass,Date,DistrictName.x,MetroName.x,CurrentOwned.x,DistrictName.y,MetroName.y,CurrentOwned.y)]
MoveInvSummMLSOP=MoveInvMLSOP[,.(TotalTransfers=.N,TransCost=mean(TransCost),TotalProfit=sum(TotalProfit)),
                              by=.(CatClass,Date,DistrictName.x,MetroName.x,CurrentOwned.x,DistrictName.y,MetroName.y,CurrentOwned.y)]

MoveInvSOP=rbind(MoveInvSummDMSOP,MoveInvSummMMSOP,MoveInvSummMLSOP,fill=TRUE)

# MoveInvDetSOP[is.na(Unit.x),Unit.x:=CurrentOwned.x-1]
# MoveInvDetSOP[is.na(Unit.y),Unit.y:=CurrentOwned.y+1]

MoveInvSOP=merge(MoveInvSOP,master_cc[,.(CatClass,CatClassDesc,CatClassGroup)],by='CatClass')
MoveInvDetSOP=merge(MoveInvDetSOP,master_cc[,.(CatClass,CatClassDesc,CatClassGroup)],by='CatClass')

MoveInvSOP=MoveInvSOP[,.(CatClass=as.character(CatClass),CatClassDesc,CatClassGroup=factor(CatClassGroup,levels=c('District/Metro','Metro/Metro','Metro/Local')),Date,DistrictName.x,MetroName.x,
                         CurrentOwned.x,OwnedTarget.x=CurrentOwned.x-TotalTransfers,DistrictName.y,MetroName.y,CurrentOwned.y,OwnedTarget.y=CurrentOwned.y+TotalTransfers,
                         TotalTransfers,TransCost,TotalProfit)][order(Date,CatClassGroup,CatClass,-TotalProfit)]

MoveInvDetSOP=MoveInvDetSOP[,.(CatClass=as.character(CatClass),CatClassDesc,CatClassGroup=factor(CatClassGroup,levels=c('District/Metro','Metro/Metro','Metro/Local')),Date,DistrictName.x,MetroName.x,CurrentOwned.x,Unit.x,Forecast.x,
                               Cost.x,Revenue.x,Proba.x,Profit.x
                               ,DistrictName.y,MetroName.y,CurrentOwned.y,Unit.y,Forecast.y,
                               Cost.y,Revenue.y,Proba.y,Profit.y,
                               TotalTransfers=Transfers,TransCost,TotalProfit)]


MoveInvSOP=MoveInvSOP[order(Date,CatClassGroup,CatClass,-TotalProfit)]
MoveInvDetSOP=MoveInvDetSOP[order(Date,CatClassGroup,CatClass,DistrictName.x,MetroName.x,DistrictName.y,MetroName.y,-TotalProfit)]

MoveInvSOP=MoveInvSOP[DistrictName.x=='MW-Front Range' | DistrictName.y=='MW-Front Range']
MoveInvDetSOP=MoveInvDetSOP[DistrictName.x=='MW-Front Range' | DistrictName.y=='MW-Front Range']

colnames(MoveInvSOP)=c('CatClass','Description','Supply Chain','Date','District','Metro',
                       'Owned Inventory (Previous Month)','Owned Inventory - Transfers',
                       'District','Metro','Owned Inventory (Previous Month)',
                       'Owned Inventory + Transfers','Total Units Transferred','Transportation Cost','Total Profit')

colnames(MoveInvDetSOP)=c('CatClass','Description','Supply Chain','Date','District','Metro',
                       'Owned Inventory (Previous Month)','Owned Inventory - Transfers',
                       'Forecast','Variable Cost','Variable Revenue','On Rent Probability',
                       'Profit','District','Metro',
                       'Owned Inventory (Previous Month)','Owned Inventory + Transfers',
                       'Forecast','Variable Cost','Variable Revenue','On Rent Probability',
                       'Profit','Total Units Transferred','Transportation Cost','Total Profit')

wb=createWorkbook()

##Styles
pct <- CellStyle(wb, dataFormat=DataFormat("0.0%"))
curr <- CellStyle(wb, dataFormat=DataFormat("$0.0"))
SummIndex <- list(curr,curr)
names(SummIndex) <- 14:15
DetailIndex <- list(curr,curr,pct,curr,curr,curr,pct,curr,curr,curr)
names(DetailIndex) <- c(10:13,19:22,24:25)

sheet1 <- createSheet(wb, sheetName="S&OP Summary")
rows  <- createRow(sheet1, rowIndex=1)
cells <- createCell(rows, colIndex=5:12) 
setCellValue(cells[[1,1]], 'Origin') 
setCellValue(cells[[1,5]], 'Destination') 
addMergedRegion(sheet1, 1,1,5,8)
addMergedRegion(sheet1, 1,1,9,12)
addDataFrame(MoveInvSOP, sheet1, startRow=2, startColumn=1,row.names = FALSE,colStyle = SummIndex)

sheet2 <- createSheet(wb, sheetName="S&OP Detail")
rows  <- createRow(sheet2, rowIndex=1)
cells <- createCell(rows, colIndex=5:22) 
setCellValue(cells[[1,1]], 'Origin') 
setCellValue(cells[[1,9]], 'Destination') 
addMergedRegion(sheet2, 1,1,5,13)
addMergedRegion(sheet2, 1,1,14,22)
addDataFrame(MoveInvDetSOP, sheet2, startRow=2, startColumn=1,row.names = FALSE,colStyle = DetailIndex)


###Get Adjusted Values

load('SOPRecommTransfDMAdjusted.Rda')
load('SOPRecommTransfMMAdjusted.Rda')
load('SOPRecommTransfMLAdjusted.Rda')

MoveInvDMSOP=merge(MoveInvDMSOP,unique(master_loc[,.(Location.x=DistrictID,DistrictName)]),by='Location.x')
MoveInvMMSOP=merge(MoveInvMMSOP,unique(master_loc[,.(Location.x=MetroID,MetroName,DistrictName)]),by='Location.x')
MoveInvMLSOP=merge(MoveInvMLSOP,unique(master_loc[,.(Location.x=MetroID,MetroName,DistrictName)]),by='Location.x')
MoveInvDMSOP=merge(MoveInvDMSOP,unique(master_loc[,.(Location.y=DistrictID,DistrictName)]),by='Location.y')
MoveInvMMSOP=merge(MoveInvMMSOP,unique(master_loc[,.(Location.y=MetroID,MetroName,DistrictName)]),by='Location.y')
MoveInvMLSOP=merge(MoveInvMLSOP,unique(master_loc[,.(Location.y=MetroID,MetroName,DistrictName)]),by='Location.y')

MoveInvDetSOP=rbind(MoveInvDMSOP,MoveInvMMSOP,MoveInvMLSOP,fill=TRUE)

MoveInvSummDMSOP=MoveInvDMSOP[,.(TotalTransfers=.N,TransCost=sum(TransCost),TotalProfit=sum(TotalProfit)),
                              by=.(CatClass,Date,DistrictName.x,CurrentOwned.x,DistrictName.y,CurrentOwned.y)]
MoveInvSummMMSOP=MoveInvMMSOP[,.(TotalTransfers=.N,TransCost=sum(TransCost),TotalProfit=sum(TotalProfit)),
                              by=.(CatClass,Date,DistrictName.x,MetroName.x,CurrentOwned.x,DistrictName.y,MetroName.y,CurrentOwned.y)]
MoveInvSummMLSOP=MoveInvMLSOP[,.(TotalTransfers=.N,TransCost=mean(TransCost),TotalProfit=sum(TotalProfit)),
                              by=.(CatClass,Date,DistrictName.x,MetroName.x,CurrentOwned.x,DistrictName.y,MetroName.y,CurrentOwned.y)]

MoveInvSOP=rbind(MoveInvSummDMSOP,MoveInvSummMMSOP,MoveInvSummMLSOP,fill=TRUE)

# MoveInvDetSOP[is.na(Unit.x),Unit.x:=CurrentOwned.x-1]
# MoveInvDetSOP[is.na(Unit.y),Unit.y:=CurrentOwned.y+1]

MoveInvSOP=merge(MoveInvSOP,master_cc[,.(CatClass,CatClassDesc,CatClassGroup)],by='CatClass')
MoveInvDetSOP=merge(MoveInvDetSOP,master_cc[,.(CatClass,CatClassDesc,CatClassGroup)],by='CatClass')

MoveInvSOP=MoveInvSOP[,.(CatClass=as.character(CatClass),CatClassDesc,CatClassGroup=factor(CatClassGroup,levels=c('District/Metro','Metro/Metro','Metro/Local')),Date,DistrictName.x,MetroName.x,
                         CurrentOwned.x,OwnedTarget.x=CurrentOwned.x-TotalTransfers,DistrictName.y,MetroName.y,CurrentOwned.y,OwnedTarget.y=CurrentOwned.y+TotalTransfers,
                         TotalTransfers,TransCost,TotalProfit)][order(Date,CatClassGroup,CatClass,-TotalProfit)]

MoveInvDetSOP=MoveInvDetSOP[,.(CatClass=as.character(CatClass),CatClassDesc,CatClassGroup=factor(CatClassGroup,levels=c('District/Metro','Metro/Metro','Metro/Local')),Date,DistrictName.x,MetroName.x,CurrentOwned.x,Unit.x,Forecast.x,
                               Cost.x,Revenue.x,Proba.x,Profit.x
                               ,DistrictName.y,MetroName.y,CurrentOwned.y,Unit.y,Forecast.y,
                               Cost.y,Revenue.y,Proba.y,Profit.y,
                               TotalTransfers=Transfers,TransCost,TotalProfit)]


MoveInvSOP=MoveInvSOP[order(Date,CatClassGroup,CatClass,-TotalProfit)]
MoveInvDetSOP=MoveInvDetSOP[order(Date,CatClassGroup,CatClass,DistrictName.x,MetroName.x,DistrictName.y,MetroName.y,-TotalProfit)]

MoveInvSOP=MoveInvSOP[DistrictName.x=='MW-Front Range' | DistrictName.y=='MW-Front Range']
MoveInvDetSOP=MoveInvDetSOP[DistrictName.x=='MW-Front Range' | DistrictName.y=='MW-Front Range']

colnames(MoveInvSOP)=c('CatClass','Description','Supply Chain','Date','District','Metro',
                       'Owned Inventory (Previous Month)','Owned Inventory - Transfers',
                       'District','Metro','Owned Inventory (Previous Month)',
                       'Owned Inventory + Transfers','Total Units Transferred','Transportation Cost','Total Profit')

colnames(MoveInvDetSOP)=c('CatClass','Description','Supply Chain','Date','District','Metro',
                          'Owned Inventory (Previous Month)','Owned Inventory - Transfers',
                          'Forecast','Variable Cost','Variable Revenue','On Rent Probability',
                          'Profit','District','Metro',
                          'Owned Inventory (Previous Month)','Owned Inventory + Transfers',
                          'Forecast','Variable Cost','Variable Revenue','On Rent Probability',
                          'Profit','Total Units Transferred','Transportation Cost','Total Profit')

sheet3 <- createSheet(wb, sheetName="S&OP Summary Adjusted")
rows  <- createRow(sheet3, rowIndex=1)
cells <- createCell(rows, colIndex=5:12) 
setCellValue(cells[[1,1]], 'Origin') 
setCellValue(cells[[1,5]], 'Destination') 
addMergedRegion(sheet3, 1,1,5,8)
addMergedRegion(sheet3, 1,1,9,12)
addDataFrame(MoveInvSOP, sheet3, startRow=2, startColumn=1,row.names = FALSE,colStyle = SummIndex)

sheet4 <- createSheet(wb, sheetName="S&OP Detail Adjusted")
rows  <- createRow(sheet4, rowIndex=1)
cells <- createCell(rows, colIndex=5:22) 
setCellValue(cells[[1,1]], 'Origin') 
setCellValue(cells[[1,9]], 'Destination') 
addMergedRegion(sheet4, 1,1,5,13)
addMergedRegion(sheet4, 1,1,14,22)
addDataFrame(MoveInvDetSOP, sheet4, startRow=2, startColumn=1,row.names = FALSE,colStyle = DetailIndex)

saveWorkbook(wb,file=paste0('TransfersInterDistrictSOP ',Sys.Date(),'.xlsx'))
