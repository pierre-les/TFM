


InvVariation=function(TargetInv,Slope,CurrentInv,MaxInv,CapTarget,MarketInc,MonthlyCap,YearlyCumCap){

  
  ### InvVariation function gets the regional constrained inventory for a single catclass
  ### TargetInv is a vector containing the regional unconstrained inventory for the catclass for the forecast period
  ### Slope is the linear regression slope obtained as output from function getSlope
  ### CurrentInv is the regional current owned inventory for the catclass
  ### MaxInv is the maximum regional unconstrained inventory for the forecast period
  ### CapTarget is Forecast+10%. Used to avoid dropping the inventory to levels that would take Time Ut above 90.9%
  ### MarketInc is the market growth factor (currently 18%)
  ### MonthlyCap is the percentage of MaxInv that can be added or removed each month (currently 6%)
  ### YearlyCumCap is the yearly cumulated percentage of MaxInv that can be added or removed yearly (currently 18%)

  ####Monthly increase and yearly cumulated increase are raised whenever the slope of the linear regression is significant
  PercMaxYearInc=YearlyCumCap+max(0,MarketInc-Slope)
  PercMaxMonthInc=MonthlyCap+max(0,MarketInc-Slope)/3
  
  ##Calculate max monthly increase/decrease and max yearly cumulated increase/decrease
  InitialInv=unique(CurrentInv)
  MaxInv=max(MaxInv,CurrentInv)
  MaxMonthInc=max(1,PercMaxMonthInc*unique(MaxInv))
  MaxMonthDec=min(-1,-1*MonthlyCap*unique(MaxInv))
  MaxYearInc=max(6,PercMaxYearInc*unique(MaxInv))
  MaxYearDec=min(-6,-1*YearlyCumCap*unique(MaxInv))
  CumInc=0
  CumDec=0
  

  ##For loop that calculates the constrained regional inventory for each month
  for (i in 1:length(CurrentInv)){
    ###MaxInv represents the maximum inventory allowed for month i
    MaxInv=min(CurrentInv[max(1,i-1)]+MaxMonthInc,CurrentInv[max(1,i-1)]+max(0,MaxYearInc-CumInc))
    MaxInv=max(MaxInv,CapTarget[i])
    ##MinInv represents the minimum inventory allowed for month i
    MinInv=max(CurrentInv[max(1,i-1)]+MaxMonthDec,CurrentInv[max(1,i-1)]+min(0,MaxYearDec-CumDec),CapTarget[i])
    ###CurentInv[i] is the constrained regional inventory for month i
    ###   MinInv < Unconstr. Inv. < MaxInv
    CurrentInv[i]=min(MaxInv,max(MinInv,TargetInv[i]))
    
    ### vector containing regional constrained inventory of past six months
    Inv=c(InitialInv,CurrentInv)[max(1,i-5):(i+1)]
    
    ##Computes the yearly cumu;ated increase and decrease
    CumInc=max(0,Inv[length(Inv)]-Inv[length(Inv)-1])+CumInc
    CumDec=min(0,Inv[length(Inv)]-Inv[length(Inv)-1])+CumDec
    
    ### Change6m contains inventory changes of months from i to i-5
    Change6m=diff(Inv)
    
    ###if inventory hasn't increased in past six months, then the yearly cumulated increase gets reset to zero
    if (sum(Change6m[Change6m>=0])==0) CumInc=0
    ###if inventory hasn't deceeased in past six months, then the yearly cumulated decrease gets reset to zero
    if (sum(Change6m[Change6m<=0])==0) CumDec=0
    
  }
  return(CurrentInv)
}





getSlope=function(active_cc,active_loc,Start,End){
  
  ###GetSlope funcion finds the slope of a linear regression y=ax+b, where y is the owned inventory of a cat-class in Region 600,
##    and x is a series of months (going from Start to End arguments in function)
  ##Function returns a data table with slopes for all cat-classes specified in active_cc
  ## and all locations specified in active_loc
  
  
  ###Load Daily rentals table to get historical inventory data
  load('rentals_Daily.Rda')
  
  rentals_Daily=merge(rentals_Daily[CatClass %in% active_cc],unique(master_loc[,.(Location,RegionID=as.character(RegionID))]),by='Location')
  rentals_Daily[is.na(OwnedInventory),'OwnedInventory']=0
  rentals_Daily[,MonthStartDate:=floor_date(DayDate,unit='month')]

  ### Aggregate historical owned inventory data up to the region level
  rentals_Region=rentals_Daily[,.(OwnedInventory=sum(OwnedInventory)),by=.(CatClass,RegionID,DayDate,MonthStartDate)]

  remove(rentals_Daily)
  rentals_Region[,CC_Loc:=paste(CatClass, RegionID, sep = "_")]
  
  ##merge with full calendar data to fill zeros
  calendar=unique(rentals_Region[,.(MonthStartDate)])
  cc_loc = unique(rentals_Region[,CC_Loc])
  calendar=data.table(CC_Loc=rep(cc_loc,each=nrow(calendar)),MonthStartDate=calendar$MonthStartDate)
  calendar[,CatClass:=substr(CC_Loc,1,8)]
  calendar[,RegionID:=substr(CC_Loc,10,nchar(CC_Loc))]
  
  ###Get owned inventory for each CatClass for Region 600
  rentals_Region=merge(rentals_Region,calendar,all=TRUE,by=c('CatClass','RegionID','CC_Loc','MonthStartDate'))
  rentals_Region[is.na(OwnedInventory),OwnedInventory:=0]
  ###Compute average owned inventory for each month and each cat class in Region 600
  rentals_Region=rentals_Region[,.(OwnedInventory=mean(OwnedInventory)),by=.(CatClass,RegionID,MonthStartDate)]
  
  ###subset owned inventory data between Start and End date variables specified as arguments
  rentals_Region=rentals_Region[MonthStartDate>=Start & MonthStartDate<=End]
  ### Create x axis variable for lienar regression (1 for first month of history, 2 for second month,etc...)
  rentals_Region[,LRXAxis:=1:.N,by=.(CatClass,RegionID)]

  
  ##Get linear regression coefficient for equation where x=Date, y=OwnedInventory
  RRSplit=split(rentals_Region,by=c('CatClass','RegionID'))
  rentals_Region=rbindlist(lapply(RRSplit,function(x) {
    
    LR=lm(OwnedInventory~LRXAxis,data=x)
    x$Slope=as.numeric(LR$coefficients[2])*12
    return(x)
    
  }))
  
  ###Create output table containing slope for each cat-class
  SlopeDT=rentals_Region[,.(Slope=mean(Slope)/mean(OwnedInventory)),by=.(CatClass,RegionID)]
  SlopeDT[is.nan(Slope),Slope:=0]
  return(SlopeDT)
  
}



getRegInvData=function(Hierarchy,active_loc,active_cc,ch,OECNA,ForecastInput=''){
  
  ### getRegInvData calculates both the unconstrained inventory at the regional and location level
  ### and the constrained inventory at the regional level

  if (Hierarchy=='District'){
    
    
    ##Load Fill Rate and Forecast tables
    load('CMDist.Rda')
    FR=CMDist
    FR[,Location:=as.character(DistrictID)]
    
    ###Get Forecast from Rda file
    
    load(paste0('MonthlyRentalForecastDistrict_Avg',ForecastInput,'.Rda'))
  #  Forecast=read.csv('Example/ForecastExample.csv')
    Forecast=forecast_District_Monthly[,.(CatClass,Location=as.character(DistrictID),MonthStartDate=as.Date(MonthStartDate),Month,Year,Forecast,Accuracy)]
    Forecast[is.na(Forecast),Forecast:=0]
    Forecast[is.infinite(Forecast),Forecast:=0]
    
    remove(forecast_District_Monthly)

    ###Get Current Owned Inventory from daily rentals table
    
    load('rentals_Daily.Rda')
    rentals_Daily=rentals_Daily[CatClass %in% Forecast$CatClass]
    ###Current owned inventory is the owned inventory from the newest data in rentals_Daily table
    rentals_Daily=rentals_Daily[DayDate==max(rentals_Daily[!is.na(OwnedInventory)]$DayDate)]
    rentals_Daily=merge(rentals_Daily,master_loc[,.(Location,DistrictID)],by='Location')
    rentals_Daily[is.na(OwnedInventory),OwnedInventory:=0]

    CurrentInv=rentals_Daily[,.(CurrentOwned=sum(OwnedInventory)),by=.(CatClass,Location=as.character(DistrictID))]
    CurrentInv[is.na(CurrentOwned),CurrentOwned:=0]
    
    
    ##Get Currrent Owned Inventory from Fleet Sheet
    
    # CurrentInv=data.table(read_excel('Dist Fleet Dashboard.xlsm',sheet='Region FLT',skip=2))
    # CurrentInv[EMCLAS<10,CatClass:=paste0(EMCATG,"-000",EMCLAS)]
    # CurrentInv[EMCLAS<100 & EMCLAS>=10,CatClass:=paste0(EMCATG,"-00",EMCLAS)]
    # CurrentInv[EMCLAS<1000 & EMCLAS>=100,CatClass:=paste0(EMCATG,"-0",EMCLAS)]
    # CurrentInv[EMCLAS>=1000,CatClass:=paste0(EMCATG,"-",EMCLAS)]
    # 
    # CurrentInv=merge(CurrentInv[CatClass %in% active_cc,.(LocationID=EMCLOC,CatClass)],master_loc[,.(LocationID,DistrictID)],by='LocationID')
    # CurrentInv=CurrentInv[,.(CurrentOwned=.N),by=.(CatClass,Location=DistrictID)]
    
    remove(rentals_Daily)
    
  }  else {
    
    if(Hierarchy=='Metro'){
      
      
      ##Get Fill Rate data from Rda file
      load('CMMet.Rda')
      FR=CMMet
      FR[,Location:=as.character(MetroID)]
      
      ###Get Forecast from Rda file
      
      load(paste0('MonthlyRentalForecastMetro_Avg',ForecastInput,'.Rda'))
      Forecast=forecast_Metro_Monthly[,.(CatClass,Location=as.character(MetroID),MonthStartDate=as.Date(MonthStartDate),Month,Year,Forecast,Accuracy)]
      Forecast[is.na(Forecast),Forecast:=0]
      Forecast[is.infinite(Forecast),Forecast:=0]
        remove(forecast_Metro_Monthly)
      
        ###Get Current Owned Inventory from daily rentals table
        
        load('rentals_Daily.Rda')
        rentals_Daily=rentals_Daily[CatClass %in% Forecast$CatClass]
        ###Current owned inventory is the owned inventory from the newest data in rentals_Daily table
        rentals_Daily=rentals_Daily[DayDate==max(rentals_Daily[!is.na(OwnedInventory)]$DayDate)]
        rentals_Daily=merge(rentals_Daily,master_loc[,.(Location,MetroID)],by='Location')
        rentals_Daily[is.na(OwnedInventory),OwnedInventory:=0]
        
        CurrentInv=rentals_Daily[,.(CurrentOwned=sum(OwnedInventory)),by=.(CatClass,Location=as.character(MetroID))]
        CurrentInv[is.na(CurrentOwned),CurrentOwned:=0]
        
      # CurrentInv=data.table(read_excel('Dist Fleet Dashboard MW 2018.xlsm',sheet='Region FLT',skip=2))
      # CurrentInv[EMCLAS<10,CatClass:=paste0(EMCATG,"-000",EMCLAS)]
      # CurrentInv[EMCLAS<100 & EMCLAS>=10,CatClass:=paste0(EMCATG,"-00",EMCLAS)]
      # CurrentInv[EMCLAS<1000 & EMCLAS>=100,CatClass:=paste0(EMCATG,"-0",EMCLAS)]
      # CurrentInv[EMCLAS>=1000,CatClass:=paste0(EMCATG,"-",EMCLAS)]
      # 
      # CurrentInv=merge(CurrentInv[CatClass %in% active_cc,.(LocationID=EMCLOC,CatClass)],master_loc[,.(LocationID,MetroID)],by='LocationID')
      # CurrentInv=CurrentInv[,.(CurrentOwned=.N),by=.(CatClass,Location=MetroID)]
      
      remove(rentals_Daily)
      
      
    } else {
      
      ###Use Metro Fill Rates for Local/Local cat-classes
      
      load('CMMet.Rda')
      FR=CMMet
      remove(CMMet)
      FR[is.na(FillRate),'FillRate']=0.5
      FR[FillRate>=1,'FillRate']=0.5
      FR[FillRate<0.5,'FillRate']=0.5
      FR[FillRate>.99,'FillRate']=.99
      
      FR=merge(FR,unique(master_loc[,.(LocationID,MetroID)]),by=c('MetroID'),allow.cartesian = TRUE)
      FR[,Location:=as.character(LocationID)]
      
      ###Get Forecast from Rda file
      
      load(paste0('MonthlyRentalForecastLocal_Avg',ForecastInput,'.Rda'))
      Forecast=forecast_Local_Monthly[,.(CatClass,Location=as.character(LocationID),MonthStartDate=as.Date(MonthStartDate),Month,Year,Forecast,Accuracy)]
      Forecast[is.na(Forecast),Forecast:=0]
      Forecast[is.infinite(Forecast),Forecast:=0]
      
      
       remove(forecast_Local_Monthly)
      # 

       ###Get Current Owned Inventory from daily rentals table
       
       load('rentals_Daily.Rda')
       rentals_Daily=rentals_Daily[CatClass %in% Forecast$CatClass]
       ###Current owned inventory is the owned inventory from the newest data in rentals_Daily table
       rentals_Daily=rentals_Daily[DayDate==max(rentals_Daily[!is.na(OwnedInventory)]$DayDate)]
       rentals_Daily=merge(rentals_Daily,master_loc[,.(Location,LocationID)],by='Location')
       rentals_Daily[is.na(OwnedInventory),OwnedInventory:=0]
       
       CurrentInv=rentals_Daily[,.(CurrentOwned=sum(OwnedInventory)),by=.(CatClass,Location=as.character(LocationID))]
       CurrentInv[is.na(CurrentOwned),CurrentOwned:=0]
       
      
      # CurrentInv=data.table(read_excel('Dist Fleet Dashboard MW 2018.xlsm',sheet='Region FLT',skip=2))
      # CurrentInv[EMCLAS<10,CatClass:=paste0(EMCATG,"-000",EMCLAS)]
      # CurrentInv[EMCLAS<100 & EMCLAS>=10,CatClass:=paste0(EMCATG,"-00",EMCLAS)]
      # CurrentInv[EMCLAS<1000 & EMCLAS>=100,CatClass:=paste0(EMCATG,"-0",EMCLAS)]
      # CurrentInv[EMCLAS>=1000,CatClass:=paste0(EMCATG,"-",EMCLAS)]
      # 
      # CurrentInv=CurrentInv[CatClass %in% active_cc,.(CurrentOwned=.N),by=.(CatClass,Location=EMCLOC)]
      
      remove(rentals_Daily)
      
    }
    
  }
  

  ##If Average Forecast <=10 use poisson distribution, if not use normal distribution for calculating Unconstrained Inventory Target
  Forecast[,AvgFcst:=mean(Forecast),by=.(CatClass,Location)]
  Forecast[,Dist:=ifelse(AvgFcst<=10,'Pois','Norm')]
  
  ##FcstDate contains date of first month of forecast
  FcstDate=min(Forecast$MonthStartDate)
  Forecast=merge(Forecast,FR[,.(CatClass,Location,FillRate,Cost,Revenue)],all.x=TRUE,by=c('CatClass','Location'))
  
  ###Convert NA fill rate values to 0.5
  Forecast[is.na(FillRate),FillRate:=.5]

  #### When Average Forecast<=10: Inventory Target is calculated using inverted poisson distribution
  ###  with probability=Fill Rate, and lambda=Forecast
  ### When Average Forecast>10: Inventory Target is calculated using inverted normal distribution
  ### with probability=Fill Rate, mean=Forecast, standard dev=Forecast*(1-ForecastAccuracy)/100
  
  Forecast[,OwnedTarget:=ceiling(ifelse(Dist=='Pois',
                                      OECNA*qpois(FillRate,Forecast),
                                      OECNA*qnorm(FillRate,mean=Forecast,sd=Forecast*(1-pmin(99,Accuracy)/100))))]
  
  ###Calculate Cap Target as Forecast*(1+OECNA)
  ###Cap Target is the inventory value where Time Ut is maximal (equal to 1/(1+OECNA))
  Forecast=Forecast[,.(Forecast=mean(Forecast),CapTarget=ceiling(max(Forecast)*OECNA),FillRate=mean(FillRate),Accuracy=mean(Accuracy),OwnedTarget=max(OwnedTarget),Cost=mean(Cost),Revenue=mean(Revenue)),by=.(CatClass,Dist,Location,Year,Month)]
  
  ######
  #Aggregate Forecast, Unconstrained Inventory and Current Inventory at the Region Level
  Targets=Forecast[,.(OwnedTarget=sum(OwnedTarget),CapTarget=sum(CapTarget)),by=.(CatClass,Year,Month)]
  Targets=merge(master_cc[,.(CatClass,CatClassGroup,AvgOEC)],Targets,by='CatClass')
  Targets[,Date:=as.yearmon(paste0(Year,"-",Month))]

  CurrentInvReg=CurrentInv[,.(CurrentOwned=sum(CurrentOwned,na.rm=TRUE)),by=.(CatClass)]
  CurrentInvReg=CurrentInvReg[,.(CurrentOwned=max(CurrentOwned)),by=CatClass]
  
  ###Get linear regression slope for historical inventory equation y=ax+b, where y is inventory and x is month date
  HistInv=getSlope(active_cc,active_loc,FcstDate-months(13),FcstDate-months(1))

  Targets[,MonthStartDate:=as.Date(Date)]
  ##Merge Targets object containing forecast and unconstr. target with objects containing linear regression slope
  ## and current regional inventory
  Targets=merge(Targets,unique(HistInv[,.(CatClass,Slope)]),all=TRUE,by=c('CatClass'))
  Targets=merge(Targets,CurrentInvReg[,.(CatClass,CurrentOwned)],all=TRUE,by=c('CatClass'))
  Targets[,Owned:=0]

    Targets[is.na(OwnedTarget),OwnedTarget:=0]
  Targets[is.na(CurrentOwned),CurrentOwned:=0]
  Targets[is.na(CapTarget),CapTarget:=0]
  ###Maximum owned inventory target for each catclass added to column MaxInv
  Targets[,MaxInv:=max(OwnedTarget),by=CatClass]
  # Targets[Date==min(Targets$Date),StartInv:=OwnedTarget]
  # Targets[is.na(StartInv),StartInv:=max(StartInv,na.rm=TRUE),by=.(CatClass)]
  
  ###Create column containing the previous month historical owned inventory 
  # Targets[,OwnedPrevMonth:=shift(Owned,1L),by=.(CatClass)]
  # ###
  # Targets[,InvChange:=(Owned-OwnedPrevMonth)]

  ###Apply InvVariation function which applies the increase and decrease constraints to the unconstrained inventory
  ##Output from applying function gives a data table containing regional constrained inventory targets
  Targets[,SeasInv:=round(InvVariation(OwnedTarget,Slope,CurrentOwned,MaxInv,CapTarget,MarketInc=.18,MonthlyCap = .06,YearlyCumCap = .18),0),by=.(CatClass)]
  TargetsReg=copy(Targets)
  
  #######
  ###Merge Forecast table with Inventory Targets table and Current Owned Inventory table
  Forecast=merge(Forecast,Targets[,.(CatClass,Year,Month,SeasInv,Slope)],by=c('CatClass','Year','Month'),all.x=TRUE)
  Forecast=merge(Forecast,CurrentInv[,.(CatClass,Location,CurrentOwned)],all.x=TRUE,by=c('CatClass','Location'))
  #Convert NAs for cost, revenue, currentowned and constrained inventory to zero
  Forecast[is.na(Cost),Cost:=0]
  Forecast[is.na(Revenue),Revenue:=0]
  Forecast[,Date:=as.yearmon(paste0(Year,"-",Month))]
  Forecast[is.na(CurrentOwned),CurrentOwned:=0]
  Forecast[is.na(SeasInv),SeasInv:=0]
  
  return(Forecast)
  
}


getInvChange=function(TargetsTMP,Forecast,AnaDate,OECNA){
  
  ###getInvChange calculates where the regional units that are added or removed each month must go to 
  ##(which District, Metro or Branch depending on the cat-class)
  ###Targets TMP argument is a table containing unconstrained inventory values for AnaDate month and constrained
  ###inventory values for all months previous to AnaDate
  ###Forecast is table containing forecast values for each cat class and each location

  ##Monthdays creates a column with the total number of days for the respective month
  TargetsTMP[,MonthDays:=days_in_month(as.Date(Date))]

  ##merge Targets TMP and Forecast
  TargetsTMP=merge(TargetsTMP[Date>=AnaDate],Forecast[Date>=AnaDate,.(CatClass,Location,Dist,Year,Month,Date,Forecast,CapTarget,Accuracy,OwnedTarget,SeasInv,Cost,Revenue)],
                   by=c('CatClass','Location','Year','Month','Date'))
  
  ###DiffSeas variable is the difference between regional constrained inventory and previous month owned inventory
  ###DiffSeas is total number of units to add to the region (when DiffSeas>0), or remove from the region (when DiffSeas<0)
  TargetsTMP[Date==AnaDate,DiffSeas:=max(SeasInv)-sum(CurrentOwned),by=.(CatClass,Date)]
  TargetsTMP[,DiffSeas:=max(DiffSeas,na.rm=TRUE),by=CatClass]
  TargetsTMP[Date!=AnaDate,CapTarget:=0,by=.(CatClass,Location,Date)]
  TargetsTMP[,CapTarget:=max(CapTarget,na.rm=TRUE),by=.(CatClass,Location)]
  InvChange=data.table(CC=1)
  InvChangeOut=NULL
  
  ###Initiate while loop
  ### While loop adds or removes units to each location based on marginal profit
  
  while(nrow(InvChange)>0){
    
    ### Calculate probability that unit in CurrentOwned variable unit will be on rent on the analyzed 
    ### 
    TargetsTMP[Cost<=Revenue,Proba:=ifelse(Dist=='Pois',
                                           1-ppois((CurrentOwned-1+(DiffSeas>0))/OECNA,Forecast),
                                           1-pnorm((CurrentOwned+(DiffSeas>0))/OECNA,mean=Forecast,sd=Forecast*(1-pmin(99,Accuracy)/100)))]
    
    TargetsTMP[Cost>Revenue,Proba:=ifelse(Dist=='Pois',
                                          1-ppois(0,Forecast),
                                          1-pnorm(0,mean=Forecast,sd=Forecast*(1-pmin(99,Accuracy)/100)))]
    
    ### Total Profit is equal to: Days in Month * (Revenue - Cost)*On Rent Probability
    TargetsTMP[Date==AnaDate,Profit:=MonthDays*(Revenue-Cost)*Proba]
    TargetsTMP[Date!=AnaDate,Profit:=0]
    TargetsTMP[Date>=AnaDate,Profit:=sum(Profit,na.rm = TRUE),by=.(CatClass,Location)]
    
    # TargetsTMP[Date==i,CurrMonthTarget:=OwnedTarget]
    # TargetsTMP[,CurrMonthTarget:=max(CurrMonthTarget,na.rm=TRUE),by=.(CatClass,Location)]
    
    ###When CurrentOwned is less than CapTarget assign profit=profit+100 Million
    ##this is to ensure that units are added to locations where currentowned is smaller than Cap Target
    TargetsTMP[,ProfitCap:=ifelse(CurrentOwned<CapTarget,1000000000+Profit,Profit)]
    
    ### Create AddInv table containing locations where units will be added
    AddInv=TargetsTMP[DiffSeas>0 & Date>=AnaDate & Profit>0]
    AddInv=AddInv[,.SD[which.max(ProfitCap)],by=.(CatClass,Month,Year)]
    AddInv[,Add:=1]
    
    ### Create  RemoveInv table containing locations where units will be removed
    RemoveInv=TargetsTMP[DiffSeas<0 & CurrentOwned>0 & Date>=AnaDate & CurrentOwned>CapTarget]
    RemoveInv=RemoveInv[,.SD[which.min(ProfitCap)],by=.(CatClass,Month,Year)]
    RemoveInv[,Remove:=1]
    
    InvChange=rbind(AddInv,RemoveInv,fill=TRUE)
    
    ###Merge TargetsTMP with InvChange table containing info of unit added/removed
    TargetsTMP=merge(TargetsTMP,InvChange[,.(CatClass,Location,Year,Month,Add,Remove)],
                     all.x = TRUE,by=c('CatClass','Location','Year','Month'))
    TargetsTMP[is.na(Add),Add:=0]
    TargetsTMP[is.na(Remove),Remove:=0]
    
    ##Update CurrentOwned field by adding/removing units contained in 'Add' or 'Remove' fields
    TargetsTMP[,CurrentOwned:=CurrentOwned+Add-Remove]
    
    ##Recalculate DiffSeas value for next iteration of while loop
    TargetsTMP[Date==AnaDate,DiffSeas:=max(SeasInv)-sum(CurrentOwned),by=.(CatClass,Date)]
    TargetsTMP[Date!=AnaDate,DiffSeas:=NA]
    TargetsTMP[,DiffSeas:=max(DiffSeas,na.rm=TRUE),by=CatClass]
    TargetsTMP[,`:=`(Add=NULL,Remove=NULL)]
    
    InvChange=InvChange[AnaDate==Date,.(Forecast=mean(Forecast),Proba=weighted.mean(Proba,MonthDays),Cost=mean(Cost),Revenue=mean(Revenue),
                           Profit=mean(Profit),TotalDays=sum(MonthDays),Add=mean(Add,na.rm=TRUE),Remove=mean(Remove,na.rm=TRUE)),by=.(CatClass,Date,Location,CurrentOwned)]
  #  InvChange$Date=as.Date(AnaDate)
    InvChangeOut=rbind(InvChangeOut,InvChange)
    
  #  print(nrow(InvChange))
  }
  
  ###Function returns a list containing  a table with data of units added or removed to each location (InvChangeOut)
  ### and a table with final constrained target inventory values in the AnaDate period
  
  return(list(InvChangeOut,TargetsTMP))
  
}


getInvMovesbyUnit=function(TargetsTMP,AnaDate,cost,OECNA){
  
  ### getInvMovesbyUnit calculates which units must be moved inside the Region for District/Metro and Metro/Metro
  ### this function assumes only one unit can be transported at a time between each O/D
  
  ###inputs:
  ###TargetsTMP is a table containing current owned inventory, forecast, accuracy, daily cost, daily revenue 
  ####for each cat-class and location
  ###AnaDate is the month for which the function calculates transfers
  ###cost is a table with transportation cost information between all posiible O/D pairs
  
  ###Create columns 'Need' and 'Excess' where Need represents the total number of units a cat-class can receive from transfers from other locations
  ### and 'Excess' represents the total number of units a cat-class can give away on transfers to other locations
  TargetsTMP[Date==AnaDate,TotRegInv:=sum(CurrentOwned),by=CatClass]
  TargetsTMP[Date==AnaDate,Need:=pmax(0,TotRegInv-CurrentOwned)]
  TargetsTMP[,TotRegInv:=NULL]
  TargetsTMP[Date==AnaDate,Excess:=pmin(0,CapTarget-CurrentOwned)]
  TargetsTMP[,`:=`(Need=max(Need,na.rm=TRUE),Excess=min(Excess,na.rm=TRUE)),by=.(CatClass,Location)]
  
  ###Merge TargetsTMP table with different variables selected to create Transfers table where
  ##variables ending in 'x' correspond to values at the origin and values ending in 'y' to values at the destination
  ## i.e. Column Forecast.x is forecast at origin, Forecast.y is forecast at destination
  Transfers=merge(TargetsTMP[,.(CatClass,Location,Dist,Year,Month,MonthDays,Forecast,Accuracy,OwnedTarget,CurrentOwned,Cost,Revenue,Date,Excess,CapTarget)],
                  TargetsTMP[,.(CatClass,Location,Dist,Year,Month,Forecast,Accuracy,OwnedTarget,CurrentOwned,Cost,Revenue,Date,Need,CapTarget)],by=c('CatClass','Month','Year','Date'),allow.cartesian = TRUE)
 
   ###Keep only rows where origin has excess units and destination has need for units
  Transfers=Transfers[Need>0 & Excess<0] 
  ###Add Transprotation Cost to Transfers table
  Transfers=merge(Transfers,cost[,.(OriginID,DestinationID,TransCost)],by.x=c('Location.x','Location.y'),by.y=c('OriginID','DestinationID'))
  Transfers[is.na(Need),Need:=0]
  Transfers[is.na(Excess),Excess:=0]
  
  ##Initialize tables
  MoveInv=data.table(CC=1)
  TargetsOut=NULL
  MoveInvOut=NULL
  
  
  #Execute while loop 
  #Loop executes while MoveInv table is not empty
  #MoveInv becomes empty when there are no more profitable transfers to execute
  
  while(nrow(MoveInv)>0){
    
    ###Proba.x is the probability that the unit to be removed will be on rent for the given month
    Transfers[Cost.x<=Revenue.x,Proba.x:=ifelse(Dist.x=='Pois',
                                                1-ppois((CurrentOwned.x-1)/OECNA,Forecast.x),
                                                1-pnorm((CurrentOwned.x)/OECNA,mean=Forecast.x,sd=Forecast.x*(1-pmin(99,Accuracy.x)/100)))]
    
    Transfers[Cost.x>Revenue.x,Proba.x:=ifelse(Dist.x=='Pois',
                                               1-ppois(0,Forecast.x),
                                               1-pnorm(0,mean=Forecast.x,sd=Forecast.x*(1-pmin(99,Accuracy.x)/100)))]
    
    ###Profit.x is the profit that will ceased to be gained from removing the unit
    Transfers[Date==AnaDate,Profit.x:=MonthDays*(Revenue.x-Cost.x)*Proba.x]
    Transfers[Date!=AnaDate,Profit.x:=0]
    Transfers[Date>=AnaDate,Profit.x:=sum(Profit.x,na.rm=TRUE),by=.(CatClass,Location.x,Location.y)]
    
    ###Proba.y is the probability that the unit to be added will be on rent for the given month
      Transfers[Cost.y<=Revenue.y,Proba.y:=ifelse(Dist.y=='Pois',
                                                1-ppois((CurrentOwned.y-1+(Need>0))/OECNA,Forecast.y),
                                                1-pnorm((CurrentOwned.y+(Need>0))/OECNA,mean=Forecast.y,sd=Forecast.y*(1-pmin(99,Accuracy.y)/100)))]
    
    Transfers[Cost.y>Revenue.y,Proba.y:=ifelse(Dist.y=='Pois',
                                               1-ppois(0,Forecast.y),
                                               1-pnorm(0,mean=Forecast.y,sd=Forecast.y*(1-pmin(99,Accuracy.y)/100)))] 
    
    ###Profit.y is the profit that will be gained from adding the unit
    Transfers[Date==AnaDate,Profit.y:=MonthDays*(Revenue.y-Cost.y)*Proba.y]
    Transfers[Date!=AnaDate,Profit.y:=0]
    Transfers[Date>=AnaDate,Profit.y:=sum(Profit.y,na.rm=TRUE),by=.(CatClass,Location.y,Location.x)]
  
      #  Transfers[,ProfitCap.y:=ifelse(CurrentOwned.y<CapTarget.y,1000000000+Profit.y,Profit.y)]
      #   Transfers[,ProfitCap.x:=ifelse(CurrentOwned.y<CapTarget.y,0,Profit.x)]
      
    ###Total profit from executing transfer is profit gained at destination minus profit that ceases to be gained
    ###at destination minus transportation cost
      Transfers[,TotalProfit:=Profit.y-Profit.x-TransCost]
      
      ###Consider transfers that would generate at least $10 of monthly profit
      Transfers=Transfers[Profit.y>=10]
      
      ###MoveInv table contains most profitable transfers for each cat-class in the given month
      MoveInv=Transfers[TotalProfit>10 & Need>0 & Excess<0,.SD[which.max(TotalProfit)],by=.(CatClass,Date)]
      MoveInv[,Move:=1]
      
    
    # Transfers=merge(Transfers,MoveInv[,.(CatClass,Location.x,Location.y,Year,Month,Date,Move)],
    #                by=c('CatClass','Location.x','Location.y','Year','Month','Date'),all.x=TRUE)
    #Transfers[is.na(Move),Move:=0]
    #Transfers[,`:=`(CurrentOwned.x=CurrentOwned.x-Move,CurrentOwned.y=CurrentOwned.y+Move,
    #                 Need=Need-Move,Excess=Excess+Move)]
      
      
    ####Add one unit of 'Excess' and remove one unit of CurrentOwned to catclasses at locations where units are being removed
    Transfers[paste0(CatClass,"_",Location.x) %in% paste0(MoveInv$CatClass,"_",MoveInv$Location.x),
              `:=`(Excess=Excess+1,CurrentOwned.x=CurrentOwned.x-1)]
    
    ####Remove one unit of 'Need' and add one unit of CurrentOwned to catclasses at locations where units are being added
    Transfers[paste0(CatClass,"_",Location.y) %in% paste0(MoveInv$CatClass,"_",MoveInv$Location.y),
              `:=`(Need=Need-1,CurrentOwned.y=CurrentOwned.y+1)]
   # print(nrow(MoveInv))
    
    ###Add MoveInv table to MoveInvOut table that will be outputted by the current function
    MoveInv=MoveInv[Date==AnaDate,.(Forecast.x=mean(Forecast.x),Proba.x=weighted.mean(Proba.x,MonthDays),Cost.x=mean(Cost.x),Revenue.x=mean(Revenue.x),
                       Profit.x=mean(Profit.x),
                       Forecast.y=mean(Forecast.y),Proba.y=weighted.mean(Proba.y,MonthDays),Cost.y=mean(Cost.y),Revenue.y=mean(Revenue.y),
                       Profit.y=mean(Profit.y),TotalDays=sum(MonthDays),TransCost=mean(TransCost),TotalProfit=mean(TotalProfit),Move=mean(Move)),by=.(CatClass,Location.x,Location.y,CurrentOwned.x,CurrentOwned.y,Date)]
  #  MoveInv$Date=as.Date(AnaDate)
    MoveInvOut=rbind(MoveInvOut,MoveInv)
    
    
  }
  
  # TransfersOut=unique(Transfers[,.(CatClass,Location.x,Year,Month,Date,CurrentOwned.x)])
  # TransfersIn=unique(Transfers[,.(CatClass,Location.y,Year,Month,Date,CurrentOwned.y)])
  # setnames(TransfersOut,c('Location.x','CurrentOwned.x'),c('Location','CurrentOwned'))
  # setnames(TransfersIn,c('Location.y','CurrentOwned.y'),c('Location','CurrentOwned'))
  # TargetsTMP=rbind(TargetsTMP[!paste0(CatClass,"_",Location) %in% paste0(TransfersIn$CatClass,"_",TransfersIn$Location) &
  #                               !paste0(CatClass,"_",Location) %in% paste0(TransfersOut$CatClass,"_",TransfersOut$Location),
  #                             .(CatClass,Location,Year,Month,Date,CurrentOwned)],
  #                  TransfersOut,TransfersIn)
  
  ###Add/Remove all transferred units to the TargesTMP object in order to generate output
  TargetsTMP=merge(TargetsTMP,MoveInvOut[Date==AnaDate,.(Remove=sum(Move)),by=.(CatClass,Location=Location.x)],by=c('CatClass','Location'),all=TRUE)
  TargetsTMP=merge(TargetsTMP,MoveInvOut[Date==AnaDate,.(Add=sum(Move)),by=.(CatClass,Location=Location.y)],by=c('CatClass','Location'),all=TRUE)
  TargetsTMP[is.na(Add),Add:=0]
  TargetsTMP[is.na(Remove),Remove:=0]
  
  TargetsTMP[,CurrentOwned:=CurrentOwned+Add-Remove]
  
  TargetsTMP=TargetsTMP[,.(CatClass,Location,Year,Month,Date,CurrentOwned)]
  
  ##Output list where first element contains table with transfer details 
  ##and second element contains table with owned inventory data after transfers execution
  
  return(list(MoveInvOut,TargetsTMP))
  
}



getInvMovesbyOD=function(TargetsTMP,AnaDate,cost,OECNA){
  
  ### getInvMovesbyOD calculates which units must be moved inside the Region for Metro/Local
  ### this function assumes several units of several different catclasses can be transported at a time between each O/D
  
  ###inputs:
  ###TargetsTMP is a table containing current owned inventory, forecast, accuracy, daily cost, daily revenue 
  ####for each cat-class and location
  ###AnaDate is the month for which the function calculates transfers
  ###cost is a table with transportation cost information between all posiible O/D pairs
  
  ###Create columns 'Need' and 'Excess' where Need represents the total number of units a cat-class can receive from transfers from other locations
  ### and 'Excess' represents the total number of units a cat-class can give away from transfers to other locations
  
  MoveInvOut=NULL
  MoveInvDetOut=NULL
  
  ##Idem as getInvMovesbyUnit function
  TargetsTMP[Date==AnaDate,TotRegInv:=sum(CurrentOwned),by=CatClass]
  TargetsTMP[Date==AnaDate,Need:=pmax(0,TotRegInv-CurrentOwned)]
  TargetsTMP[,TotRegInv:=NULL]
  TargetsTMP[Date==AnaDate,Excess:=pmin(0,CapTarget-CurrentOwned)]
  TargetsTMP[,`:=`(Need=max(Need,na.rm=TRUE),Excess=min(Excess,na.rm=TRUE)),by=.(CatClass,Location)]
  
  # TargetsTMP[Date==i,Need:=pmax(0,OwnedTarget-CurrentOwned)]
  # TargetsTMP[Date==i,Excess:=pmin(0,OwnedTarget-CurrentOwned)]
  # TargetsTMP[,`:=`(Need=max(Need,na.rm=TRUE),Excess=min(Excess,na.rm=TRUE)),by=.(CatClass,Location)]
  
  Transfers=merge(TargetsTMP[Excess<0,.(CatClass,Location,Dist,Year,Month,MonthDays,Forecast,Accuracy,OwnedTarget,CurrentOwned,Cost,Revenue,Date,Excess,CapTarget)],
                  TargetsTMP[Need>0,.(CatClass,Location,Dist,Year,Month,Forecast,Accuracy,OwnedTarget,CurrentOwned,Cost,Revenue,Date,Need,CapTarget)],by=c('CatClass','Month','Year','Date'),allow.cartesian = TRUE)
  Transfers=Transfers[Need>0 & Excess<0] 
  Transfers=merge(Transfers,cost[,.(OriginID,DestinationID,TransCost)],by.x=c('Location.x','Location.y'),by.y=c('OriginID','DestinationID'))
  Transfers[is.na(Need),Need:=0]
  Transfers[is.na(Excess),Excess:=0]
  
  MoveInv=data.table(CC=1)
  
  ###Current function allows for several units to be transferred simultaneously
  ###Column 'Transfers' contains total units that can be transferred (minimum between Need and Excess) between O/D for
  ###a specific catclass
  Transfers[,Transfers:=pmin(Need,-Excess)]
  
  ###Create TransUnits table where each row
  ### i.e. if Transfers table indicates three units for cat-class X can be transferred from A to B, then 
  ### TransUnits contains three rows, each representing one of the units for cat-class X that can be transferred from A to B
  TransUnits=Transfers[rep(1:.N,Transfers)][,Units:=1:.N,by=.(CatClass,Location.x,Location.y,Month,Year,Date)]
  
  
  ##Execute while loop just like in getInvMovesbyUnit function
  
  while(nrow(MoveInv)>0){
    
    ###Unit.x column is the unit being removed, Unit.y the unit being added to the
    ##respective cat-class location
    TransUnits[,Unit.x:=CurrentOwned.x-Units]
    TransUnits[,Unit.y:=CurrentOwned.y+Units]
    ####Keep only transfers where units are needed at the destination and are in excess at the origin
    TransUnits=TransUnits[Need>0 & Excess<0]
    ###Probabilities and profits calculated the same way as in getInvMovesbyUnit function
    TransUnits[Cost.x<=Revenue.x,Proba.x:=ifelse(Dist.x=='Pois',
                                                 1-ppois(Unit.x/OECNA,Forecast.x),
                                                 1-pnorm((Unit.x+1)/OECNA,mean=Forecast.x,sd=Forecast.x*(1-pmin(99,Accuracy.x)/100)))]
    
    TransUnits[Cost.x>Revenue.x,Proba.x:=ifelse(Dist.x=='Pois',
                                                1-ppois(0,Forecast.x),
                                                1-pnorm(0,mean=Forecast.x,sd=Forecast.x*(1-pmin(99,Accuracy.x)/100)))]
    
    TransUnits[Date==AnaDate,Profit.x:=MonthDays*(Revenue.x-Cost.x)*Proba.x]
    TransUnits[Date!=AnaDate,Profit.x:=0]
    TransUnits[Date>=AnaDate,Profit.x:=sum(Profit.x),by=.(CatClass,Location.x,Location.y,Units)]
    TransUnits[Profit.x<0,Profit.x:=0]
    
    TransUnits[Cost.y<=Revenue.y,Proba.y:=ifelse(Dist.y=='Pois',
                                                 1-ppois((Unit.y-1)/OECNA,Forecast.y),
                                                 1-pnorm((Unit.y)/OECNA,mean=Forecast.y,sd=Forecast.y*(1-pmin(99,Accuracy.y)/100)))]
    
    TransUnits[Cost.y>Revenue.y,Proba.y:=ifelse(Dist.y=='Pois',
                                                1-ppois(0,Forecast.y),
                                                1-pnorm(0,mean=Forecast.y,sd=Forecast.y*(1-pmin(99,Accuracy.y)/100)))]
    
    TransUnits[Date==AnaDate,Profit.y:=MonthDays*(Revenue.y-Cost.y)*Proba.y]
    TransUnits[Date!=AnaDate,Profit.y:=0]
    TransUnits[Date>=AnaDate,Profit.y:=sum(Profit.y),by=.(CatClass,Location.x,Location.y,Units)]
    TransUnits[Profit.y<0,Profit.y:=0]
    
    # Transfers[,ProfitCap.y:=ifelse(CurrentOwned.y<CapTarget.y,1000000000+Profit.y,Profit.y)]
    # Transfers[,ProfitCap.x:=ifelse(CurrentOwned.y<CapTarget.y,0,Profit.x)]
    
    ##Avoid executing transfers that would remove units below the CapTarget level
    TransUnits=TransUnits[Unit.x>=CapTarget.x]
    ###Make Profit equal to zero whenever Units>Transfers
    TransUnits[Units>Transfers,`:=`(Profit.x=0,Profit.y=0)]
    ###Keep only transfers where profit at destination is 10$ higher than profit at origin
    TransUnits=TransUnits[(Profit.y>(Profit.x+10))]
    
    
      ###TransTot is a table containing the total profit of executing all transfers between an Origin-Destination pair
      TransTot=TransUnits[Date==AnaDate,.(Date=min(Date),TotalTransfers=.N,TotalProfit=sum(Profit.y)-sum(Profit.x),TransCost=mean(TransCost)),
                          by=.(CatClass,Location.x,Location.y)]
      
      ####TotalODProfit is the column containing the value of total profit from executing all transfers between
      ###an O-D pair considering transportation cosr 
      TransTot[,TotalODProfit:=sum(TotalProfit)-mean(TransCost),by=.(Location.x,Location.y)]
      
      ###MoveInv table contains the transfers for the most profitable O-D pair
      MoveInv=TransTot[TotalODProfit>10 & TotalODProfit==max(TransTot$TotalODProfit)]   
    
    ###If MoveInv table contains more than one O-D pair, keep the only the first one
    if (nrow(unique(MoveInv[,.(Location.x,Location.y)]))>1) MoveInv=MoveInv[Location.x==MoveInv$Location.x[1] & Location.y==MoveInv$Location.y[1]]
    
      ##MoveInvDet contains the specific details (forecast, cost, revenue, profit) for the most profitable O-D transfer
      MoveInvDet=merge(MoveInv,TransUnits,by=c('CatClass','Date','Location.x','Location.y'))
      
      ###Bind MoveInv and MoveInvDet for output purposes
      MoveInvOut=rbind(MoveInvOut,MoveInv[Date==AnaDate],fill=TRUE)
      MoveInvDetOut=rbind(MoveInvDetOut,MoveInvDet[Date==AnaDate],fill=TRUE)
      
    ##Update TransUnits table by adding the transfers in and transfers out contained in MoveInv table  
    TransUnits=merge(TransUnits,MoveInv[,.(CatClass,Location.x,TotalTransfers)],all.x=TRUE,by=c('CatClass','Location.x'))
    setnames(TransUnits,'TotalTransfers','TransfersOut')
    TransUnits=merge(TransUnits,MoveInv[,.(CatClass,Location.y,TotalTransfers)],all.x=TRUE,by=c('CatClass','Location.y'))
    setnames(TransUnits,'TotalTransfers','TransfersIn')
    
    ##Update CurrentOwned and Need,Excess columns with data from MoveInv transfers
    TransUnits[is.na(TransfersOut),TransfersOut:=0]
    TransUnits[is.na(TransfersIn),TransfersIn:=0]
    TransUnits[,`:=`(CurrentOwned.x=CurrentOwned.x-TransfersOut,
                     CurrentOwned.y=CurrentOwned.y+TransfersIn,
                     Excess=Excess+TransfersOut,
                     Need=Need-TransfersIn)]
    
    ###Remove all data in TransUnits where O-D pair is the one contained in MoveInv table
    TransUnits=TransUnits[!paste0(CatClass,"_",Location.y) %in% paste0(MoveInv$CatClass,"_",MoveInv$Location.x)]
    TransUnits=TransUnits[!paste0(CatClass,"_",Location.x) %in% paste0(MoveInv$CatClass,"_",MoveInv$Location.y)]
    
    # TransUnits=TransUnits[paste0(Location.x,"_",Location.y) %in% paste0(MoveInv$Location.x,"_",MoveInv$Location.y),
    #                       `:=`(Need=0,Excess=0)]
    
    #Update TransUnits for next iteration of the while loop
    TransUnits[,`:=`(TransfersIn=NULL,TransfersOut=NULL)]
    TransUnits[,Transfers:=pmin(Need,-Excess)]
    
   # print(sum(unique(MoveInv$TotalODProfit)))
    
  }
  
  
  # TransUnits.x=unique(TransUnits[,.(CatClass,Location.x,Month,Year,Date,CurrentOwned.x)] )       
  # setnames(TransUnits.x,c('Location.x','CurrentOwned.x'),c('Location','CurrentOwned')) 
  # TransUnits.y=unique(TransUnits[,.(CatClass,Location.y,Month,Year,Date,CurrentOwned.y)]  )      
  # setnames(TransUnits.y,c('Location.y','CurrentOwned.y'),c('Location','CurrentOwned')) 
  # 
  # TargetsTMP=rbind(TargetsTMP[!paste0(CatClass,"_",Location) %in% c(paste0(TransUnits.x$CatClass,"_",TransUnits.x$Location),
  #                                                                  paste0(TransUnits.y$CatClass,"_",TransUnits.y$Location)),
  #                             .(CatClass,Location,Year,Month,Date,CurrentOwned)],
  #                  TransUnits.x,TransUnits.y)
  
  ###Update CurrentOwned columns in the TargetsTMP table to reflect transfers executed in AnaDate month
  ##
  TransOut=MoveInvOut[Date==AnaDate,.(TransfersOut=sum(TotalTransfers)),by=.(CatClass,Location=Location.x)]
  TransIn=MoveInvOut[Date==AnaDate,.(TransfersIn=sum(TotalTransfers)),by=.(CatClass,Location=Location.y)]
  Transfers=merge(TransOut,TransIn,by=c('CatClass','Location'),all=TRUE)
  
  TargetsTMP=merge(TargetsTMP,Transfers,by=c('CatClass','Location'),all=TRUE)
  
  TargetsTMP[is.na(TransfersOut),TransfersOut:=0]
  TargetsTMP[is.na(TransfersIn),TransfersIn:=0]
  
  TargetsTMP[Date>=i,CurrentOwned:=CurrentOwned-TransfersOut+TransfersIn]
  
  TargetsTMP=TargetsTMP[,.(CatClass,Location,Year,Month,Date,CurrentOwned)]
  
  ###Function will output general information of transfers in first element
  ### Updated TargetsTMP object in second element
  ### and detail of all executed transfers in third element
  
  
  return(list(MoveInvOut,TargetsTMP,MoveInvDetOut))
  
}

# 
# 
# 
# getSOPMoves=function(TargetsTMP,AnaDate,cost,SC){
#   
#   MoveInvOut=NULL
#   MoveInvDetOut=NULL
#   
#   TargetsTMP[Date==AnaDate,TotRegInv:=sum(CurrentOwned),by=CatClass]
#   TargetsTMP[Date==AnaDate,Need:=pmax(0,TotRegInv-CurrentOwned)]
#   TargetsTMP[,TotRegInv:=NULL]
#   TargetsTMP[Date==AnaDate,Excess:=pmin(0,CapTarget-CurrentOwned)]
#   TargetsTMP[,`:=`(Need=max(Need,na.rm=TRUE),Excess=min(Excess,na.rm=TRUE)),by=.(CatClass,Location)]
#   
#   # TargetsTMP[Date==i,Need:=pmax(0,OwnedTarget-CurrentOwned)]
#   # TargetsTMP[Date==i,Excess:=pmin(0,OwnedTarget-CurrentOwned)]
#   # TargetsTMP[,`:=`(Need=max(Need,na.rm=TRUE),Excess=min(Excess,na.rm=TRUE)),by=.(CatClass,Location)]
#   
#   Transfers=merge(TargetsTMP[Excess<0,.(CatClass,Location,Dist,Year,Month,MonthDays,Forecast,Accuracy,OwnedTarget,CurrentOwned,Cost,Revenue,Date,Excess,CapTarget)],
#                   TargetsTMP[Need>0,.(CatClass,Location,Dist,Year,Month,Forecast,Accuracy,OwnedTarget,CurrentOwned,Cost,Revenue,Date,Need,CapTarget)],by=c('CatClass','Month','Year','Date'),allow.cartesian = TRUE)
#   Transfers=Transfers[Need>0 & Excess<0] 
#   Transfers=merge(Transfers,cost[,.(OriginID,DestinationID,TransCost)],by.x=c('Location.x','Location.y'),by.y=c('OriginID','DestinationID'))
#   Transfers[is.na(Need),Need:=0]
#   Transfers[is.na(Excess),Excess:=0]
#   
#   MoveInv=data.table(CC=1)
#   
#   Transfers[,Transfers:=pmin(Need,-Excess)]
#   
#   TransUnits=Transfers[rep(1:.N,Transfers)][,Units:=1:.N,by=.(CatClass,Location.x,Location.y,Month,Year,Date)]
#   
#   
#     TransUnits[,Unit.x:=CurrentOwned.x-Units]
#     TransUnits[,Unit.y:=CurrentOwned.y+Units]
#     TransUnits=TransUnits[Need>0 & Excess<0]
#     TransUnits[Cost.x<=Revenue.x,Proba.x:=ifelse(Dist.x=='Pois',
#                                                  1-ppois(Unit.x/OECNA,Forecast.x),
#                                                  1-pnorm((Unit.x+1)/OECNA,mean=Forecast.x,sd=Forecast.x*(1-pmin(99,Accuracy.x)/100)))]
#     
#     TransUnits[Cost.x>Revenue.x,Proba.x:=ifelse(Dist.x=='Pois',
#                                                 1-ppois(0,Forecast.x),
#                                                 1-pnorm(0,mean=Forecast.x,sd=Forecast.x*(1-pmin(99,Accuracy.x)/100)))]
#     
#     TransUnits[Date==AnaDate,Profit.x:=MonthDays*(Revenue.x-Cost.x)*Proba.x]
#     TransUnits[Date!=AnaDate,Profit.x:=0]
#     TransUnits[Date>=AnaDate,Profit.x:=sum(Profit.x),by=.(CatClass,Location.x,Location.y,Units)]
#     TransUnits[Profit.x<0,Profit.x:=0]
#     
#     TransUnits[Cost.y<=Revenue.y,Proba.y:=ifelse(Dist.y=='Pois',
#                                                  1-ppois((Unit.y-1)/OECNA,Forecast.y),
#                                                  1-pnorm((Unit.y)/OECNA,mean=Forecast.y,sd=Forecast.y*(1-pmin(99,Accuracy.y)/100)))]
#     
#     TransUnits[Cost.y>Revenue.y,Proba.y:=ifelse(Dist.y=='Pois',
#                                                 1-ppois(0,Forecast.y),
#                                                 1-pnorm(0,mean=Forecast.y,sd=Forecast.y*(1-pmin(99,Accuracy.y)/100)))]
#     
#     TransUnits[Date==AnaDate,Profit.y:=MonthDays*(Revenue.y-Cost.y)*Proba.y]
#     TransUnits[Date!=AnaDate,Profit.y:=0]
#     TransUnits[Date>=AnaDate,Profit.y:=sum(Profit.y),by=.(CatClass,Location.x,Location.y,Units)]
#     TransUnits[Profit.y<0,Profit.y:=0]
#     
#     # Transfers[,ProfitCap.y:=ifelse(CurrentOwned.y<CapTarget.y,1000000000+Profit.y,Profit.y)]
#     # Transfers[,ProfitCap.x:=ifelse(CurrentOwned.y<CapTarget.y,0,Profit.x)]
#     
#     TransUnits=TransUnits[Unit.x>=CapTarget.x]
#     TransUnits[Units>Transfers,`:=`(Profit.x=0,Profit.y=0)]
#     
#     if (SC!='Metro/Local'){
#     TransUnits[,TotalProfit:=Profit.y-Profit.x-TransCost]
#     TransUnits=TransUnits[TotalProfit>0 & Date==AnaDate]
#     
#     } else {
#       
#       TransUnits=TransUnits[Date==AnaDate & Profit.y>Profit.x]
#       TransUnits[,`:=`(TotalProfit=sum(Profit.y)-sum(Profit.x)-mean(TransCost)),
#                           by=.(CatClass,Date,Location.x,Location.y)]
#       
#       TransUnits=TransUnits[TotalProfit>0 & Date==AnaDate]
#       
#       
#     }
#     
#     TransUnits[,Transfers:=NULL]
#     TransUnits[,Transfers:=.N,by=.(CatClass,Date,Location.x,Location.y)]
#     
# #    TransUnits=TransUnits[(Profit.y>(Profit.x+10))]
#     return(TransUnits)
#     
#     
#     #   TransUnitsSub=TransUnits[Profit.y>Profit.x+10]
#     
#     
#   }
#   
#   

getSOPMovesUnc=function(TargetsTransf,AnaDate,cost,SC,OECNA){
  
  ###getSOPMovesUnc outputs a table with the profitability of all possible profitable transfers
  ###for all cat-classes 
  ###Input TargetsTransf is equivalent to TargetsTMP in getInvMovesbyUnit function
  ##SC input is the respective supply chain being analyzed ('District/Metro','Metro/Metro', or 'Metro/Local')
   
  
  MoveInvOut=NULL
  MoveInvDetOut=NULL
  
  TargetsTMP=copy(TargetsTransf)
  TargetsTMP[Date==AnaDate,TotRegInv:=sum(CurrentOwned),by=CatClass]
  TargetsTMP[Date==AnaDate,Need:=pmax(0,TotRegInv-CurrentOwned)]
  TargetsTMP[,TotRegInv:=NULL]
  TargetsTMP[Date==AnaDate,Excess:=pmin(0,CapTarget-CurrentOwned)]
  TargetsTMP[,`:=`(Need=max(Need,na.rm=TRUE),Excess=min(Excess,na.rm=TRUE)),by=.(CatClass,Location)]
  
  # TargetsTMP[Date==i,Need:=pmax(0,OwnedTarget-CurrentOwned)]
  # TargetsTMP[Date==i,Excess:=pmin(0,OwnedTarget-CurrentOwned)]
  # TargetsTMP[,`:=`(Need=max(Need,na.rm=TRUE),Excess=min(Excess,na.rm=TRUE)),by=.(CatClass,Location)]
  
  Transfers=merge(TargetsTMP[Excess<0,.(CatClass,Location,Dist,Year,Month,MonthDays,Forecast,Accuracy,OwnedTarget,CurrentOwned,Cost,Revenue,Date,Excess,CapTarget)],
                  TargetsTMP[Need>0,.(CatClass,Location,Dist,Year,Month,Forecast,Accuracy,OwnedTarget,CurrentOwned,Cost,Revenue,Date,Need,CapTarget)],by=c('CatClass','Month','Year','Date'),allow.cartesian = TRUE)
  Transfers=Transfers[Need>0 & Excess<0] 
  Transfers=merge(Transfers,cost[,.(OriginID,DestinationID,TransCost)],by.x=c('Location.x','Location.y'),by.y=c('OriginID','DestinationID'))
  Transfers[is.na(Need),Need:=0]
  Transfers[is.na(Excess),Excess:=0]
  
  MoveInv=data.table(CC=1)
  
  Transfers[,Transfers:=pmin(Need,-Excess)]
  
  TransUnits=Transfers[rep(1:.N,Transfers)][,Units:=1:.N,by=.(CatClass,Location.x,Location.y,Month,Year,Date)]
  
  
  TransUnits[,Unit.x:=CurrentOwned.x-Units]
  TransUnits[,Unit.y:=CurrentOwned.y+Units]
  TransUnits=TransUnits[Need>0 & Excess<0]
  TransUnits[Cost.x<=Revenue.x,Proba.x:=ifelse(Dist.x=='Pois',
                                               1-ppois(Unit.x/OECNA,Forecast.x),
                                               1-pnorm((Unit.x+1)/OECNA,mean=Forecast.x,sd=Forecast.x*(1-pmin(99,Accuracy.x)/100)))]
  
  TransUnits[Cost.x>Revenue.x,Proba.x:=ifelse(Dist.x=='Pois',
                                              1-ppois(0,Forecast.x),
                                              1-pnorm(0,mean=Forecast.x,sd=Forecast.x*(1-pmin(99,Accuracy.x)/100)))]
  
  TransUnits[Date==AnaDate,Profit.x:=MonthDays*(Revenue.x-Cost.x)*Proba.x]
  TransUnits[Date!=AnaDate,Profit.x:=0]
  TransUnits[Date>=AnaDate,Profit.x:=sum(Profit.x),by=.(CatClass,Location.x,Location.y,Units)]
  TransUnits[Profit.x<0,Profit.x:=0]
  
  TransUnits[Cost.y<=Revenue.y,Proba.y:=ifelse(Dist.y=='Pois',
                                               1-ppois((Unit.y-1)/OECNA,Forecast.y),
                                               1-pnorm((Unit.y)/OECNA,mean=Forecast.y,sd=Forecast.y*(1-pmin(99,Accuracy.y)/100)))]
  
  TransUnits[Cost.y>Revenue.y,Proba.y:=ifelse(Dist.y=='Pois',
                                              1-ppois(0,Forecast.y),
                                              1-pnorm(0,mean=Forecast.y,sd=Forecast.y*(1-pmin(99,Accuracy.y)/100)))]
  
  TransUnits[Date==AnaDate,Profit.y:=MonthDays*(Revenue.y-Cost.y)*Proba.y]
  TransUnits[Date!=AnaDate,Profit.y:=0]
  TransUnits[Date>=AnaDate,Profit.y:=sum(Profit.y),by=.(CatClass,Location.x,Location.y,Units)]
  TransUnits[Profit.y<0,Profit.y:=0]
  
  # Transfers[,ProfitCap.y:=ifelse(CurrentOwned.y<CapTarget.y,1000000000+Profit.y,Profit.y)]
  # Transfers[,ProfitCap.x:=ifelse(CurrentOwned.y<CapTarget.y,0,Profit.x)]
  
  TransUnits=TransUnits[Unit.x>=CapTarget.x]
  TransUnits[Units>Transfers,`:=`(Profit.x=0,Profit.y=0)]
  
  if (SC!='Metro/Local'){
    TransUnits[,TotalProfit:=Profit.y-Profit.x-TransCost]
    TransUnits=TransUnits[TotalProfit>0 & Date==AnaDate]
    
  } else {
    
    TransUnits=TransUnits[Date==AnaDate & Profit.y>Profit.x]
    TransUnits[,`:=`(TotalProfit=sum(Profit.y)-sum(Profit.x)-mean(TransCost)),
               by=.(CatClass,Date,Location.x,Location.y)]
    
    TransUnits=TransUnits[TotalProfit>0 & Date==AnaDate]
    
    
  }
  
  TransUnits[,Transfers:=NULL]
  TransUnits[,Transfers:=.N,by=.(CatClass,Date,Location.x,Location.y)]
  
  #    TransUnits=TransUnits[(Profit.y>(Profit.x+10))]
  return(TransUnits)
  
  
  #   TransUnitsSub=TransUnits[Profit.y>Profit.x+10]
  
  
}

