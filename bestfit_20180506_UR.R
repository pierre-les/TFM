bestfit = function(x, test_periods = 12, lead_time = 1:4,minObs=3,stepSize=1,maxObs=Inf,methods=c('ses','des','tes','hw','croston','arima','sma','wma','lr'),
                   outliers=FALSE,transformation='none',bfMetric='MAPE',FcstHor=NULL,FcstbyHor=FALSE,wmaOrder=3,smaOrder=3,xreg=NULL){
  
  
  ####bestfit function generates backcast, and accuracy metrics for a time-series object x
  ####Arguments:
  ### test_periods: number of backcasting periods to test
  ### lead_time: forecast lag for backcast. can be a single number, or a vector. If it's a vector it'll test the multiple lags specified
  ### minObs: minimum observations necessary to generate a backcast
  ### maxObs: maximum number of observations to be used in backcasting:
  ### methods: vector containing name of methods to be used
  ### outliers: boolean specifying whether outlier treatment should be applied
  ### transformation: 'none' if no data transformation is to be applied. Either 'log1p' or 'boxcox' if transformation is to be applied
  ### bfMetric: accuracy metric used to choose best fit algorithm. Options are 'MAE' or 'MAPE' 
  ### FcstHor: Number of forecast periods to generate using ebst fit algorihtm. NULL if no forecast has to be generated.
  ### FcstbyHor: TRUE if a different algorihtm can be used for each forecast period, FALSE if not
  ### wmaOrder: order of WMA algorithm
  ### smaOrder: order of SMA algorithm
  ### xreg: vector containing time-series of external data. Only to be used with algorithms allowed use of external data.
  
  
  
  library(data.table)
  library(forecast)
  library(strucchange)
  library(lubridate)
  library(randomForest)
  library(forecTheta)
#  library(prophet)
  library(glmnet)
  

  ##Forecasting functions for different methods
  ### x is time-series, m is order of sma, wma if methods are to be used,
  ### h id forecast horizon
  
  
  #TBATS
  tbatsForecast <- function(x,m, h) {
    fcst=try(
      {
        fit <- forecast::tbats(x)
        forecast::forecast(fit, h=h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  ##TBATS with damped trend
  tbatstrendForecast <- function(x,m, h) {
    fcst=try(
      {
        fit <- forecast::tbats(x,use.trend = TRUE,use.damped.trend = TRUE)
        forecast::forecast(fit, h=h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  ##TBATS without trend component
  tbatsnotrendForecast <- function(x,m, h) {
    fcst=try(
      {
        fit <- forecast::tbats(x,use.trend = FALSE,seasonal.periods = frequency(x))
        forecast::forecast(fit, h=h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  #Exponential smoothing
  etsForecast <- function(x,m, h) {
    fcst=try(
      {
        fit <- forecast::ets(x)
        forecast::forecast(fit, h=h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  ##Naive
  naiveForecast <- function(x,m, h) {
    fcst=try(
      {
        # fit <- forecast::naive(x)
        
        forecast::naive(x, h=h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  ##Seasonal naive (same as last year)
  snaiveForecast <- function(x,m, h) {
    fcst=try(
      {
        # fit <- forecast::naive(x)
        
        forecast::forecast(snaive(x), h=h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  ###STLM (seasonal decomposition using loess)
  stlmForecast <- function(x,m, h) {
    fcst=try(
      {   
        
        fit <- forecast::stlf(x)
        forecast::forecast(fit, h=h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  ###MSTL (multiple seasonal decomposition)
  mstlForecast <- function(x,m, h) {
    fcst=try(
      {   
        
        fit <- forecast::mstl(x)
        forecast::forecast(fit, h=h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  ##ARIMA
  arimaForecast <- function(x,m,h) {
    fcst=try(
      { 
        fit <- forecast::auto.arima(x,D=1,max.d=1)
        forecast::forecast(fit, h=h)$mean 
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error'){
      fcst=try(
        { 
          
          fit <- forecast::auto.arima(x)
          forecast::forecast(fit, h=h)$mean }
        ,silent=TRUE)
    } 
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
  }
  
  ###ARIMA with external regressors
  arimaDecForecast <- function(x,m,h,xreg) {
    fcst=try(
      { 
        xreg=ts(xreg[1:length(x)],frequency = frequency(x))
        xreg=try(decompose(xreg,type='multiplicative')$seasonal,silent=TRUE)
        xregFut=try(xreg[(length(xreg)+1-frequency(x)):(length(xreg)+h-frequency(x))],silent=TRUE)
        
        if (class(xreg)=='try-error'){
          fit <- forecast::auto.arima(x, seasonal.test = 'seas',max.d=1)
          forecast::forecast(fit, h=h)$mean 
          
        } else {
        fit <- forecast::auto.arima(x, seasonal.test = 'seas',max.d=1,xreg=xreg)
        forecast::forecast(fit, h=h,xreg=xregFut)$mean 
        }
      }
      ,silent=TRUE)
    

    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
  }
  

  ### Simple weighted moving average
  smaForecast = function(x,m = smaOrder,h){
    fcst=try(
      { 
        
        temp = mean(x[(length(x)-m+1):length(x)])
        y = x
        for(i in 1:h){
          temp = mean(y[(length(y)-m+1):length(y)])
          y = c(y,temp) 
        }
        return(y[(length(x)+1):(length(x)+h)])
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
    
  }
  
  
  ### Rolling weighted moving average 
  wmaForecast = function(x,m = wmaOrder,h,wts=1:min(m,length(x))){
    fcst=try(
      { 
        
        wma = function(x,m = m, wts=1:min(m,length(x))){
          
          y = sum(x[max(1,length(x)-m+1):length(x)]*wts)/cumsum(1:min(m,length(x)))[min(m,length(x))]
          return(y)
        }
        y = x
        for(i in 1:h){ 
          temp = wma(y,m)
          y = c(y,temp)
        }
        return(y[(length(x)+1):(length(x)+h)])
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
  }
  
  ##Theta
  thetaForecast <- function(x,m,h) {
    fcst=try(
      { 
        
        forecTheta::stheta(x, h)$mean
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error' | length(fcst)==0) fcst=rep(0,h)
    
    return(fcst)
  }
  
  ##Theta with external seasonal component
  thetaDecForecast <- function(x,m,h,xreg) {
    fcst=try(
      { 
     #   xreg=ts(xreg,frequency=frequency(x))
        
        ####extract seasonal component of xreg
        xreg=try(decompose(x,type='additive')$seasonal,silent=TRUE)
        
        ###if not able to extract seasonal component 
        #### forecast theta without seasonality
        
        if (class(xreg)=='try-error' | any(is.na(xreg))){
          forecTheta::stheta(x, h)$mean
        }else {
          
        ##else add seasonal component to theta forecast
        xreg=xreg[(length(xreg)+1-frequency(x)):(length(xreg))]
        if (h>frequency(x)) xreg=rep(xreg,ceiling(h/frequency(x)))[1:h]
        forecTheta::stheta(x, h)$mean+xreg[1:h]
        }
        
  
      }
      ,silent=TRUE)
    
    if(class(fcst)=='try-error' | length(fcst)==0) fcst=rep(0,h)
    
    return(fcst)
  }
  
  ##Random Forest
  randomForestForecast <- function(x,m,h) {
    fcst=try(
      { 
        freq=frequency(x)
        lags<-4
        xframe <- data.table(x=as.numeric(x),id=1:length(x),lag1=c(rep(NA, 1), x)[1 : length(x)]
                             ,lag2=c(rep(NA, 2), x)[1 : length(x)],lag3=c(rep(NA, 3), x)[1 : length(x)],lag4=c(rep(NA, 4), x)[1 : length(x)])
        xframe[,s:=(id%%freq)+1]
        tail <- tail(xframe,4)
        xframe2 <- xframe[id>4]
        rf <- randomForest::randomForest(formula = xframe2$x ~ id + s + lag1 + lag2 + lag3 + lag4, data=xframe2, keep.forest = TRUE, keep.inbag=TRUE)
        test <- data.table(id2 = 1, id=length(x)+1,s=((length(x)+1)%%freq)+1, lag1 = tail[4]$x,lag2 = tail[3]$x,lag3 = tail[2]$x,lag4 = tail[1]$x)
        fcst2 <- NULL
        for(j in 1:h){
          rf2 <- randomForestCI::randomForestInfJack(rf, newdata=test[j])$y.hat
          if(j==1)
            test2 <- data.table(id2 = j+1, id=length(x)+j+1,s=((length(x)+1+j)%%freq)+1, lag1=rf2,lag2  = tail[4]$x,lag3  = tail[3]$x,lag4 = tail[2]$x)
          if(j>1){
            test2 <- data.table(id2 = j+1, id=length(x)+j+1,s=((length(x)+1+j)%%freq)+1, lag1  =rf2,lag2  = test[j]$lag1 ,lag3  = test[j]$lag2,lag4 = test[j]$lag3 )
          }
          test <- rbind(test,test2)
          fcst2 <- rbind(fcst2,cbind(id2=j,id=length(x)+j+1,rf2))
        }
        fcst2[,3]
      },silent=FALSE)
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
  }
  
 ## ARIMA with external regressors
  arimaxregForecast <- function(x,m,h,xreg) {
    fcst=try(
      { 
        
        fit <- forecast::auto.arima(x,xreg=xreg[1:length(x)], seasonal.test = 'seas',max.d=1)
        forecast::forecast(fit, h=h,xreg=xreg[(1+length(x)):(h+length(x))])$mean
      }
      ,silent=TRUE)
    
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
  }
  
  ##GLMNET with external variables (Lasso regression)
  glmnetDecForecast <- function(x,m,h,xreg){
    fcst=try(
      { 
        xreg=ts(xreg[1:length(x)],frequency = frequency(x))
        xreg=try(decompose(xreg,type='multiplicative')$seasonal,silent=TRUE)
        
        if(class(xreg)=='error' | any(is.na(xreg))){
          xreg=rep(0,length(x))
          xregFut=xreg[(length(xreg)+1-frequency(x)):length(xreg)]
          xreg=c(xreg,xregFut)
          
        } else {
        xregFut=xreg[(length(xreg)+1-frequency(x)):length(xreg)]
        xreg=c(xreg,xregFut)
        }
        
        freq=frequency(x)
        lags<-4
        xframe <- data.table(x=as.numeric(x),id=1:length(x),lag1=c(rep(NA, 1), x)[1 : length(x)]
                             ,lag2=c(rep(NA, 2), x)[1 : length(x)],lag3=c(rep(NA, 3), x)[1 : length(x)],lag4=c(rep(NA, 4), x)[1 : length(x)],
                             seas=c(rep(NA, freq), x)[1 : length(x)])
    #    xframe[,s:=(id%%freq)+1]
        tail <- tail(xframe,freq)
        xframe2 <- xframe[id>freq]
        
        xreg2=xreg[(length(x)-nrow(xframe2)+1):length(x)]
        var=cbind(xframe2,xreg2)
        
        fit=glmnet(x=as.matrix(var[,3:ncol(var)]),
                   y=x[(length(x)-nrow(xframe2)+1):length(x)],
                   lambda=cv.glmnet(x=as.matrix(var[,3:ncol(var)]),
                                    y=x[(length(x)-nrow(xframe2)+1):length(x)])$lambda.min)
        
        fcst=NULL
        for (i in 1:h){
          indx=c(1,3:5)
          if (i==1){
          futxframe=var[nrow(xframe2),..indx]
          } else {
          futxframe[,2:4]=futxframe[,1:3]
          futxframe$x=fcst[i-1]
          
          }
          
          futxframe$seas=xframe2[nrow(xframe2)-freq+i]$x
          futxframe$xreg=xreg[length(x)+i]

          fcst[i]=predict(fit,newx=as.matrix(futxframe))[,1]
        }
        
        fcst
        
      }
      ,silent=TRUE)
    
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
  }
  
  ##GLMNET (Lasso regression)
  glmnetForecast <- function(x,m,h){
    fcst=try(
      { 
        freq=frequency(x)
        lags<-4
        xframe <- data.table(x=as.numeric(x),id=1:length(x),lag1=c(rep(NA, 1), x)[1 : length(x)]
                             ,lag2=c(rep(NA, 2), x)[1 : length(x)],lag3=c(rep(NA, 3), x)[1 : length(x)],lag4=c(rep(NA, 4), x)[1 : length(x)],
                             seas=c(rep(NA, freq), x)[1 : length(x)])
        #    xframe[,s:=(id%%freq)+1]
        tail <- tail(xframe,freq)
        xframe2 <- xframe[id>freq]
        
        var=xframe2
        
        fit=glmnet(x=as.matrix(var[,3:ncol(var)]),
                   y=x[(length(x)-nrow(xframe2)+1):length(x)],
                   lambda=cv.glmnet(x=as.matrix(var[,3:ncol(var)]),
                                    y=x[(length(x)-nrow(xframe2)+1):length(x)])$lambda.min)
        
        fcst=NULL
        for (i in 1:h){
          indx=c(1,3:5)
          if (i==1){
            futxframe=var[nrow(xframe2),..indx]
          } else {
            futxframe[,2:4]=futxframe[,1:3]
            futxframe$x=fcst[i-1]
            
          }
          
          futxframe$seas=xframe2[nrow(xframe2)-freq+h]$x
       #   futxframe$xreg=xreg[length(x)+i]
          
          fcst[i]=predict(fit,newx=as.matrix(futxframe))[,1]
        }
        
        fcst
        
      }
      ,silent=TRUE)
    
    
    if(class(fcst)=='try-error') fcst=rep(0,h)
    
    return(fcst)
  }
  
  ###OUTLIER DETECTION FUNCTIONS
  
  #Outlier detection using IQ range (additive outliers) and breakpoints (level shift outliers)
  outlier_detection=function(x){
    
    options(warn=-1)
    o_hist = x
    init_len = length(x)
    library(data.table)
    library(forecast)
    library(strucchange)
    library(lubridate)
    ## lead_time in weeks
    # outlier logic
    perc_zeroes = sum(x == 0)/length(x)
    #
    outlier_effects_u = 0
    outlier_periods_u = 0
    outlier_effects_l = 0
    outlier_periods_l = 0
    #
    if(perc_zeroes <= 75 && sum(x!=0) >= frequency(x)/2 && length(x) <= frequency(x)*2){
      Q1 = quantile(x, c(0.25, 0.833), names = FALSE)[1]
      Q3 = quantile(x, c(0.25, 0.833), names = FALSE)[2]
      U = round(Q3 + 1.5*(Q3-Q1),0)
      L = round(Q1 - 1.5*(Q3-Q1),0)
      k_u = which((x > U))
      k_l = which((x < L))
      #
      outlier_periods_u = k_u
      outlier_periods_l = k_l
      #
      if(length(k_u) > 0){
        last_one_year_outlier_indx = k_u[k_u >= (length(x) - frequency(x))]
        tmp_diff = diff(last_one_year_outlier_indx)
        consecutive_indx = which(tmp_diff == 1)
        #
        if(length(consecutive_indx) > 0){
          invalid_outlier_indx = last_one_year_outlier_indx[sort(unique(c(consecutive_indx, consecutive_indx + 1)))]
          if(length(invalid_outlier_indx) > 0){
            outlier_periods_u = outlier_periods_u[outlier_periods_u != invalid_outlier_indx]
          }
        }
      }
      #
      outlier_effects_u = x[outlier_periods_u] - U
      x[outlier_periods_u] = U
      #
      if(length(k_l) > 0){
        outlier_periods_l = k_l
      }
      #
      outlier_effects_l = x[outlier_periods_l] - L
      x[outlier_periods_l] = L
    }
    #
    if(length(x)> 2*frequency(x) && sum(x!=0) >= frequency(x)/2 && perc_zeroes<= 0.70){
      Q1 = quantile(x, c(0.25, 0.75), names = FALSE)[1]
      Q3 = quantile(x, c(0.25, 0.75), names = FALSE)[2]
      U = round(Q3 + 2.5*(Q3-Q1),0)
      L = round(Q1 - 2.5*(Q3-Q1),0)
      k_u = which((x > U))
      k_l = which((x < L))
      #
      outlier_periods_u = k_u
      outlier_periods_l = k_l
      #
      if(length(k_u) > 0){
        last_one_year_outlier_indx = k_u[k_u >= (length(x) - frequency(x))]
        tmp_diff = diff(last_one_year_outlier_indx)
        consecutive_indx = which(tmp_diff == 1)
        #
        if(length(consecutive_indx) > 0){
          invalid_outlier_indx = last_one_year_outlier_indx[sort(unique(c(consecutive_indx, consecutive_indx + 1)))]
          if(length(invalid_outlier_indx) > 0){
            outlier_periods_u = outlier_periods_u[!outlier_periods_u %in% invalid_outlier_indx]
          }
        }
      }
      #
      outlier_effects_u = x[outlier_periods_u] - U
      x[outlier_periods_u] = U
      #
      if(length(k_l) > 0){
        outlier_periods_l = k_l
      }
      #
      outlier_effects_l = x[outlier_periods_l] - L
      x[outlier_periods_l] = L
      #
      ### Change in trend
      brk = breakpoints(x ~ 1, breaks = 1)
      if(!is.na(brk$breakpoints) && (length(x) - brk$breakpoints) > 2*frequency(x)){
        x = ts(x[(brk$breakpoints + 1):length(x)], frequency = frequency(x), end = end(x))
      }
    }
    
    #
    if(exists("k_l") && length(outlier_effects_l) > 0){
      outlier_effects_l = outlier_effects_l[outlier_periods_l > init_len - length(x)]
    }
    if(exists("k_u") && length(outlier_effects_u) > 0){
      outlier_effects_u = outlier_effects_u[outlier_periods_u > init_len - length(x)]
    }
    #
    outlier_effects = c(outlier_effects_l, outlier_effects_u)
    outlier_periods= c(outlier_periods_l, outlier_periods_u) 
    #
    return(list(
                series=x))
  }
  

  ###Function generates accuracy metrics
  accuracy <- function(f,x) {
    
    ### f argument is forecast
    ### x is actual data
    
    n=length(x)
    test=which(!is.na(f))
    
    ff <- f
    xx <- x
    
    error <- (ff[1:n]-xx)[test]
    pe <- ifelse(xx[test]!=0,error/xx[test] * 100,ifelse(ff[test]!=0,100,0))
    pe = pmax(-100,pmin(pe,100))
    
    
    mae <- mean(abs(error), na.rm=TRUE)
    mape <- mean(abs(pe), na.rm=TRUE)
    #mape2: sum of mae divided by sum of actuals
    mape2 <- ifelse(sum(xx[test])==0,ifelse(sum(abs(error),na.rm=TRUE)==0,0,100),100*sum(abs(error),na.rm=TRUE)/sum(xx[test]))
    bias = mean(pe,na.rm=TRUE)
    sd = sd(abs(pe),na.rm=TRUE)
    
    out <- c(mae,mape,mape2,bias,sd)
    names(out) <- c("MAE","MAPE","MAPE2","BIAS","SD_APE")
    
    
    return(out)
  }
  
  
  cv.ts <- function(x,m, FUN, lead_time,test_periods,minObs,maxObs,xreg) {
    
    ##Generates backcast for the specified test periods and lead time
    ### x is time series data
    ### m is order of wma or sma
    ### FUN is forecasting function to be used
    ### lead_time is forecasting lags to be tested
    ### test_periods are the number of bacjcasting periods to be tested
    ###minObs the minimum number of observations to generate a backcast
    ### maxObs maximum number of observations to use in time series hostory
    ### xreg is vector of external data
    
    #Define additional parameters
    freq <- frequency(x)
    n <- length(x)
    st <- max(n-test_periods-max(lead_time)+1,minObs)
    end=n-min(lead_time)
    actuals=x
    
    #Create a list of training windows
    steps <- seq(1,(end-st+1),by=stepSize)
    lead_time=lead_time[lead_time<=(length(x)-minObs)]
    
    #At each point in time, calculate 'maxHorizon' forecasts ahead
    maxHorizon=max(lead_time)
    
    #Generate list with training time-series
    train=list(0)
    train_xreg=list(0)
    for (i in steps){
      if(maxObs==Inf){ 
        train[[i]] <- ts(x[1:(st+i-1)],frequency=freq) 
        train_xreg[[i]] <- ts(xreg[1:(st+i-1)],frequency=freq)
      } else {
        train[[i]] <- ts(x[max(1,(st+i-maxObs)):(st+i-1)],frequency=freq) 
        train_xreg[[i]] <- ts(xreg[max(1,(st+i-maxObs)):(st+i-1)],frequency=freq) 
        
      }
    }
    
    ###apply outlier detection if outliers==TRUE
    if (outliers==TRUE) train=lapply(train,function(x) outlier_detection(x)$series)
    if (outliers==TRUE) train_xreg=lapply(1:length(train_xreg),function(indx) {
      
      series=train_xreg[[indx]][(length(train_xreg[[indx]])+1-length(train[[indx]])):length(train_xreg[[indx]])]
      series=ts(series,frequency=frequency(train[[indx]]))
      
    })
    
    train1=train
    
    ##apply transformation if transformation!='none'
    
    if (transformation=='log1p'){
      train1=lapply(train,log1p)
    } else {
    if (transformation=='boxcox') {
      
      lambda=lapply(train,BoxCox.lambda)
      
      train1=mapply(function(X,Y){
        BoxCox(X,lambda=Y)
        
      },X=train,Y=lambda,SIMPLIFY = FALSE)
    }
    }
    
    ###forecast using external data xreg is algorithm used is either 'arimaDec', 'glmnetDec' or 'thetaDec'
    
    if (!FUN %in% c('arimaDecForecast','glmnetDecForecast','thetaDecForecast')){
      FUN=get(FUN)
      
      forecasts=lapply(train1,function(x) FUN(x,m, h=maxHorizon))
      
    } else {
      FUN=get(FUN)
      forecasts=mapply(function(X,Y) {
        FUN(X,m,h=maxHorizon,Y)
      }, X=train1, Y=train_xreg,SIMPLIFY = FALSE)   
    }
    
    
    ###apply transformation if transformation!='none'
    
    if (transformation=='log1p'){
      forecasts=lapply(forecasts,expm1)
    } else {
      if (transformation=='boxcox') {
        forecasts=mapply(function(X,Y){
          InvBoxCox(X,lambda=Y)
          
        },X=forecasts,Y=lambda,SIMPLIFY = FALSE)
      }
    }
    
    #Generates backcast table
    
    forecasts2=do.call('rbind',forecasts)
    forecasts3=data.table(forecasts2[,lead_time])
    restable=data.table(cbind(forecasts3,as.matrix(actuals[(length(actuals)-nrow(forecasts3)+1):length(actuals)])))
    colnames(restable)=c(paste0("lag",lead_time),'actuals')
    lags=(ncol(restable)-1)
    for (i in (1:lags)){
      restable[,i]=shift(restable[,..i],n=lead_time[i]-lead_time[1])[[1]]
    }
    
    
    restable=restable[max(1,nrow(restable)-test_periods+1):nrow(restable)]
    
    
    
    #Calculate accuracy at each horizon
    out <- data.frame(
      plyr::ldply(paste0("lag",lead_time),
                  function(horizon) {
                    P <- unlist(restable[,..horizon])
                    A <- restable$actuals
                    #  P <- P[1:length(A)]
                    #  P <- na.omit(P)
                    #  A <- A[(length(A)):length(A)]
                    accuracy(P,A)
                  }
      )
    )
    
    #Add average accuracy, across all horizons
    overall <- colMeans(out)
    out <- rbind(out,overall)
    results <- data.frame(horizon=c(lead_time,'All'),out)
    
    #Add a column for which horizon and output
    return(list(forecasts=restable, results=results))
  }
  
  
 
  ###Generate results tables
  methodsfull=paste0(methods,'Forecast')
  
  if (test_periods>0){
    
  backcast=list(0)
  
  ###apply cv.ts function using all forecast methods in methodsfull string vector
  backcast=lapply(methodsfull,function(i) cv.ts(x,m=ifelse(i=='smaForecast',smaOrder,ifelse(i=='wmaForecast',wmaOrder,NULL)),i,lead_time,test_periods,minObs,maxObs,xreg))
  
  
  for (i in 1:length(backcast)){
    fc2=cbind(period=1:nrow(backcast[[i]]$forecasts),backcast[[i]]$forecasts)
    colnames(fc2)[2:(ncol(fc2)-1)]=c(paste0(methods[i],"_",colnames(fc2)[2:(ncol(fc2)-1)]))
    fc=if(i==1) fc2[,1:(ncol(fc2))] else merge(fc,fc2)
    
  }
  
  for (i in 1:length(backcast)){
    fe2=backcast[[i]]$results
    colnames(fe2)[2:(ncol(fe2))]=c(paste0(methods[i],"_",colnames(fe2)[2:(ncol(fe2))]))
    fe=if(i==1) fe2[,1:(ncol(fe2))] else merge(fe,fe2)
    
  }
  
  ##Put together output table of MAPEs and accuracy indicators
  MAPEcols=which(substr(colnames(fe),nchar(colnames(fe))-(nchar(bfMetric)-1),nchar(colnames(fe)))==bfMetric)
  MAPE=data.table(fe[,c(1,MAPEcols)])
  MAPE=cbind(MAPE,MAPE_BestFit=apply(MAPE[,2:ncol(MAPE)],1,function(x) min(x,na.rm=TRUE)))
  setnames(MAPE,'MAPE_BestFit',paste0(bfMetric,"_BestFit"))
  MAPE=cbind(MAPE,Method_BestFit=apply(MAPE[,2:ncol(MAPE)],1,function(x) methods[which(x==x[length(x)])[1]]))
  
  
  ##Generates forecast using bes fit method if FcstHor is not NULL
  BF_Method=MAPE[horizon=='All']$Method_BestFit
  } else {
    BF_Method=methods
    MAPE=NULL
    fc=NULL
    fe=NULL
  }
  
  if (is.null(FcstHor)){
    Forecast=NULL
    
  } else {
    m=NULL
    ##Apply outliers logic if TRUE
    if(outliers==TRUE) x=outlier_detection(x)$series
    
    ###Apply transformation if not 'none'
    if (transformation=='log1p'){
      x=log1p(x)
    } else {
      if (transformation=='boxcox') {
       
          lambda=BoxCox.lambda(x)
          x=BoxCox(x,lambda)
          
        }
    }
    
    ###Generate forecast using best fit methos
    if (FcstbyHor==FALSE){
      if (BF_Method %in% c('arimaDec','glmnetDec','thetaDec','arimaxreg')){
        
        Forecast=get(paste0(BF_Method,'Forecast'))(x,h=FcstHor,xreg=xreg,m)
        
      }  else {
        
        if(BF_Method=='sma') m=smaOrder
        if(BF_Method=='wma') m=wmaOrder
        
        Forecast=get(paste0(BF_Method,'Forecast'))(x,h=FcstHor,m=m)
        
      }
      
    }else{
      
      fcst_methods=MAPE$Method_BestFit
      fcst_methods=fcst_methods[1:(length(fcst_methods)-1)]
      fcst_horizons=as.numeric(MAPE$horizon)
      fcst_horizons=fcst_horizons[1:(length(fcst_horizons)-1)]
      
      Forecast=NULL
      for (i in fcst_horizons){
        if (fcst_methods[i] %in% c('arimaxreg','glmnetDec','thetaDec')){
          
          Forecast[i]=get(paste0(fcst_methods[i],'Forecast'))(x,h=i,xreg=xreg,m=m)[i]
          
        } else{
          
          if(fcst_methods[i]=='sma') m=smaOrder
          if(fcst_methods[i]=='wma') m=wmaOrder
          
          Forecast[i]=get(paste0(fcst_methods[i],'Forecast'))(x,h=i,m=m)[i]
          
        }
      }
    }
    
   Forecast=ts(Forecast,frequency = frequency(x),start=end(x)[1]+1/frequency(x))
    
   ##apply inverted transformtion to get forecast (if transformation!='none')
    if (transformation=='log1p'){
      Forecast=expm1(Forecast)
    } else {
      if (transformation=='boxcox') {
        
        Forecast=InvBoxCox(Forecast,lambda=lambda)
        
      } 
    }
    
    }
    
    
  
   
  
  
  
  
   return(list(Accuracy=MAPE,Backcast=fc,Metrics=fe,Forecast=Forecast)) 
}



ensemble_forecast=function(x,xreg,EnsMethods,MarketGrowth,Horizon){

  
  ##ensemble_forecast function generates the ensemble forecast for a time series x
  ## xreg is a vector of external data
  ## EnsMEthods is a data table containing the weights to be appleid for each forecasting method
  ## MarketGrowth is the market growth input to be used
  ## Horizon is the forecast horizon
  
### if EnsMethods has at least one method with Weight>0 then apply ensemble forecast
#### else apply 'naive' forecast

if (length(EnsMethods[Weight>0]$Method)>0){
  
  ResFcasts=lapply(EnsMethods[Weight>0]$Method,function(Meth) {
    
    ###apply bestfit function with test_periods=0 to generate forecast for Method named 'Meth'
    bestfit(x,test_periods = 0,lead_time=3:4,methods=Meth,outliers=TRUE,xreg=xreg,FcstHor=Horizon)$Forecast*EnsMethods[Method==Meth]$Weight
    
  })
  
} else {
  
  ResFcasts=lapply('naive',function(Meth) {
    
    ###apply bestfit function with test_periods=0 to generate forecast for Method named 'Meth'
    bestfit(x,test_periods = 0,lead_time=3:4,methods=Meth,outliers=TRUE,xreg=xreg,FcstHor=Horizon)$Forecast
    
  })
  
  
}

Forecast=0

###Aggregate forecasts of different methods into single ensemble forecast
for (k in seq(length(ResFcasts))){
  
  Forecast=Forecast+ResFcasts[[k]]
  
}

Forecast=as.numeric(Forecast)

##Remove negative forecasts
Forecast[Forecast<0]=0


####Compute de-trended forecast

if (x[length(x)]>0){
  Growth=Forecast[12]/x[length(x)]
  DeTrendFcst=Forecast/(1+(1:12)*(Growth-1)/12)
  DeTrendFcst[is.nan(DeTrendFcst)]=0
  DeTrendFcst[is.infinite(DeTrendFcst)]=0
  
} else {
  DeTrendFcst=Forecast
  
}

##Apply grwoth factor to de-trended forecast

Forecast=DeTrendFcst*(1+(1:12)*MarketGrowth/12)
Forecast=pmax(round(Forecast,3),0)


}
