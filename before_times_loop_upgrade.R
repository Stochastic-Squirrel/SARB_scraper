# Set up -----------------------------

#Ensures reproducibility. This will download a snapshot of packages used as of the below date.
#Those dated packages will only be used for THIS project. Doing the initial download and install may take a few minutes.
library(checkpoint)
checkpoint("2017-11-15")

library(tidyverse)
library(rvest)
library(httr)
library(XML)
library(RSelenium)
library(stringr)
library(data.table)
library(utils)
library(readxl)
library(progress)
library(lubridate)
library(foreach)
library(doParallel)




website_url <- "https://www.resbank.co.za"

#set working directory when running script through terminal
setwd("/t-drive/Internal/Stats_team/Eighty20/treasury_scrape")

eCaps <- list(
  chromeOptions =
    list(prefs = list(
      "profile.default_content_settings.popups" = 0L,
      "download.prompt_for_download" = FALSE,
      # Set the download file path here... in this case this is where the VM downloads the files. need to map this volume to the host machine through docker
      "download.default_directory" = "/home/seluser",
      "marionette"= TRUE
    )
    )
)



#Docker container info. This may change. Check with Johan/Server admin
#4447 for R 6012  .... for mapped chrome
#4448 for R 6013 for Firefox 
#4449 for R 6014 for debug Chrome without folder map

#remote_browser <- remoteDriver(remoteServerAddr = "192.168.1.6", port = 4447L,extraCapabilities=eCaps,browser="chrome")
#remote_browser$open()


debug_chrome <-  remoteDriver(remoteServerAddr = "192.168.99.100", port = 4445L,extraCapabilities=eCaps,browser="chrome")
debug_chrome$open()


#only run this line when you want to debug and see what is happening in the chrome browser
#remote_browser <- debug_chrome
#remote_browser$open()


# Utilities ----------------

extract_links <- function(webpage, div){
  
  node <- html_nodes(webpage,div) 
  
  
  # links_text <- node %>%
  #   html_text(trim=TRUE) %>%
  #   strsplit("\r\n") %>% unlist()
  
  node_links <-  html_nodes(node,"a") %>% 
    html_attr("href") %>% 
    paste0(website_url,.)
  
  
  
  return(data_frame(links = node_links))
}

sleep <- function(){
  runif(1,0,10) %>% Sys.sleep()
}

download_file <- function(url, dest_directory, optional_file_names , replace_spaces = FALSE){
  
  
  
  links_to_download <- url$links
  
  if(replace_spaces ==TRUE){
    links_to_download <-  str_replace_all(links_to_download," ","%20")
  }
  links_to_download <-  str_replace_all(links_to_download,"https","http")
  # progress_bar <- winProgressBar(
  #   min = 0,
  #   max = length(links_to_download),
  #   title = paste0("Download status for ", dest_directory)
  # )
  
  for (i in 1:length(links_to_download)){
    
    # getWinProgressBar(progress_bar)
    
    if(!missing(optional_file_names)){
      download.file(url = links_to_download[i],destfile = paste0("download/",
                                                                 dest_directory,
                                                                 "/", 
                                                                 optional_file_names[i]),
                    mode = "wb")
      
    }
    
    else{
      download.file(url = links_to_download[i],
                    destfile = paste0("download/",
                                      dest_directory,
                                      "/", 
                                      basename(links_to_download[i])),
                    mode = "wb")
      
    }
    # setWinProgressBar(progress_bar,value=i)
    sleep()
  }
  
  # close(progress_bar)
  
}

extract_indicator_items <- function(){
  #This function will fetch all of the indicator codes to be used in scrape_online_statistical
  #In firefox you can just go $navigate for the javascript postback functions
  #in chrome you must use $executeScript
  
  
  
  
  
  debug_chrome$navigate("https://wwwrs.resbank.co.za/qbquery/TimeSeriesQuery.aspx")
  
  # get all codes
  webElem <-
    debug_chrome$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtTsCode']")
  webElem$sendKeysToElement(list("*", key = "enter"))
  
  
  #WARNING! Page takes a while to load so you may need to sleep
  Sys.sleep(20)
  
  #Get the page number
  page_no <-
    debug_chrome$findElements(using = "css", value = "span")
  page_no <- page_no[[2]]
  page_no <-
    page_no$getElementText() %>% 
    str_extract(. , '[:digit:]+:$') %>% 
    str_replace(. , ":", "") %>% 
    as.integer()
  
  indicator_codes <- data_frame(Code=character(),Description=character(),Frequencies=character())
  time_series_codes <- data_frame( Code=character(),Frequency=character(),'Start Date'=character() , 'End Date'=character() , 'Type Description'=character() )
  
  #used to handle last couple of pages of indicators where the next page ID changes
  end_case_button_id <- 17
  
  #for each page of indicator codes
  for (page in 1:(page_no)){
    
    table_element <-  debug_chrome$findElement(using = "xpath", "//*/table[@id = 'Wizard1_gvClasses']")
    table <- table_element$getPageSource()[[1]] %>% 
      read_html() %>% 
      html_table(fill = TRUE)
    table <- table[[4]] %>% as_data_frame()
    indicator_codes <- bind_rows(indicator_codes, select(table, Code:Frequencies)[1:(nrow(table)-1),])
    
    
    #For given indicator page, go to each indicator code and scrape the timeseries information\
    if(page == page_no){ 
      number_of_indicators <- dim(table)[1]
      
    }else{
      number_of_indicators <- dim(table)[1] - 1
      
    }
    
    #scrape timeseries info for each indicator on a given page
    for (indicator in 1:number_of_indicators) {
      
      indicator_ts_type <- unlist(table[indicator,4]) %>% as.character()
      #If the Frequency includes Daily (6 Days) ...skip it. This is an error their side
      if(  indicator_ts_type=='Daily (6 Days)' ){
        
        next()
      }
      if (indicator < 9){
        
        checkbox <- debug_chrome$findElement(using = "xpath", paste0("//*/input[@id = 'Wizard1_gvClasses_ctl0",indicator+1,"_CheckBoxButton']") )
        checkbox$sendKeysToElement(list(key = "space"))   #use space key to select
      }else{
        checkbox <- debug_chrome$findElement(using = "xpath", paste0("//*/input[@id = 'Wizard1_gvClasses_ctl",indicator+1,"_CheckBoxButton']") )
        checkbox$sendKeysToElement(list(key = "space"))   #use space key to select
        
        
      }
      Sys.sleep(8)
      next_button <-  debug_chrome$findElement(using = "xpath", "//*/input[@id = 'Wizard1_StepNavigationTemplateContainerID_StepNextButton']")
      next_button$clickElement()
      Sys.sleep(12)
      #scrape timeseries information
      ts_table_element <-  debug_chrome$findElement(using = "xpath", "//*/table[@id = 'Wizard1_gvVersions']")
      ts_table <- ts_table_element$getPageSource()[[1]] %>%
        read_html() %>%
        html_table(fill = TRUE)
      ts_table <- ts_table[[4]] %>% as_data_frame()
      #make sure that dates are read in as strings
      ts_table$`Start Date` <- as.character(ts_table$`Start Date`)
      ts_table$`End Date` <- as.character(ts_table$`End Date`)
      time_series_codes <- bind_rows(time_series_codes, ts_table[ , -1] )
      
      debug_chrome$goBack()
      sleep()
      
      saveRDS(time_series_codes,"in_process_timeseries.rds")
    }
    
    
    
    
    
    
    
    
    
    
    #go to next page
    #For some reason, next page link will skip exactly 6 pages once you have hit page 6. Need to be more specific
    #Also, bug with RSelenium, need to pass args=list("dummy") for executeScript to work
    if (page!=page_no){  #if you are on the last page, no need to click next 
      #unique case when you start off the page  
      if (page %in% (1:6)){
        button_id <- 3 + 2*page
        if(page %in% (1:3)){
          debug_chrome$executeScript(paste0("javascript:__doPostBack('Wizard1$gvClasses$ctl13$ctl0",button_id,"','')"), args=list("dummy") ) 
          
        }else{
          debug_chrome$executeScript(paste0("javascript:__doPostBack('Wizard1$gvClasses$ctl13$ctl",button_id,"','')") , args=list("dummy"))
        }
        
      }
      
      #case for when you approach the last couple of pages
      
      else if ( page >= page_no-4 ){
        
        
        debug_chrome$executeScript(paste0("javascript:__doPostBack('Wizard1$gvClasses$ctl13$ctl",end_case_button_id,"','')"), args=list("dummy") )
        end_case_button_id <- end_case_button_id +2 #iterate by 2
        
      }else{
        debug_chrome$executeScript("javascript:__doPostBack('Wizard1$gvClasses$ctl13$ctl15','')", args=list("dummy"))
      }
      
    }
    
    Sys.sleep(18)
    saveRDS(indicator_codes,"in_process_indicators.rds")
    
  }
  indicator_codes$code_number <-
    indicator_codes$Code %>%
    str_replace_all(. , "KBP", "") %>%
    as.integer()
  
  number_only <- time_series_codes$Code %>% str_replace(. ,"KBP","") %>% substr(. , 1 , 4)
  
  time_series_codes <-  time_series_codes  %>% mutate(code_number = as.integer(number_only) )
  time_series_codes <- time_series_codes %>% group_by(code_number) %>% mutate(number_of_ts = n())
  
  indicator_codes <- inner_join(indicator_codes, time_series_codes , by="code_number") %>% rename(.,Code = Code.x, ts_code = Code.y)
  
  fwrite(indicator_codes,"indicator_codes.csv")
  
  
}

#function to clean raw, downloaded query data
clean_ts_download <- function(){
  
  
  file_list <- paste0( "download/online_statistical_query/raw/Downloads/",list.files("download/online_statistical_query/raw/Downloads/"))
  #don't process partially downloaded files
  file_list <- file_list[!str_detect(file_list,"crdownload")]
  
  
  #if there are no files to process, do nothing
  if(file_list[1] != "download/online_statistical_query/raw/Downloads/") {
    for (file in file_list){
      
      
      dirty <- try(read_xls(file),silent=TRUE)
      #Check if there was an error in reading file. SARB seems to have some corrupt files
      #sees if the class of dirty is an error, deletes it, then moves on
      if(class(dirty)[1]=="try-error" ){
        cat("Deleting file ",paste0(file)," because it is corrupt.\n")
        file.remove(file)
        next()
      }
      
      #extract meta data
      #remove reserved symbols
      description <- dirty[2,]  %>% as_vector() %>%
        str_replace_all(. , " ","_") %>% str_replace_all(. ,":","") %>% 
        str_replace_all(. , "/", "") %>% str_replace_all(. ,"\\*","") %>%
        str_replace_all(. , '\\\\', "") %>% str_replace_all(. ,"|","") %>%
        str_replace_all(. , "\\?", "") %>% str_replace_all(. ,">","") %>%
        str_replace_all(. , "<", "") %>% str_replace_all(. ,'"',"") %>% 
        paste(. , collapse = "#")
      
      dirty <- dirty[6:nrow(dirty), 1:2]
      
      dirty <- dirty[2:nrow(dirty),]
      names(dirty) <- NULL
      colnames(dirty) <- c("Date",description)
      fwrite(x=dirty, paste0("download/online_statistical_query/processed/",description,".csv"))
      
      
      
    }
    file.remove(file_list)
  }
}
#function to check which files have been downloaded
#Note. This is based off what is in the processed folder and not the database_table.csv
check_progress <- function(){
  #Check progress returns a vector of indicator codes which are NOT present in processed
  #In the case where a given indicator is partially finished i.e. 1000J is there but not 1000m,
  #The indicator 1000 will be declared unfinished and thus 1000J will be downloaded again
  file_list <- list.files("download/online_statistical_query/processed/") %>% substring(. , 1, 8)
  if(length(file_list)== 0){return(vector())} #if no files in directory
  
  
  indicator_codes <- fread("indicator_codes.csv")
  not_finished <- indicator_codes %>% 
                        filter(!(ts_code %in% file_list)) %>% 
                        select(code_number) %>% 
                        unlist() %>% 
                        unique()
  
  
  return(not_finished)
}

# Scrapers---------------


#WARNING
#The server gets iffy if you run repeated scrape_query and they don't complete
#Just restart the container then you should be good
scrape_online_statistical_query <- function(custom_selection=NULL , cores = 3){
  
  
  
  #load indicator codes
  if(is.null(custom_selection)){
    indicator_codes <- fread("indicator_codes.csv") %>% filter(Frequencies !='Daily (6 Days)')
  }else{
    
    indicator_codes <- fread("indicator_codes.csv") %>% filter(Frequencies !='Daily (6 Days)') %>%
      filter(code_number %in% custom_selection)
    
  }
  code_list <- unique(indicator_codes$code_number)
  #seems to be an error their side regarding these indicators (daily 6 days)
  
  
  
  (cl <- (cores) %>%  makeCluster(. , outfile="parallel_thread_logs.txt") ) %>% registerDoParallel
  clusterEvalQ(cl, {
    library(RSelenium)
    eCaps <- list(
      chromeOptions =
        list(prefs = list(
          "profile.default_content_settings.popups" = 0L,
          "download.prompt_for_download" = FALSE,
          # Set the download file path here... in this case this is where the VM downloads the files. need to map this volume to the host machine through docker
          "download.default_directory" = "/home/seluser",
          "marionette"= FALSE
        )
        )
    )
    #use debug_Chrome details
    remote_browser <-  remoteDriver(remoteServerAddr = "192.168.99.100", port = 4013L,extraCapabilities=eCaps,browser="chrome")
    remote_browser$open()
    
  })
  #export necessary functions and variables to clusters
  clusterExport(cl,varlist = c('sleep','clean_ts_download'))
  
  
 
  
  parallel_results <-foreach (index = 1:length(code_list),.errorhandling = "pass",.verbose = TRUE )  %dopar%
  {
    
    
    
    library(tidyverse)
    library(rvest)
    library(httr)
    library(XML)
    library(RSelenium)
    library(stringr)
    library(data.table)
    library(utils)
    library(readxl)
    library(progress)
    library(lubridate)
    
    #TODO you may be overwriting your progress. work on passing actual code numbers ins
    #Loop for each time series option
    code <- code_list[index]
    remote_browser$navigate("https://wwwrs.resbank.co.za/qbquery/TimeSeriesQuery.aspx")
    time_series_number <- indicator_codes %>% filter(code_number==code) %>% select(. ,number_of_ts)
    time_series_number <- time_series_number$number_of_ts[1]
    sleep()
    for (timeseries in 1:time_series_number){
      
      # remote_browser$navigate("https://wwwrs.resbank.co.za/qbquery/TimeSeriesQuery.aspx")
      # Sys.sleep(4)
      webElem <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtTsCode']")
      webElem$sendKeysToElement(list(paste0(code), key = "enter"))
      Sys.sleep(8)
      checkbox <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_gvClasses_ctl02_CheckBoxButton']")
      checkbox$sendKeysToElement(list(key = "space"))   #use space key to select 
      next_button <-  remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_StepNavigationTemplateContainerID_StepNextButton']")
      next_button$clickElement() 
      Sys.sleep(6)
      
      
      checkbox_code <- timeseries+1
      
      checkbox_ts <- remote_browser$findElement(using = "xpath", paste0( "//*/input[@id = 'Wizard1_gvVersions_ctl0",checkbox_code,"_CheckBoxButton']"))
      checkbox_ts$sendKeysToElement(list(key = "space"))   #use space key to select 
      
      next_button <-  remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_StepNavigationTemplateContainerID_StepNextButton']")
      next_button$clickElement() 
      
      #Through Experimentation, it will give you all data if set start to 1900 and end to 2100. You can ascertain this but trying to throw a range error
      
      try(start_year <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtStartYear']"))
      try(start_year$clearElement())
      try(start_year$sendKeysToElement(list("1900",key="space")))
      
      try(end_year <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtEndYear']"))
      try(end_year$clearElement())
      try(end_year$sendKeysToElement(list("2100",key="space")))
      
      #Write out case for most explicit Timeseries case, errors will be thrown if for eg. Month argument isn't on form. 
      #This is fine as form will still submit with correct information
      
      
      try(start_month <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtStartMonth']"))
      try(start_month$sendKeysToElement(list("01",key="space")))
      
      try(end_month <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtEndMonth']"))
      try(end_month$sendKeysToElement(list("12",key="space")))
      
      try(start_day <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtStartDay']"))
      try(start_day$sendKeysToElement(list("01",key="space")))
      
      try(end_day <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtEndDay']"))
      try(end_day$sendKeysToElement(list("05",key="space")))
      
      
      finish_button <- remote_browser$findElement(using = 'xpath', "//*/input[@id = 'Wizard1_FinishNavigationTemplateContainerID_FinishButton']") 
      finish_button$clickElement()
      sleep()
      clean_ts_download()
      sleep()
      
      
      
      #download file
      download_menu <- remote_browser$findElement(using = "xpath", "//*/div[@id = 'Wizard1_rvSingleTs_ctl06_ctl04_ctl00_Menu']")
      excel <- download_menu$findChildElements(using = "xpath", "//*/a")[[4]]
      dropdown <-  remote_browser$findElement(using = "xpath", "//*/a[@id = 'Wizard1_rvSingleTs_ctl06_ctl04_ctl00_ButtonLink']")
      Sys.sleep(12)
      dropdown$clickElement()
      excel$clickElement()
      
      Sys.sleep(15)
      
      #go back to original menu and clear previous submission
      remote_browser$goBack()
      remote_browser$goBack()
      remote_browser$goBack()
      remote_browser$goBack()
      sleep()
      webElem <- remote_browser$findElement(using = "xpath", "//*/input[@id = 'Wizard1_txtTsCode']")
      webElem$clearElement()
      
      
      gc()
      
      
    }  #End of timeseries loop
    #Now clean up all of the files
    clean_ts_download()
    
    #add to vector of progress
    # code_progress <-c( code , code_progress)
    # saveRDS(code_progress,"code_progress.rds")
  } # end of indicator code loop 
  
  #clean up any files which may have not been processed yet
  clean_ts_download()
  
  # remove progress file as script ran to completion
  #file.remove("code_progress.rds")
  
  
  # close browser on each node
  clusterEvalQ(cl, {
    remote_browser$closeall()
  })
  
  stopImplicitCluster()
  
}

# Processing ----------------

create_database_table <- function(){
  #takes all processesd, online statistical query csvs and writes them all into a single long format table
  file_list <- paste0( "download/online_statistical_query/processed/",list.files("download/online_statistical_query/processed"))
  #don't process partially downloaded files
  file_list <- file_list[!str_detect(file_list,"crdownload")]
  file_list <- file_list[!str_detect(file_list,"database_table.csv")]
  #Read them all into one list
  table_list <- map(file_list,fread) %>% 
    map(. , as_data_frame)
  
  for (i in 1:length(table_list)){
    #create meta data column
    table_list[[i]]$meta_data <- colnames( table_list[[i]])[2]
    table_list[[i]]$Date <- as.character(table_list[[i]]$Date)
    #rename indicator column to measurement
    colnames( table_list[[i]])[2] <- "measurement"
  }            
  
  database_table <- reduce(.x = table_list, bind_rows)
  #Format database_table
  indicator_codes <- str_extract(  database_table$meta_data ,"KBP.....") %>%
    str_replace_all(. ,"KBP","") %>%
    substr(. , 1 , 4)
  time_series_code <-
    str_extract(database_table$meta_data , "KBP.....") 
  measurement_units <-  str_extract(database_table$meta_data , "#.+#") %>%
    str_replace_all(. , "#", "")
  
  
  
  #add in useful fields
  database_table$meta_data <- NULL
  database_table$measurement_units <- measurement_units
  database_table$code_number <- as.numeric(indicator_codes)
  database_table$time_series_code <- time_series_code
  
  #join to the indicator_codes.csv lookup table
  lookup_table <- fread("indicator_codes.csv") %>% as_data_frame()
  #add in the ts_code_symbol
  database_table <- inner_join(database_table , lookup_table , by=c("time_series_code"="ts_code") )
  
  categories <- str_split(database_table$Description , ":")
  for (i in 1:length(categories)){
    #if more than two categories are identified, collapse every category except for the first one into one category
    if( length(categories[[i]]) > 2){
      
      categories[[i]][2] <- paste( categories[[i]][2:length(categories[[i]])] ,collapse=":" ) 
      #Set other elements as NULL in order to also ensure one main category and only one subcategory
      categories[[i]] <- categories[[i]][1:2]
      
    }
    
  }
  
  
  database_table$category <- NULL
  database_table$sub_category <- NULL
  
  for (i in 1:length(categories)) {
    
    if( length(categories[[i]]) == 2){
      database_table[i , "category"] <- categories[[i]][1]
      database_table[i , "sub_category"] <- categories[[i]][2]
    }
    else{
      database_table[i , "category"] <- categories[[i]][1]
    }
    
    
  }
  
  
  
  quarterly_data <- filter(database_table, Frequency=="Quarterly")
  quarterly_dates <- quarterly_data$Frequency
  #Remove quarterly info for now
  database_table <- filter(database_table, Frequency!="Quarterly")
  
  
  if(length(quarterly_dates!=0)){ #only if there is quarterly data available
    for (i in 1:length(quarterly_dates)){
      current_data <- str_split(quarterly_dates[i],"/") %>% unlist()
      current_quarter <- current_data[2]
      current_quarter = case_when(
        current_quarter=="01" ~ "03",
        current_quarter=="02" ~ "06",
        current_quarter=="03" ~ "09",
        current_quarter=="04" ~ "12",
        TRUE ~ current_quarter
      )
      current_data[2] <- current_quarter
      quarterly_dates[i] <- paste(collapse="/",current_data)
    }
  }
  
  quarterly_data$Date <-  quarterly_dates
  
  #add back in quarterly info
  database_table <- bind_rows(database_table , quarterly_data)
  database_table <- database_table %>% select(-Frequencies,-code_number.y) %>% rename(code_number = code_number.x)
  
  
  #write results
  fwrite(database_table,"download/online_statistical_query/database_table.csv")
  
  
}

check_for_updates <- function(){
  
  
  indicators <- fread("indicator_codes.csv") %>% as_data_frame()
  indicators$`End Date` <- parse_date_time(indicators$`End Date`,orders=c("Y","Ym","Ymd"))
  
  
  #Now we read in previously scraped data
  current_data <- fread("download/online_statistical_query/database_table.csv") %>% as_data_frame()
  current_data$Date <- parse_date_time(current_data$Date,orders=c("Y","Ym","Ymd"))
  
  
  today <- Sys.Date() %>% parse_date_time(. , orders=c("Ymd"))
  
}






# Run -------------

scrape_time <- system.time({  
  if( length(check_progress()) == 0 ){
    #download everything as there is nothing in processed
    try(scrape_online_statistical_query(cores=8)) 
  }else{
    try(scrape_online_statistical_query(custom_selection = check_progress() ,cores=8)) 
  }
  create_database_table()
})
print(scrape_time)
#TODO finish check for updates 
#TODO move cluster creation to global environment so that you can terminate clusters if function ends abnormally
#TODO look at check_progress()
#TODO pass position of timeseries indicator as an argument to scraping_function()


