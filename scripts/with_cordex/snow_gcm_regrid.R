source("~/00-mount.R")
source("scripts/setup.R")

library(dplyr)
library(purrr)
library(stringr)
library(lubridate)
library(stars)
library(units)
library(furrr)
library(tictoc)

# enable multicore
options(future.fork.enable = T)


"~/bucket_risk/RCM_raw_data/REMO2015/lm_files/lm_NAM.nc" -> lm

"~/bucket_risk/CMIP5_raw_data/daily_data/surface_snow_amount/" %>% 
  list.files(full.names = T) %>% 
  walk(function(f){
    
    f %>% 
      str_split("/", simplify = T) %>% 
      {.[,ncol(.)]} %>% 
      {str_glue("~/bucket_mine/cmip5/NAM_snw/{.}")} -> outfile
    
    
    system(str_glue("cdo remapnn,{lm} {f} {outfile}"))
    
  })



