
# load libraries
library(cffdrs)
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


# define variables
vars <- c("hurs", "sfcWind", "tas", "pr") %>% set_names()
vars_long <- c("surface_relative_humidity", "surface_wind_speed", "average_temperature", "precipitation")


# build table of files
str_glue("~/bucket_risk/RCM_regridded_data/CORDEX_22/{dom}/daily/") %>% 
  list.dirs(recursive = F) %>% 
  .[str_detect(., str_flatten(vars_long, "|"))] %>% 
  map_dfr(function(d){
    
    pos_model <- 3
    pos_rmodel <- 6
    pos_date <- 9
    
    tibble(file = d %>%
             list.files()) %>%
      mutate(
        
        var = file %>%
          str_split("_", simplify = T) %>%
          .[ , 1],
        
        model = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_model],
        
        rmodel = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_rmodel] %>% 
          str_split("-", simplify = T) %>% 
          .[ , 2],
        
        t_i = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_date] %>%
          str_sub(end = 8),
        
        t_f = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_date] %>%
          str_sub(start = 10, end = 17)
        
      )
  }) %>% 
  mutate(model = str_glue("{rmodel}_{model}")) %>% 
  select(-rmodel) %>%
  rowwise() %>% 
  mutate(var_long = vars_long[which(vars == var)]) %>% 
  ungroup() -> tb_files
