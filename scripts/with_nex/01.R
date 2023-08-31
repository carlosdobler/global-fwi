

# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(stars)
library(units)
library(furrr)


plan(multisession)
options(future.globals.maxSize = 1000*1024^2)
options(rgdal_show_exportToProj4_warnings = "none")


dir_data <- "/mnt/pers_disk/data"
fs::dir_create(dir_data)

dir_gs <- "/mnt/bucket_mine/cmip6/nex/daily"


models <- c("GFDL-ESM4",
            "MPI-ESM1-2-HR",
            "MRI-ESM2-0",
            "UKESM1-0-LL",
            "IPSL-CM6A-LR")






# MODEL LOOP ------------------------------------------------------------------

model <- models[1]

## DOWNLOAD DATA -----

# dir_vars <-
#   dir_gs %>% 
#   fs::dir_ls(regexp = model) %>% 
#   fs::dir_ls(regexp = "maximum_temperature|relative_humidity|specific_humidity|wind_speed|precipitation")
# 
# dir_vars %>% 
#   map(fs::dir_ls, regexp = seq(2000,2020) %>% str_flatten("|")) %>% 
#   unlist(use.names = F) %>% 
#   str_replace("/mnt/bucket_mine", "gs://clim_data_reg_useast1") %>% 
#   
#   future_walk(function(f) {
#     
#     system(str_glue("gsutil cp {f} {d}", d = dir_data),
#            ignore.stdout = T, ignore.stderr = T)
#     
#     
#   })




## TILE -----

# subset western US for now

# s <- 
#   dir_data %>% 
#   fs::dir_ls() %>% 
#   .[1] %>% 
#   read_ncdf(ncsub = cbind(start = c(1,1,1),
#                           count = c(NA,NA,1))) %>% 
#   adrop()
# 
# s[, 935:1025,360:455] %>% plot()
# 
# rm(s)




## IMPORT -----




# variables <- c("tasmax", "pr", "huss", "sfcWind")
variables <- c("tas", "pr", "hurs", "sfcWind")


ls_data <- 
  dir_data %>% 
  fs::dir_ls()

time_vector <- 
  ls_data %>% 
  str_subset(variables[1]) %>% 
  map(read_ncdf, proxy = T) %>% 
  suppressMessages() %>% 
  map(st_get_dimension_values, "time") %>% 
  map(as.character) %>% 
  unlist() %>% 
  str_sub(end = 10) %>% 
  as_date()


s_variables <- 
  
  variables %>% 
  set_names() %>% 
  
  map(function(variable) {
    
    print(str_glue("Importing {variable}"))
    
    s <- 
      ls_data %>% 
      str_subset(variable) %>%
      
      future_map(function(f) {
        
        read_ncdf(f, ncsub = cbind(start = c(935, 360, 1),
                                   count = c(95, 95, NA))) %>% 
          suppressMessages()
        
      },
      .options = furrr_options(seed = NULL))
    
    s <- 
      do.call(c, s) %>% 
      st_set_dimensions("time", values = time_vector) %>% 
      setNames("v")
    
    
    if (variable == "tasmax" | variable == "tas") {
      
      s %>% 
        mutate(v = 
                 v %>% 
                 set_units(degC) %>% 
                 set_units(NULL)) %>% 
        setNames(variable)
      
    } else if (variable == "pr") {
      
      s %>% 
        mutate(v =
                 v %>% 
                 set_units(kg/m^2/d) %>% 
                 set_units(NULL) %>% 
                 if_else(. < 0, 0, .)) %>% 
        setNames(variable)
      
    } else if (variable == "huss" | variable == "hurs") {
      
      s %>% 
        mutate(v = 
                 v %>% 
                 set_units(NULL) %>% 
                 if_else(. < 0, 0, .)) %>% 
        setNames(variable)
      
    } else if (variable == "sfcWind") {
      
      s %>% 
        mutate(v = 
                 v %>% 
                 set_units(km/h) %>% 
                 set_units(NULL) %>% 
                 if_else(. < 0, 0, .)) %>% 
        setNames(variable)
      
    }
  
})


s_variables[["lat"]] <- 
  s_variables[[1]] %>% 
  st_dim_to_attr(2)


# elev <-
#   "/mnt/bucket_mine/misc_data/gmted/mn30_grd/w001001.adf" %>%
#   read_stars(proxy = F)
# 
# elev <-
#   elev %>%
#   st_warp(s_variables[[1]] %>% slice(time, 1),
#           method = "average", use_gdal = T) %>%
#   setNames("elev") %>%
#   st_warp(s_variables[[1]] %>% slice(time, 1))
# 
# pressure <-
#   elev %>%
#   mutate(pressure = 101.3 * ((293 - 0.0065 * elev) / 293)^5.26, # kPa
#          pressure = pressure * 1000) %>% # Pa
#   select(pressure)
# 
# gc()
# 
# 
# 
# ## CONVERSIONS ------
# 
# e_s <-
#   s_variables %>%
#   pluck("tasmax") %>%
#   mutate(# Tetens:
#          # e_s = 0.61078 * exp((17.27 * tasmax) / (tasmax + 237.3))) %>%
# 
#          # Petty:
#          e_s = 6.112 * exp((17.67 * tasmax) / (tasmax + 243.5))) %>%
# 
#   select(e_s) # Pa
# 
# w_s <-
#   seq_len(dim(e_s)[3]) %>%
#   future_map(function(ts) slice(e_s, time, ts) * 0.622 / pressure)
# 
# w_s <-
#   do.call(c, c(w_s, along = "time")) %>%
#   st_set_dimensions("time", values = st_get_dimension_values(e_s, "time")) %>%
#   setNames("w_s")
# 
# w <-
#   s_variables[["huss"]] %>%
#   mutate(w = huss / (1 - huss)) %>%
#   select(w)
# 
# # w <-
# #   s_variables[["huss"]] %>%
# #   mutate(w = huss) %>%
# #   select(w)
# 
# s_variables[["hursmin"]] <- setNames(w/w_s, "hursmin")
# 
# s_variables <- s_variables[-which(names(s_variables) == "huss")]

s_variables_m <- 
  do.call(c, c(s_variables, along = "variables"))

dates_tb <-
  time_vector %>% 
  tibble(time = .) %>% 
  mutate(mon = month(time),
         day = day(time)) %>% 
  select(-time)











plan(multisession, workers = 6)

tictoc::tic()
s_fwi <- 
  s_variables_m %>%
  st_apply(c(1,2), function(x){
    
    if(any(is.na(x[,1]))) {
      
      r <- rep(NA, 7665*7)
      
    } else {
      
      r <- 
        as_tibble(x) %>% 
        
        # setNames(c("temp", "prec", "ws", "lat", "rh")) %>% 
        setNames(c("temp", "prec", "rh",  "ws", "lat")) %>%                     # ***********************************
        
        bind_cols(dates_tb) %>% 
        cffdrs::fwi(out = "fwi")
      
      r <- unname(unlist(r))
      
    }
    
    gc()
    
    return(r)
    
  },
  .fname = "fwi_vars",
  FUTURE = T)
tictoc::toc() # 2327 # 2300


name_fwi_vars <- c("ffmc", "dmc", "dc", "isi", "bui", "fwi", "dsr")
breaks <- seq(1, dim(s_fwi)[1], by = 7665)

s_fwi_2 <- 
  pmap(list(breaks, breaks + 7664, name_fwi_vars), function(split_i, split_f, name_fwi_var) {
    
    s_fwi %>% 
      slice(fwi_vars, split_i:split_f) %>% 
      st_set_dimensions("fwi_vars",
                        names = "time",
                        values = time_vector) %>% 
      setNames(name_fwi_var)
    
  }) %>% 
  do.call(c, .) %>% 
  aperm(c(2,3,1))

rm(s_fwi)
gc()




# filename <- "/mnt/bucket_cmip5/christopher/workflow_fwi_test/fwi_carlos_hursmin_tasmax.nc"
filename <- "/mnt/bucket_cmip5/christopher/workflow_fwi_test/fwi_carlos_hurs_tas.nc"


# define dimensions
dim_lon <- ncdf4::ncdim_def(name = "lon", 
                            units = "degrees_east", 
                            vals = s_fwi_2 %>% 
                              st_get_dimension_values(1))

dim_lat <- ncdf4::ncdim_def(name = "lat", 
                            units = "degrees_north", 
                            vals = s_fwi_2 %>% 
                              st_get_dimension_values(2))

dim_time <- ncdf4::ncdim_def(name = "time", 
                             units = "days since 1970-01-01", 
                             vals = s_fwi_2 %>% 
                               st_get_dimension_values(3) %>% 
                               as.integer())

# define variables
varis <- 
  names(s_fwi_2) %>% 
  map(~ncdf4::ncvar_def(name = .x,
                        units = "",
                        dim = list(dim_lon, dim_lat, dim_time)))


# create file
ncnew <- ncdf4::nc_create(filename = filename, 
                          vars = varis,
                          force_v4 = TRUE)

# write data
seq_along(names(s_fwi_2)) %>% 
  walk(~ncdf4::ncvar_put(nc = ncnew, 
                         varid = varis[[.x]], 
                         vals = s_fwi_2 %>% select(all_of(.x)) %>% pull(1)))

ncdf4::nc_close(ncnew)



tibble(
  hursmin = s2 %>% select(fwi) %>% pull() %>% .[40,40,],
  hurs = s %>% select(fwi) %>% pull() %>% .[40,40,],
  time = time_vector
) %>% 
  
  pivot_longer(-time) %>% 
  
  filter(year(time) > 2005 & year(time) < 2010) %>% 
  
  ggplot(aes(x = time, y = value, color = name, group = name)) +
  geom_line()

