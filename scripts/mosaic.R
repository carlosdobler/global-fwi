
dom <- "SEA"       # domain
m <- 2          # model number # skip 1
ti <- 1         # starting tile 


# ******************************************************************************


# mount buckets
source("scripts/mount.R")

# load libraries
# build table of files
source("scripts/setup.R")

tb_files %>% 
  distinct(var, model, t_i, t_f, .keep_all = T) -> tb_files

mod <- unique(tb_files$model)[m]

dir_mos <- str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/mosaics")
dir.create(dir_mos)

# ******************************************************************************


# TILING SECTION

# file for land mask
str_glue("~/bucket_risk/RCM_regridded_data/REMO2015/{dom}/daily/fire_weather_index/") %>% 
  list.files(full.names = T) %>% 
  .[1] %>% 
  read_ncdf(ncsub = cbind(start = c(1,1,1),
                          count = c(NA,NA,1))) %>% 
  adrop() -> s_proxy_remo

# file for extent
str_glue("~/bucket_risk/RCM_regridded_data/CORDEX_22/{dom}/daily/") %>% 
  list.dirs(recursive = F) %>% 
  .[str_detect(., "relative_humidity")] %>% 
  list.files(full.names = T) %>% 
  .[1] -> f

# size of tile
sz <- 28

source("scripts/tiling.R")

rm(s_proxy_remo)





# ******************************************************************************

str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/") %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., mod)] %>% 
  .[1] %>% 
  read_stars(var = "fwi", proxy = T) %>% 
  st_get_dimension_values("time") -> d

d %>% 
  str_sub(end = 4) %>% 
  unique() -> d_yrs

round(length(d_yrs)/10) -> n_lon

split(d_yrs, 
      ceiling(seq_along(d_yrs)/(length(d_yrs)/n_lon))) -> yrs_chunks

yrs_chunks %>% 
  map(function(y){
    
    d %>% 
      str_sub(end = 4) %>% 
      {. %in% y} %>% 
      which() %>% 
      {c(first(.), last(.))}
    
  }) -> d_pos


if(str_glue("{str_sub(d[350*50], 1,4)}-02-30") %in% str_sub(d, 1, 10)){
  cal <- "360_day"
} else if ("1972-02-29" %in% str_sub(d, 1, 10)){
  cal <- "gregorian"
} else {
  cal <- "365_day"
}


# *******


unique(chunks_ind$lat_ch) %>% 
  sort() %>% 
  map(function(i){
    chunks_ind %>% 
      filter(lat_ch == i) %>% 
      pull(r) %>% 
      str_pad(3, "left", "0")
  }) -> tiles

tiles %>% 
  map_int(length) %>% 
  which.max() -> max_t

str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/") %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., mod)] %>% 
  .[str_detect(., str_flatten(tiles[[max_t]],"|"))] %>% 
  
  map(read_ncdf, 
      var = "fwi", 
      ncsub = cbind(start = c(1,1,1),
                    count = c(NA,NA,1))) %>% 
  map(adrop) %>% 
  {do.call(c, c(., along = 1))} -> row_max


# *******

d_pos %>% 
  iwalk(function(d_p, i){
    
    print(str_glue("{i} / {length(d_pos)}"))
    
    tiles %>%
      map(function(r){
        
        "~/bucket_mine/results/global_fwi_ww/SEA/" %>% 
          list.files(full.names = T) %>% 
          .[str_detect(., mod)] %>% 
          .[str_detect(., str_flatten(r,"|"))] %>% 
          
          map(read_ncdf,
              proxy = F,
              var = "fwi", 
              ncsub = cbind(start = c(1,1,d_p[1]),
                            count = c(NA,NA,(d_p[2]-d_p[1]+1)))) %>%
          
          suppressMessages() %>% 
          {do.call(c, c(., along = 1))} -> roww
        
        matrix(NA, dim(row_max)[1], dim(roww)[2]) %>% 
          st_as_stars() %>% 
          st_set_dimensions(1, name = "lon", 
                            values = st_get_dimension_values(row_max, "lon")) %>% 
          st_set_dimensions(2, name = "lat", 
                            values = st_get_dimension_values(roww, "lat")) %>% 
          st_set_crs(4326) -> mm
        
        st_warp(roww, mm) -> roww_mm
        
        return(roww_mm)
        
      }) -> l_rows
    
    do.call(c, c(l_rows, along = 2)) -> mos
    
    
    # **********
    
    {
      # define dimensions
      dim_lon <- ncdf4::ncdim_def(name = "lon", 
                                  units = "degrees_east", 
                                  vals = mos %>% st_get_dimension_values(1, center = F))
      
      dim_lat <- ncdf4::ncdim_def(name = "lat", 
                                  units = "degrees_north", 
                                  vals = mos %>% st_get_dimension_values(2, center = F))
      
      dim_time <- ncdf4::ncdim_def(name = "time", 
                                   units = str_glue("days since {str_sub(d[1], 1,10)}"),
                                   vals = seq((d_p[1]-1), (d_p[2]-1)),
                                   calendar = cal)
      
      # define variables
      ncdf4::ncvar_def(name = "fwi",
                       units = "",
                       dim = list(dim_lon, dim_lat, dim_time), 
                       missval = -9999) -> vari
      
      d[d_p[1]] %>% str_remove_all("-") -> t_i
      d[d_p[2]] %>% str_remove_all("-") -> t_f
      
      # create empty nc file
      ncnew <- ncdf4::nc_create(filename = str_glue("{dir_mos}/fwi_{dom}_{mod}_day_{t_i}-{t_f}.nc"),
                                vars = vari,
                                force_v4 = TRUE)
      
      # write data to file
      ncdf4::ncvar_put(nc = ncnew, 
                       varid = vari, 
                       vals = mos[[1]])
      
      ncdf4::nc_close(ncnew)
    }
    
  })











