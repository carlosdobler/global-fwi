

# mount buckets
source("scripts/mount.R")

# load libraries
# build table of files
source(textConnection(readLines("scripts/setup.R")[4:16]))

plan(multicore, gc = T)



for(dom in c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS")){
  
  
  dir_mos <- str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/mosaics")
  if(!dir.exists(dir_mos)){
    dir.create(dir_mos)
  }
  
  
  # ******************************************************************************
  
  
  # TILING
  
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
  
  
  
  # *****************************************************************************
  
  str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/") %>% 
    list.files() %>% 
    .[str_detect(., "mosaics", negate = T)] %>% 
    str_split("_", simplify = T) %>% 
    {str_glue("{.[,2]}_{.[,3]}")} %>% 
    unique() -> mods
  

  
  for(mod in mods){
    
    print(str_glue(" "))
    print(str_glue("PROCESSING MODEL {which(mods == mod)} / {length(mods)}"))
    
    
    # copy files (tiles)
    print(str_glue("Copying files..."))
    tic("-- Done")
    
    dir_tiles <- "~/pers_disk/dir_tiles"                                                              # DIR TILES
    dir.create(dir_tiles)
    
    str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/") %>% 
      list.files() %>% 
      .[str_detect(., mod)] %>% 
      future_walk(function(f){
        
        orig <- str_glue("gs://clim_data_reg_useast1/results/global_fwi_ww/{dom}/{f}")
        dest <- str_glue("{dir_tiles}/{f}")
        
        system(str_glue("gsutil cp {orig} {dest}"), ignore.stdout = TRUE, ignore.stderr = TRUE)
        
      })
    toc()
    
    
    print(str_glue("Preparing..."))
    
    # obtain date vector
    dir_tiles %>% 
      list.files(full.names = T) %>% 
      .[str_detect(., mod)] %>% 
      .[1] %>% 
      read_ncdf(var = "fwi", proxy = T) %>% 
      suppressMessages() %>% 
      st_get_dimension_values("time") -> d
    
    # obtain years
    d %>% 
      str_sub(end = 4) %>% 
      unique() -> d_yrs
    
    d_yrs %>% 
      future_map(function(y){
        
        d %>% 
          str_sub(end = 4) %>% 
          {. %in% y} %>% 
          which() %>% 
          {c(first(.), last(.))}
        
      }) -> d_pos
    
    
    # identify calendar type
    if(str_glue("{str_sub(d[350*50], 1,4)}-02-30") %in% str_sub(d, 1, 10)){
      cal <- "360_day"
    } else if ("2032-02-29" %in% str_sub(d, 1, 10)){
      cal <- "gregorian"
    } else {
      cal <- "365_day"
    }
    
    
    # split tiles per rows
    unique(chunks_ind$lat_ch) %>% 
      as.numeric() %>% 
      sort() %>% 
      map(function(i){
        chunks_ind %>% 
          filter(lat_ch == i) %>% 
          pull(r) %>% 
          str_pad(3, "left", "0")
      }) -> tiles
    
    
    str_glue("~/bucket_risk/RCM_regridded_data/CORDEX_22/{dom}/daily/") %>% 
      list.dirs(recursive = F) %>% 
      .[str_detect(., "relative_humidity")] %>% 
      list.files(full.names = T) %>% 
      .[1] %>% 
      read_ncdf(ncsub = cbind(start = c(1,1,1),
                              count = c(NA,NA,1))) %>% 
      suppressMessages() %>% 
      adrop() -> s_ref
    
    s_ref %>% 
      st_get_dimension_values("lon") -> ref_lon
    
    
    
    print(str_glue("Mosaicking..."))
    
    d_pos %>%
      iwalk(function(d_p, i){
        
        print(str_glue(" "))
        print(str_glue("  Processing period {i} / {length(d_pos)}"))
        tic(str_glue("  -- Done"))
        
        tiles %>%
          future_map(function(r){
            
            ff <- character()
            while(length(ff) == 0){
              
              dir_tiles %>% 
                list.files(full.names = T) %>% 
                # .[str_detect(., mod)] %>% 
                .[str_detect(., str_flatten(r,"|"))] -> ff
              
            }
            
            ff %>%
              map(read_ncdf,
                  proxy = F,
                  var = "fwi",
                  ncsub = cbind(start = c(1,1,d_p[1]),
                                count = c(NA,NA,(d_p[2]-d_p[1]+1)))) %>%
              
              suppressMessages() %>%
              {do.call(c, c(., along = 1))} -> roww
            
            matrix(NA, length(ref_lon), dim(roww)[2]) %>%
              st_as_stars() %>%
              st_set_dimensions(1, values = ref_lon) %>%
              st_set_dimensions(2, values = st_get_dimension_values(roww, "lat")) %>%
              st_set_crs(4326) -> mm
            
            mm %>%
              st_set_dimensions(1, name = "lon") %>%
              st_set_dimensions(2, name = "lat") -> mm
            
            st_warp(roww, mm) -> roww_mm
            
            roww_mm %>% 
              st_dimensions() -> roww_mm_dim
            
            which(!round(st_get_dimension_values(mm, "lon", center = F),1) %in% round(st_get_dimension_values(roww, "lon"),1)) -> ind_not
            
            roww_mm %>% 
              pull(1) -> roww_mm
            
            roww_mm[ind_not,,] <- NA
            
            roww_mm %>% 
              st_as_stars() -> roww_mm
            
            st_dimensions(roww_mm) <- roww_mm_dim
            
            return(roww_mm)
            
          },
          .options = furrr_options(seed = NULL)
          ) -> l_rows
        
        
        do.call(c, c(l_rows, along = 2)) -> mos
        
        
        
        # **********
        
        
        print(str_glue("  Saving"))
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
        
        toc()
        
      })
    
    unlink(dir_tiles, recursive = T)
    
  }

}













