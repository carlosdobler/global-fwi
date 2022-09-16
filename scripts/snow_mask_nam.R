# mount buckets
source("scripts/mount.R")

# load libraries
# build table of files
source(textConnection(readLines("scripts/setup.R")[4:16]))

plan(multicore, gc = T)



dom <- "NAM"
  
  
dir_res <- str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/mosaics_snw_mask")
dir.create(dir_res)

str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/") %>% 
  list.files() %>% 
  .[str_detect(., "mosaics", negate = T)] %>% 
  str_split("_", simplify = T) %>% 
  {str_glue("{.[,2]}_{.[,3]}")} %>% 
  unique() -> mods


for(mod in mods){
  
  print(str_glue("PROCESSING MODEL {which(mods == mod)} / {length(mods)}"))
  
  # mod <- mods[2]
  
  mod %>% 
    str_split("_", simplify = T) %>% 
    .[,2] -> mod_sh
  
  mod_sh %>% 
    str_split("-", simplify = T) %>% 
    .[,2] -> mod_shh
  
  if(str_detect(mod_sh, "Had|GFDL")){
    
    dir_src <- "~/bucket_mine/cmip5/NAM_snw/"
    
    dir_src %>% 
      list.files() %>% 
      .[str_detect(., mod_shh)] -> f_all_snow
    
  } else {
    
    "~/bucket_risk/RCM_regridded_data/REMO2015/NAM/daily/surface_snow_amount" %>% 
      list.files() %>% 
      .[str_detect(., "MPI")] %>% 
      .[str_length(.) > 80] -> f_all_snow
    
  }
  
  f_all_snow %>% 
    str_split("_", simplify = T) %>% 
    .[, ncol(.)] %>% 
    str_split("-", simplify = T) %>% 
    as_tibble(.name_repair = "minimal") %>% 
    set_names(c("t_i", "t_f")) %>% 
    mutate(t_f = str_sub(t_f, end = 8)) %>% 
    arrange(t_i) %>% 
    mutate(r = row_number()) -> tb_time
  
  str_glue("~/bucket_mine/results/global_fwi_ww/{dom}/mosaics/") %>% 
    list.files() %>% 
    .[str_detect(., mod_sh)] -> f_all_fwi
  
    
  # COPY FILES
  
  print(str_glue("  Copying files ..."))
  
  {
    tic(str_glue("    SNW files copied"))
    
    dir_snw <- "~/pers_disk/snw"
    dir.create(dir_snw)
    
    f_all_snow %>%  
      future_walk(function(f){
        
        if(str_detect(mod_sh, "Had|GFDL")){
          
          str_glue("gs://clim_data_reg_useast1/cmip5/NAM_snw/{f}") -> orig
          
        } else {
          
          str_glue("gs://cmip5_data/RCM_regridded_data/REMO2015/NAM/daily/surface_snow_amount/{f}") -> orig
          
        }
        
        
        
        dest <- str_glue("{dir_snw}/{f}")
        
        system(str_glue("gsutil cp {orig} {dest}"), ignore.stdout = TRUE, ignore.stderr = TRUE)
        
      })
    
    toc()
  }
  
  {
    tic(str_glue("    FWI files copied"))
    
    dir_fwi <- "~/pers_disk/fwi"
    dir.create(dir_fwi)
    
    f_all_fwi %>%  
      future_walk(function(f){
        
        orig <- str_glue("gs://clim_data_reg_useast1/results/global_fwi_ww/{dom}/mosaics/{f}")
        dest <- str_glue("{dir_fwi}/{f}")
        
        system(str_glue("gsutil cp {orig} {dest}"), ignore.stdout = TRUE, ignore.stderr = TRUE)
        
      })
    
    toc()
  }
  
    
    
  # obtain full dates vector
  f_all_fwi %>% 
    map(function(f){
      
      str_glue("{dir_fwi}/{f}") %>% 
        # read_ncdf(ncsub = cbind(start = c(1,1,1),
        #                         count = c(1,1,NA))) %>% 
        read_stars(proxy = T) %>% 
        suppressMessages() %>% 
        suppressWarnings() %>% 
        st_get_dimension_values("time")
      
    }#,
    #.options = furrr_options(seed = NULL)
    ) -> d
  
  d %>% do.call(c, .) -> d
  
  if(str_glue("{str_sub(d[350*50], 1,4)}-02-30") %in% str_sub(d, 1, 10)){
    cal <- "360_day"
  } else if ("2032-02-29" %in% str_sub(d, 1, 10)){
    cal <- "gregorian"
  } else {
    cal <- "365_day"
  }
    
  
  # obtain lon/lat
  "~/pers_disk/fwi/" %>% 
    list.files(full.names = T) %>% 
    .[1] %>% 
    read_ncdf(ncsub = cbind(start = c(1,1,1),
                            count = c(NA,NA,1))) %>% 
    adrop() %>% 
    suppressMessages() -> fwi_proxy
  
  # dimensions
  dim_lon <- ncdf4::ncdim_def(name = "lon",
                              units = "degrees_east",
                              vals = fwi_proxy %>% st_get_dimension_values(1, center = F))
  
  dim_lat <- ncdf4::ncdim_def(name = "lat",
                              units = "degrees_north",
                              vals = fwi_proxy %>% st_get_dimension_values(2, center = F))
    
    
    
    
  # PROCESS MASK
  print(str_glue("  Masking"))
  tic(str_glue("    Done"))
  
  plan(multicore, workers = 4)
  
  # iwalk(f_all_fwi, function(f, i){
  walk(f_all_fwi[1:length(f_all_fwi)], function(f){
    
    print(f)
    
    # tic(str_glue("    Processed t {i} / {length(f_all_fwi)}"))
    
    # f <- f_all_fwi[1]
    
    str_glue("{dir_fwi}/{f}") %>%
      read_ncdf() %>% 
      suppressMessages() -> s_fwi
    
    s_fwi %>% 
      st_get_dimension_values("time") -> d_yr
    
    s_fwi %>% 
      st_set_dimensions("time",
                        values = d_yr %>% 
                          as.character()) -> s_fwi
    
    str_split(f, "_", simplify = T) %>% 
      .[,ncol(.)] %>% 
      str_sub(end = -4) %>% 
      str_split("-", simplify = T) %>% 
      {c(.[,1], .[,2])} -> t_i_f
    
    tb_time %>% 
      filter(t_f >= t_i_f[1],
             t_i <= t_i_f[2]) -> tb_snw_id
    
    if(nrow(tb_snw_id) > 0){
      
      f_all_snow[tb_snw_id$r] %>%
        {str_glue("{dir_snw}/{.}")} %>%
        read_stars(proxy = T) %>% 
        st_get_dimension_values("time") %>% 
        str_sub(end = 10) -> dd
      
      which(dd == t_i_f[1] %>% as_date %>% as.character) -> ini
      which(dd == t_i_f[2] %>% as_date %>% as.character) -> fin
      fin-ini+1 -> cnt
      
      if(nrow(tb_snw_id) > 1){
        
        f_all_snow[tb_snw_id$r] %>%
          {str_glue("{dir_snw}/{.}")} %>% 
          map(read_ncdf) %>% 
          do.call(c, .) %>% 
          suppressMessages() -> s_snw
        
        s_snw[,,,ini:fin] -> s_snw
        
        s_snw %>% 
          st_set_dimensions("time",
                          st_get_dimension_values(s_fwi, "time")) -> s_snw
        
      } else {
        
        f_all_snow[tb_snw_id$r] %>%
          {str_glue("{dir_snw}/{.}")} %>%
          read_ncdf(ncsub = cbind(start = c(1,1,ini),
                                  count = c(NA,NA,cnt))) %>% 
          
          suppressMessages() %>% 
          st_set_dimensions("time",
                            st_get_dimension_values(s_fwi, "time")) -> s_snw
        
        
      }
      
      
      
      # st_get_dimension_values(s_snw, "time") %>% 
      #   duplicated() %>% 
      #   which() -> dup
      # 
      # if(length(dup) > 0){
      #   
      #   s_snw %>% 
      #     slice(time, -dup) -> s_snw
      #   
      # }
      
      s_snw %>% 
        mutate(snw = drop_units(snw),
               snw = ifelse(is.na(snw) | snw < 2, 0, 1)) -> s_snw
      
      
      # which(!(st_get_dimension_values(s_fwi, "time") %in% st_get_dimension_values(s_snw, "time"))) -> missing_snw
      # 
      # if(length(missing_snw) > 0){
      #   
      #   s_fwi %>% 
      #     slice(time, -missing_snw) -> s_fwi_sub 
      #   
      #   if(any(dim(s_snw) != dim(s_fwi_sub))){
      #     s_snw %>% 
      #       st_warp(s_fwi_sub) -> s_snw
      #   }
      #   
      #   st_dimensions(s_snw) <- st_dimensions(s_fwi_sub)
      #   
      #   c(s_fwi_sub, s_snw) %>% 
      #     mutate(fwi_m = ifelse(snw == 1 & !is.na(fwi), 0, fwi)) %>%
      #     select(fwi_m) -> s_fwi_masked
      #   
      #   s_fwi %>% 
      #     slice(time, missing_snw) %>% 
      #     c(s_fwi_masked) -> s_fwi_masked
      #   
      #   pmatch(d_yr, st_get_dimension_values(s_fwi_masked, "time")) -> p_m
      #   
      #   s_fwi_masked[,,,p_m] -> s_fwi_masked
      #   
      # } else {
        
        try(c(s_fwi, s_snw)) -> fwi_snw
        
        if(class(fwi_snw) == "try-error"){
          
          s_snw %>% 
            st_warp(s_fwi) -> s_snw
          
          c(s_fwi, s_snw) -> fwi_snw
          
        }
        
        fwi_snw %>%
          # split("nd") %>%
          # setNames(c("fwi", "snw")) %>%
          mutate(fwi_m = ifelse(snw == 1 & !is.na(fwi), 0, fwi)) %>%
          select(fwi_m) -> s_fwi_masked
        
      # }
      
      
      
      
      # time dimension
      dim_time <- ncdf4::ncdim_def(name = "time",
                                   units = str_glue("days since {str_sub(d_yr[1], 1,10)}"),
                                   vals = seq(0, length(d_yr)-1),
                                   calendar = cal)
      
      # define variables
      vari <- ncdf4::ncvar_def(name = "fwi",
                               units = "",
                               dim = list(dim_lon, dim_lat, dim_time),
                               missval = -9999)
      
      ncnew <- ncdf4::nc_create(filename = str_glue("{dir_res}/{f}"),
                                vars = vari,
                                force_v4 = TRUE)
      
      # write data to file
      ncdf4::ncvar_put(nc = ncnew,
                       varid = vari,
                       vals = s_fwi_masked[[1]])
      
      ncdf4::nc_close(ncnew)
      
      # toc()
      
      rm(s_fwi, s_snw, s_fwi_masked)
      gc()
      
    } else {
      
      f %>% str_sub(end = -4) %>% {str_glue("{.}_nomask.nc")} -> ff
      system(str_glue("gsutil cp {dir_fwi}/{f} gs://clim_data_reg_useast1/results/global_fwi_ww/{dom}/mosaics_snw_mask/{ff}"),
             ignore.stdout = TRUE, ignore.stderr = TRUE)
      rm(s_fwi)
      gc()
      
    }
    
    
    
  }#,
  #.options = furrr_options(seed = NULL)
  )
  
  unlink(dir_fwi, recursive = T)
  unlink(dir_snw, recursive = T)
  
  plan(sequential)
  plan(multicore, gc = T)
  
}


