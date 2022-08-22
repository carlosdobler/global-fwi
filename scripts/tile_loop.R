pwalk(st_drop_geometry(chunks_ind)[ti:nrow(chunks_ind),], function(lon_ch, lat_ch, r, ...){
  
  # r <- chunks_ind$r[1]
  # lon_ch <- chunks_ind$lon_ch[1]
  # lat_ch <- chunks_ind$lat_ch[1]
  
  print(str_glue(" "))
  print(str_glue("PROCESSING TILE {r} / {nrow(chunks_ind)}"))
  tic(str_glue("DONE W/TILE {r} / {nrow(chunks_ind)}"))
  
  
  
  
  # IMPORT FILES
  # (~ 7 min)
  {
    tic(" -- everything loaded")
    plan(multicore, gc = T)
    map(vars, function(var_){  # future_?
      
      # import
      tic(str_glue("         {var_} done!"))
      
      tb_files %>% 
        filter(year(as_date(t_i)) >= 1970) %>% 
        filter(model == mod,
               var == var_) %>%
        
        
        future_pmap(function(file, t_i, t_f, dir_ = dir_model_files, r_ = r, ...){
          
          # print(file)
          
          cbind(start = c(lon_chunks[[lon_ch]][1], lat_chunks[[lat_ch]][1], 1),
                count = c(lon_chunks[[lon_ch]][2] - lon_chunks[[lon_ch]][1]+1,
                          lat_chunks[[lat_ch]][2] - lat_chunks[[lat_ch]][1]+1,
                          NA)) -> ncs
          
          if(dom == "EAS" & var_ == "sfcWind"){
            cbind(start = c(lon_chunks[[lon_ch]][1], lat_chunks[[lat_ch]][1],1, 1),
                  count = c(lon_chunks[[lon_ch]][2] - lon_chunks[[lon_ch]][1]+1,
                            lat_chunks[[lat_ch]][2] - lat_chunks[[lat_ch]][1]+1,
                            1,
                            NA)) -> ncs
          }
          
          file %>%
            {str_glue("{dir_}/{.}")} %>% 
            read_ncdf(ncsub = ncs) %>%
            suppressMessages()
          
        },
        .options = furrr_options(seed = NULL)) %>%
        do.call(c, .) %>% 
        setNames("v") -> s
      
      # fix duplicates and dates
      st_get_dimension_values(s, "time") -> d
      # d %>% as.POSIXct() %>% suppressWarnings() %>% as_date() -> d
      
      if(dom == "EAS" & mod == "RegCM4_NCC-NorESM1-M"){
        d %>% 
          str_sub(1,4) %>% 
          {. == "2100"} %>% 
          which() -> over2100
        
        if(length(over2100 > 0)){
          s %>% 
            slice("time", -over2100) %>% 
            suppressWarnings() -> s
        }
        
      }
      
      d %>%
        duplicated() %>% 
        which() -> dup
      
      if(length(dup) > 0){
        
        print(str_glue("  {var_} dupls: {d[dup]}"))
        
        s %>% 
          slice("time", -dup) %>% 
          suppressWarnings() -> s
        
        st_get_dimension_values(s, "time") -> d
        # d %>% as.POSIXct() %>% suppressWarnings() %>% as_date() -> d
      }
      
      # st_set_dimensions(s, 
      #                   "time", 
      #                   values = d) -> s
      
      # d %>%
      #   as.character() %>% 
      #   str_sub(end = 10) -> dd
      # 
      # str_c(rep(seq(1970,2099), each = 360), "-",
      #       rep(seq(1,12), each = 30, times = 2099-1970+1) %>% str_pad(2, "left", "0"), "-",
      #       rep(seq(1,30), times = 12*(2099-1970+1)) %>% str_pad(2, "left", "0")) -> d_comp
      # 
      # d_comp[which(!d_comp %in% dd)]
      
      
      # change units
      if(str_detect(var_, "tas")){
        
        s %>% 
          mutate(v = set_units(v, degC) %>% 
                   set_units(NULL)) -> s
        
      } else if(var_ == "pr"){
        
        s %>% 
          mutate(v = set_units(v, kg/m^2/d) %>%
                   set_units(NULL)) -> s
        
      } else if(var_ == "sfcWind"){
        
        s %>% 
          mutate(v = set_units(v, km/h) %>% 
                   set_units(NULL)) -> s
        
      } else {
        s %>% 
          mutate(v = set_units(v, NULL)) -> s
      }
      
      toc()
      
      return(s)
      
    }#,
    #.options = furrr_options(seed = NULL)
    ) -> l_s_vars
    
    toc()
  }
  
  adrop(l_s_vars[[2]]) -> l_s_vars[[2]]
  
  plan(sequential)
  gc()
  
  
  
  # # HOMOGENEIZE TIME DIM
  # l_s_vars %>%
  #   map_int(~dim(.x)[3]) -> tdim
  # 
  # if(var(tdim) != 0){
  # 
  #   tdim %>%
  #     which.min() -> min_tdim
  # 
  #   l_s_vars %>%
  #     pluck(min_tdim) %>%
  #     st_get_dimension_values("time") -> min_time
  # 
  #   for(i in seq_len(4)[-min_tdim]){
  #     l_s_vars[[i]] %>%
  #       filter(time %in% min_time) -> l_s_vars[[i]]
  #   }
  # 
  # }
  
  
  
  # REMOVE OCEAN / UNNECESSARY CELLS
  st_warp(land, 
          l_s_vars[[1]] %>% slice(time, 1)) -> land_tile
  
  
  l_s_vars %>% 
    map(function(s){
      
      s[is.na(land_tile)] <- NA
      return(s)
      
    }) -> l_s_vars
  
  
  
  
  # COLUMN LOOP
  dim_lon <- seq_len(dim(l_s_vars[[1]])[1])
  
  map(dim_lon, function(lon_){
    
    print(str_glue("processing col {lon_} / {length(dim_lon)}"))
    tic(str_glue("   done with col"))
    
    l_s_vars %>% 
      imap(function(s, i){
        
        s %>% 
          .[,lon_,,]
        
      }) -> l_s_vars_col
    
    plan(multicore, workers = 29, gc = T)
    
    # split into tables (1 per pixel)
    future_map(seq_len(dim(l_s_vars[[1]])[2]), function(lat_){
      
      l_s_vars_col %>% 
        imap_dfr(function(s, i){
          
          s %>% 
            .[,,lat_,] %>% 
            as_tibble() %>% 
            mutate(var = i)
          
        }) %>% 
        tidyr::pivot_wider(names_from = var, values_from = v) %>% 
        rename_with(.cols = c(1, 4:7), ~c("long", "rh", "ws", "temp", "prec"))-> tb
      
      if(any(!is.na(tb$rh))){
        
        tb %>%
          mutate(day = as.integer(str_sub(time, 9,10)),
                 mon = as.integer(str_sub(time, 6,7)),
                 yr = as.integer(str_sub(time, 1,4))) %>%
          select(-time) %>%
          arrange(yr, mon, day) %>%
          mutate(across(.cols = c(rh, ws, prec), ~ifelse(.x < 0, 0, .x))) %>%
          mutate(across(.cols = c(rh, ws, temp, prec), ~na_interpolation(.x, maxgap = 7))) -> tb
        
      }
      
      return(tb)
      
    }) -> l_tb
    
    
    # process tables (parallel)
    tic(str_glue("      fwi proc"))
    l_tb %>% 
      future_map(function(tb){
        
        # if(all(is.na(tb$rh))){
        if("time" %in% names(tb)){
          
          matrix(NA, nrow(tb), 7) #-> m
          # colnames(m) <- c("FFMC", "DMC", "DC", "ISI", "BUI", "FWI", "DSR")
          # return(m)
          
        } else {
          
          # tic()
          fwi(input = tb, init=c(85,6,15,tb$lat[1]), out = "fwi") %>% 
            suppressWarnings() %>% 
            as.matrix() %>% 
            unname()
          # toc()
          
        }
        
      }#,
      #.options = furrr_options(seed = NULL)
      ) -> l_fwi
    toc()
    
    plan(sequential)
    gc()
    
    # abind into array (dims = time, vars, lat)
    l_fwi %>% 
      {do.call(abind, c(., along = 3))} -> l_fwi
    
    toc()
    
    return(l_fwi)
    
  }) %>% 
    # abind into array (dims = time, vars, lat, lon)
    {do.call(abind, c(., along = 4))} -> tile
  
  # names(dim(tile)) <- c("time", "var", "lat", "lon")
  
  tile %>% 
    aperm(c(4,3,2,1)) -> tile
  
  
  
  
  # SAVE TILE AS NC
  {
    
    l_s_vars %>%
      map_int(~dim(.x)[3]) -> tdim
    
    tdim %>%
      which.max() -> max_tdim
    
    l_s_vars[[max_tdim]] %>% 
      st_get_dimension_values(3) -> d
    

    if(str_glue("{str_sub(d[350*50], 1,4)}-02-30") %in% str_sub(d, 1, 10)){
      cal <- "360_day"
    } else if ("2032-02-29" %in% str_sub(d, 1, 10)){
      cal <- "gregorian"
    } else {
      cal <- "365_day"
    }
    
    
    
    # define dimensions
    dim_lon <- ncdf4::ncdim_def(name = "lon", 
                                units = "degrees_east", 
                                vals = l_s_vars[[1]] %>% st_get_dimension_values(1))
    
    dim_lat <- ncdf4::ncdim_def(name = "lat", 
                                units = "degrees_north", 
                                vals = l_s_vars[[1]] %>% st_get_dimension_values(2))
    
    dim_time <- ncdf4::ncdim_def(name = "time", 
                                 # units = "days since 1970-01-01", 
                                 units = str_glue("days since {str_sub(d[1], 1,10)}"),
                                 # vals = l_s_vars[[1]] %>% 
                                 #   st_get_dimension_values(3) %>% 
                                 #   # as.character() %>% 
                                 #   # as_date() %>% 
                                 #   as.integer()
                                 vals = seq(0, length(d)-1),
                                 calendar = cal
    )
    
    # define variables
    c("ffmc", "dmc", "dc", "isi", "bui", "fwi", "dsr") %>% 
      map(~ncdf4::ncvar_def(name = .x,
                            units = "",
                            dim = list(dim_lon, dim_lat, dim_time), 
                            missval = -9999)) -> varis
    
    # create empty nc file
    ncnew <- ncdf4::nc_create(filename = str_glue("{dir_tiles}/{dom}_{mod}_{str_pad(r, 3, 'left', '0')}.nc"),
                              vars = varis,
                              force_v4 = TRUE)
    
    # write data to file
    seq_len(7) %>% 
      walk(~ncdf4::ncvar_put(nc = ncnew, 
                             varid = varis[[.x]], 
                             vals = tile[,,.x,]))
    
    ncdf4::nc_close(ncnew)
  }
  
  plan(sequential)
  rm(tile)
  rm(l_s_vars)
  gc()
  
  toc()
  
})
