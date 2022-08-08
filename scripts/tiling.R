

f %>% 
  read_ncdf(ncsub = cbind(start = c(1, 1, 1),
                          count = c(NA,NA,1))) %>% 
  suppressMessages() %>% 
  slice(time, 1) -> s_proxy

c(s_proxy, s_proxy_remo) %>% 
  mutate(a = ifelse(is.na(hurs) | is.na(fwi), NA, 1)) %>% 
  select(a) -> land


# lon *****
s_proxy %>% 
  dim() %>% 
  .[1] %>% 
  seq_len() -> d_lon

round(length(d_lon)/sz) -> n_lon

split(d_lon, 
      ceiling(seq_along(d_lon)/(length(d_lon)/n_lon))) %>% 
  map(~c(first(.x), last(.x))) -> lon_chunks

# lat *****
s_proxy %>% 
  dim() %>% 
  .[2] %>% 
  seq_len() -> d_lat

round(length(d_lat)/sz) -> n_lat

split(d_lat, 
      ceiling(seq_along(d_lat)/(length(d_lat)/n_lat))) %>% 
  map(~c(first(.x), last(.x))) -> lat_chunks



# table + polygons

imap(lon_chunks, function(lon_ch, lon_i){
  imap(lat_chunks, function(lat_ch, lat_i){
    
    s_proxy %>%
      slice(lon, lon_ch[1]:lon_ch[2]) %>%
      slice(lat, lat_ch[1]:lat_ch[2]) -> s_proxy_sub
    
    st_warp(land,
            s_proxy_sub) -> land_rast_sub
    
    s_proxy_sub %>%
      st_bbox() %>%
      st_as_sfc() %>%
      st_sf() %>%
      mutate(lon_ch = lon_i,
             lat_ch = lat_i) -> pol_tile
    
    pol_tile %>% 
      mutate(cover = ifelse(all(is.na(pull(land_rast_sub, 1))) | all(is.na(pull(s_proxy_sub, 1))), F, T)) -> pol_tile
    
    if(pol_tile$cover == T){
      land_rast_sub %>%
        st_as_sf() %>% 
        summarize() %>%
        suppressMessages() %>%
        mutate(lon_ch = lon_i,
               lat_ch = lat_i) -> pol_land
    } else {
      pol_land <- NULL
    }
    
    list(pol_tile, pol_land)
    
  })
  
}) %>% 
  do.call(c, .) -> pols

pols %>% 
  map_dfr(pluck, 1) -> tb_ref

tb_ref %>% 
  filter(cover == T) %>% 
  mutate(r = row_number()) -> chunks_ind

pols[tb_ref$cover %>% which()] %>% 
  map_dfr(pluck, 2) %>% 
  mutate(r = row_number()) -> chunks_ind_land



rm(f, s_proxy, tb_ref,
   d_lon, n_lon, d_lat, n_lat, sz,
   pols)


