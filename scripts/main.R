
# INPUT SECTION

dom <- "CAM"    # domain
m <- 1          # model number
ti <- 1         # starting tile 


# ******************************************************************************


# mount buckets
source("scripts/mount.R")

# load libraries
# build table of files
source("scripts/setup.R")

mod <- unique(tb_files$model)[m]


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


# DOWNLOAD MODEL FILES
# (~10 min)

dir_model_files <- str_glue("z_dir_wholefiles")
dir.create(dir_model_files)

plan(multicore)
{
  tic()
  future_pwalk(tb_files %>% filter(model == mod), function(file,
                                                           var_long,
                                                           dom_ = dom,
                                                           dir_ = dir_model_files,
                                                           ...){
    
    orig <- file %>%
      {str_glue("gs://cmip5_data/RCM_regridded_data/CORDEX_22/{dom}/daily/{var_long}/{.}")}
    
    dest <- file %>%
      {str_glue("{dir_}/{.}")}
    
    system(str_glue("gsutil cp {orig} {dest}"),
           ignore.stdout = TRUE, ignore.stderr = TRUE)
    
  })
  toc()
}


# ******************************************************************************


# TILE LOOP

dir_tiles <- str_glue("~/bucket_mine/results/global_fwi_ww/{dom}")
dir.create(dir_tiles)

source("scripts/tile_loop.R")


# ******************************************************************************


# s_tile <- tile
# names(dim(s_tile)) <- c("lon", "lat", "var", "time")
# s_tile %>%
#   st_as_stars() -> s_tile
# 
# st_dimensions(s_tile)$lon <- st_dimensions(l_s_vars[[1]])$lon
# st_dimensions(s_tile)$lat <- st_dimensions(l_s_vars[[1]])$lat
# st_set_dimensions(s_tile, "time",
#                   values = st_get_dimension_values(l_s_vars[[1]], "time")) -> s_tile
# st_set_dimensions(s_tile, "var", values = c("FFMC", "DMC", "DC", "ISI", "BUI", "FWI", "DSR")) -> s_tile












