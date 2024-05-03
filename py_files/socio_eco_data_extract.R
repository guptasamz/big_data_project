library(dplyr)
library(tidyverse)
library(tidycensus)
library(tidytransit)
library(car)
library(maptools)
library(rgdal)
library(maps)
library(ggthemes)
library(mapproj)
library(sf)



census_api_key("bf25006494b0b6456ec5a8c509e421b56df2c9c1", overwrite = TRUE, install = TRUE)
readRenviron("~/.Renviron")

#all variables of acs5
myvariable <- load_variables(2010, "acs5", cache = TRUE)
View(myvariable)

#selected variables
my_variables <- c("B19057_002E", "B19057_001E", "B22001_002E", "B22001_001E", 
                  "B25070_007E", "B25070_008E", "B25070_009E", "B25070_010E", "B25070_001E", 
                  "B03002_003E", "B03002_001E", "B03002_004E", "B03002_012E", "B01003_001E", "B25064_001E",
                  "B25077_001E", "B25008_001E", "B25008_002E", "B25008_003E", "B25002_001E",
                  "B25002_002E", "B25002_003E", "B08201_001E", "B08201_002E", "B08141_001E", 
                  "B08141_016E", "B08141_017E", "B19013_001E", "B17020_002E", "B17020_001E",
                  "B17001_002E", "B17001_001E", "B17012_002E", "B17012_001E", "B08012_001E", "B08012_002E", "B08012_003E",
                  "B08012_004E", "B08012_005E", "B08012_006E", "B08012_007E", "B08012_008E", "B08012_009E", "B08012_010E",
                  "B08012_011E", "B08012_012E", "B08012_013E")


#############################################
# Tract level data of Davidson County, Tennessee

#############################################
Chatt_Dem <- get_acs(geography = "tract", 
                     state = "TN", 
                     county = "Davidson", 
                     year = 2021,
                     variables = my_variables,
                     output = "wide",
                     geometry = TRUE, 
                     cache_table = TRUE) 

View(Chatt_Dem)
Chatt_Dem1<-Chatt_Dem

#Adding another columns of year and geographic level
Chatt_Dem1["Year"]<-2021
Chatt_Dem1["Geography"]<-"tract"

#Adding additional variables of percentages of existing variables
Chatt_Dem1<-Chatt_Dem1%>%  mutate( gross_rent_30 = ((B25070_007E 
                                                     +B25070_008E +B25070_009E+ B25070_010E)/B25070_001E)*100,
                                   white_pct = (B03002_003E/B03002_001E)*100, 
                                   black_pct = (B03002_004E/B03002_001E)*100,  
                                   hispanic_pct = (B03002_012E/B03002_001E)*100, 
                                   owner_occupied_pct = (B25008_002E/B25008_001E)*100,
                                   renter_occupied_pct = (B25008_003E/B25008_001E)*100,
                                   vacant_homes_pct = (B25002_003E/B25002_001E)*100,
                                   public_transit_pct = (B08141_016E/B08141_001E)*100,
                                   public_transit_NoCar_pct = (B08141_017E/B08141_001E)*100,
                                   poverty_pct = (B17001_002E/B17001_001E)*100,
                                   travel_time_less_than5min_pct = (B08012_002E/B08012_001E)*100,
                                   travel_time_5_to_9min_pct = (B08012_003E/B08012_001E)*100,
                                   travel_time_10_to_14min_pct = (B08012_004E/B08012_001E)*100,
                                   travel_time_15_to_19min_pct = (B08012_005E/B08012_001E)*100,
                                   travel_time_20_to_24min_pct = (B08012_006E/B08012_001E)*100,
                                   travel_time_25_to_29min_pct = (B08012_007E/B08012_001E)*100,
                                   travel_time_30_to_34min_pct = (B08012_008E/B08012_001E)*100,
                                   travel_time_35_to_39min_pct = (B08012_009E/B08012_001E)*100,
                                   travel_time_40_to_44min_pct = (B08012_010E/B08012_001E)*100,
                                   travel_time_45_to_59min_pct = (B08012_011E/B08012_001E)*100,
                                   travel_time_60_to_89min_pct = (B08012_012E/B08012_001E)*100,
                                   travel_time_90_or_more_min_pct = (B08012_013E/B08012_001E)*100)


#Renaming census variables from codes to common variable names
Chatt_Dem1<-Chatt_Dem1%>% rename(
  receive_public_assitance_income	=	B19057_002E	,
  total_surveyed_public_income_assistance	=	B19057_001E	,
  receive_foodstamps_last12months	=	B22001_002E 	,
  total_surveyed_receive_foodstamps_last12months	=	B22001_001E	,
  percent_gross_rent_of_hhincome_30.0_to_34.9	=	B25070_007E	,
  percent_gross_rent_of_hhincome_35.0_to_39.9	=	B25070_008E	,
  percent_gross_rent_of_hhincome_40.0_to_49.9	=	B25070_009E	,
  percent_gross_rent_of_hhincome_50_or_more	=	B25070_010E	,
  total_percent_gross_rent_of_hhincome	=	B25070_001E	,
  not_hispanic_or_latino_white	=	B03002_003E	,
  total_surveyed_race	=	B03002_001E	,
  not_hispanic_or_latino_black	=	B03002_004E	,
  hispanic_or_latino	=	B03002_012E	,
  total_population	=	B01003_001E	,
  median_gross_rent	=	B25064_001E	,
  median_home_value	=	B25077_001E	,
  total_surveyed_occupied_housing	=	B25008_001E	,
  owner_occupied	=	B25008_002E	,
  renter_occupied	=	B25008_003E	,
  total_housing_units	=	B25002_001E	,
  occupied_housing_units	=	B25002_002E	,
  vacant_housing_units	=	B25002_003E	,
  total_surveyed_vehicles_in_hh	=	B08201_001E	,
  no_vehicles_in_hh	=	B08201_002E	,
  total_surveyed_vehicles_for_work	=	B08141_001E	,
  total_surveyed_public_transportation_for_work	=	B08141_016E	,
  no_public_transport_for_work	=	B08141_017E	,
  median_income_last12months	=	B19013_001E	,
  income_below_poverty_level_last12months_by_age	=	B17020_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_age	=	B17020_001E	,
  income_below_poverty_level_last12months_by_sex_and_age	=	B17001_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_001E	,
  income_below_poverty_level_last12months_by_hh_type_children	=	B17012_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_001E	,
  total_travel_time_to_work	=	B08012_001E	,
  travel_time_to_work_less_than_5min	=	B08012_002E	,
  travel_time_to_work_5_to_9min	=	B08012_003E	,
  travel_time_to_work_10_to_14min	=	B08012_004E	,
  travel_time_to_work_15_to_19min	=	B08012_005E	,
  travel_time_to_work_20_to_24min	=	B08012_006E	,
  travel_time_to_work_25_to_29min	=	B08012_007E	,
  travel_time_to_work_30_to_34min	=	B08012_008E	,
  travel_time_to_work_35_to_39min	=	B08012_009E	,
  travel_time_to_work_40_to_44min	=	B08012_010E	,
  travel_time_to_work_45_to_59min	=	B08012_011E	,
  travel_time_to_work_60_to_89min	=	B08012_012E	,
  travel_time_to_work_90_or_more_min	=	B08012_013E,
  margin_error_receive_public_assitance_income	=	B19057_002M	,
  margin_error_total_surveyed_public_income_assistance	=	B19057_001M	,
  margin_error_receive_foodstamps_last12months	=	B22001_002M,
  margin_error_total_surveyed_receive_foodstamps_last12months	=	B22001_001M	,
  margin_error_percent_gross_rent_of_hhincome_30.0_to_34.9	=	B25070_007M	,
  margin_error_percent_gross_rent_of_hhincome_35.0_to_39.9	=	B25070_008M	,
  margin_error_percent_gross_rent_of_hhincome_40.0_to_49.9	=	B25070_009M	,
  margin_error_percent_gross_rent_of_hhincome_50_or_more	=	B25070_010M	,
  margin_error_total_percent_gross_rent_of_hhincome	=	B25070_001M	,
  margin_error_not_hispanic_or_latino_white	=	B03002_003M	,
  margin_error_total_surveyed_race	=	B03002_001M	,
  margin_error_not_hispanic_or_latino_black	=	B03002_004M	,
  margin_error_hispanic_or_latino	=	B03002_012M	,
  margin_error_total_population	=	B01003_001M	,
  margin_error_median_gross_rent	=	B25064_001M	,
  margin_error_median_home_value	=	B25077_001M	,
  margin_error_total_surveyed_occupied_housing	=	B25008_001M	,
  margin_error_owner_occupied	=	B25008_002M	,
  margin_error_renter_occupied	=	B25008_003M	,
  margin_error_total_housing_units	=	B25002_001M	,
  margin_error_occupied_housing_units	=	B25002_002M	,
  margin_error_vacant_housing_units	=	B25002_003M	,
  margin_error_total_surveyed_vehicles_in_hh	=	B08201_001M	,
  margin_error_no_vehicles_in_hh	=	B08201_002M	,
  margin_error_total_surveyed_vehicles_for_work	=	B08141_001M	,
  margin_error_total_surveyed_public_transportation_for_work	=	B08141_016M	,
  margin_error_no_public_transport_for_work	=	B08141_017M	,
  margin_error_median_income_last12months	=	B19013_001M	,
  margin_error_income_below_poverty_level_last12months_by_age	=	B17020_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_age	=	B17020_001M	,
  margin_error_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_001M	,
  margin_error_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_001M	,
  margin_error_total_travel_time_to_work	=	B08012_001M	,
  margin_error_travel_time_to_work_less_than_5min	=	B08012_002M	,
  margin_error_travel_time_to_work_5_to_9min	=	B08012_003M	,
  margin_error_travel_time_to_work_10_to_14min	=	B08012_004M	,
  margin_error_travel_time_to_work_15_to_19min	=	B08012_005M	,
  margin_error_travel_time_to_work_20_to_24min	=	B08012_006M	,
  margin_error_travel_time_to_work_25_to_29min	=	B08012_007M	,
  margin_error_travel_time_to_work_30_to_34min	=	B08012_008M	,
  margin_error_travel_time_to_work_35_to_39min	=	B08012_009M	,
  margin_error_travel_time_to_work_40_to_44min	=	B08012_010M	,
  margin_error_travel_time_to_work_45_to_59min	=	B08012_011M	,
  margin_error_travel_time_to_work_60_to_89min	=	B08012_012M	,
  margin_error_travel_time_to_work_90_or_more_min	=	B08012_013M)


Loc1=Chatt_Dem1 %>%select(GEOID, NAME, geometry)

df= str_split_fixed(Chatt_Dem1$NAME, ", ", 3)
df1= data.frame(df)
colnames(df1)<-c("Census Tract", "County", "State")
Loc1=bind_cols(Loc1, df1)

#exporting dataframe into geoJSON
my_chatt_census_sf <- st_as_sf(Chatt_Dem1)
view(my_chatt_census_sf)
st_write(my_chatt_census_sf, '../data/socio_economic/2021_census_tract_davidson.geojson',append = FALSE) 
Loc1_sf<-st_as_sf(Loc1)
st_write(Loc1_sf, '../data/socio_economic/2021_census_tract_group_davidson_location_table.geojson') 

#############################################
# Block Group level data of Tennessee
#############################################
Chatt_bg_Dem <- get_acs(geography = "block group", 
                        state = "TN", 
                        county = "Davidson", 
                        year = 2021,
                        variables = my_variables,
                        output = "wide",
                        geometry = TRUE, 
                        cache_table = TRUE) 
view(Chatt_bg_Dem)
Chatt_bg_Dem1<-Chatt_bg_Dem
#Adding another columns of year and geographic level
Chatt_bg_Dem1["Year"]<-2021
Chatt_bg_Dem1["Geography"]<-"Block Group"

#Adding additional variables of percentages of existing variables
Chatt_bg_Dem1<-Chatt_bg_Dem1%>%  mutate( gross_rent_30 = ((B25070_007E 
                                                           +B25070_008E +B25070_009E+ B25070_010E)/B25070_001E)*100,
                                         white_pct = (B03002_003E/B03002_001E)*100, 
                                         black_pct = (B03002_004E/B03002_001E)*100,  
                                         hispanic_pct = (B03002_012E/B03002_001E)*100, 
                                         owner_occupied_pct = (B25008_002E/B25008_001E)*100,
                                         renter_occupied_pct = (B25008_003E/B25008_001E)*100,
                                         vacant_homes_pct = (B25002_003E/B25002_001E)*100,
                                         public_transit_pct = (B08141_016E/B08141_001E)*100,
                                         public_transit_NoCar_pct = (B08141_017E/B08141_001E)*100,
                                         poverty_pct = (B17001_002E/B17001_001E)*100,
                                         travel_time_less_than5min_pct = (B08012_002E/B08012_001E)*100,
                                         travel_time_5_to_9min_pct = (B08012_003E/B08012_001E)*100,
                                         travel_time_10_to_14min_pct = (B08012_004E/B08012_001E)*100,
                                         travel_time_15_to_19min_pct = (B08012_005E/B08012_001E)*100,
                                         travel_time_20_to_24min_pct = (B08012_006E/B08012_001E)*100,
                                         travel_time_25_to_29min_pct = (B08012_007E/B08012_001E)*100,
                                         travel_time_30_to_34min_pct = (B08012_008E/B08012_001E)*100,
                                         travel_time_35_to_39min_pct = (B08012_009E/B08012_001E)*100,
                                         travel_time_40_to_44min_pct = (B08012_010E/B08012_001E)*100,
                                         travel_time_45_to_59min_pct = (B08012_011E/B08012_001E)*100,
                                         travel_time_60_to_89min_pct = (B08012_012E/B08012_001E)*100,
                                         travel_time_90_or_more_min_pct = (B08012_013E/B08012_001E)*100)

#Renaming census variables from codes to common variable names
Chatt_bg_Dem1<-Chatt_bg_Dem1%>% rename(
  receive_public_assitance_income	=	B19057_002E	,
  total_surveyed_public_income_assistance	=	B19057_001E	,
  receive_foodstamps_last12months	=	B22001_002E 	,
  total_surveyed_receive_foodstamps_last12months	=	B22001_001E	,
  percent_gross_rent_of_hhincome_30.0_to_34.9	=	B25070_007E	,
  percent_gross_rent_of_hhincome_35.0_to_39.9	=	B25070_008E	,
  percent_gross_rent_of_hhincome_40.0_to_49.9	=	B25070_009E	,
  percent_gross_rent_of_hhincome_50_or_more	=	B25070_010E	,
  total_percent_gross_rent_of_hhincome	=	B25070_001E	,
  not_hispanic_or_latino_white	=	B03002_003E	,
  total_surveyed_race	=	B03002_001E	,
  not_hispanic_or_latino_black	=	B03002_004E	,
  hispanic_or_latino	=	B03002_012E	,
  total_population	=	B01003_001E	,
  median_gross_rent	=	B25064_001E	,
  median_home_value	=	B25077_001E	,
  total_surveyed_occupied_housing	=	B25008_001E	,
  owner_occupied	=	B25008_002E	,
  renter_occupied	=	B25008_003E	,
  total_housing_units	=	B25002_001E	,
  occupied_housing_units	=	B25002_002E	,
  vacant_housing_units	=	B25002_003E	,
  total_surveyed_vehicles_in_hh	=	B08201_001E	,
  no_vehicles_in_hh	=	B08201_002E	,
  total_surveyed_vehicles_for_work	=	B08141_001E	,
  total_surveyed_public_transportation_for_work	=	B08141_016E	,
  no_public_transport_for_work	=	B08141_017E	,
  median_income_last12months	=	B19013_001E	,
  income_below_poverty_level_last12months_by_age	=	B17020_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_age	=	B17020_001E	,
  income_below_poverty_level_last12months_by_sex_and_age	=	B17001_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_001E	,
  income_below_poverty_level_last12months_by_hh_type_children	=	B17012_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_001E	,
  total_travel_time_to_work	=	B08012_001E	,
  travel_time_to_work_less_than_5min	=	B08012_002E	,
  travel_time_to_work_5_to_9min	=	B08012_003E	,
  travel_time_to_work_10_to_14min	=	B08012_004E	,
  travel_time_to_work_15_to_19min	=	B08012_005E	,
  travel_time_to_work_20_to_24min	=	B08012_006E	,
  travel_time_to_work_25_to_29min	=	B08012_007E	,
  travel_time_to_work_30_to_34min	=	B08012_008E	,
  travel_time_to_work_35_to_39min	=	B08012_009E	,
  travel_time_to_work_40_to_44min	=	B08012_010E	,
  travel_time_to_work_45_to_59min	=	B08012_011E	,
  travel_time_to_work_60_to_89min	=	B08012_012E	,
  travel_time_to_work_90_or_more_min	=	B08012_013E,
  margin_error_receive_public_assitance_income	=	B19057_002M	,
  margin_error_total_surveyed_public_income_assistance	=	B19057_001M	,
  margin_error_receive_foodstamps_last12months	=	B22001_002M,
  margin_error_total_surveyed_receive_foodstamps_last12months	=	B22001_001M	,
  margin_error_percent_gross_rent_of_hhincome_30.0_to_34.9	=	B25070_007M	,
  margin_error_percent_gross_rent_of_hhincome_35.0_to_39.9	=	B25070_008M	,
  margin_error_percent_gross_rent_of_hhincome_40.0_to_49.9	=	B25070_009M	,
  margin_error_percent_gross_rent_of_hhincome_50_or_more	=	B25070_010M	,
  margin_error_total_percent_gross_rent_of_hhincome	=	B25070_001M	,
  margin_error_not_hispanic_or_latino_white	=	B03002_003M	,
  margin_error_total_surveyed_race	=	B03002_001M	,
  margin_error_not_hispanic_or_latino_black	=	B03002_004M	,
  margin_error_hispanic_or_latino	=	B03002_012M	,
  margin_error_total_population	=	B01003_001M	,
  margin_error_median_gross_rent	=	B25064_001M	,
  margin_error_median_home_value	=	B25077_001M	,
  margin_error_total_surveyed_occupied_housing	=	B25008_001M	,
  margin_error_owner_occupied	=	B25008_002M	,
  margin_error_renter_occupied	=	B25008_003M	,
  margin_error_total_housing_units	=	B25002_001M	,
  margin_error_occupied_housing_units	=	B25002_002M	,
  margin_error_vacant_housing_units	=	B25002_003M	,
  margin_error_total_surveyed_vehicles_in_hh	=	B08201_001M	,
  margin_error_no_vehicles_in_hh	=	B08201_002M	,
  margin_error_total_surveyed_vehicles_for_work	=	B08141_001M	,
  margin_error_total_surveyed_public_transportation_for_work	=	B08141_016M	,
  margin_error_no_public_transport_for_work	=	B08141_017M	,
  margin_error_median_income_last12months	=	B19013_001M	,
  margin_error_income_below_poverty_level_last12months_by_age	=	B17020_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_age	=	B17020_001M	,
  margin_error_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_001M	,
  margin_error_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_001M	,
  margin_error_total_travel_time_to_work	=	B08012_001M	,
  margin_error_travel_time_to_work_less_than_5min	=	B08012_002M	,
  margin_error_travel_time_to_work_5_to_9min	=	B08012_003M	,
  margin_error_travel_time_to_work_10_to_14min	=	B08012_004M	,
  margin_error_travel_time_to_work_15_to_19min	=	B08012_005M	,
  margin_error_travel_time_to_work_20_to_24min	=	B08012_006M	,
  margin_error_travel_time_to_work_25_to_29min	=	B08012_007M	,
  margin_error_travel_time_to_work_30_to_34min	=	B08012_008M	,
  margin_error_travel_time_to_work_35_to_39min	=	B08012_009M	,
  margin_error_travel_time_to_work_40_to_44min	=	B08012_010M	,
  margin_error_travel_time_to_work_45_to_59min	=	B08012_011M	,
  margin_error_travel_time_to_work_60_to_89min	=	B08012_012M	,
  margin_error_travel_time_to_work_90_or_more_min	=	B08012_013M	)

#Dropping empty columns
emptycols <- sapply(Chatt_bg_Dem1, function (k) all(is.na(k)))
Chatt_bg_Dem2 <- Chatt_bg_Dem1[!emptycols]

Loc2=Chatt_bg_Dem1 %>%select(GEOID, NAME, geometry)

df= str_split_fixed(Chatt_bg_Dem1$NAME, ", ", 4)
df2= data.frame(df)
colnames(df2)<-c("Block Group","Census Tract", "County", "State")
Loc2=bind_cols(Loc2, df2)

#exporting dataframe into geoJSON
my_Chatt_census_sf <- st_as_sf(Chatt_bg_Dem2)
st_write(my_Chatt_census_sf, '../data/socio_economic/2021_block_group_davidson.geojson') 
Loc2_sf<-st_as_sf(Loc2)
st_write(Loc2_sf, '../data/socio_economic/2021_block_group_davidson_location_table.geojson') 

#############################################
# Block level data of Davidson County, Tennessee
#############################################


census_api_key("bf25006494b0b6456ec5a8c509e421b56df2c9c1", overwrite = TRUE, install = TRUE)
readRenviron("~/.Renviron")

#all variables of acs5
myvariable2 <- load_variables(2010, "sf1", cache = TRUE)
view(myvariable2)

#selected variables
my_variables2 <- c("P005001",
                   "P005003",
                   "P005004",
                   "P005010",
                   "P001001",
                   "H011001",
                   "H011002",
                   "H011003",
                   "H011004",
                   "H003001",
                   "H003002",
                   "H003003")

# Block level data of Davidson County, Tennessee
Chatt_Block <- get_decennial(geography = "block", 
                             state = "TN", 
                             county = "Davidson", 
                             year = 2010,
                             variables = 	my_variables2,
                             output = "wide",
                             geometry = TRUE, 
                             cache_table = TRUE) 
View(Chatt_Block)
Chatt_Block1<-Chatt_Block

#Adding another columns of year and geographic level
Chatt_Block1["Year"]<-2010
Chatt_Block1["Geography"]<-"Block"

#Adding additional variables of percentages of existing variables
Chatt_Block1<-Chatt_Block1%>%  mutate( 
  White_pct = (P005003/P005001)*100, 
  Black_pct = (P005004/P005001)*100,  
  Hispanic_pct = (P005010/P005001)*100, 
  Owner_Occupied_pct = ((H011002+H011003)/H011001)*100,
  Renter_Occupied_pct = (H011004/H011001)*100,
  Vacant_Homes_pct = (H003003/H003001)*100)

#Renaming census variables from codes to common variable names
Chatt_Block1<-Chatt_Block1%>% rename(
  non_hispanic_or_latino_white	=	P005003	,
  total_surveyed_race	=	P005001	,
  non_hispanic_or_latino_black	=	P005004	,
  hispanic_or_latino	=	P005010 ,
  total_population	=	P001001	,
  total_surveyed_occupied_housing	=	H011001,
  owner_occupied_mortgage	=	H011002	,
  owner_occupied_clear= H011003,
  renter_occupied	=	H011004	,
  total_surveyed_housing_occupancy_units	=	H003001	,
  occupied_housing_units	=	H003002	,
  vacant_housing_units	=	H003003	
)

Loc3=Chatt_Block1 %>%select(GEOID, NAME, geometry)

df= str_split_fixed(Chatt_Block1$NAME, ", ", 5)
df3= data.frame(df)
colnames(df3)<-c("Block","Block Group","Census Tract", "County", "State")
Loc3=bind_cols(Loc3, df3)


#exporting dataframe into geoJSON
my_Chatt_census_sf <- st_as_sf(Chatt_Block1)
Loc3_sf<-st_as_sf(Loc3)
st_write(my_Chatt_census_sf, '../data/socio_economic/2010_block_davidson_location_table.geojson') 
st_write(Loc3_sf, '../data/socio_economic/2010_block_davidson.geojson') 

#############################################
# Tract level data of  Tennessee

#############################################
TN_Dem <- get_acs(geography = "tract", 
                  state = "TN", 
                  
                  year = 2021,
                  variables = my_variables,
                  output = "wide",
                  geometry = TRUE, 
                  cache_table = TRUE) 

View(TN_Dem)
TN_Dem1<-TN_Dem

#Adding another columns of year and geographic level
TN_Dem1["Year"]<-2021
TN_Dem1["Geography"]<-"tract"

#Adding additional variables of percentages of existing variables
TN_Dem1<-TN_Dem1%>%  mutate( gross_rent_30 = ((B25070_007E 
                                               +B25070_008E +B25070_009E+ B25070_010E)/B25070_001E)*100,
                             white_pct = (B03002_003E/B03002_001E)*100, 
                             black_pct = (B03002_004E/B03002_001E)*100,  
                             hispanic_pct = (B03002_012E/B03002_001E)*100, 
                             owner_occupied_pct = (B25008_002E/B25008_001E)*100,
                             renter_occupied_pct = (B25008_003E/B25008_001E)*100,
                             vacant_homes_pct = (B25002_003E/B25002_001E)*100,
                             public_transit_pct = (B08141_016E/B08141_001E)*100,
                             public_transit_NoCar_pct = (B08141_017E/B08141_001E)*100,
                             poverty_pct = (B17001_002E/B17001_001E)*100,
                             travel_time_less_than5min_pct = (B08012_002E/B08012_001E)*100,
                             travel_time_5_to_9min_pct = (B08012_003E/B08012_001E)*100,
                             travel_time_10_to_14min_pct = (B08012_004E/B08012_001E)*100,
                             travel_time_15_to_19min_pct = (B08012_005E/B08012_001E)*100,
                             travel_time_20_to_24min_pct = (B08012_006E/B08012_001E)*100,
                             travel_time_25_to_29min_pct = (B08012_007E/B08012_001E)*100,
                             travel_time_30_to_34min_pct = (B08012_008E/B08012_001E)*100,
                             travel_time_35_to_39min_pct = (B08012_009E/B08012_001E)*100,
                             travel_time_40_to_44min_pct = (B08012_010E/B08012_001E)*100,
                             travel_time_45_to_59min_pct = (B08012_011E/B08012_001E)*100,
                             travel_time_60_to_89min_pct = (B08012_012E/B08012_001E)*100,
                             travel_time_90_or_more_min_pct = (B08012_013E/B08012_001E)*100)


#Renaming census variables from codes to common variable names
TN_Dem1<-TN_Dem1%>% rename(
  receive_public_assitance_income	=	B19057_002E	,
  total_surveyed_public_income_assistance	=	B19057_001E	,
  receive_foodstamps_last12months	=	B22001_002E 	,
  total_surveyed_receive_foodstamps_last12months	=	B22001_001E	,
  percent_gross_rent_of_hhincome_30.0_to_34.9	=	B25070_007E	,
  percent_gross_rent_of_hhincome_35.0_to_39.9	=	B25070_008E	,
  percent_gross_rent_of_hhincome_40.0_to_49.9	=	B25070_009E	,
  percent_gross_rent_of_hhincome_50_or_more	=	B25070_010E	,
  total_percent_gross_rent_of_hhincome	=	B25070_001E	,
  not_hispanic_or_latino_white	=	B03002_003E	,
  total_surveyed_race	=	B03002_001E	,
  not_hispanic_or_latino_black	=	B03002_004E	,
  hispanic_or_latino	=	B03002_012E	,
  total_population	=	B01003_001E	,
  median_gross_rent	=	B25064_001E	,
  median_home_value	=	B25077_001E	,
  total_surveyed_occupied_housing	=	B25008_001E	,
  owner_occupied	=	B25008_002E	,
  renter_occupied	=	B25008_003E	,
  total_housing_units	=	B25002_001E	,
  occupied_housing_units	=	B25002_002E	,
  vacant_housing_units	=	B25002_003E	,
  total_surveyed_vehicles_in_hh	=	B08201_001E	,
  no_vehicles_in_hh	=	B08201_002E	,
  total_surveyed_vehicles_for_work	=	B08141_001E	,
  total_surveyed_public_transportation_for_work	=	B08141_016E	,
  no_public_transport_for_work	=	B08141_017E	,
  median_income_last12months	=	B19013_001E	,
  income_below_poverty_level_last12months_by_age	=	B17020_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_age	=	B17020_001E	,
  income_below_poverty_level_last12months_by_sex_and_age	=	B17001_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_001E	,
  income_below_poverty_level_last12months_by_hh_type_children	=	B17012_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_001E	,
  total_travel_time_to_work	=	B08012_001E	,
  travel_time_to_work_less_than_5min	=	B08012_002E	,
  travel_time_to_work_5_to_9min	=	B08012_003E	,
  travel_time_to_work_10_to_14min	=	B08012_004E	,
  travel_time_to_work_15_to_19min	=	B08012_005E	,
  travel_time_to_work_20_to_24min	=	B08012_006E	,
  travel_time_to_work_25_to_29min	=	B08012_007E	,
  travel_time_to_work_30_to_34min	=	B08012_008E	,
  travel_time_to_work_35_to_39min	=	B08012_009E	,
  travel_time_to_work_40_to_44min	=	B08012_010E	,
  travel_time_to_work_45_to_59min	=	B08012_011E	,
  travel_time_to_work_60_to_89min	=	B08012_012E	,
  travel_time_to_work_90_or_more_min	=	B08012_013E,
  margin_error_receive_public_assitance_income	=	B19057_002M	,
  margin_error_total_surveyed_public_income_assistance	=	B19057_001M	,
  margin_error_receive_foodstamps_last12months	=	B22001_002M,
  margin_error_total_surveyed_receive_foodstamps_last12months	=	B22001_001M	,
  margin_error_percent_gross_rent_of_hhincome_30.0_to_34.9	=	B25070_007M	,
  margin_error_percent_gross_rent_of_hhincome_35.0_to_39.9	=	B25070_008M	,
  margin_error_percent_gross_rent_of_hhincome_40.0_to_49.9	=	B25070_009M	,
  margin_error_percent_gross_rent_of_hhincome_50_or_more	=	B25070_010M	,
  margin_error_total_percent_gross_rent_of_hhincome	=	B25070_001M	,
  margin_error_not_hispanic_or_latino_white	=	B03002_003M	,
  margin_error_total_surveyed_race	=	B03002_001M	,
  margin_error_not_hispanic_or_latino_black	=	B03002_004M	,
  margin_error_hispanic_or_latino	=	B03002_012M	,
  margin_error_total_population	=	B01003_001M	,
  margin_error_median_gross_rent	=	B25064_001M	,
  margin_error_median_home_value	=	B25077_001M	,
  margin_error_total_surveyed_occupied_housing	=	B25008_001M	,
  margin_error_owner_occupied	=	B25008_002M	,
  margin_error_renter_occupied	=	B25008_003M	,
  margin_error_total_housing_units	=	B25002_001M	,
  margin_error_occupied_housing_units	=	B25002_002M	,
  margin_error_vacant_housing_units	=	B25002_003M	,
  margin_error_total_surveyed_vehicles_in_hh	=	B08201_001M	,
  margin_error_no_vehicles_in_hh	=	B08201_002M	,
  margin_error_total_surveyed_vehicles_for_work	=	B08141_001M	,
  margin_error_total_surveyed_public_transportation_for_work	=	B08141_016M	,
  margin_error_no_public_transport_for_work	=	B08141_017M	,
  margin_error_median_income_last12months	=	B19013_001M	,
  margin_error_income_below_poverty_level_last12months_by_age	=	B17020_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_age	=	B17020_001M	,
  margin_error_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_001M	,
  margin_error_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_001M	,
  margin_error_total_travel_time_to_work	=	B08012_001M	,
  margin_error_travel_time_to_work_less_than_5min	=	B08012_002M	,
  margin_error_travel_time_to_work_5_to_9min	=	B08012_003M	,
  margin_error_travel_time_to_work_10_to_14min	=	B08012_004M	,
  margin_error_travel_time_to_work_15_to_19min	=	B08012_005M	,
  margin_error_travel_time_to_work_20_to_24min	=	B08012_006M	,
  margin_error_travel_time_to_work_25_to_29min	=	B08012_007M	,
  margin_error_travel_time_to_work_30_to_34min	=	B08012_008M	,
  margin_error_travel_time_to_work_35_to_39min	=	B08012_009M	,
  margin_error_travel_time_to_work_40_to_44min	=	B08012_010M	,
  margin_error_travel_time_to_work_45_to_59min	=	B08012_011M	,
  margin_error_travel_time_to_work_60_to_89min	=	B08012_012M	,
  margin_error_travel_time_to_work_90_or_more_min	=	B08012_013M)

view(TN_Dem1)

Loc4=TN_Dem1 %>%select(GEOID, NAME, geometry)

df= str_split_fixed(TN_Dem1$NAME, ", ", 3)
df4= data.frame(df)
colnames(df1)<-c("Census Tract", "County", "State")
Loc4=bind_cols(Loc4, df4)

#exporting dataframe into geoJSON
my_TN_census_sf <- st_as_sf(TN_Dem1)
view(my_TN_census_sf)
st_write(my_TN_census_sf, '../data/socio_economic/2021_census_tract_TN.geojson',append = FALSE) 
Loc4_sf<-st_as_sf(Loc4)
st_write(Loc4_sf, '../data/socio_economic/2021_census_tract_TN_location_table.geojson') 

#############################################
# Block Group level data of Tennessee
#############################################
TN_bg_Dem <- get_acs(geography = "block group", 
                     state = "TN", 
                     year = 2021,
                     variables = my_variables,
                     output = "wide",
                     geometry = TRUE, 
                     cache_table = TRUE) 

TN_bg_Dem1<-TN_bg_Dem
#Adding another columns of year and geographic level
TN_bg_Dem1["Year"]<-2021
TN_bg_Dem1["Geography"]<-"Block Group"

#Adding additional variables of percentages of existing variables
TN_bg_Dem1<-TN_bg_Dem1%>%  mutate( gross_rent_30 = ((B25070_007E 
                                                     +B25070_008E +B25070_009E+ B25070_010E)/B25070_001E)*100,
                                   white_pct = (B03002_003E/B03002_001E)*100, 
                                   black_pct = (B03002_004E/B03002_001E)*100,  
                                   hispanic_pct = (B03002_012E/B03002_001E)*100, 
                                   owner_occupied_pct = (B25008_002E/B25008_001E)*100,
                                   renter_occupied_pct = (B25008_003E/B25008_001E)*100,
                                   vacant_homes_pct = (B25002_003E/B25002_001E)*100,
                                   public_transit_pct = (B08141_016E/B08141_001E)*100,
                                   public_transit_NoCar_pct = (B08141_017E/B08141_001E)*100,
                                   poverty_pct = (B17001_002E/B17001_001E)*100,
                                   travel_time_less_than5min_pct = (B08012_002E/B08012_001E)*100,
                                   travel_time_5_to_9min_pct = (B08012_003E/B08012_001E)*100,
                                   travel_time_10_to_14min_pct = (B08012_004E/B08012_001E)*100,
                                   travel_time_15_to_19min_pct = (B08012_005E/B08012_001E)*100,
                                   travel_time_20_to_24min_pct = (B08012_006E/B08012_001E)*100,
                                   travel_time_25_to_29min_pct = (B08012_007E/B08012_001E)*100,
                                   travel_time_30_to_34min_pct = (B08012_008E/B08012_001E)*100,
                                   travel_time_35_to_39min_pct = (B08012_009E/B08012_001E)*100,
                                   travel_time_40_to_44min_pct = (B08012_010E/B08012_001E)*100,
                                   travel_time_45_to_59min_pct = (B08012_011E/B08012_001E)*100,
                                   travel_time_60_to_89min_pct = (B08012_012E/B08012_001E)*100,
                                   travel_time_90_or_more_min_pct = (B08012_013E/B08012_001E)*100)

#Renaming census variables from codes to common variable names
TN_bg_Dem1<-TN_bg_Dem1%>% rename(
  receive_public_assitance_income	=	B19057_002E	,
  total_surveyed_public_income_assistance	=	B19057_001E	,
  receive_foodstamps_last12months	=	B22001_002E 	,
  total_surveyed_receive_foodstamps_last12months	=	B22001_001E	,
  percent_gross_rent_of_hhincome_30.0_to_34.9	=	B25070_007E	,
  percent_gross_rent_of_hhincome_35.0_to_39.9	=	B25070_008E	,
  percent_gross_rent_of_hhincome_40.0_to_49.9	=	B25070_009E	,
  percent_gross_rent_of_hhincome_50_or_more	=	B25070_010E	,
  total_percent_gross_rent_of_hhincome	=	B25070_001E	,
  not_hispanic_or_latino_white	=	B03002_003E	,
  total_surveyed_race	=	B03002_001E	,
  not_hispanic_or_latino_black	=	B03002_004E	,
  hispanic_or_latino	=	B03002_012E	,
  total_population	=	B01003_001E	,
  median_gross_rent	=	B25064_001E	,
  median_home_value	=	B25077_001E	,
  total_surveyed_occupied_housing	=	B25008_001E	,
  owner_occupied	=	B25008_002E	,
  renter_occupied	=	B25008_003E	,
  total_housing_units	=	B25002_001E	,
  occupied_housing_units	=	B25002_002E	,
  vacant_housing_units	=	B25002_003E	,
  total_surveyed_vehicles_in_hh	=	B08201_001E	,
  no_vehicles_in_hh	=	B08201_002E	,
  total_surveyed_vehicles_for_work	=	B08141_001E	,
  total_surveyed_public_transportation_for_work	=	B08141_016E	,
  no_public_transport_for_work	=	B08141_017E	,
  median_income_last12months	=	B19013_001E	,
  income_below_poverty_level_last12months_by_age	=	B17020_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_age	=	B17020_001E	,
  income_below_poverty_level_last12months_by_sex_and_age	=	B17001_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_001E	,
  income_below_poverty_level_last12months_by_hh_type_children	=	B17012_002E	,
  total_surveyed_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_001E	,
  total_travel_time_to_work	=	B08012_001E	,
  travel_time_to_work_less_than_5min	=	B08012_002E	,
  travel_time_to_work_5_to_9min	=	B08012_003E	,
  travel_time_to_work_10_to_14min	=	B08012_004E	,
  travel_time_to_work_15_to_19min	=	B08012_005E	,
  travel_time_to_work_20_to_24min	=	B08012_006E	,
  travel_time_to_work_25_to_29min	=	B08012_007E	,
  travel_time_to_work_30_to_34min	=	B08012_008E	,
  travel_time_to_work_35_to_39min	=	B08012_009E	,
  travel_time_to_work_40_to_44min	=	B08012_010E	,
  travel_time_to_work_45_to_59min	=	B08012_011E	,
  travel_time_to_work_60_to_89min	=	B08012_012E	,
  travel_time_to_work_90_or_more_min	=	B08012_013E,
  margin_error_receive_public_assitance_income	=	B19057_002M	,
  margin_error_total_surveyed_public_income_assistance	=	B19057_001M	,
  margin_error_receive_foodstamps_last12months	=	B22001_002M,
  margin_error_total_surveyed_receive_foodstamps_last12months	=	B22001_001M	,
  margin_error_percent_gross_rent_of_hhincome_30.0_to_34.9	=	B25070_007M	,
  margin_error_percent_gross_rent_of_hhincome_35.0_to_39.9	=	B25070_008M	,
  margin_error_percent_gross_rent_of_hhincome_40.0_to_49.9	=	B25070_009M	,
  margin_error_percent_gross_rent_of_hhincome_50_or_more	=	B25070_010M	,
  margin_error_total_percent_gross_rent_of_hhincome	=	B25070_001M	,
  margin_error_not_hispanic_or_latino_white	=	B03002_003M	,
  margin_error_total_surveyed_race	=	B03002_001M	,
  margin_error_not_hispanic_or_latino_black	=	B03002_004M	,
  margin_error_hispanic_or_latino	=	B03002_012M	,
  margin_error_total_population	=	B01003_001M	,
  margin_error_median_gross_rent	=	B25064_001M	,
  margin_error_median_home_value	=	B25077_001M	,
  margin_error_total_surveyed_occupied_housing	=	B25008_001M	,
  margin_error_owner_occupied	=	B25008_002M	,
  margin_error_renter_occupied	=	B25008_003M	,
  margin_error_total_housing_units	=	B25002_001M	,
  margin_error_occupied_housing_units	=	B25002_002M	,
  margin_error_vacant_housing_units	=	B25002_003M	,
  margin_error_total_surveyed_vehicles_in_hh	=	B08201_001M	,
  margin_error_no_vehicles_in_hh	=	B08201_002M	,
  margin_error_total_surveyed_vehicles_for_work	=	B08141_001M	,
  margin_error_total_surveyed_public_transportation_for_work	=	B08141_016M	,
  margin_error_no_public_transport_for_work	=	B08141_017M	,
  margin_error_median_income_last12months	=	B19013_001M	,
  margin_error_income_below_poverty_level_last12months_by_age	=	B17020_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_age	=	B17020_001M	,
  margin_error_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_sex_and_age	=	B17001_001M	,
  margin_error_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_002M	,
  margin_error_total_surveyed_income_below_poverty_level_last12months_by_hh_type_children	=	B17012_001M	,
  margin_error_total_travel_time_to_work	=	B08012_001M	,
  margin_error_travel_time_to_work_less_than_5min	=	B08012_002M	,
  margin_error_travel_time_to_work_5_to_9min	=	B08012_003M	,
  margin_error_travel_time_to_work_10_to_14min	=	B08012_004M	,
  margin_error_travel_time_to_work_15_to_19min	=	B08012_005M	,
  margin_error_travel_time_to_work_20_to_24min	=	B08012_006M	,
  margin_error_travel_time_to_work_25_to_29min	=	B08012_007M	,
  margin_error_travel_time_to_work_30_to_34min	=	B08012_008M	,
  margin_error_travel_time_to_work_35_to_39min	=	B08012_009M	,
  margin_error_travel_time_to_work_40_to_44min	=	B08012_010M	,
  margin_error_travel_time_to_work_45_to_59min	=	B08012_011M	,
  margin_error_travel_time_to_work_60_to_89min	=	B08012_012M	,
  margin_error_travel_time_to_work_90_or_more_min	=	B08012_013M	)

#Dropping empty columns
emptycols <- sapply(TN_bg_Dem1, function (k) all(is.na(k)))
TN_bg_Dem2 <- TN_bg_Dem1[!emptycols]

Loc5=TN_bg_Dem1 %>%select(GEOID, NAME, geometry)

df= str_split_fixed(TN_bg_Dem1$NAME, ", ", 4)
df5= data.frame(df)
colnames(df5)<-c("Block Group","Census Tract", "County", "State")
Loc5=bind_cols(Loc5, df5)

#exporting dataframe into geoJSON
my_TN_census_sf <- st_as_sf(TN_bg_Dem2)
st_write(my_TN_census_sf, '../data/socio_economic/2021_block_group_TN.geojson') 
Loc5_sf<-st_as_sf(Loc5)
st_write(Loc5_sf, '../data/socio_economic/2021_block_group_TN_location_table.geojson') 

#############################################
# Block level data of  Tennessee
#############################################



# Block level data of Tennessee
TN_Block <- get_decennial(geography = "block", 
                          state = "TN", 
                          year = 2010,
                          variables = 	my_variables2,
                          output = "wide",
                          geometry = TRUE, 
                          cache_table = TRUE) 
View(TN_Block)
TN_Block1<-TN_Block

#Adding another columns of year and geographic level
TN_Block1["Year"]<-2010
TN_Block1["Geography"]<-"Block"

#Adding additional variables of percentages of existing variables
TN_Block1<-TN_Block1%>%  mutate( 
  White_pct = (P005003/P005001)*100, 
  Black_pct = (P005004/P005001)*100,  
  Hispanic_pct = (P005010/P005001)*100, 
  Owner_Occupied_pct = ((H011002+H011003)/H011001)*100,
  Renter_Occupied_pct = (H011004/H011001)*100,
  Vacant_Homes_pct = (H003003/H003001)*100)

#Renaming census variables from codes to common variable names
TN_Block1<-TN_Block1%>% rename(
  non_hispanic_or_latino_white	=	P005003	,
  total_surveyed_race	=	P005001	,
  non_hispanic_or_latino_black	=	P005004	,
  hispanic_or_latino	=	P005010 ,
  total_population	=	P001001	,
  total_surveyed_occupied_housing	=	H011001,
  owner_occupied_mortgage	=	H011002	,
  owner_occupied_clear= H011003,
  renter_occupied	=	H011004	,
  total_surveyed_housing_occupancy_units	=	H003001	,
  occupied_housing_units	=	H003002	,
  vacant_housing_units	=	H003003	
)

Loc6=TN_Block1 %>%select(GEOID, NAME, geometry)

df= str_split_fixed(TN_Block1$NAME, ", ", 5)
df6= data.frame(df)
colnames(df6)<-c("Block","Block Group","Census Tract", "County", "State")
Loc6=bind_cols(Loc6, df6)

#exporting dataframe into geoJSON
my_TN_census_sf <- st_as_sf(TN_Block1)
st_write(my_TN_census_sf, '../data/socio_economic/2010_census_block_TN.geojson') 
Loc6_sf<-st_as_sf(Loc6)
st_write(Loc6_sf, '../data/socio_economic/2010_block_TN_location_table.geojson') 