##########################################################
################### DEVELOPMENT IDEAS ####################
##########################################################

##############################################
################### SETUP ####################
##############################################

#Load packages

library(readxl)
library(writexl)
library(tidyverse)
library(data.table)
library(readODS)
library(writexl)
library(janitor)
library(aws.s3)

#Clean up the global environment

rm(list = ls())

#Directories in S3

IHT_bucket <- "s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp"
ASC_subfolder <- "ASC and Finance Report"
R_workbench <- path.expand("~")
localgit <- dirname(rstudioapi::getSourceEditorContext()$path)

#######################################################
################### 2021 Census data ##################
#######################################################

#England

carers_census_2021_age_sex_eng <- s3read_using(read_excel,
                                               object = paste0(ASC_subfolder,"/2021 Census/sc012021reftablesengland1.xlsx"),
                                               bucket = IHT_bucket,
                                               sheet="Table 5",skip=3)

carers_census_2021_age_sex_hours_eng <- s3read_using(read_excel,
                                                     object = paste0(ASC_subfolder,"/2021 Census/sc012021reftablesengland1.xlsx"),
                                                     bucket = IHT_bucket,
                                                     sheet="Table 17",skip=3)

#Wales

carers_census_2021_age_sex_wales <- s3read_using(read_excel,
                                                 object = paste0(ASC_subfolder,"/2021 Census/sc012021reftableswales1.xlsx"),
                                                 bucket = IHT_bucket,
                                                 sheet="Table 3",skip=3)

carers_census_2021_age_sex_hours_wales <- s3read_using(read_excel,
                                                       object = paste0(ASC_subfolder,"/2021 Census/sc012021reftableswales1.xlsx"),
                                                       bucket = IHT_bucket,
                                                       sheet="Table 11",skip=3)

#Combined

carers_census_2021_age_sex_engwales <- plyr::rbind.fill(carers_census_2021_age_sex_eng,
                                                        carers_census_2021_age_sex_wales)

carers_census_2021_age_sex_hours_engwales <- plyr::rbind.fill(carers_census_2021_age_sex_hours_eng,
                                                              carers_census_2021_age_sex_hours_wales)

rm(carers_census_2021_age_sex_eng,carers_census_2021_age_sex_wales,carers_census_2021_age_sex_hours_eng,carers_census_2021_age_sex_hours_wales)

########################################################
################### Clean Census data ##################
########################################################

#Import Census data
census_filtered <- carers_census_2021_age_sex_engwales %>%
  janitor::clean_names() %>%
  select(local_authority,unpaid_carer_status,age,sex,count) %>% 
  filter(str_detect(tolower(local_authority),"port talbot")|str_detect(tolower(local_authority),"swansea")|
           local_authority=="Liverpool"|local_authority=="Wirral"|local_authority=="Leeds"|
           local_authority=="Harrow"|local_authority=="Brent"|local_authority=="Hillingdon"|local_authority=="Ealing"|
           local_authority=="Hounslow"|local_authority=="Hammersmith and Fulham"|local_authority=="Kensington and Chelsea"|
           local_authority=="City of London and Westminster") %>%
  filter(unpaid_carer_status=="Unpaid carer") %>%
  mutate(source="2021 Census",
         period_start="21/3/2021",
         period_end="21/3/2021") %>%
  mutate(local_authority_comb=ifelse(local_authority %in% c("Harrow","Brent","Hillingdon","Ealing","Hounslow","Hammersmith and Fulham","Kensington and Chelsea","City of London and Westminster"),"North West London",local_authority)) %>%
  mutate(local_authority_comb=ifelse(local_authority_comb %in% c("Liverpool","Wirral"),"Liverpool and Wirral",local_authority_comb)) %>%
  group_by(local_authority_comb,source,period_start,period_end,unpaid_carer_status,age,sex) %>%
  summarise(count=sum(as.numeric(count),na.rm=TRUE)) %>% 
  ungroup() %>%
  rename(local_authority=local_authority_comb) %>% 
  select(-"unpaid_carer_status")

#All carers
census_filtered_all <- census_filtered %>%
  filter(sex=="Persons") %>%
  group_by(source,period_start,period_end,local_authority) %>%
  summarise(count=sum(as.numeric(count),na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(type="all carers",
         type_level="all carers")

#By sex
census_filtered_sex <- census_filtered %>%
  filter(sex!="Persons") %>%
  group_by(source,period_start,period_end,local_authority,sex) %>%
  summarise(count=sum(as.numeric(count),na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(type="sex") %>%
  rename(type_level=sex)

#By age
census_filtered_age <- census_filtered %>%
  filter(sex=="Persons") %>%
  mutate(type="age") %>%
  rename(type_level=age) %>%
  select(-c("sex"))

census_filtered_age_wales <- census_filtered_age %>%
  filter(local_authority %in% c("Neath Port Talbot","Swansea")) %>%
  rename(type_level_raw=type_level) %>%
  mutate(type_level=case_when(type_level_raw %in% c("18 to 24","25 to 29","30 to 34","35 to 39") ~ "under 40",
                              type_level_raw %in% c("40 to 44","45 to 49") ~ "40-49",
                              type_level_raw %in% c("50 to 54","55 to 59") ~ "50-59",
                              type_level_raw %in% c("60 to 64","65 to 69") ~ "60-69",
                              type_level_raw %in% c("70 to 74","75 to 79") ~ "70-79",
                              type_level_raw %in% c("80 to 84","85 to 89","90+") ~ "80+",
                              TRUE ~ "NA")) %>%
  filter(!is.na(type_level)&type_level!="NA") %>%
  mutate(count=as.numeric(count)) %>% 
  group_by(local_authority,source,period_start,period_end,type,type_level) %>%
  summarise(count=sum(count,na.rm = TRUE)) %>%
  ungroup()

census_filtered_age_nwlondon <- census_filtered_age %>%
  filter(local_authority %in% c("North West London")) %>%
  rename(type_level_raw=type_level) %>%
  mutate(type_level=case_when(type_level_raw %in% c("18 to 24","25 to 29") ~ "18-29",
                              type_level_raw %in% c("30 to 34","35 to 39") ~ "30-39",
                              type_level_raw %in% c("40 to 44","45 to 49") ~ "40-49",
                              type_level_raw %in% c("50 to 54","55 to 59") ~ "50-59",
                              type_level_raw %in% c("60 to 64","65 to 69") ~ "60-69",
                              type_level_raw %in% c("70 to 74","75 to 79") ~ "70-79",
                              type_level_raw %in% c("80 to 84","85 to 89","90+") ~ "80+",
                              TRUE ~ "NA")) %>%
  filter(!is.na(type_level)&type_level!="NA") %>%
  mutate(count=as.numeric(count)) %>% 
  group_by(local_authority,source,period_start,period_end,type,type_level) %>%
  summarise(count=sum(count,na.rm = TRUE)) %>%
  ungroup()

census_filtered_age_liverpoolwirral <- census_filtered_age %>%
  filter(local_authority %in% c("Liverpool and Wirral")) %>%
  rename(type_level_raw=type_level) %>%
  mutate(type_level=case_when(type_level_raw %in% c("18 to 24","25 to 29") ~ "18-29",
                              type_level_raw %in% c("30 to 34","35 to 39") ~ "30-39",
                              type_level_raw %in% c("40 to 44","45 to 49") ~ "40-49",
                              type_level_raw %in% c("50 to 54","55 to 59") ~ "50-59",
                              type_level_raw %in% c("60 to 64","65 to 69") ~ "60-69",
                              type_level_raw %in% c("70 to 74","75 to 79") ~ "70-79",
                              type_level_raw %in% c("80 to 84","85 to 89","90+") ~ "80+",
                              TRUE ~ "NA")) %>%
  filter(!is.na(type_level)&type_level!="NA") %>%
  mutate(count=as.numeric(count)) %>% 
  group_by(local_authority,source,period_start,period_end,type,type_level) %>%
  summarise(count=sum(count,na.rm = TRUE)) %>%
  ungroup()

census_filtered_age_leeds <- census_filtered_age %>%
  filter(local_authority %in% c("Leeds")) %>%
  rename(type_level_raw=type_level) %>%
  mutate(type_level=case_when(type_level_raw %in% c("18 to 24","25 to 29") ~ "18-29",
                              type_level_raw %in% c("30 to 34","35 to 39") ~ "30-39",
                              type_level_raw %in% c("40 to 44","45 to 49") ~ "40-49",
                              type_level_raw %in% c("50 to 54","55 to 59") ~ "50-59",
                              type_level_raw %in% c("60 to 64","65 to 69") ~ "60-69",
                              type_level_raw %in% c("70 to 74","75 to 79") ~ "70-79",
                              type_level_raw %in% c("80 to 84","85 to 89","90+") ~ "80+",
                              TRUE ~ "NA")) %>%
  filter(!is.na(type_level)&type_level!="NA") %>%
  mutate(count=as.numeric(count)) %>% 
  group_by(local_authority,source,period_start,period_end,type,type_level) %>%
  summarise(count=sum(count,na.rm = TRUE)) %>%
  ungroup()

#Append
census_filtered_clean <- plyr::rbind.fill(census_filtered_all,census_filtered_sex,
                                          census_filtered_age_wales,
                                          census_filtered_age_nwlondon,
                                          census_filtered_age_liverpoolwirral,
                                          census_filtered_age_leeds)
rm(census_filtered,census_filtered_all,census_filtered_age,census_filtered_age_wales,census_filtered_age_nwlondon,
   census_filtered_age_liverpoolwirral,census_filtered_age_leeds,census_filtered_sex)
rm(carers_census_2021_age_sex_engwales,carers_census_2021_age_sex_hours_engwales)

############################################
################### Leeds ##################
############################################

#1/1/2016 and 31/12/2021

#All carers

leeds_table_one <- s3read_using(read_excel,
                                     object = "NDL-carers-partner-data/Leeds/a1_comb.xlsx",
                                     bucket = IHT_bucket, sheet="T1_1_overall") %>%
  mutate(period_start="1/1/2016",
         period_end="31/12/2021",
         local_authority="Leeds",
         type="all carers",
         type_level="all carers") %>%
  select(-"overall") %>% 
  rename(source=cohort,
         count=number.carers) %>%
  mutate(source=ifelse(source=="Overlap","GP and LA",source)) %>%
  pivot_wider(names_from = source,
              names_sep = ".",
              values_from = c(count)) %>%
  mutate(`GP only`=`GP`-`GP and LA`,
         `LA only`=`LA`-`GP and LA`,
         `GP or LA`=`LA`+`GP`-`GP and LA`) %>%
  pivot_longer(!(period_start:type_level), names_to = "source", values_to = "count")

#By demographics

#sex

leeds_table_sex <- s3read_using(read_excel,
                                object = "NDL-carers-partner-data/Leeds/a1_comb.xlsx",
                                bucket = IHT_bucket, sheet="T1_3_gender") %>%
  mutate(period_start="1/1/2016",
         period_end="31/12/2021",
         local_authority="Leeds",
         type="sex") %>% 
  rename(type_level=gender,
         source=cohort,
         count=number.carers) %>%
  mutate(source=ifelse(source=="Overlap","GP and LA",source)) %>%
  pivot_wider(names_from = source,
              names_sep = ".",
              values_from = c(count)) %>%
  mutate(`GP only`=`GP`-`GP and LA`,
         `LA only`=`LA`-`GP and LA`) %>%
  pivot_longer(!(type_level:type), names_to = "source", values_to = "count")

#age

leeds_table_age <- s3read_using(read_excel,
                                object = "NDL-carers-partner-data/Leeds/a1_comb.xlsx",
                                bucket = IHT_bucket, sheet="T1_2_age_band") %>%
  mutate(period_start="1/1/2016",
         period_end="31/12/2021",
         local_authority="Leeds",
         type="age") %>% 
  rename(type_level=age_band,
         source=cohort,
         count=number.carers) %>%
  mutate(source=ifelse(source=="Overlap","GP and LA",source)) %>%
  pivot_wider(names_from = source,
              names_sep = ".",
              values_from = c(count)) %>%
  mutate(`GP only`=as.numeric(`GP`)-as.numeric(`GP and LA`),
         `LA only`=as.numeric(`LA`)-as.numeric(`GP and LA`)) %>%
  mutate(across(everything(), as.character)) %>% 
  pivot_longer(!(type_level:type), names_to = "source", values_to = "count")

#imd

leeds_table_imd <- s3read_using(read_excel,
                                object = "NDL-carers-partner-data/Leeds/a1_comb.xlsx",
                                bucket = IHT_bucket, sheet="T1_4_imd_decile") %>%
  mutate(period_start="1/1/2016",
         period_end="31/12/2021",
         local_authority="Leeds",
         type="imd") %>% 
  rename(type_level=imd_decile,
         source=cohort,
         count=number.carers) %>%
  mutate(source=ifelse(source=="Overlap","GP and LA",source)) %>%
  pivot_wider(names_from = source,
              names_sep = ".",
              values_from = c(count)) %>%
  mutate(`GP only`=as.numeric(`GP`)-as.numeric(`GP and LA`),
         `LA only`=as.numeric(`LA`)-as.numeric(`GP and LA`)) %>%
  mutate(across(everything(), as.character)) %>% 
  pivot_longer(!(type_level:type), names_to = "source", values_to = "count")

#Combine

leeds_table_clean <- leeds_table_one %>%
  plyr::rbind.fill(.,leeds_table_sex,leeds_table_age,leeds_table_imd) %>%
  group_by(local_authority) %>%
  tidyr::fill(period_start,period_end) %>% 
  ungroup()

rm(leeds_table_one,leeds_table_sex,leeds_table_age,leeds_table_imd)

###########################################################
################### Liverpool and Wirral ##################
###########################################################

#Jan 2016 – Dec 2021

#All carers

liverpoolwirral_table_one <- s3read_using(fread,
                                          object = "NDL-carers-partner-data/Liverpool and Wirral/table1.csv",
                                          bucket = IHT_bucket)

liverpoolwirral_overall <- liverpoolwirral_table_one %>%
  pivot_longer(!c("V1"), names_to = "GP registered", values_to = "count") %>%
  rename(`ASC flagged`=V1) %>%
  mutate(local_authority="Liverpool and Wirral",
         period_start="01/01/2016",
         period_end="31/12/2021",
         type="all carers",
         type_level="all carers") %>%
  mutate(source=case_when(`ASC flagged`=="ASC flagged"&`GP registered`=="Total"~ "LA",
                          `ASC flagged`=="Total"&`GP registered`=="GP registered"~ "GP",
                          `ASC flagged`=="ASC flagged"&`GP registered`=="GP registered"~ "GP and LA",
                          `ASC flagged`=="Total"&`GP registered`=="Total"~ "GP or LA",
                          `ASC flagged`=="Not ASC flagged"&`GP registered`=="GP registered" ~ "GP only",
                          `ASC flagged`=="ASC flagged"&`GP registered`=="Not GP registered" ~ "LA only",
                          TRUE ~ "NA")) %>%
  select(-c(`ASC flagged`,`GP registered`)) %>%
  filter(!is.na(count)&!is.na(source)&source!="NA")

rm(liverpoolwirral_table_one)

#By demographics

liverpoolwirral_demos <- s3read_using(fread,
                                      object = "NDL-carers-partner-data/Liverpool and Wirral/HF_carers_count_rates_age_groups_IMD_LW.csv",
                                      bucket = IHT_bucket) %>%
  select(age_group,gender,imd_decile,carer_count_GP:carer_count_GP_or_ASC) %>%
  pivot_longer(!c("age_group","gender","imd_decile"), names_to = "source", values_to = "count") %>%
  mutate(source=str_replace_all(source,"carer_count_","") %>%  str_replace_all(.,"ASC","LA") %>% str_replace_all(.,"_"," "))

liverpoolwirral_sex <- liverpoolwirral_demos %>%
  group_by(gender,source) %>%
  summarise(count=sum(count,na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(type="sex",
         local_authority="Liverpool and Wirral") %>%
  rename(type_level=gender)

liverpoolwirral_age <- liverpoolwirral_demos %>%
  group_by(age_group,source) %>%
  summarise(count=sum(count,na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(type="age",
         local_authority="Liverpool and Wirral") %>%
  rename(type_level=age_group)

liverpoolwirral_imd <- liverpoolwirral_demos %>%
  group_by(imd_decile,source) %>%
  summarise(count=sum(count,na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(type="imd",
         local_authority="Liverpool and Wirral") %>%
  rename(type_level=imd_decile)
  
#Combine

liverpool_wirral_table_clean <- liverpoolwirral_overall %>%
  plyr::rbind.fill(.,liverpoolwirral_sex,liverpoolwirral_age,liverpoolwirral_imd) %>%
  group_by(local_authority) %>%
  tidyr::fill(period_start,period_end) %>% 
  ungroup()

rm(liverpoolwirral_overall,liverpoolwirral_demos,liverpoolwirral_sex,liverpoolwirral_age,liverpoolwirral_imd)

################################################
################### NW London ##################
################################################

# NW London	01.01.2016 – 31.12.2021

nwlondon_table_clean <- s3read_using(read_excel,
                                     object = "NDL-carers-partner-data/NW London/NWL Central Analysis Tables.xlsx",
                                     bucket = IHT_bucket, sheet="Analysis 1 Tables") %>%
  select(2:4) %>%
  rename(type=`...2`,
         type_level=n,
         count=`54679`) %>%
  mutate(local_authority="North West London",
         source="GP",
         period_start="01/01/2016",
         period_end="31/12/2021") %>%
  mutate(type=tolower(type),
         type_level=tolower(type_level)) %>%
  mutate(type=ifelse(type=="gender","sex",type)) %>%
  mutate(type=ifelse(type=="age group","age",type)) %>% 
  filter(!is.na(type_level)&!is.na(type)&type!="na") %>%
  filter(type %in% c("sex","age","imd","borough"))

nwlondon_table_overall <- nwlondon_table_clean %>%
  filter(type=="sex") %>%
  group_by(local_authority,source,type,period_start,period_end) %>%
  summarise(count=sum(count,na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(type="all carers",
         type_level="all carers")

nwlondon_table_clean <- plyr::rbind.fill(nwlondon_table_overall,
                                   nwlondon_table_clean)
rm(nwlondon_table_overall)

############################################
################### Wales ##################
############################################

# Swansea	04/2021 - 06/2022	436
# Neath Port Talbot (NPT)	07/2017 - 05/2022	537

#All carers

wales_table_raw <- s3read_using(read_excel,
                                object = "NDL-carers-partner-data/Wales/Table2.xlsx",
                                bucket = IHT_bucket)

wales_table <- wales_table_raw %>%
  janitor::clean_names() %>% 
  select(local_authority,contains("ever"),contains("total")) %>%
  mutate(`Only GP`=ever_identified_via_read_code-ever_identified_via_both_la_and_wlgp_data,
         `Only LA`=ever_identified_via_la_carers_assessment-ever_identified_via_both_la_and_wlgp_data) %>% 
  pivot_longer(!local_authority, names_to = "source", values_to = "count") %>%
  filter(local_authority!="Total") %>%
  mutate(source=case_when(source=="ever_identified_via_la_carers_assessment" ~ "LA",
                          source=="ever_identified_via_read_code" ~ "GP",
                          source=="ever_identified_via_both_la_and_wlgp_data" ~ "GP and LA",
                          source=="total_unpaid_carer_cohort" ~ "GP or LA",
                          source=="Only GP" ~ "GP only",
                          source=="Only LA" ~ "LA only",
                          TRUE ~ "NA"),
         period_start=case_when(local_authority=="Neath Port Talbot (NPT)" ~ "01/07/2017",
                                local_authority=="Swansea" ~ "01/04/2021",
                                TRUE ~ "NA"),
         period_end=case_when(local_authority=="Neath Port Talbot (NPT)" ~ "30/06/2022",
                              local_authority=="Swansea" ~ "31/05/2022",
                              TRUE ~ "NA"),
         type="all carers",
         type_level="all carers")

rm(wales_table_raw)

#NPT

npt_demographics_sex <- s3read_using(read_excel,
                                     object = "NDL-carers-partner-data/Wales/Figure 1+6+9+13+16+17_1429_npt_demographics_countsperc_peje.xlsx",
                                     bucket = IHT_bucket,
                                     sheet="Figure 6") %>%
  select(variable,identifiedby,factor_levels,count) %>%
  mutate(local_authority="Neath Port Talbot (NPT)") %>% 
  rename(type=variable,
         type_level=factor_levels,
         source=identifiedby)

npt_demographics_age <- s3read_using(read_excel,
                                     object = "NDL-carers-partner-data/Wales/Figure 1+6+9+13+16+17_1429_npt_demographics_countsperc_peje.xlsx",
                                     bucket = IHT_bucket,
                                     sheet="Figure 9") %>%
  select(variable,identifiedby,factor_levels,count) %>%
  mutate(local_authority="Neath Port Talbot (NPT)") %>%
  rename(type=variable,
         type_level=factor_levels,
         source=identifiedby)

npt_demographics_wimd <- s3read_using(read_excel,
                                      object = "NDL-carers-partner-data/Wales/Figure 1+6+9+13+16+17_1429_npt_demographics_countsperc_peje.xlsx",
                                      bucket = IHT_bucket,
                                      sheet="Figure 13") %>%
  select(variable,identifiedby,factor_levels,count) %>%
  mutate(local_authority="Neath Port Talbot (NPT)") %>%
  rename(type=variable,
         type_level=factor_levels,
         source=identifiedby)

#Swansea

swansea_demographics_sex <- s3read_using(read_excel,
                                         object = "NDL-carers-partner-data/Wales/Figure 2+20+23+27+30+31_1429_swansea_demographics_countsperc_peje.xlsx",
                                         bucket = IHT_bucket,
                                         sheet="Figure 20") %>%
  select(variable,identifiedby,factor_levels,count) %>%
  mutate(local_authority="Swansea") %>% 
  rename(type=variable,
         type_level=factor_levels,
         source=identifiedby)

swansea_demographics_age <- s3read_using(read_excel,
                                         object = "NDL-carers-partner-data/Wales/Figure 2+20+23+27+30+31_1429_swansea_demographics_countsperc_peje.xlsx",
                                         bucket = IHT_bucket,
                                         sheet="Figure 23") %>%
  select(variable,identifiedby,factor_levels,count) %>%
  mutate(local_authority="Swansea") %>%
  rename(type=variable,
         type_level=factor_levels,
         source=identifiedby)

swansea_demographics_wimd <- s3read_using(read_excel,
                                          object = "NDL-carers-partner-data/Wales/Figure 2+20+23+27+30+31_1429_swansea_demographics_countsperc_peje.xlsx",
                                          bucket = IHT_bucket,
                                          sheet="Figure 27") %>%
  select(variable,identifiedby,factor_levels,count) %>%
  mutate(local_authority="Swansea") %>%
  rename(type=variable,
         type_level=factor_levels,
         source=identifiedby)

#Combine

wales_table_clean <- wales_table %>%
  plyr::rbind.fill(.,npt_demographics_sex,npt_demographics_age,npt_demographics_wimd) %>%
  plyr::rbind.fill(.,swansea_demographics_sex,swansea_demographics_age,swansea_demographics_wimd) %>%
  group_by(local_authority) %>%
  tidyr::fill(period_start,period_end) %>% 
  ungroup() %>%
  mutate(local_authority=ifelse(local_authority=="Neath Port Talbot (NPT)","Neath Port Talbot",local_authority))

rm(wales_table)
rm(npt_demographics_sex,npt_demographics_age,npt_demographics_wimd)
rm(swansea_demographics_sex,swansea_demographics_age,swansea_demographics_wimd)

#############################################################
################### Combine local analyses ##################
#############################################################

ndl_carers_central <- plyr::rbind.fill(census_filtered_clean,
                                       wales_table_clean,
                                       nwlondon_table_clean,
                                       liverpool_wirral_table_clean,
                                       leeds_table_clean) %>%
  mutate(count=as.numeric(gsub(",", "", count)),
         type=tolower(type),
         type_level=tolower(type_level)) %>%
  filter(!is.na(type_level)&type_level!="na")

rm(census_filtered_clean,
   wales_table_clean,
   nwlondon_table_clean,
   liverpool_wirral_table_clean,
   leeds_table_clean)

s3write_using(ndl_carers_central, FUN = fwrite,
                               object = "NDL-carers-partner-data/ndl_carers_central.csv",
                               bucket = "s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp")
