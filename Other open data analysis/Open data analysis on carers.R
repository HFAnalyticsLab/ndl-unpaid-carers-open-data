##########################################################
################### DEVELOPMENT IDEAS ####################
##########################################################

##############################################
################### SETUP ####################
##############################################

#Load packages

library(tidyverse)
library(readxl)
library(data.table)
library(readODS)
library(writexl)
library(janitor)
library(aws.s3)
library(onsr)

#Clean up the global environment

rm(list = ls())

#Directories in S3

IHT_bucket <- "s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp"
ASC_subfolder <- "ASC and Finance Report"
R_workbench <- path.expand("~")
localgit <- dirname(rstudioapi::getSourceEditorContext()$path)

#Themes

theme_vert <- theme(strip.text = element_text(size=10),
                    text = element_text(size = 10),
                    legend.title=element_text(size=10),
                    legend.text=element_text(size=10),
                    axis.text = element_text(size = 10),
                    axis.text.y = element_text(size = 10),
                    axis.text.x = element_text(angle = 90, hjust = 1,size = 5),
                    axis.title.x = element_text(margin = unit(c(3, 0, 0, 0), "mm"),size = 10),
                    axis.title.y = element_text(size = 10))

#######################################################
################### 2011 Census data ##################
#######################################################

#Raw

carers_census_2011_age_sex <- s3read_using(read_excel,
                                           object = paste0(ASC_subfolder,"/2011 Census/sc012011reftablesenglandandwales1.xlsx"),
                                           bucket = IHT_bucket,
                                           sheet="Table 1",skip=3) %>%
  select(1:8)

#######################################################
################### 2021 Census data ##################
#######################################################

#Northern Ireland

ni_carers_census_2021 <-  s3read_using(read_excel,
                                    object = paste0(ASC_subfolder,"/2011 Census/northern-ireland-census-2021-ms-d17.xlsx"),
                                    bucket = IHT_bucket,
                                    sheet="MS-D17",skip=23) %>%
  select(1:4) %>%
  mutate(pct_carer_21=(1-`All usual residents aged 5 and over:\r\nProvides no unpaid care`)*100) %>%
  select(Geography,`Geography code`,pct_carer_21) %>%
  rename(Code=`Geography code`)

#Raw

carers_census_2021 <-  s3read_using(fread,
                                    object = paste0(ASC_subfolder,"/2011 Census/TS039-2021-1.csv"),
                                    bucket = IHT_bucket, header=TRUE)

carers_census_2021 <- carers_census_2021 %>%
  select(-"Unpaid care (7 categories) Code") %>%
  pivot_wider(names_from = `Unpaid care (7 categories)`,
              names_sep = ".",
              values_from = c(Observation)) %>%
  select(-c("No code required","Provides no unpaid care")) %>%
  mutate(carers21=rowSums(across(starts_with("Provides")))) %>%
  select(-starts_with("Provides"))

carer21_england <- carers_census_2021 %>% filter(str_detect(`Lower Tier Local Authorities Code`, "^E")) %>% pull(carers21) %>% sum(.,na.rm=TRUE)
carer21_englandwales <- carers_census_2021 %>% pull(carers21) %>% sum(.,na.rm=TRUE)

#Standardized

carers_census_2021_adj <-  s3read_using(fread,
                                        object = paste0(ASC_subfolder,"/2011 Census/TS039ASP-2021-1.csv"),
                                        bucket = IHT_bucket, header=TRUE)

carers_census_2021_adj <- carers_census_2021_adj %>%
  select(-"Unpaid care (5 categories) Code") %>%
  pivot_wider(names_from = `Unpaid care (5 categories)`,
              names_sep = ".",
              values_from = c(Observation)) %>%
  select(-c("Does not apply","Provides no unpaid care")) %>%
  mutate(pct_adj_carers21=rowSums(across(starts_with("Provides")))) %>%
  select(-starts_with("Provides"))

#By age and sex

carers_census_2021_age_sex <- s3read_using(read_excel,
                                           object = paste0(ASC_subfolder,"/2011 Census/sc012021reftablesenglandandwales2.xlsx"),
                                           bucket = IHT_bucket,
                                           sheet="Table 1",skip=3) %>%
  select(1:8)

###############################################
################### DWP data ##################
###############################################

#Part 1 of time series (older data)

dwp_part1_total <- s3read_using(read_excel,
                                        object = paste0(ASC_subfolder,"/DWP/dwp-benefits-part1.xlsx"), # File to open
                                        bucket = IHT_bucket,
                                        sheet=4,skip=10) %>%
  select(-1) %>%
  filter(!is.na(`Feb-18`)) %>%
  rename(Area="...2") %>%
  pivot_longer(!c("Area"),names_to = "month", values_to = "claimants") %>%
  mutate(date=paste0("01-",month) %>% lubridate::dmy(.),
         type="total")

dwp_part1_entitled <- s3read_using(read_excel,
                                   object = paste0(ASC_subfolder,"/DWP/dwp-benefits-part1.xlsx"), # File to open
                                   bucket = IHT_bucket,
                                   sheet=1,skip=10) %>%
  select(-1) %>%
  filter(!is.na(`Feb-18`)) %>%
  rename(Area="...2") %>%
  mutate(across(everything(), as.character)) %>% 
  pivot_longer(!c("Area"),names_to = "month", values_to = "claimants") %>%
  mutate(date=paste0("01-",month) %>% lubridate::dmy(.),
         type="entitled only")

dwp_part1_claiming <- s3read_using(read_excel,
                                   object = paste0(ASC_subfolder,"/DWP/dwp-benefits-part1.xlsx"), # File to open
                                   bucket = IHT_bucket,
                                   sheet=3,skip=10) %>%
  select(-1) %>%
  filter(!is.na(`Feb-18`)) %>%
  rename(Area="...2") %>%
  mutate(across(everything(), as.character)) %>% 
  pivot_longer(!c("Area"),names_to = "month", values_to = "claimants") %>%
  mutate(date=paste0("01-",month) %>% lubridate::dmy(.),
         type="claiming")

dwp_part1 <- plyr::rbind.fill(dwp_part1_total,dwp_part1_entitled,dwp_part1_claiming)
rm(dwp_part1_total,dwp_part1_entitled,dwp_part1_claiming)

#Part 2 of time series (newer data)

dwp_part2_total <- s3read_using(read_excel,
                          object = paste0(ASC_subfolder,"/DWP/dwp-benefits-part2.xlsx"), # File to open
                          bucket = IHT_bucket,
                          sheet=4,skip=10) %>%
  select(-1) %>%
  filter(!is.na(`Aug-22`)) %>%
  rename(Area="...2") %>%
  pivot_longer(!c("Area"),names_to = "month", values_to = "claimants") %>%
  mutate(date=paste0("01-",month) %>% lubridate::dmy(.),
         type="total")

dwp_part2_entitled <- s3read_using(read_excel,
                                object = paste0(ASC_subfolder,"/DWP/dwp-benefits-part2.xlsx"), # File to open
                                bucket = IHT_bucket,
                                sheet=1,skip=10) %>%
  select(-1) %>%
  filter(!is.na(`Aug-22`)) %>%
  rename(Area="...2") %>%
  pivot_longer(!c("Area"),names_to = "month", values_to = "claimants") %>%
  mutate(date=paste0("01-",month) %>% lubridate::dmy(.),
         type="entitled only")

dwp_part2_claiming <- s3read_using(read_excel,
                                   object = paste0(ASC_subfolder,"/DWP/dwp-benefits-part2.xlsx"), # File to open
                                   bucket = IHT_bucket,
                                   sheet=2,skip=10) %>%
  select(-1) %>%
  filter(!is.na(`Aug-22`)) %>%
  rename(Area="...2") %>%
  pivot_longer(!c("Area"),names_to = "month", values_to = "claimants") %>%
  mutate(date=paste0("01-",month) %>% lubridate::dmy(.),
         type="claiming")

dwp_part2 <- plyr::rbind.fill(dwp_part2_total,dwp_part2_entitled,dwp_part2_claiming)
rm(dwp_part2_total,dwp_part2_entitled,dwp_part2_claiming)

#Joining

dwp <- plyr::rbind.fill(dwp_part1,dwp_part2) %>%
  arrange(Area,date)
rm(dwp_part1,dwp_part2)

##############################################
################### LA data ##################
##############################################

LA_data <- s3read_using(read_excel,
                          object = paste0(ASC_subfolder,"/LA requests for support(1).xlsx"), # File to open
                          bucket = IHT_bucket,
                          sheet=1) %>%
  select(-`Year bis`) %>%
  mutate(date=paste0("01-04-",`Year`) %>% lubridate::dmy())

######################################################
################### Population data ##################
######################################################

# Usual resident population

pop21cens <- s3read_using(read_excel,
                          object = paste0(ASC_subfolder,"/2011 Census/census2021firstresultsenglandwales1(1).xlsx"), # File to open
                          sheet="P02",
                          bucket = IHT_bucket,
                          skip=7) %>%
  select(1:4) %>%
  rename(Code=`Area code [note 2]`,
         Geography=`Area name`,
         pop21_cens_all=`All persons`) %>%
  mutate(pop21_cens_5plus=pop21_cens_all-`Aged 4 years and under\r\n[note 12]`) %>%
  select(Code,Geography,pop21_cens_all,pop21_cens_5plus)

# ONS mid-year population estimates

pop_part1 <- s3read_using(fread,
                          object = paste0(ASC_subfolder,"/2011 Census/MYEB1_detailed_population_estimates_series_UK_(2020_geog20).csv"), # File to open
                          bucket = IHT_bucket) %>% 
  select(ladcode20,country,sex,age,starts_with("population")) %>%
  pivot_longer(!c("ladcode20","country","sex","age"), names_to = "year", values_to = "pop") %>%
  mutate(year=str_replace_all(year,"population_","")) %>%
  mutate(pop0plus=ifelse(age>=0,pop,NA),
         pop5plus=ifelse(age>=5,pop,NA),
         pop16plus=ifelse(age>=16,pop,NA),
         pop18plus=ifelse(age>=18,pop,NA)) %>%
  group_by(country,year) %>%
  summarise(pop0plus=sum(pop0plus,na.rm=TRUE),
            pop5plus=sum(pop5plus,na.rm=TRUE),
            pop16plus=sum(pop16plus,na.rm=TRUE),
            pop18plus=sum(pop18plus,na.rm=TRUE)) %>% 
  ungroup() %>%
  janitor::clean_names()

pop_part1_ew <- pop_part1 %>%
  filter(country %in% c("E","W")) %>%
  group_by(year) %>%
  summarise(pop0plus=sum(pop0plus,na.rm=TRUE),
            pop5plus=sum(pop5plus,na.rm=TRUE),
            pop16plus=sum(pop16plus,na.rm=TRUE),
            pop18plus=sum(pop18plus,na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(country="England and Wales")

pop_part1_uk <- pop_part1 %>%
  group_by(year) %>%
  summarise(pop0plus=sum(pop0plus,na.rm=TRUE),
            pop5plus=sum(pop5plus,na.rm=TRUE),
            pop16plus=sum(pop16plus,na.rm=TRUE),
            pop18plus=sum(pop18plus,na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(country="United Kingdom")

pop_part1 <- pop_part1 %>%
  filter(country=="E") %>%
  mutate(country="England") %>%
  plyr::rbind.fill(.,pop_part1_ew,pop_part1_uk) %>%
  rename(geography=country) %>%
  janitor::clean_names()
rm(pop_part1_ew,pop_part1_uk)
  
pop_part2 <- ons_get(id = "mid-year-pop-est") %>%
  filter(Sex=="All"&(tolower(Geography) %in% c("england","england and wales","united kingdom"))) %>%
  select(v4_0,Time,`administrative-geography`,Geography,Sex,Age) %>%
  rename(pop=v4_0,Code=`administrative-geography`,
         year=Time) %>%
  mutate(pop0plus=ifelse(Age %in% c(0:89,"90+"),pop,NA),
         pop5plus=ifelse(Age %in% c(5:89,"90+"),pop,NA),
         pop16plus=ifelse(Age %in% c(16:89,"90+"),pop,NA),
         pop18plus=ifelse(Age %in% c(18:89,"90+"),pop,NA),
         Geography=str_to_title(Geography)) %>%
  group_by(year,Geography) %>%
  summarise(pop0plus=sum(pop0plus,na.rm=TRUE),
            pop5plus=sum(pop5plus,na.rm=TRUE),
            pop16plus=sum(pop16plus,na.rm=TRUE),
            pop18plus=sum(pop18plus,na.rm=TRUE)) %>% 
  ungroup() %>%
  janitor::clean_names()

pop_ons <- plyr::rbind.fill(pop_part1,pop_part2) %>%
  arrange(geography,year) %>%
  pivot_longer(!c("geography","year"), names_to = "age", values_to = "pop") %>%
  mutate(age=str_replace_all(age,"pop","") %>% str_replace_all(.,"plus","+"),
         geography=ifelse(geography=="United Kingdom","UK",geography) %>% str_to_title(.))
rm(pop_part1,pop_part2)

#####################################################
################### Census hex-map ##################
#####################################################

### Percentage carer in 2021

pct_carers_2021 <- carers_census_2021 %>%
  right_join(.,select(pop21cens,Code,Geography,pop21_cens_5plus),by=c("Lower Tier Local Authorities Code"="Code")) %>% 
  rename(Code=`Lower Tier Local Authorities Code`) %>% 
  mutate(pct_carer_21=carers21/pop21_cens_5plus*100,
         Geography=str_to_title(Geography)) %>%
  select(Code,Geography,pct_carer_21,) %>%
  plyr::rbind.fill(.,ni_carers_census_2021)

#Flourish

utla_hex_template <- s3read_using(read_excel,
                                  object = paste0(ASC_subfolder,"/hexmap-lad-template.xlsx"), # File to open
                                  bucket = IHT_bucket,
                                  sheet=1)

flourish_pct_carers <- utla_hex_template %>%
  left_join(.,pct_carers_2021,by=c("lacode"="Code"))

fwrite(flourish_pct_carers, paste0(R_workbench,"/Charts/Carers/","flourish_pct_carers.csv"))

##########################################################################
################### LA: how many carers getting support ##################
##########################################################################

LA_data %>%
  select(-"date") %>% 
  mutate(age="0+",Year=as.character(Year)) %>%
  left_join(.,pop_ons,by=c("Geography"="geography","Year"="year","age"="age")) %>%
  mutate(carers_census=ifelse(Year=="2021",carer21_england,NA)) %>%
  mutate(pct_cares_req_support=`Number requests`/carers_census*100)

##########################################################################
################### DWP: how many carers getting support ##################
##########################################################################

dwp %>%
  filter(month=="Feb-21"&Area=="England") %>% 
  select(-"date") %>% 
  mutate(age="18+",Year="2021") %>% 
  left_join(.,pop_ons,by=c("Area"="geography","Year"="year","age"="age")) %>%
  mutate(carers_census=ifelse(Year=="2021",carer21_england,NA),
         claimants=as.numeric(claimants)) %>%
  mutate(pct_carers_allowance=claimants/carers_census*100)

##############################################################
################### Multi-survey comparison ##################
##############################################################

#Read in summary data

multi_survey_comparison <- s3read_using(read_excel,
                              object = paste0(ASC_subfolder,"/2011 Census/Prevalence summary(1).xlsx"), # File to open
                              bucket = IHT_bucket,
                              sheet=1) %>%
  mutate(`Year standard`=as.character(`Year standard`),
         Region=str_to_title(Region)) %>% 
  left_join(.,pop_ons,by=c("Region"="geography","Year standard"="year","Coverage"="age")) %>%
  mutate(count=Percentage*pop)

#Plot: percentages

ts_pct_plot <- multi_survey_comparison %>%
  filter(`Complete source`!="Census (England and Wales, 5+)") %>%
  mutate(date_year=lubridate::ymd(paste(`Year standard`,"-01-01"))) %>% 
  ggplot(., aes(col=`Complete source`, y=Percentage, x=date_year,group=`Complete source`)) +
  geom_point() +
  geom_line(linewidth = 1, alpha=0.8) +
  ggtitle("Percentage of unpaid carers [non-standardised]") +
  scale_y_continuous(labels = scales::percent, name="Percent",limits=c(0,0.25)) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  xlab("Year of survey") +
  labs(col = "Survey (coverage, age range)") +
  theme_bw() +
  scale_fill_brewer(palette="Set2") +
  theme_vert
ts_pct_plot

ts_pct_flourish <- multi_survey_comparison %>%
  filter(!(`Complete source` %in% c("Census (England and Wales, 5+)","Family Resources Survey (UK, 0+)"))) %>%
  select(`Year standard`,Percentage,`Complete source`) %>%
  mutate(Percentage=100*Percentage) %>% 
  pivot_wider(
    names_from = `Complete source`,
    names_sep = ".",
    values_from = c(Percentage)
  ) %>%
  mutate(`Year standard`=as.numeric(`Year standard`)) %>%
  arrange(`Year standard`)

fwrite(ts_pct_flourish, paste0(R_workbench,"/Charts/Carers/","ts_pct_flourish.csv"))

#Plot: counts

ts_counts_plot <- multi_survey_comparison %>%
  filter(`Complete source`!="Census (England and Wales, 5+)") %>%
  mutate(date_year=lubridate::ymd(paste(`Year standard`,"-01-01"))) %>% 
  ggplot(., aes(col=`Complete source`, y=count, x=date_year,group=`Complete source`)) +
  geom_point() +
  geom_line(linewidth = 1, alpha=0.8) +
  ggtitle("Estimated number of unpaid carers") +
  scale_y_continuous(labels = scales::comma, name="Carers", limits=c(0,15000000)) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  xlab("Year of survey") +
  labs(col = "Survey (coverage, age range)") +
  theme_bw() +
  scale_fill_brewer(palette="Set2") +
  theme_vert
ts_counts_plot

#Plot: admin data

admin_part_1 <- dwp %>%
  filter(Area=="England"&date>=lubridate::ymd("2011-01-01")) %>%
  select(type,date,Area,claimants) %>%
  pivot_wider(names_from = type,
              names_sep = ".",
              values_from = c(claimants)) %>%
  janitor::clean_names()

fwrite(admin_part_1, paste0(R_workbench,"/Charts/Carers/","admin_part_1.csv"))

admin_part_2 <- LA_data %>%
  select(FY,`Number requests`,`Direct support`) %>%
  mutate(no_direct_support=`Number requests`-`Direct support`) %>% 
  janitor::clean_names()

fwrite(admin_part_2, paste0(R_workbench,"/Charts/Carers/","admin_part_2.csv"))

# admin_data_collated <- plyr::rbind.fill(admin_part_1,admin_part_2)
# rm(admin_part_1,admin_part_2)
# 
# ts_admin_plot <- admin_data_collated %>%
#   ggplot(., aes(col=source, y=carers, x=date,group=source)) +
#   geom_point() +
#   geom_line(linewidth = 1, alpha=0.8) +
#   ggtitle("Carers found in electronic records") +
#   scale_y_continuous(labels = scales::comma, name="Carers") +
#   scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
#   xlab("Year of survey") +
#   labs(col = "Source") +
#   theme_bw() +
#   scale_fill_brewer(palette="Set2") +
#   theme_vert
# ts_admin_plot