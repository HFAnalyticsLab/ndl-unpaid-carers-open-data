###############################
###### Development ideas ######
###############################

#######################
###### Libraries ######
#######################

library(aws.s3)
library(tidyverse)
library(readxl)
library(data.table)
library(janitor)
library(survey)
library(gtsummary)
library(srvyr)
library(plotly)
library(writexl)

###################
###### Setup ######
###################

#Clean up the global environment
rm(list = ls())

#Directories in S3
IHT_bucket <- "s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp"
USOC_subfolder <- "/Understanding Society/UKDA-6614-tab/tab/ukhls"
ASC_subfolder <- "ASC and Finance Report"
R_workbench <- path.expand("~")
localgit <- dirname(rstudioapi::getSourceEditorContext()$path)

####### Read in data

usoc_long_carers <- s3read_using(fread
                          , object = paste0("/Understanding Society/","USOC long carers analysis.csv") # File to open
                          , bucket = IHT_bucket) # Bucket name defined above

####### Additional cleaning

usoc_long_carers <- usoc_long_carers %>%
  mutate(age70=case_when(agedv>=70 ~ "Over 70",
                         agedv<70 ~ "Under 70",
                         TRUE ~ "NA")) %>% 
  mutate(.,finnow=fct_relevel(finnow, c("Living comfortably","Doing alright","Just about getting by",
                                                  "Finding it quite difficult","Finding it very difficult"))) %>%
  mutate(equ_annual_income_dec=fct_relevel(equ_annual_income_dec,
                                           c("1 (10% poorest)",2:9,"10 (10% richest)"))) %>%
  mutate(.,hours_cared=fct_relevel(hours_cared, c("0 - 4 hours per week","5 - 9 hours per week","10 - 19 hours per week",
                                                  "20 - 34 hours per week","35 - 49 hours per week","50 - 99 hours per week",
                                                  "100 or more hours per week/continuous care","Varies under 20 hours","Varies 20 hours or more")))

####### Plot themes

mytheme <- theme(panel.border = element_blank(),
                 strip.text = element_text(size=10),
                 text = element_text(size = 10),
                 legend.title=element_text(size=10),
                 legend.text=element_text(size=10),
                 axis.text = element_text(size = 10),
                 axis.text.y = element_text(size = 10),
                 axis.text.x = element_text(angle = 45, hjust = 1,size = 10),
                 axis.title.x = element_text(margin = unit(c(3, 0, 0, 0), "mm"),size = 10),
                 axis.title.y = element_text(size = 10))

####### Set up survey design with weights

#Filter data by weight design
usoc_long_carers_2to5 <- usoc_long_carers %>%
  filter(.,!is.na(hidp),wave_num>1,wave_num<=5,!is.na(indscubxw))
usoc_long_carers_6to12 <- usoc_long_carers %>%
  filter(.,!is.na(hidp),wave_num>=6,!is.na(indscuixw))

#Create survey objects
options(survey.lonely.psu="adjust")
usoc_design_2to5 <- svydesign(id= ~psu, strata= ~strata, survey.lonely.psu="adjust",
                               weights= ~indscubxw, data=usoc_long_carers_2to5,nest=TRUE)
usoc_design_6to12 <- svydesign(id= ~psu, strata= ~strata, survey.lonely.psu="adjust",
                      weights= ~indscuixw, data=usoc_long_carers_6to12,nest=TRUE)

#Save survey objects
s3write_using(usoc_design_2to5 # What R object we are saving
              , FUN = saveRDS # Which R function we are using to save
              , object = paste0("/Understanding Society/","usoc_design_2to5.rds") # Name of the file to save to (include file type)
              , bucket = IHT_bucket) # Bucket name defined above

s3write_using(usoc_design_6to12 # What R object we are saving
              , FUN = saveRDS # Which R function we are using to save
              , object = paste0("/Understanding Society/","usoc_design_6to12.rds") # Name of the file to save to (include file type)
              , bucket = IHT_bucket) # Bucket name defined above

#Load survey objects
usoc_design_2to5 <- s3read_using(readRDS
                            , object = paste0("/Understanding Society/","usoc_design_2to5.rds") # File to open
                            , bucket = IHT_bucket) # Bucket name defined above
usoc_design_6to12 <- s3read_using(readRDS
                            , object = paste0("/Understanding Society/","usoc_design_6to12.rds") # File to open
                            , bucket = IHT_bucket) # Bucket name defined above

####### Equivalised income deciles

# latest_income_sample <- usoc_design_6to12 %>%
#   subset(wave_num=="12"&!is.na(equ_annual_household_income))
#   
# latest_income_sample
# 
# svyquantile(~equ_annual_household_income, latest_income_sample, seq(0,1,by=0.1),ci=FALSE)

####### Percentage of carers by wave

ts_2to5 <- usoc_design_2to5 %>%
  as_survey(.) %>%
  group_by(wave_num,year_min_max,is_carer_anywhere) %>%
  summarize(n = survey_total()) %>%
  ungroup() %>%
  filter(!is.na(is_carer_anywhere)) %>%
  group_by(wave_num,year_min_max) %>%
  mutate(total=sum(n,na.rm=TRUE)) %>% 
  ungroup() %>% 
  mutate(pct=n/total*100,type="Anywhere") %>%
  filter(is_carer_anywhere=="Yes") %>%
  select(wave_num,year_min_max,pct,type)

ts_6to12 <- usoc_design_6to12 %>%
  as_survey(.) %>%
  group_by(wave_num,year_min_max,is_carer_anywhere) %>%
  summarize(n = survey_total()) %>%
  ungroup() %>%
  filter(!is.na(is_carer_anywhere)) %>%
  group_by(wave_num,year_min_max) %>%
  mutate(total=sum(n,na.rm=TRUE)) %>% 
  ungroup() %>% 
  mutate(pct=n/total*100,type="Anywhere") %>%
  filter(is_carer_anywhere=="Yes") %>%
  select(wave_num,year_min_max,pct,type)

ts <- plyr::rbind.fill(ts_2to5,ts_6to12)

ts_plot <- ts %>%
  ggplot(., aes(col=type, y=pct/100, x=year_min_max,group=type)) + 
  geom_line(linewidth = 1.5) +
  ggtitle("Percentage of adults 16+ who are unpaid carers") +
  scale_y_continuous(labels = scales::percent, name="Percent",limits=c(0,0.25)) +
  xlab("Year of survey") +
  labs(col = "Location") +
  theme_bw() +
  scale_fill_brewer(palette="Set2") +
  mytheme

ts_plot

write_xlsx(ts, paste0(R_workbench,"/usoc-carers-ndl/Flourish data/","usoc_prevalence_timeline.xlsx"))

####### Location of caring

usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_anywhere=="Yes") %>% 
  tbl_svysummary(include = c(caring_location),
                 type=everything()~"categorical") %>% 
  bold_labels()

######## Sex of carers

# sex_carers_data <- usoc_design_6to12 %>%
#   subset(wave_num=="12") %>%
#   as_survey(.) %>%
#   group_by(is_carer_anywhere,sexdv) %>% 
#   summarize(n = survey_total()) %>%
#   ungroup() %>%
#   mutate(is_carer_anywhere=ifelse(is.na(is_carer_anywhere),"Unknown",is_carer_anywhere)) %>%
#   filter(sexdv!=""&is_carer_anywhere!="Unknown") %>%
#   group_by(is_carer_anywhere) %>%
#   mutate(sex_total=sum(n)) %>%
#   ungroup() %>%
#   mutate(.,pct_sex=n/sex_total*100)
# 
# sex_carers_chart <- sex_carers_data %>%
#   ggplot(., aes(fill=sexdv, y=pct_sex/100, x=is_carer_anywhere)) +
#   geom_bar(position="stack", stat="identity",col="black",lwd=0.3) +
#   ggtitle("Sex of carers") +
#   labs(fill = "Sex") +
#   scale_y_continuous(labels = scales::percent, name="Percent") +
#   xlab("Is carer") +
#   theme_bw() +
#   scale_fill_brewer(palette="Set1") +
#   mytheme
# 
# sex_carers_chart

######## Age of carers

#Median age by relationship

latest_parent_carers <- usoc_design_6to12 %>%
  subset(wave_num=="12"&cared_for_inhh_child==1)

age_carers_table <- usoc_design_6to12 %>%
  subset(wave_num=="12"&!is.na(is_carer_anywhere)&!is.na(agecatlong)) %>%
  as_survey(.) %>%
  group_by(is_carer_anywhere,agecatlong) %>%
  summarize(n = survey_total()) %>%
  ungroup() %>%
  group_by(is_carer_anywhere) %>%
  mutate(n_age=sum(n,na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(pct=n/n_age*100)

# age_carers_table_census_comparison <- usoc_design_6to12 %>%
#   subset(wave_num=="12"&!is.na(is_carer_anywhere)&!is.na(agecatlong_cens)) %>%
#   as_survey(.) %>%
#   group_by(is_carer_anywhere,agecatlong_cens) %>% 
#   summarize(n = survey_total()) %>%
#   ungroup() %>%
#   group_by(is_carer_anywhere) %>%
#   mutate(n_age=sum(n,na.rm=TRUE)) %>% 
#   ungroup() %>%
#   mutate(pct=n/n_age*100)

####### Hours cared, latest wave (in-home, and all carers)

#Census comparison
# hours_carers_table_census_comparison <- usoc_design_6to12 %>%
#   subset(wave_num=="12"&is_carer_anywhere=="Yes"&!is.na(hours_cared_smaller)&hours_cared_smaller!="NA") %>%
#   as_survey(.) %>%
#   group_by(is_carer_anywhere,hours_cared_smaller) %>% 
#   summarize(n = survey_total()) %>%
#   ungroup() %>%
#   group_by(is_carer_anywhere) %>%
#   mutate(n_hours=sum(n,na.rm=TRUE)) %>% 
#   ungroup() %>%
#   mutate(pct=n/n_hours*100)

#Overall
# usoc_design_6to12 %>%
#   subset(wave_num=="12"&is_carer_anywhere=="Yes"&hours_cared!="") %>% 
#   tbl_svysummary(include = c(hours_cared),
#                  type=everything()~"categorical") %>% 
#   bold_labels()

#By sex
# usoc_design_6to12 %>%
#   subset(wave_num=="12"&is_carer_anywhere=="Yes"&hours_cared!="") %>% 
#   tbl_svysummary(include = c(hours_cared),
#                  type=everything()~"categorical",
#                  by="sexdv",
#                  label=list(hours_cared = "Hours")) %>% 
#   bold_labels()

#By caring location
usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_anywhere=="Yes"&hours_cared_even_smaller!="") %>% 
  tbl_svysummary(include = c(hours_cared_even_smaller),
                 type=everything()~"categorical",
                 by="caring_location",
                 label=list(hours_cared_even_smaller = "Hours")) %>% 
  bold_labels()

#By relationship
usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_anywhere=="Yes"&!is.na(hours_cared)&hours_cared!=""&hours_cared!="Varies 20 hours or more") %>%
  tbl_svysummary(include = c(hours_cared),
                 type=everything()~"categorical",
                 by="cared_for_inhh_child",
                 label=list(cared_for_inhh_child = "Cares for child")) %>%
  bold_labels()

####### Relationship to cared-for, latest wave (in-home, and all carers)

# usoc_design_6to12 %>%
#   subset(wave_num=="12"&is_carer_anywhere=="Yes") %>%
#   tbl_svysummary(include = c(care_rel_anywhere),
#                  type=everything()~"categorical",
#                  by="caring_location",
#                  label=list(care_rel_anywhere ~ "Cared-for person is")) %>%
#   bold_labels()

usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_anywhere=="Yes") %>%
  tbl_svysummary(include = c(cares_parent_anywhere,
                             cared_for_inhh_partner,
                             cared_for_inhh_child),
                 type=everything()~"categorical",
                 by="caring_location",
                 label=list(is_carer_anywhere = "Carer",
                            cares_parent_anywhere = "Cares for parent",
                            cared_for_inhh_partner = "Cares for partner",
                            cared_for_inhh_child = "Cares for child")) %>%
  bold_labels()

####### Relationship to cared-for, latest wave, by age

# usoc_design_6to12 %>%
#   subset(wave_num=="12"&is_carer_anywhere=="Yes") %>% 
#   tbl_svysummary(include = c(cares_parent_anywhere,
#                              cared_for_inhh_partner,
#                              cared_for_inhh_child),
#                  type=everything()~"categorical",
#                  by="agecat",
#                  label=list(is_carer_anywhere = "Carer",
#                             cares_parent_anywhere = "Cares for parent",
#                             cared_for_inhh_partner = "Cares for partner",
#                             cared_for_inhh_child = "Cares for child")) %>% 
#   bold_labels()

#Carers under 70
under70_bylocation <- usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_anywhere=="Yes"&agedv<=70) %>% 
  tbl_svysummary(include = c(cares_parent_anywhere,cared_for_inhh_partner),
                 type=everything()~"categorical",
                 by="caring_location",
                 label=list(is_carer_anywhere = "Carer",
                            cares_parent_anywhere = "Cares for parent (both in-household and outside household quest.)",
                            cared_for_inhh_partner = "Cares for partner (in-household questionnaire only)")) %>% 
  bold_labels() %>%
  modify_caption("**Carers under 70**")

under70_overall <- usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_anywhere=="Yes"&agedv<=70) %>% 
  tbl_svysummary(include = c(cares_parent_anywhere,cared_for_inhh_partner),
                 type=everything()~"categorical",
                 label=list(is_carer_anywhere = "Carer",
                            cares_parent_anywhere = "Cares for parent (both in-household and outside household quest.)",
                            cared_for_inhh_partner = "Cares for partner (in-household questionnaire only)")) %>% 
  bold_labels() %>%
  modify_caption("**Carers under 70**") 

tbl_merge_under70 <-
  tbl_merge(
    tbls = list(under70_overall,under70_bylocation),
    tab_spanner = c("**Overall**", "**By location**")
  )

tbl_merge_under70

#Carers over 70
over70_bylocation <- usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_anywhere=="Yes"&agedv>70) %>% 
  tbl_svysummary(include = c(cares_parent_anywhere,cared_for_inhh_partner),
                 type=everything()~"categorical",
                 by="caring_location",
                 label=list(is_carer_anywhere = "Carer",
                            cares_parent_anywhere = "Cares for parent (both in-household and outside household quest.)",
                            cared_for_inhh_partner = "Cares for partner (in-household questionnaire only)")) %>% 
  bold_labels() %>%
  modify_caption("**Carers over 70**")

over70_overall <- usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_anywhere=="Yes"&agedv>70) %>% 
  tbl_svysummary(include = c(cares_parent_anywhere,cared_for_inhh_partner),
                 type=everything()~"categorical",
                 label=list(is_carer_anywhere = "Carer",
                            cares_parent_anywhere = "Cares for parent (both in-household and outside household quest.)",
                            cared_for_inhh_partner = "Cares for partner (in-household questionnaire only)")) %>% 
  bold_labels() %>%
  modify_caption("**Carers over 70**") 

tbl_merge_over70 <-
  tbl_merge(
    tbls = list(over70_overall,over70_bylocation),
    tab_spanner = c("**Overall**", "**By location**")
  )

tbl_merge_over70

#everyone over 70 outside-household cares for a parent: (297+49+349)/742 [the total number of carers over 70] - > 94%
#nobody over 70 outside-household cares for a parent: (49+349)/742 [the total number of carers over 70] - > 53%
#(49+349)/742
#(2035)/(3878)

####### Age of different groups

# age_parent <- usoc_design_6to12 %>%
#   subset(wave_num=="12"&is_carer_anywhere=="Yes") %>%
#   as_survey(.) %>%
#   group_by(cares_parent_anywhere,agecat) %>%
#   summarize(n = survey_total()) %>%
#   ungroup() %>%
#   filter(cares_parent_anywhere==1) %>%
#   mutate(total=sum(n)) %>%
#   mutate(pct=n/total*100) %>%
#   mutate(rel="parent") %>%
#   select(rel,agecat,n,total,pct)
# 
# age_partner <- usoc_design_6to12 %>%
#   subset(wave_num=="12"&is_carer_anywhere=="Yes") %>%
#   as_survey(.) %>%
#   group_by(cared_for_inhh_partner,agecat) %>%
#   summarize(n = survey_total()) %>%
#   ungroup() %>%
#   filter(cared_for_inhh_partner==1) %>%
#   mutate(total=sum(n)) %>%
#   mutate(pct=n/total*100) %>%
#   mutate(rel="partner") %>%
#   select(rel,agecat,n,total,pct)
# 
# age_child <- usoc_design_6to12 %>%
#   subset(wave_num=="12"&is_carer_anywhere=="Yes") %>%
#   as_survey(.) %>%
#   group_by(cared_for_inhh_child,agecat) %>%
#   summarize(n = survey_total()) %>%
#   ungroup() %>%
#   filter(cared_for_inhh_child==1) %>%
#   mutate(total=sum(n)) %>%
#   mutate(pct=n/total*100) %>%
#   mutate(rel="child") %>%
#   select(rel,agecat,n,total,pct)
# 
# age_roles <- plyr::rbind.fill(age_parent,age_partner,age_child)
# rm(age_parent,age_partner,age_child)
# 
# age_carers_rel_chart <- age_roles %>%
#   ggplot(., aes(fill=agecat, y=pct/100, x=rel)) +
#   geom_bar(position="stack", stat="identity",col="black",lwd=0.3) +
#   ggtitle("Age of carers based on relationship to cared-for \n in 2021/22") +
#   labs(fill = "Age group") +
#   scale_y_continuous(labels = scales::percent, name="Percent") +
#   xlab("Relationship to cared-for (*overlapping, non-exclusive groups)") +
#   theme_bw() +
#   scale_fill_brewer(palette="Greens") +
#   mytheme
# 
# age_carers_rel_chart

######## Animated survey chart

#One row per respondent

survey_data_flourish <- usoc_long_carers %>%
  filter(.,!is.na(hidp)&wave_num=="12"&!is.na(indscuixw)&!is.na(agecat)) %>%
  filter(is_carer_anywhere=="Yes") %>%
  select(pidp,wave_num,indscuixw,is_carer_anywhere,agecat,caring_location,care_rel_anywhere,
         cares_parent_anywhere,cared_for_inhh_partner,cared_for_inhh_child) %>%
  mutate(.,cares_parent_anywhere=case_when(cares_parent_anywhere=="NA"|is.na(cares_parent_anywhere) ~ "Unknown",
                                           cares_parent_anywhere=="1" ~ "Yes",
                                           cares_parent_anywhere=="0" ~ "No",
                                   TRUE ~ "NA"),
         cared_for_inhh_partner=case_when(cared_for_inhh_partner=="NA"|is.na(cared_for_inhh_partner) ~ "Unknown",
                                          cared_for_inhh_partner=="1" ~ "Yes",
                                          cared_for_inhh_partner=="0" ~ "No",
                                         TRUE ~ "NA"),
         cared_for_inhh_child=case_when(cared_for_inhh_child=="NA"|is.na(cared_for_inhh_child) ~ "Unknown",
                                        cared_for_inhh_child=="1" ~ "Yes",
                                        cared_for_inhh_child=="0" ~ "No",
                                         TRUE ~ "NA"),
         care_rel_anywhere=str_to_title(care_rel_anywhere),
         caring_location_bis=ifelse(caring_location=="Both inside and outside household",NA,caring_location) %>%
           str_replace_all(.,"Only","") %>% trimws(.,"both") %>% str_to_title(.)) %>%
  replace_na(list(care_rel_anywhere = "Unknown")) %>%
  rename(Age=agecat,
         'Caring location'=caring_location,
         'Caring location (excl both)'=caring_location_bis,
         'Who are they caring for?'=care_rel_anywhere,
         'Is caring for parent?'=cares_parent_anywhere,
         'Is caring for partner?'=cared_for_inhh_partner,
         'Is caring for child?'=cared_for_inhh_child,
         'Is carer?'=is_carer_anywhere)

write_xlsx(survey_data_flourish, paste0(R_workbench,"/unpaid-carers/Flourish data/","survey_data_flourish.xlsx"))

#Synthetic data

  #Size of UK population
pop21 <- s3read_using(read_excel,
                      object = paste0(ASC_subfolder,"/2011 Census/ukpopestimatesmid2021on2021geographyfinal.xls"), # File to open
                      sheet="MYE2 - Persons",
                      bucket = IHT_bucket,
                      skip=7) %>%
  select(Code,Name,"16":"90+") %>%
  mutate(pop21=rowSums(across(as.character(c(16:89,"90+")),na.rm = TRUE))) %>%
  select(-as.character(c(16:89,"90+"))) %>% 
  mutate(Time=2021,Sex="All") %>%
  rename(Geography=Name)
pop21_uk <- filter(pop21,Geography=="UNITED KINGDOM") %>% pull(pop21)

  #Summary stats to start with
big_stats_table <- usoc_design_6to12 %>%
  subset(wave_num=="12"&!is.na(indscuixw)&!is.na(agecat)&!is.na(is_carer_anywhere)) %>%
  as_survey(.) %>%
  group_by(is_carer_anywhere,agecat,caring_location,care_rel_anywhere) %>%
  summarize(n = survey_total()) %>%
  ungroup() %>%
  replace_na(list(care_rel_anywhere = "Unknown")) %>%
  replace_na(list(caring_location = "Unknown")) %>% 
  mutate(total=sum(n,na.rm=TRUE)) %>%
  mutate(pct=n/total*100,
         n_pop=round(pct/100*pop21_uk,0),
         n_pop_thous=round(n_pop/500,0)) %>%
  select(-c("n_se","total")) %>%
  mutate(stratum=1:n()) %>% 
  select(stratum,is_carer_anywhere,agecat,caring_location,care_rel_anywhere,pct,n_pop,n_pop_thous)

  #Create dummy table that we can populate

  #Create empty data
dummy_data <- data.frame(dummy_id=1:sum(big_stats_table$n_pop_thous),stratum=NA)

  #Impute strata at random
for (i in 1:max(big_stats_table$stratum)){
  sampled_rows <- sample(dummy_data$dummy_id[which(is.na(dummy_data$stratum))], #Sample among unclaimed id's
                         big_stats_table$n_pop_thous[i], #The number of people each stratum should have
                         replace=FALSE) #without replacement
  dummy_data <- dummy_data %>%
    mutate(.,stratum=ifelse(dummy_id %in% sampled_rows,big_stats_table$stratum[i],stratum))
}

  #Merge characteristics back in
dummy_data_flourish <- dummy_data %>%
  left_join(.,big_stats_table,by="stratum") %>%
  arrange(stratum,dummy_id) %>%
  filter(is_carer_anywhere=="Yes") %>%
  mutate(care_rel_anywhere=str_to_title(care_rel_anywhere),
         caring_location_bis=ifelse(caring_location=="Both inside and outside household",NA,caring_location) %>%
           str_replace_all(.,"Only","") %>% trimws(.,"both") %>% str_to_title(.)) %>%
  select(-c("caring_location")) %>% 
  rename(Age=agecat,
         'Caring location'=caring_location_bis,
         'Who are they caring for?'=care_rel_anywhere,
         'Is carer?'=is_carer_anywhere)

write_xlsx(dummy_data_flourish, paste0(R_workbench,"/unpaid-carers/Flourish data/","dummy_data_flourish.xlsx"))

####### Caring and work

#Within the household, and not retired
usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_inhh=="Yes"&jbstat!="Retired"&aideft!="") %>% 
  tbl_svysummary(include = c(aideft),
                 type=everything()~"categorical",
                 label=list(aideft ~ "Does caring prevent work")) %>% 
  bold_labels()

#By persona
usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_inhh=="Yes"&jbstat!="Retired"&aideft!="") %>% 
  tbl_svysummary(include = c(aideft),
                 type=everything()~"categorical",
                 by="cared_for_inhh_child",
                 label=list(aideft ~ "Does caring prevent work")) %>% 
  bold_labels()

usoc_design_6to12 %>%
  subset(wave_num=="12"&is_carer_inhh=="Yes"&jbstat!="Retired"&aideft!="") %>% 
  tbl_svysummary(include = c(aideft),
                 type=everything()~"categorical",
                 by="caredfor_inhh_rel",
                 label=list(aideft ~ "Does caring prevent work")) %>% 
  bold_labels()

####### Household income

#Overall

# household_income_data <- usoc_design_6to12 %>%
#   subset(wave_num=="12") %>%
#   as_survey(.) %>%
#   group_by(is_carer_anywhere,equ_annual_income_dec) %>%
#   summarize(n = survey_total()) %>%
#   ungroup() %>%
#   filter(!is.na(is_carer_anywhere)&!is.na(equ_annual_income_dec)&equ_annual_income_dec!="") %>%
#   group_by(is_carer_anywhere) %>%
#   mutate(total_carer = sum(n)) %>%
#   ungroup() %>%
#   mutate(pct=n/total_carer*100)
# 
# household_income_chart <- household_income_data %>%
#   ggplot(., aes(x=equ_annual_income_dec, y=pct/100, group=is_carer_anywhere, colour=is_carer_anywhere)) + 
#   geom_line(size=2) +
#   geom_point() +
#   geom_hline(yintercept=0.1, linetype="dashed", color = "black") +
#   ggtitle("Distribution of equivalised household income, by caring status") +
#   labs(col = "Is carer") +
#   scale_y_continuous(labels = scales::percent, name="Percent",lim=c(0,0.2)) +
#   xlab("Equivalised household income decile") +
#   theme_bw() +
#   scale_fill_brewer(palette="Set1") +
#   mytheme
# household_income_chart

#By retirement age

# household_income_byret_data <- usoc_design_6to12 %>%
#   subset(wave_num=="12") %>%
#   as_survey(.) %>%
#   group_by(is_carer_anywhere,over65_or_retired,equ_annual_income_dec) %>%
#   summarize(n = survey_total()) %>%
#   ungroup() %>%
#   filter(!is.na(is_carer_anywhere)&!is.na(equ_annual_income_dec)&equ_annual_income_dec!=""&
#            !is.na(over65_or_retired)&over65_or_retired!="") %>%
#   group_by(is_carer_anywhere,over65_or_retired) %>%
#   mutate(total_carer = sum(n)) %>%
#   ungroup() %>%
#   mutate(pct=n/total_carer*100)
# 
# household_income_chart_byret <- household_income_byret_data %>%
#   ggplot(., aes(x=equ_annual_income_dec, y=pct/100, group=is_carer_anywhere, colour=is_carer_anywhere)) +
#   facet_wrap(~over65_or_retired,ncol=2) +
#   geom_line(size=2) +
#   geom_point() +
#   geom_hline(yintercept=0.1, linetype="dashed", color = "black") +
#   ggtitle("Distribution of equivalised household income, by caring status and retirement status") +
#   labs(col = "Is carer") +
#   scale_y_continuous(labels = scales::percent, name="Percent",lim=c(0,0.2)) +
#   xlab("Equivalised household income decile") +
#   theme_bw() +
#   scale_fill_brewer(palette="Set1") +
#   mytheme
# 
# household_income_chart_byret

#By retirement age and hours worked

####### Hours cared, by household income decile

#All age groups

hours_deprivation_data <- usoc_design_6to12 %>%
  subset(wave_num=="12"&(is_carer_anywhere %in% c("Yes","No"))&
           over65_or_retired=="Under 65 and not retired"&
           !is.na(hours_cared_even_smaller)&hours_cared_even_smaller!=""&hours_cared_even_smaller!="NA"&
           equ_annual_income_dec!=""&!is.na(equ_annual_income_dec)) %>%
  as_survey(.) %>%
  group_by(is_carer_anywhere,equ_annual_income_dec,hours_cared_even_smaller) %>%
  summarize(n = survey_total()) %>%
  ungroup() %>%
  group_by(is_carer_anywhere,hours_cared_even_smaller) %>%
  mutate(pct_income=n/sum(n)*100) %>% 
  ungroup()

carers_income_hours_flourish <- hours_deprivation_data %>%
  select(equ_annual_income_dec,hours_cared_even_smaller,pct_income) %>%
  pivot_wider(names_from = hours_cared_even_smaller,
              names_sep = ".",
              values_from = c(pct_income)) %>%
  rename(`Income decile`=equ_annual_income_dec,
         `Non-carers`=`Not a carer`,
         `Carers: under 20 hours a week`=`0-20 hours per week`,
         `Carers: 20+ hours a week`=`20+ hours per week`)

fwrite(carers_income_hours_flourish,
       paste0(R_workbench,"/unpaid-carers/Flourish data/","carers_income_hours_flourish.csv"))