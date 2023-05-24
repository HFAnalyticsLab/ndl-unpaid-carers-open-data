##########################################################
################### DEVELOPMENT IDEAS ####################
##########################################################

#Add group with 50 hours+ to scatterplot
#Add two hexmaps with % long hour carers
#Simple descriptives of partner data

##############################################
################### SETUP ####################
##############################################

#Load packages

library(readxl)
library(tidyverse)
library(viridis)
library(data.table)
library(readODS)
library(writexl)
library(janitor)
library(aws.s3)
library(onsr)
library(splitstackshape)
library(ggpubr)
library(ggrepel)
library(readODS)

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

carers_census_2021_sex_hours_eng <- s3read_using(read_excel,
                                                 object = paste0(ASC_subfolder,"/2021 Census/sc012021reftablesengland1.xlsx"),
                                                 bucket = IHT_bucket,
                                                 sheet="Table 18",skip=3)

#Wales

carers_census_2021_age_sex_wales <- s3read_using(read_excel,
                                               object = paste0(ASC_subfolder,"/2021 Census/sc012021reftableswales1.xlsx"),
                                               bucket = IHT_bucket,
                                               sheet="Table 3",skip=3)

carers_census_2021_age_sex_hours_wales <- s3read_using(read_excel,
                                                     object = paste0(ASC_subfolder,"/2021 Census/sc012021reftableswales1.xlsx"),
                                                     bucket = IHT_bucket,
                                                     sheet="Table 11",skip=3)

carers_census_2021_sex_hours_wales <- s3read_using(read_excel,
                                                   object = paste0(ASC_subfolder,"/2021 Census/sc012021reftableswales1.xlsx"),
                                                   bucket = IHT_bucket,
                                                   sheet="Table 12",skip=3)

#Combined

carers_census_2021_age_sex_engwales <- plyr::rbind.fill(carers_census_2021_age_sex_eng,
                                                        carers_census_2021_age_sex_wales)

carers_census_2021_age_sex_hours_engwales <- plyr::rbind.fill(carers_census_2021_age_sex_hours_eng,
                                                              carers_census_2021_age_sex_hours_wales)

carers_census_2021_sex_hours_engwales <- plyr::rbind.fill(carers_census_2021_sex_hours_eng,
                                                              carers_census_2021_sex_hours_wales)


##########################################
################### Sex ##################
##########################################

la_sex <- carers_census_2021_age_sex_hours_engwales %>%
  mutate(carer_status=case_when(`Unpaid Carer Status` %in% c("9 hours or less",
                                                              "10 to 19 hours",
                                                              "20 to 34 hours",
                                                              "35 to 49 hours",
                                                              "50 or more hours") ~ "Carer",
                                `Unpaid Carer Status` %in% c("Non-carer") ~ "Non-carer",
                                TRUE ~ "NA")) %>%
  mutate(sex_carer_status=paste(carer_status,Sex,sep="-")) %>% 
  group_by(`Local Authority`,`Area Code`,sex_carer_status) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  pivot_wider(names_from = sex_carer_status,
              names_sep = ".",
              values_from = c(Count)) %>%
  mutate(pct_carer=`Carer-Persons`/(`Non-carer-Persons`+`Carer-Persons`)*100,
         pct_carer_female=`Carer-Female`/(`Carer-Persons`)*100,
         pct_everyone_female=(`Carer-Female`+`Non-carer-Female`)/(`Non-carer-Persons`+`Carer-Persons`)*100) %>%
  select(`Local Authority`,`Area Code`,starts_with("pct"))

############################################
################### Ages ################### 
############################################

la_age <- carers_census_2021_age_sex_hours_engwales %>%
  mutate(carer_status=case_when(`Unpaid Carer Status` %in% c("9 hours or less",
                                                             "10 to 19 hours",
                                                             "20 to 34 hours",
                                                             "35 to 49 hours",
                                                             "50 or more hours") ~ "Carer",
                                `Unpaid Carer Status` %in% c("Non-carer") ~ "Non-carer",
                                TRUE ~ "NA"),
         age_group=case_when(Age %in% c("05 to 17","18 to 24","25 to 29") ~ "under 30",
                             Age %in% c("30 to 34","35 to 39","40 to 44","45 to 49") ~ "30 to 49",
                             Age %in% c("50 to 54","55 to 59","60 to 64","65 to 69") ~ "50 to 69",
                             Age %in% c("70 to 74","75 to 79","80 to 84","85 to 89","90+") ~ "over 70",
                             TRUE ~ "NA")) %>%
  mutate(age_carer_status=paste(carer_status,age_group,sep="-")) %>% 
  group_by(`Local Authority`,`Area Code`,age_carer_status) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  pivot_wider(names_from = age_carer_status,
              names_sep = ".",
              values_from = c(Count)) %>%
  mutate(carers=rowSums(across(starts_with("Carer"))),
         noncarers=rowSums(across(starts_with("Non-carer")))) %>% 
  mutate(pct_carer_under30=`Carer-under 30`/carers*100,
         pct_carer_30to50=`Carer-30 to 49`/carers*100,
         pct_carer_50to70=`Carer-50 to 69`/carers*100,
         pct_carer_over70=`Carer-over 70`/carers*100,
         pct_everyone_under30=(`Carer-under 30`+`Non-carer-under 30`)/(carers+noncarers)*100,
         pct_everyone_30to50=(`Carer-30 to 49`+`Non-carer-30 to 49`)/(carers+noncarers)*100,
         pct_everyone_50to70=(`Carer-50 to 69`+`Non-carer-50 to 69`)/(carers+noncarers)*100,
         pct_everyone_over70=(`Carer-over 70`+`Non-carer-over 70`)/(carers+noncarers)*100) %>%
  select(`Area Code`,starts_with("pct"))

####################################################
################### Hours worked ################### 
####################################################

la_hours <- carers_census_2021_age_sex_hours_engwales %>%
  filter(`Unpaid Carer Status`!="Non-carer") %>%
  group_by(`Local Authority`,`Area Code`,`Unpaid Carer Status`) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  pivot_wider(names_from = `Unpaid Carer Status`,
              names_sep = ".",
              values_from = c(Count)) %>%
  mutate(allhours=rowSums(across(contains("hours")))) %>%
  mutate(pct_under_20_hours=(`9 hours or less`+`10 to 19 hours`)/allhours*100,
         pct_20_to_50_hours=(`20 to 34 hours`+`35 to 49 hours`)/allhours*100,
         pct_over_50_hours=(`50 or more hours`)/allhours*100) %>%
  select(`Area Code`,starts_with("pct"))

##################################################
################### Merged data ##################
##################################################

la_summary <- la_sex %>%
  left_join(.,la_age,by="Area Code") %>%
  left_join(.,la_hours,by="Area Code")

############################################
################### Plots ##################
############################################

####### Heatmap of hours worked

hours_heatmap_data <- carers_census_2021_age_sex_hours_engwales %>%
  filter(Sex=="Persons"&`Unpaid Carer Status`!="Non-carer") %>%
  group_by(`Local Authority`,`Area Code`,`Unpaid Carer Status`) %>% #Sum over ages
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  group_by(`Local Authority`,`Area Code`) %>%
  mutate(all=sum(as.numeric(Count),na.rm=TRUE)) %>% #add total over all hours
  ungroup() %>%
  mutate(pct_hours=Count/all,
         `Unpaid Carer Status`=fct_relevel(`Unpaid Carer Status`,
                                           c("9 hours or less","10 to 19 hours","20 to 34 hours","35 to 49 hours","50 or more hours")),
         over_below_20_hours=case_when(`Unpaid Carer Status` %in% c("9 hours or less","10 to 19 hours") ~ "under 20 hours",
                                 `Unpaid Carer Status` %in% c("20 to 34 hours","35 to 49 hours","50 or more hours") ~ "over 20 hours",
                                 TRUE ~ "NA"),
         over_below_50_hours=case_when(`Unpaid Carer Status` %in% c("9 hours or less","10 to 19 hours","20 to 34 hours","35 to 49 hours") ~ "under 50 hours",
                                      `Unpaid Carer Status` %in% c("50 or more hours") ~ "over 50 hours",
                                      TRUE ~ "NA")) %>%
  arrange(`Local Authority`,`Unpaid Carer Status`) %>%
  group_by(`Local Authority`,`Area Code`,over_below_20_hours) %>%
  mutate(pct_over_below_20_hours=sum(Count,na.rm=TRUE)/all*100) %>% #Add pct under/over 20 hours
  ungroup() %>%
  group_by(`Local Authority`,`Area Code`,over_below_50_hours) %>%
  mutate(pct_over_below_50_hours=sum(Count,na.rm=TRUE)/all*100) %>% #Add pct under/over 50 hours
  ungroup() %>% 
  mutate(pct_below_20_hours=ifelse(over_below_20_hours=="under 20 hours",pct_over_below_20_hours,NA),
         pct_over_20_hours=ifelse(over_below_20_hours=="over 20 hours",pct_over_below_20_hours,NA),
         pct_below_50_hours=ifelse(over_below_50_hours=="under 50 hours",pct_over_below_50_hours,NA),
         pct_over_50_hours=ifelse(over_below_50_hours=="over 50 hours",pct_over_below_50_hours,NA)) %>%
  group_by(`Local Authority`,`Area Code`) %>%
  mutate(pct_below_20_hours=max(pct_below_20_hours,na.rm=TRUE),
         pct_over_20_hours=max(pct_over_20_hours,na.rm=TRUE),
         pct_below_50_hours=max(pct_below_50_hours,na.rm=TRUE),
         pct_over_50_hours=max(pct_over_50_hours,na.rm=TRUE)) %>%     
  ungroup() %>%
  select(-c("over_below_20_hours","pct_over_below_20_hours","over_below_50_hours","pct_over_below_50_hours")) %>%
  arrange(`Local Authority`,`Unpaid Carer Status`) %>%
  janitor::clean_names()

hours_heatmap_data %>%
  ggplot(., aes(y = unpaid_carer_status,
                x = reorder(local_authority,pct_below_20_hours),
                fill = pct_hours))+
  geom_tile() +
  scale_fill_viridis_c(option="A", name="% of carers in band", labels = scales::percent,direction=-1) +
  xlab("LA") +
  ylab("Hours per week") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 30, hjust = 1, vjust = 1, size=6),
        axis.ticks = element_blank())

  #Top 10 LAs in terms of hours
hours_heatmap_data %>%
  group_by(local_authority,area_code) %>%
  summarise(pct_over_20_hours=first(pct_over_20_hours)) %>% 
  ungroup() %>%
  slice_max(pct_over_20_hours,n=10)

  #Top 10 LAs in terms of hours
hours_heatmap_data %>%
  group_by(local_authority,area_code) %>%
  summarise(pct_over_20_hours=first(pct_over_20_hours)) %>% 
  ungroup() %>%
  slice_min(pct_over_20_hours,n=10)

####### Percentage of carers caring 50+ hours

pop_by_la <- carers_census_2021_age_sex_engwales %>% janitor::clean_names() %>% filter(sex=="Persons") %>% group_by(local_authority,area_code) %>% summarise(pop=sum(as.numeric(count),na.rm=TRUE)) %>% ungroup()
carers_by_la <- carers_census_2021_age_sex_engwales %>% janitor::clean_names() %>% filter(sex=="Persons"&unpaid_carer_status=="Unpaid carer") %>% group_by(area_code) %>% summarise(carers=sum(as.numeric(count),na.rm=TRUE)) %>% ungroup()
carers_hours_50plus <- carers_census_2021_age_sex_hours_engwales %>% janitor::clean_names() %>% filter(sex=="Persons"&unpaid_carer_status=="50 or more hours") %>% group_by(area_code) %>% summarise(carers_hours_50plus=sum(as.numeric(count),na.rm=TRUE)) %>% ungroup()
carers_hours_20plus <- carers_census_2021_age_sex_hours_engwales %>% janitor::clean_names() %>% filter(sex=="Persons"&(unpaid_carer_status %in% c("20 to 34 hours" ,"35 to 49 hours","50 or more hours"))) %>% group_by(area_code) %>% summarise(carers_hours_20plus=sum(as.numeric(count),na.rm=TRUE)) %>% ungroup()

hours_hex_map_data <- pop_by_la %>%
  left_join(.,carers_by_la,by="area_code") %>% 
  left_join(.,carers_hours_50plus,by="area_code") %>%
  left_join(.,carers_hours_20plus,by="area_code") %>%
  mutate(pct_pop_hours_50plus=carers_hours_50plus/pop*100,
         pct_carers_hours_50plus=carers_hours_50plus/carers*100,
         pct_pop_hours_20plus=carers_hours_20plus/pop*100,
         pct_carers_hours_20plus=carers_hours_20plus/carers*100)

rm(pop_by_la,carers_by_la,carers_hours_50plus,carers_hours_20plus)

utla_hex_template <- s3read_using(read_excel,
                                  object = paste0(ASC_subfolder,"/hexmap-lad-template.xlsx"), # File to open
                                  bucket = IHT_bucket,
                                  sheet=1)

hours_hex_map_data <- utla_hex_template %>%
  left_join(.,hours_hex_map_data,by=c("lacode"="area_code"))
rm(utla_hex_template)

fwrite(hours_hex_map_data, paste0(R_workbench,"/Charts/Carers/","hours_hex_map_data.csv"))

####### Heatmap of ages

ages_heatmap_data <- carers_census_2021_age_sex_hours_engwales %>%
  filter(Sex=="Persons"&`Unpaid Carer Status`!="Non-carer") %>%
  mutate(age_lower_band=word(Age,1,sep="to") %>% trimws("both") %>% as.numeric(),
         age_upper_band=word(Age,2,sep="to") %>% trimws("both") %>% as.numeric()) %>%
  mutate(age_lower_band=ifelse(Age=="90+",90,age_lower_band),
         age_upper_band=ifelse(Age=="90+",90,age_upper_band)) %>%
  mutate(midpoint_age=(age_lower_band+age_upper_band)/2) %>%
  group_by(`Local Authority`,`Area Code`,Age) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE),
            midpoint_age=first(midpoint_age)) %>% 
  ungroup() %>%
  group_by(`Local Authority`,`Area Code`) %>%
  mutate(all=sum(Count,na.rm=TRUE),
         pct_age=Count/sum(Count,na.rm=TRUE),
         mean_age=weighted.mean(midpoint_age,Count)) %>% 
  ungroup() %>%
  janitor::clean_names()
  
ages_heatmap_data %>%
  ggplot(., aes(y = age,
                x = reorder(local_authority,mean_age),
                fill = pct_age))+
  geom_tile() +
  scale_fill_viridis_c(option="D", name="% of carers in band", labels = scales::percent) +
  ylab("Age band") +
  xlab("LA") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 30, hjust = 1, vjust = 1, size=6),
        axis.ticks = element_blank())

#Top 10 oldest LAs
ages_heatmap_data %>%
  group_by(local_authority,area_code) %>%
  summarise(mean_age=first(mean_age)) %>% 
  ungroup() %>%
  slice_max(mean_age,n=10)

#Top 10 youngest LAs
ages_heatmap_data %>%
  group_by(local_authority,area_code) %>%
  summarise(mean_age=first(mean_age)) %>% 
  ungroup() %>%
  slice_min(mean_age,n=10)

####### Age vs hours worked

age_mini <- ages_heatmap_data %>% group_by(local_authority,area_code) %>% summarise(mean_age=first(mean_age)) %>% ungroup()
hours_mini <-hours_heatmap_data %>% group_by(area_code) %>% summarise(pct_over_20_hours=first(pct_over_20_hours), pct_over_50_hours=first(pct_over_50_hours)) %>% ungroup()
prev_mini <- carers_census_2021_age_sex_engwales %>% janitor::clean_names() %>% filter(sex=="Persons"&unpaid_carer_status=="Unpaid carer") %>% group_by(area_code) %>% summarise(count=sum(as.numeric(count),na.rm=TRUE),pop=sum(as.numeric(population),na.rm=TRUE)) %>% ungroup() %>% mutate(pct_carer=count/pop*100) %>% select(-c("count","pop")) 
  
age_hours_corr_data <- age_mini %>%
  left_join(.,hours_mini,by="area_code") %>%
  left_join(.,prev_mini,by="area_code")
rm(age_mini,hours_mini)

age_hours_corr_plot_one <- ggplot(age_hours_corr_data, aes(x=mean_age, y=pct_over_20_hours/100,
                                                       size=pct_carer, label=local_authority)) +
  geom_point(colour="cornflowerblue",pch=21, alpha=0.75) +
  geom_label_repel(size = 3,box.padding = unit(0.2, "lines"),alpha = 0.6,) +
  stat_cor(method = "pearson", label.y = 0.65, show.legend = FALSE) +
  scale_x_continuous(name="Average age of carers") +
  scale_y_continuous(labels = scales::percent, name="Percentage of carers\ncaring 20+ hours") +
  guides(size=guide_legend(title="Carers prevalence (%)")) +
  theme_bw()

age_hours_corr_plot_two <- ggplot(age_hours_corr_data, aes(x=mean_age, y=pct_over_50_hours/100,
                                                           size=pct_carer, label=local_authority)) +
  geom_point(colour="darkblue",pch=21, alpha=0.75) +
  geom_label_repel(size = 3,box.padding = unit(0.2, "lines"),alpha = 0.6,) +
  stat_cor(method = "pearson", label.y = 0.65, show.legend = FALSE) +
  scale_x_continuous(name="Average age of carers") +
  scale_y_continuous(labels = scales::percent, name="Percentage of carers\ncaring 50+ hours") +
  guides(size=guide_legend(title="Carers prevalence (%)")) +
  theme_bw()

#Variation in age bands

# age_band_chart_data <- la_summary %>%
#   select(`Local Authority`,pct_carer_under30,pct_carer_30to50,pct_carer_50to70,pct_carer_over70) %>%
#   pivot_longer(!c("Local Authority"), names_to = "age_group", values_to = "pct") %>%
#   mutate(age_group=str_replace_all(age_group,"pct_carer_","")) %>%
#   mutate(age_group=fct_relevel(age_group,c("over70","50to70","30to50","under30"))) %>%
#   group_by(age_group) %>%
#   mutate(group.mean=mean(pct,na.rm=TRUE)) %>% 
#   ungroup()

# age_band_chart <- ggplot(age_band_chart_data, aes(x=pct, fill=age_group)) +
#   geom_density(alpha=.50) +
#   geom_vline(aes(xintercept=group.mean),linetype="dashed",col="black") +
#   scale_fill_brewer(palette="Greens",name="Age band") +
#   facet_wrap(~age_group, ncol=1) +
#   labs(title='LAs support unpaid carers with different age compositions', subtitle='2021 Census in England and Wales') +
#   xlab("Percentage of unpaid carers in age band") +
#   ylab("Density") +
#   theme_bw()

# ggplot(age_band_chart_data, aes(x = pct, y = age_group, fill=age_group)) +
#   geom_density_ridges(
#     jittered_points = TRUE,
#     position = position_points_jitter(width = 0.05, height = 0),
#     point_shape = '|', point_size = 3, point_alpha = 1, alpha = 0.7,
#   ) +
#   scale_fill_brewer(palette="Greens",name="Age band",direction=-1) +
#   labs(title='LAs support unpaid carers with different age compositions', subtitle='2021 Census in England and Wales') +
#   xlab("Density of percentages (LAs)") +
#   ylab("Age band") +
#   theme_bw()

#Relationship to people's age

gen_pop_comp_data <- la_summary %>%
  select(`Local Authority`,pct_carer_under30,pct_carer_30to50,pct_carer_50to70,pct_carer_over70,
         starts_with("pct_everyone")) %>%
  select(-"pct_everyone_female") %>%
  merged.stack(., var.stubs = c("pct_carer","pct_everyone"), sep = "_") %>%
  rename(age_group=.time_2) %>% 
  group_by(`Local Authority`,age_group) %>%
  summarise(pct_carer=max(pct_carer,na.rm=TRUE),
            pct_everyone=max(pct_everyone,na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(age_group=fct_relevel(age_group,c("over70","50to70","30to50","under30")[4:1]))

gen_pop_comp_chart <- ggplot(gen_pop_comp_data, aes(x=pct_everyone/100, y=pct_carer/100, fill=age_group)) +
  geom_point(colour="black",pch=21, size=2, alpha=0.5) +
  stat_cor(method = "pearson", label.y = 0.50) +
  geom_abline(intercept =0 , slope = 1,lty="dashed") +
  xlim(0,0.50)+ylim(0,0.50) +
  facet_wrap(~age_group, ncol=2) +
  scale_fill_brewer(palette="Greens",name="Age band") +
  labs(title='The age of carers reflects the age of the underlying population', subtitle='2021 Census in England and Wales') +
  scale_x_continuous(labels = scales::percent, name="Percentage of all residents\nin age band") +
  scale_y_continuous(labels = scales::percent, name="Percentage of unpaid carers\nin age band") +
  annotate("text", x = 0.25, y = 0.40, size = 3, label = "More among carers than gen-pop",col="grey") +
  annotate("text", x = 0.25, y = 0.10, size = 3, label = "Less among carers than gen-pop",col="grey") +
  theme_bw()
gen_pop_comp_chart

#Relationship between overall age and percent caring

gen_pop_comp_bis_data <- la_summary %>%
  select(`Local Authority`,pct_carer,starts_with("pct_everyone")) %>%
  select(-"pct_everyone_female") %>%
  merged.stack(., var.stubs = c("pct_carer","pct_everyone"), sep = "_") %>%
  rename(age_group=.time_2) %>% 
  group_by(`Local Authority`) %>%
  mutate(pct_carer=max(pct_carer,na.rm=TRUE)) %>% 
  ungroup() %>%
  filter(!is.na(age_group)) %>%
  select(-".time_1") %>% 
  mutate(age_group=fct_relevel(age_group,c("over70","50to70","30to50","under30")[4:1])) %>% 
  arrange(`Local Authority`,age_group)

gen_pop_comp_bis_chart <- ggplot(gen_pop_comp_bis_data,
                                 aes(x=pct_everyone/100, y=pct_carer/100, fill=age_group)) +
  geom_point(colour="black",pch=21, size=2, alpha=0.5) +
  stat_cor(method = "pearson", label.y = 0.15) +
  xlim(0,0.5)+ylim(0,0.25) +
  facet_wrap(~age_group, ncol=2, scales = "free") +
  scale_fill_brewer(palette="Greens",name="Age band") +
  labs(title='The age of carers reflects the age of the underlying population', subtitle='2021 Census in England and Wales') +
  scale_x_continuous(labels = scales::percent, name="Percentage of all residents\nin age band") +
  scale_y_continuous(labels = scales::percent, name="Percentage of all residents in age band\nwho are unpaid carers") +
  theme_bw()
gen_pop_comp_bis_chart

#Variation in hours cared

hours_chart_data <- la_summary %>%
  select(`Local Authority`,contains("hours")) %>%
  pivot_longer(!c("Local Authority"), names_to = "hours_group", values_to = "pct") %>%
  mutate(hours_group=str_replace_all(hours_group,"pct_","") %>% str_replace_all(.,"_"," ")) %>%
  mutate(hours_group=fct_relevel(hours_group,c("over 50 hours","20 to 50 hours","under 20 hours"))) %>%
  group_by(hours_group) %>%
  mutate(group.mean=mean(pct,na.rm=TRUE)) %>% 
  ungroup()

ggplot(hours_chart_data, aes(x = pct, y = hours_group, fill=hours_group)) +
  geom_density_ridges(
    jittered_points = TRUE,
    position = position_points_jitter(width = 0.05, height = 0),
    point_shape = '|', point_size = 3, point_alpha = 1, alpha = 0.7,
  ) +
  scale_fill_brewer(palette="Purples",name="Hours cared per week",direction=-1) +
  labs(title='LAs support unpaid carers with different age compositions', subtitle='2021 Census in England and Wales') +
  xlab("Density of percentages (LAs)") +
  ylab("Hours per week") +
  theme_bw()

###############################################
################### Pyramids ##################
###############################################

#Overall population pyramid: by sex

base_pyramid <- carers_census_2021_age_sex_engwales %>%
  filter(`Unpaid Carer Status`=="Unpaid carer"&Sex!="Persons") %>% 
  group_by(`Sex`,`Age`) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(Count=ifelse(Sex=="Male",-1*Count,Count))

pop_range_breaks <- pretty(range(base_pyramid$Count), n = 7)

ggplot(base_pyramid,
       aes(x = Count,
           y = Age,
           fill = Sex)) +
  geom_col() +
  scale_x_continuous(breaks  = pop_range_breaks,
                     labels = scales::comma(abs(pop_range_breaks))) +
  scale_fill_brewer(palette = "Set1") +
  xlab("Number of carers") +
  theme_bw() +
  theme(legend.position = "top") 

#Overall population pyramid: by hours cared

hours_pyramid <- carers_census_2021_age_sex_hours_engwales %>%
  filter(`Unpaid Carer Status`!="Non-carer"&Sex!="Persons") %>%
  mutate(hours_status=case_when(`Unpaid Carer Status` %in% c("9 hours or less",
                                                             "10 to 19 hours") ~ "Under 20 hours",
                                `Unpaid Carer Status` %in% c("20 to 34 hours",
                                                             "35 to 49 hours",
                                                             "50 or more hours") ~ "Over 20 hours",
                                TRUE ~ "NA")) %>% 
  group_by(`hours_status`,`Age`) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(Count=ifelse(hours_status=="Under 20 hours",-1*Count,Count))

pop_range_breaks_hours <- pretty(range(hours_pyramid$Count), n = 7)

ggplot(hours_pyramid,
       aes(x = Count,
           y = Age,
           fill = hours_status)) +
  geom_col() +
  scale_x_continuous(breaks  = pop_range_breaks_hours,
                     labels = scales::comma(abs(pop_range_breaks_hours))) +
  scale_fill_brewer(palette = "Paired", name="Hours cared per week", direction=-1) +
  xlab("Number of carers") +
  theme_bw() +
  theme(legend.position = "top")

#Overall population pyramid: by hours cared and sex

hours_sex_pyramid <- carers_census_2021_age_sex_hours_engwales %>%
  filter(`Unpaid Carer Status`!="Non-carer"&Sex!="Persons") %>%
  mutate(hours_status=case_when(`Unpaid Carer Status` %in% c("9 hours or less",
                                                             "10 to 19 hours") ~ "Under 20 hours",
                                `Unpaid Carer Status` %in% c("20 to 34 hours",
                                                             "35 to 49 hours",
                                                             "50 or more hours") ~ "Over 20 hours",
                                TRUE ~ "NA")) %>% 
  group_by(`hours_status`,`Age`,Sex) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(Count=ifelse(Sex=="Male",-1*Count,Count))

pop_range_breaks_hours_sex <- pretty(range(hours_sex_pyramid$Count), n = 5)

ggplot(hours_sex_pyramid,
       aes(x = Count,
           y = Age,
           fill = Sex)) +
  geom_col() +
  facet_wrap(~hours_status, ncol=2) +
  scale_x_continuous(breaks  = pop_range_breaks_hours_sex,
                     labels = scales::comma(abs(pop_range_breaks_hours_sex))) +
  scale_fill_brewer(palette = "Set1", name="Sex") +
  xlab("Number of carers") +
  theme_bw() +
  theme(legend.position = "top")

###########################################################
################### Pyramids by location ##################
###########################################################

#Age and sex

base_pyramid_national <- carers_census_2021_age_sex_engwales %>%
  mutate(ndl_location_grouping="England and Wales") %>% 
  filter(`Unpaid Carer Status`=="Unpaid carer"&Sex!="Persons"&ndl_location_grouping!="Non NDL") %>% 
  group_by(ndl_location_grouping,`Sex`,`Age`) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  group_by(ndl_location_grouping) %>%
  mutate(Total=sum(Count,na.rm=TRUE),
         Percent=Count/sum(Count,na.rm=TRUE)) %>% 
  ungroup() %>% 
  mutate(Count=ifelse(Sex=="Male",-1*Count,Count),
         Percent=ifelse(Sex=="Male",-1*Percent,Percent))

base_pyramid_locations <- carers_census_2021_age_sex_engwales %>%
  mutate(ndl_location_grouping=case_when(`Local Authority` %in% c("Liverpool","Wirral") ~ "Liverpool and Wirral",
                                         `Local Authority` %in% c("Leeds") ~ "Leeds",
                                         `Local Authority` %in% c("Harrow","Brent",
                                                                  "Hillingdon","Ealing",
                                                                  "Hounslow","Hammersmith and Fulham",
                                                                  "Kensington and Chelsea",
                                                                  "City of London and Westminster") ~ "North West London",
                                         `Local Authority` %in% c("Neath Port Talbot") ~ "Neath Port Talbot",
                                         `Local Authority` %in% c("Swansea") ~ "Swansea",
                                         TRUE ~ "Non NDL")) %>% 
  filter(`Unpaid Carer Status`=="Unpaid carer"&Sex!="Persons"&ndl_location_grouping!="Non NDL") %>% 
  group_by(ndl_location_grouping,`Sex`,`Age`) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  group_by(ndl_location_grouping) %>%
  mutate(Total=sum(Count,na.rm=TRUE),
         Percent=Count/sum(Count,na.rm=TRUE)) %>% 
  ungroup() %>% 
  mutate(Count=ifelse(Sex=="Male",-1*Count,Count),
         Percent=ifelse(Sex=="Male",-1*Percent,Percent))

base_pyramid_locations <- plyr::rbind.fill(base_pyramid_national,base_pyramid_locations)
rm(base_pyramid_national)

pop_range_breaks_locations <- pretty(range(base_pyramid_locations$Percent), n = 5)

ggplot(base_pyramid_locations,
       aes(x = Percent,
           y = Age,
           fill = Sex)) +
  geom_col() +
  facet_wrap(~ndl_location_grouping, ncol=3) +
  scale_x_continuous(breaks  = pop_range_breaks_locations,
                     labels = scales::percent(abs(pop_range_breaks_locations))) +
  scale_fill_brewer(palette = "Set1", name="Sex") +
  xlab("Percentage of carers") +
  theme_bw() +
  theme(legend.position = "top")

#Age and hours

base_hours_pyramid_national <- carers_census_2021_age_sex_hours_engwales %>%
  mutate(ndl_location_grouping="England and Wales",
         hours_status=case_when(`Unpaid Carer Status` %in% c("9 hours or less",
                                                             "10 to 19 hours") ~ "Under 20 hours",
                                `Unpaid Carer Status` %in% c("20 to 34 hours",
                                                             "35 to 49 hours",
                                                             "50 or more hours") ~ "Over 20 hours",
                                TRUE ~ "NA")) %>% 
  filter(`Unpaid Carer Status`!="Non-carer"&Sex!="Persons"&ndl_location_grouping!="Non NDL") %>% 
  group_by(ndl_location_grouping,hours_status,`Age`) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  group_by(ndl_location_grouping) %>%
  mutate(Total=sum(Count,na.rm=TRUE),
         Percent=Count/sum(Count,na.rm=TRUE)) %>% 
  ungroup() %>% 
  mutate(Count=ifelse(hours_status=="Under 20 hours",-1*Count,Count),
         Percent=ifelse(hours_status=="Under 20 hours",-1*Percent,Percent))

base_hours_pyramid_locations <- carers_census_2021_age_sex_hours_engwales %>%
  mutate(ndl_location_grouping=case_when(`Local Authority` %in% c("Liverpool","Wirral") ~ "Liverpool and Wirral",
                                         `Local Authority` %in% c("Leeds") ~ "Leeds",
                                         `Local Authority` %in% c("Harrow","Brent",
                                                                  "Hillingdon","Ealing",
                                                                  "Hounslow","Hammersmith and Fulham",
                                                                  "Kensington and Chelsea",
                                                                  "City of London and Westminster") ~ "North West London",
                                         `Local Authority` %in% c("Neath Port Talbot") ~ "Neath Port Talbot",
                                         `Local Authority` %in% c("Swansea") ~ "Swansea",
                                         TRUE ~ "Non NDL"),
         hours_status=case_when(`Unpaid Carer Status` %in% c("9 hours or less",
                                                             "10 to 19 hours") ~ "Under 20 hours",
                                `Unpaid Carer Status` %in% c("20 to 34 hours",
                                                             "35 to 49 hours",
                                                             "50 or more hours") ~ "Over 20 hours",
                                TRUE ~ "NA")) %>% 
  filter(`Unpaid Carer Status`!="Non-carer"&Sex!="Persons"&ndl_location_grouping!="Non NDL") %>% 
  group_by(ndl_location_grouping,hours_status,`Age`) %>%
  summarise(Count=sum(as.numeric(Count),na.rm=TRUE)) %>% 
  ungroup() %>%
  group_by(ndl_location_grouping) %>%
  mutate(Total=sum(Count,na.rm=TRUE),
         Percent=Count/sum(Count,na.rm=TRUE)) %>% 
  ungroup() %>% 
  mutate(Count=ifelse(hours_status=="Under 20 hours",-1*Count,Count),
         Percent=ifelse(hours_status=="Under 20 hours",-1*Percent,Percent))

base_hours_pyramid_locations <- plyr::rbind.fill(base_hours_pyramid_national,base_hours_pyramid_locations)
rm(base_hours_pyramid_national)

pop_range_breaks_hours_locations <- pretty(range(base_hours_pyramid_locations$Percent), n = 5)

ggplot(base_hours_pyramid_locations,
       aes(x = Percent,
           y = Age,
           fill = hours_status)) +
  geom_col() +
  facet_wrap(~ndl_location_grouping, ncol=3) +
  scale_x_continuous(breaks  = pop_range_breaks_hours_locations,
                     labels = scales::percent(abs(pop_range_breaks_hours_locations))) +
  scale_fill_brewer(palette = "Paired", name="Hours cared per week", direction=-1) +
  xlab("Percentage of carers") +
  guides(fill = guide_legend(reverse=TRUE)) +
  theme_bw() +
  theme(legend.position = "top")