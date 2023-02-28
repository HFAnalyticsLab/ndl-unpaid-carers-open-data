###################
###### To-do ######
###################

#######################
###### Libraries ######
#######################

library(tidyverse)
library(pbapply)
library(data.table)
library(janitor)
library(aws.s3)
library(parallel)
library(pbmcapply)

##########################
###### Read in data ######
##########################

#Clean up the global environment

rm(list = ls())

#Directories in S3

IHT_bucket <- "s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp"
USOC_subfolder <- "/Understanding Society/UKDA-6614-tab/tab/ukhls"
R_workbench <- path.expand("~")
localgit <- dirname(rstudioapi::getSourceEditorContext()$path)

#Read in data

usoc_long <- s3read_using(fread
                          , object = paste0("/Understanding Society/","USOC long.csv") # File to open
                          , bucket = IHT_bucket) # Bucket name defined above

egoalt_long <- s3read_using(fread
                            , object = paste0("/Understanding Society/","egoalt long.csv") # File to open
                            , bucket = IHT_bucket) # Bucket name defined above

#Set up multicore

core.num <- parallel::detectCores(all.tests = FALSE, logical = TRUE)/2

#Add wave variable

wave.mini <- data.frame(wave=letters,wave_num=1:length(letters))

usoc_long <- usoc_long %>%
  mutate(wave=as.character(wave)) %>% 
  left_join(.,wave.mini,by="wave") %>%
  mutate(wave_pidp=paste(wave_num,pidp,sep="-"))

egoalt_long <- egoalt_long %>%
  mutate(wave=as.character(wave)) %>% 
  left_join(.,wave.mini,by="wave")

#Date variable

usoc_long <- usoc_long %>%
  mutate(.,itv_date=lubridate::ymd(paste(intdatydv,intdatmdv,intdatddv,sep="-")),
         itv_year=as.character(ifelse(intdatydv>0,intdatydv,NA)))

#Was interviewed (85%)

usoc_long <- usoc_long %>%
  mutate(interviewed=ifelse(indall==0&!is.na(indall),1,ifelse(is.na(indall)|indall==1,0,0)))

#Add wave years
wave_years_small <- usoc_long %>%
  group_by(wave_num) %>%
  summarise(year_min=min(itv_year,na.rm=TRUE),
            year_max=max(itv_year,na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(year_min_max=paste(year_min,year_max,sep="-")) %>%
  select(wave_num,year_min_max)

usoc_long <- usoc_long %>%
  left_join(.,wave_years_small,by="wave_num")
rm(wave_years_small)

###########################
###### New variables ######
###########################

#Age groups
usoc_long <- usoc_long %>%
  mutate(agedv=ifelse(agedv<0,NA,agedv)) %>%
  mutate(.,agecatlong=case_when(agedv>=16&agedv<=30 ~ "16 to 30",
                            agedv>=31&agedv<=40 ~ "30 to 40",
                            agedv>=41&agedv<=50 ~ "40 to 50",
                            agedv>=51&agedv<=60 ~ "50 to 60",
                            agedv>=61&agedv<=70 ~ "60 to 70",
                            agedv>=71 ~ "70+",
                            TRUE ~ "NA"),
         agecat=case_when(agedv>=16&agedv<=30 ~ "16 to 30",
                            agedv>=31&agedv<=50 ~ "31 to 50",
                            agedv>=51&agedv<=70 ~ "51 to 70",
                            agedv>=71 ~ "70+",
                            TRUE ~ "NA"),
         agecatlong_cens=case_when(agedv>=18&agedv<=29 ~ "18 to 30",
                                agedv>=30&agedv<=39 ~ "30 to 40",
                                agedv>=40&agedv<=49 ~ "40 to 50",
                                agedv>=50&agedv<=59 ~ "50 to 60",
                                agedv>=60&agedv<=69 ~ "60 to 70",
                                agedv>=70 ~ "70+",
                                TRUE ~ "NA"))

#Recode sex
usoc_long <- usoc_long %>%
  mutate(.,sexdv=factor(x=as.character(sexdv),
                        labels=c("Male","Female"),
                        levels = as.character(1:2)))
#Recode region

# Value label	Value	Absolute frequency	Relative frequency	
# missing	-9	6	0.07%	
# North East	1	276	3.22%	
# North West	2	967	11.29%	
# Yorkshire and the Humber	3	812	9.48%	
# East Midlands	4	587	6.85%	
# West Midlands	5	807	9.42%	
# East of England	6	746	8.71%	
# London	7	1239	14.46%	
# South East	8	933	10.89%	
# South West	9	628	7.33%	
# Wales	10	469	5.47%	
# Scotland	11	588	6.86%	
# Northern Ireland	12	510	5.95%	

usoc_long <- usoc_long %>%
  mutate(.,gordv=factor(x=as.character(gordv),
                        labels=c("North East","North West","Yorkshire and the Humber",
                                 "East Midlands","West Midlands","East of England",
                                 "London","South East","South West",
                                 "Wales","Scotland","Northern Ireland"),
                        levels = as.character(1:12)))

#Recode job status

# Refused	-2	51	0.16%	
# Dont know	-1	18	0.06%	
# Self employed	1	2431	7.6%	
# Paid employment(ft/pt)	2	15000	46.87%	
# Unemployed	3	1303	4.07%	
# Retired	4	8405	26.26%	
# On maternity leave	5	178	0.56%	
# Family care or home	6	1241	3.88%	
# Full-time student	7	1770	5.53%	
# LT sick or disabled	8	1093	3.41%	
# Govt training scheme	9	16	0.05%	
# Unpaid, family business	10	22	0.07%	
# On apprenticeship	11	80	0.25%	
# 12	156	0.49%	
# 13	25	0.08%	
# Doing something else	97	217	0.68%

usoc_long <- usoc_long %>%
  mutate(.,jbstat=ifelse(jbstat==12|jbstat==13,11,jbstat)) %>% 
  mutate(.,jbstat=factor(x=as.character(jbstat),
                         labels=c("Self employed","Paid employment (ft/pt)","Unemployed",
                                  "Retired","On maternity leave","Family care or home",
                                  "Full-time student","LT sick or disabled","Govt training scheme",
                                  "Unpaid, family business","On apprenticeship",
                                  "Doing something else"),
                         levels = as.character(c(1:11,97))))

#Recode relationship

# Value label	Value	Absolute frequency	Relative frequency	
# missing	-9	160	0.15%	
# husband/wife	1	18882	17.6%	
# partner/cohabitee	2	4440	4.14%	
# civil partner	3	118	0.11%	
# natural son/daughter	4	26335	24.55%	
# adopted son/daughter	5	225	0.21%	
# foster child	6	70	0.07%	
# stepson/stepdaughter	7	889	0.83%	
# son-in-law/daughter-in-law	8	416	0.39%	
# natural parent	9	26335	24.55%	
# adoptive parent	10	225	0.21%	
# foster parent	11	70	0.07%	
# step-parent	12	889	0.83%	
# parent-in-law	13	416	0.39%	
# natural brother/sister	14	19528	18.2%	
# half-brother/sister	15	1595	1.49%	
# step-brother/sister	16	283	0.26%	
# adopted brother/sister	17	108	0.1%	
# foster brother/sister	18	50	0.05%	
# brother/sister-in-law	19	434	0.4%	
# grand-child	20	1097	1.02%	
# grand-parent	21	1097	1.02%	
# cousin	22	258	0.24%	
# aunt/uncle	23	581	0.54%	
# niece/nephew	24	581	0.54%	
# other relative	25	164	0.15%	
# employee	26	2	0.0%	
# employer	27	2	0.0%	
# lodger/boarder/tenant	28	111	0.1%	
# landlord/landlady	29	111	0.1%	
# other non-relative	30	1804	1.68%

egoalt_long <- egoalt_long %>%
  mutate(.,relationshipdv=factor(x=as.character(relationship_dv),
                                 labels=c("husband/wife","partner/cohabitee","civil partner","natural son/daughter","adopted son/daughter",
                                          "foster child","stepson/stepdaughter","son-in-law/daughter-in-law","natural parent","adoptive parent",
                                          "foster parent","step-parent","parent-in-law","natural brother/sister","half-brother/sister",
                                          "step-brother/sister","adopted brother/sister","foster brother/sister","brother/sister-in-law","grand-child",
                                          "grand-parent","cousin","aunt/uncle","niece/nephew","other relative",
                                          "employee","employer","lodger/boarder/tenant","landlord/landlady","other non-relative"),
                                 levels = as.character(1:30)))

#Is a carer: in household

# aidhh
# Text:
#   Is there anyone living with you who is sick, disabled or elderly whom you look after or give special help to (for example, a sick, disabled or elderly relative, husband, wife or friend etc)?
#   Value label	Value	Absolute frequency	Relative frequency	
# Missing	-9	5	0.02%	
# inapplicable	-8	4964	15.51%	
# Refused	-2	76	0.24%	
# Dont know	-1	17	0.05%	
# Yes	1	2380	7.44%	
# No	2	24564	76.75%

usoc_long <- usoc_long %>%
  mutate(aidhh=ifelse(hhsize==1,2,aidhh)) %>% 
  mutate(.,is_carer_inhh=case_when(aidhh<0 ~ "NA",
                              aidhh==1 ~ "Yes",
                              aidhh==2 ~ "No",
                              TRUE ~ "NA")) %>%
  mutate(.,is_carer_exhh=case_when(aidxhh<0 ~ "NA",
                                   aidxhh==1 ~ "Yes",
                                   aidxhh==2 ~ "No",
                                   TRUE ~ "NA")) %>%
  mutate(.,is_carer_anywhere=case_when(is_carer_inhh=="Yes"|is_carer_exhh=="Yes"~ "Yes",
                                       is_carer_inhh=="No"&is_carer_exhh=="No"~ "No",
                                       TRUE ~ "NA"))

#Hours

# Value label	Value	Absolute frequency	Relative frequency	
# Inapplicable	-8	26436	82.6%	
# Proxy	-7	464	1.45%	
# Refused	-2	11	0.03%	
# Dont know	-1	22	0.07%	
# 0 - 4 hours per week	1	1945	6.08%	
# 5 - 9 hours per week	2	944	2.95%	
# 10 - 19 hours per week	3	607	1.9%	
# 20 - 34 hours per week	4	328	1.02%	
# 35 - 49 hours per week	5	319	1.0%	
# 50 - 99 hours per week	6	154	0.48%	
# 100 or more hours per week/continuous care	7	386	1.21%	
# Varies under 20 hours	8	144	0.45%	
# Varies 20 hours or more	9	162	0.51%	
# Other	97	84	0.26%	
# Total	32006	100.0%

usoc_long <- usoc_long %>%
  mutate(.,hours_cared=ifelse(as.numeric(aidhrs)<0|as.numeric(aidhrs)>90,NA,as.numeric(aidhrs))) %>%
  mutate(.,hours_cared=factor(x=as.character(hours_cared),
                              labels=c("0 - 4 hours per week","5 - 9 hours per week","10 - 19 hours per week",
                                       "20 - 34 hours per week","35 - 49 hours per week","50 - 99 hours per week",
                                       "100 or more hours per week/continuous care","Varies under 20 hours","Varies 20 hours or more"),
                              levels = as.character(1:9))) %>%
  mutate(.,hours_cared=fct_relevel(hours_cared, c("0 - 4 hours per week","5 - 9 hours per week","10 - 19 hours per week",
                                                  "20 - 34 hours per week","35 - 49 hours per week","50 - 99 hours per week",
                                                  "100 or more hours per week/continuous care","Varies under 20 hours","Varies 20 hours or more")))

# #Children in the household
# 
# usoc_long <- usoc_long %>%
#   mutate(nchilddv=as.numeric(nchilddv)) %>%
#   mutate(nchilddv_bin=ifelse(nchilddv<0,NA,ifelse(nchilddv>0,1,0)))

#Missing work because of caring

# Inapplicable	-8	29917	93.47%	
# Proxy	-7	464	1.45%	
# Refused	-2	24	0.07%	
# Dont know	-1	25	0.08%	
# Unable to work at all	1	353	1.1%	
# Unable to do as much paid work as you might	2	280	0.87%	
# Or this doesn't prevent you from working?	3	943	2.95%

usoc_long <- usoc_long %>%
  mutate(aideft=as.numeric(aideft)) %>%
  mutate(aideft=ifelse(aideft<0,NA,aideft)) %>%
  mutate(aideft=factor(x=as.character(aideft),
                      labels=c("Unable to work at all","Unable to do as much paid work as you might","Or this doesn't prevent you from working?"),
                      levels = as.character(1:3)))

#Managing financially

# Missing	-9	1	0.0%	
# Proxy	-7	464	1.45%	
# Refused	-2	126	0.39%	
# Dont know	-1	72	0.22%	
# Living comfortably	1	9624	30.07%	
# Doing alright	2	13140	41.05%	
# Just about getting by	3	6296	19.67%	
# Finding it quite difficult	4	1644	5.14%	
# Finding it very difficult	5	639	2.0%	

usoc_long <- usoc_long %>%
  mutate(finnow=as.numeric(finnow)) %>%
  mutate(finnow=ifelse(finnow<0,NA,finnow)) %>%
  mutate(finnow=factor(x=as.character(finnow),
                       labels=c("Living comfortably",
                                "Doing alright",
                                "Just about getting by",
                                "Finding it quite difficult",
                                "Finding it very difficult"),
                       levels = as.character(1:5))) %>%
  mutate(finnow_bin=case_when(finnow %in% c("Finding it quite difficult",
                                             "Finding it very difficult") ~ "not managing",
                          finnow %in% c("Living comfortably",
                                        "Doing alright",
                                        "Just about getting by") ~ "managing",
                          TRUE ~ "NA"))

#Gross and equivalised household income

usoc_long <- usoc_long %>%
  mutate(fihhmngrsdv=as.numeric(fihhmngrsdv),
         hhsize=as.numeric(hhsize)) %>%
  mutate(annual_household_income=12*fihhmngrsdv) %>%
  mutate(equ_annual_household_income=annual_household_income/sqrt(hhsize)) %>%
  mutate(annual_household_income_pp=annual_household_income/hhsize)

usoc_long <- usoc_long %>%
  mutate(equ_annual_income_dec=case_when(equ_annual_household_income>=0&equ_annual_household_income<=12042 ~ "1 (10% poorest)",
                                         equ_annual_household_income>12042&equ_annual_household_income<=16099.02 ~ "2",
                                         equ_annual_household_income>16099.02&equ_annual_household_income<=19521.83 ~ "3",
                                         equ_annual_household_income>19521.83&equ_annual_household_income<=23035.33 ~ "4",
                                         equ_annual_household_income>23035.33&equ_annual_household_income<=26919.31 ~ "5",
                                         equ_annual_household_income>26919.31&equ_annual_household_income<=31448.52 ~ "6",
                                         equ_annual_household_income>31448.52&equ_annual_household_income<=36706.52 ~ "7",
                                         equ_annual_household_income>36706.52&equ_annual_household_income<=44249.52 ~ "8",
                                         equ_annual_household_income>44249.52&equ_annual_household_income<=56594.76 ~ "9",
                                         equ_annual_household_income>56594.76 ~ "10 (10% richest)")) %>%
  mutate(equ_annual_income_dec=as.factor(equ_annual_income_dec)) %>% 
  mutate(equ_annual_income_dec=fct_relevel(equ_annual_income_dec,
                                           c("1 (10% poorest)",2:9,"10 (10% richest)")))

#Link to cared-for person
# We could add the relationship if we look for file 'egoalt' and variable 'relationship_dv'
#Recode cared-for flags

usoc_long <- usoc_long %>% 
  mutate_at(vars(starts_with("aidhua")), funs(recode(., `0`=0, `1`=1, .default = NaN))) %>%
  mutate_at(vars(starts_with("aidhua")), as.numeric) %>%
  mutate(found_cared_for=rowSums(across(starts_with("aidhua")))  ) %>%
  mutate(found_cared_for_bin=ifelse(found_cared_for>=1,1,found_cared_for)) %>%
  mutate(found_cared_for_bin=case_when(found_cared_for_bin==0 ~ "Not found",
                                         found_cared_for_bin==1 ~ "Found",
                                         TRUE ~ "NA"))

#Number of people cared for

usoc_long <- usoc_long %>%
  mutate(.,number_cared_for_exhh=ifelse(naidxhh<0,NA,naidxhh)) %>% 
  mutate(.,number_cared_for_all=ifelse(is.na(number_cared_for_exhh),0,number_cared_for_exhh) +
           ifelse(is.na(found_cared_for),0,found_cared_for)) %>%
  mutate(number_cared_for_all=ifelse(is.na(found_cared_for)&is.na(number_cared_for_exhh),NA,number_cared_for_all))

#Relationship outside the household

# 1	Parent/parent-in-law
# 2	Grandparent
# 3	Aunt/uncle
# 4	Other relative
# 5	Friend or neighbour
# 6	Client(s) of voluntary organisation
# 97	Other

usoc_long <- usoc_long %>%
  mutate(.,aidhu1=ifelse(aidhu1<0|(aidhu1>6&aidhu1!=97),NA,aidhu1),
         aidhu2=ifelse(aidhu2<0|(aidhu2>6&aidhu2!=97),NA,aidhu2)) %>%
  mutate(.,aidhu1=as.numeric(aidhu1),aidhu2=as.numeric(aidhu2)) %>%
  mutate(rel_exhh=coalesce(aidhu1,aidhu2)) %>%
  mutate(multiple_rel_exhh=ifelse((!is.na(aidhu1))&(!is.na(aidhu2))&(aidhu1!=aidhu2),1,0)) %>%
  mutate(rel_exhh=ifelse(multiple_rel_exhh==1,"98",rel_exhh)) %>%
  mutate(.,rel_exhh=factor(x=as.character(rel_exhh),
                                 labels=c("Parent/parent-in-law","Grandparent","Aunt/uncle",
                                          "Other relative","Friend or neighbour","Client(s) of voluntary organisation",
                                          "Other","Multiple care relationships"),
                                 levels = c(1:6,97,98))) %>% 
  select(-"multiple_rel_exhh") %>%
  mutate(exhh_care_parent=case_when(is.na(aidhu1)&is.na(aidhu2) ~ "NA",
                                    aidhu1=="1"|aidhu2=="1" ~ "1",
                                    TRUE ~ "0"))

#Carers with a PNO match

carers_with_match <- usoc_long %>%
  filter(.,is_carer_inhh=="Yes"&found_cared_for>0) %>%
  select(.,pidp,wave_num,hidp,found_cared_for) %>%
  mutate(.,found_cared_for=as.character(found_cared_for),
         wave_pidp=paste(wave_num,pidp,sep="-"))
carers_with_single_match <- carers_with_match %>%
  filter(.,found_cared_for==1)

#Mini datasets for slow functions

usoc_long_mini <- usoc_long %>%
  select(wave_num,pidp,hidp,wave_pidp,interviewed,pno,agedv,sexdv,starts_with("aidhua")) %>%
  mutate(across(everything(), as.character)) %>% 
  as.data.table() 

#Matrix of carer relationships
#Get PNOs first & relationship
#If it's cared-for info, flag that

get_pairs <- function(wave_pidp_arg){
  
  # pidp_arg <- "68056451"
  # wave_num_arg <- "11"
  
  aux_hidp <- usoc_long_mini %>%
    filter(.,wave_pidp==wave_pidp_arg) %>%
    pull("hidp")
  
  aux_pno <- usoc_long_mini %>%
    filter(.,wave_pidp==wave_pidp_arg) %>%
    pull("pno")
  
  aux_pnos <- usoc_long_mini %>%
    filter(.,wave_pidp==wave_pidp_arg) %>%
    select(.,starts_with("aidhua")) %>%
    unlist()
  
  aux_pnos_t <- aux_pnos %>% t() %>%
    as.data.frame() %>% t() %>%
    as.data.frame() %>%
    filter(.,V1=="1") %>%
    rownames() %>%
    str_replace_all(.,"aidhua","")
  
  rel.skeleton <- data.table(wave_num=rep(word(wave_pidp_arg,start=1,sep="-"),length(aux_pnos_t)),
                             pidp=rep(word(wave_pidp_arg,start=2,sep="-"),length(aux_pnos_t)),
                             hidp=rep(aux_hidp,length(aux_pnos_t)),
                             pno=rep(aux_pno,length(aux_pnos_t)),
                             caredfor_pno=aux_pnos_t) %>%
    mutate(across(everything(), as.character))
  
  if(nrow(rel.skeleton)==0) stop('pair not found')
  
  return(rel.skeleton)
  
}

get_pairs_result <- pbmclapply(carers_with_match$wave_pidp, get_pairs,mc.cores = (core.num))
get_pairs_result <- get_pairs_result %>%
  rbindlist()

#Add relationships

get_pairs_result <- get_pairs_result %>%
  mutate(rel_key=paste(wave_num,pidp,pno,caredfor_pno,sep="-"))

relationships_result <- egoalt_long %>%
  mutate(rel_key=paste(wave_num,apidp,apno,pno,sep="-")) %>% 
  filter(.,rel_key %in% get_pairs_result$rel_key) %>%
  select(wave_num,apidp,pno,relationshipdv) %>%
  rename(pidp=apidp,caredfor_pno=pno) %>%
  mutate(across(everything(), as.character))

get_pairs_result <- get_pairs_result %>%
  left_join(.,relationships_result,by=c("wave_num","pidp","caredfor_pno")) %>%
  select(-"rel_key") %>%
  mutate(parent=ifelse(relationshipdv %in% c("natural parent","step-parent","parent-in-law","adoptive parent"),1,0),
         partner=ifelse(relationshipdv %in% c("partner/cohabitee","husband/wife","civil partner"),1,0),
         child=ifelse(relationshipdv %in% c("natural son/daughter","foster child","stepson/stepdaughter","son-in-law/daughter-in-law","adopted son/daughter"),1,0))

#Get pidp of cared-for and other info

get_pairs_result <- get_pairs_result %>%
  mutate(rel_key_bis=paste(wave_num,hidp,caredfor_pno,sep="-"))

caredfor_vars <- usoc_long_mini %>%
  mutate(rel_key_bis=paste(wave_num,hidp,pno,sep="-")) %>%
  filter(rel_key_bis %in% get_pairs_result$rel_key_bis) %>%
  select(-starts_with("aidhua")) %>%
  rename_with( ~ paste0("caredfor_", .x)) %>%
  rename(wave_num=caredfor_wave_num,
         hidp=caredfor_hidp,
         rel_key_bis=caredfor_rel_key_bis) %>%
  select(-c("rel_key_bis"))

get_pairs_result <- get_pairs_result %>%
  left_join(.,caredfor_vars,by=c("wave_num","hidp","caredfor_pno")) %>%
  select(-"rel_key_bis")

#Collapse back to the person/carer level
#Merge all cared--for variables back into dataset

pairs_person_level <- get_pairs_result %>%
  mutate(caredfor_sexdvnum=ifelse(caredfor_sexdv=="Female",0,ifelse(caredfor_sexdv=="Male",1,NA))) %>%
  group_by(pidp,wave_num) %>%
  summarise(.,caredfor_inhh_first=first(relationshipdv),
            caredfor_inhh_number=n_distinct(caredfor_pno),
            caredfor_inhh_unique_rels=n_distinct(relationshipdv),
            cared_for_inhh_parent=max(parent,na.rm=TRUE),
            cared_for_inhh_partner=max(partner,na.rm=TRUE),
            cared_for_inhh_child=max(child,na.rm=TRUE),
            caredfor_inhh_agemax=max(caredfor_agedv,na.rm=TRUE),
            caredfor_inhh_agemin=min(caredfor_agedv,na.rm=TRUE),
            caredfor_inhh_agemean=mean(caredfor_agedv,na.rm=TRUE),
            caredfor_inhh_sexes=n_distinct(caredfor_sexdvnum,na.rm=TRUE),
            caredfor_inhh_sexmax=max(caredfor_sexdvnum,na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(.,caredfor_inhh_sex=case_when(caredfor_inhh_sexes==2 ~ "male and female",
                                  caredfor_inhh_sexes==1&caredfor_inhh_sexmax==1 ~ "male",
                                  caredfor_inhh_sexes==1&caredfor_inhh_sexmax==0 ~ "female",
                                  TRUE ~ "NA"),
         caredfor_inhh_rel=ifelse(caredfor_inhh_unique_rels==1,caredfor_inhh_first,"multiple care relationships")) %>%
  select(-c("caredfor_inhh_sexes","caredfor_inhh_sexmax"))

usoc_long <- usoc_long %>%
  mutate(.,pidp=as.character(pidp),
         wave_num=as.character(wave_num)) %>%
  left_join(.,pairs_person_level,by=c("pidp","wave_num")) %>%
  apply(.,2,as.character) %>%
  as.data.frame()

#Consolidating relationship variable (cared-for), for both inside and outside of household caring

usoc_long <- usoc_long %>%
  mutate(has_ex_hh_rel=ifelse(!is.na(rel_exhh)&rel_exhh!="NA",1,0),
         has_in_hh_rel=ifelse(!is.na(caredfor_inhh_rel)&caredfor_inhh_rel!="NA",1,0)) %>%
  mutate(care_rel_anywhere_raw=paste(caredfor_inhh_rel,rel_exhh,sep=" // ")) %>%
  mutate(care_rel_anywhere_raw=str_replace_all(care_rel_anywhere_raw," // NA","")) %>%
  mutate(care_rel_anywhere_raw=str_replace_all(care_rel_anywhere_raw,"NA //","")) %>%
  mutate(care_rel_anywhere_raw=trimws(care_rel_anywhere_raw,"both")) %>% 
  mutate(care_rel_anywhere=fct_collapse(care_rel_anywhere_raw,
                                    child = 
                                      c( "natural son/daughter"                                              ,
                                         "foster child"                                                      ,
                                         "stepson/stepdaughter"                                              ,
                                         "son-in-law/daughter-in-law"                                        ,
                                         "adopted son/daughter"                                              ),
                                    grandparent=c( "Grandparent"                                                       ,
                                                   "grand-parent"                                                      ,
                                                   "grand-parent // Grandparent"                                       ),
                                    `multiple care relationships`=c( "multiple care relationships"                                       ,
                                                                     "natural brother/sister // Grandparent"                             ,
                                                                     "natural brother/sister // Other relative"                          ,
                                                                     "Multiple care relationships"                                       ,
                                                                     "natural son/daughter // Parent/parent-in-law"                      ,
                                                                     "husband/wife // Multiple care relationships"                       ,
                                                                     "natural parent // Friend or neighbour"                             ,
                                                                     "natural parent // Multiple care relationships"                     ,
                                                                     "husband/wife // Other relative"                                    ,
                                                                     "husband/wife // Parent/parent-in-law"                              ,
                                                                     "natural parent // Grandparent"                                     ,
                                                                     "natural son/daughter // Friend or neighbour"                       ,
                                                                     "natural son/daughter // Multiple care relationships"               ,
                                                                     "natural son/daughter // Other relative"                            ,
                                                                     "natural son/daughter // Grandparent"                               ,
                                                                     "natural son/daughter // Aunt/uncle"                                ,
                                                                     "multiple care relationships // Other relative"                     ,
                                                                     "husband/wife // Other"                                             ,
                                                                     "husband/wife // Aunt/uncle"                                        ,
                                                                     "husband/wife // Friend or neighbour"                               ,
                                                                     "other non-relative // Multiple care relationships"                 ,
                                                                     "multiple care relationships // Multiple care relationships"        ,
                                                                     "grand-child // Parent/parent-in-law"                               ,
                                                                     "partner/cohabitee // Parent/parent-in-law"                         ,
                                                                     "natural parent // Aunt/uncle"                                      ,
                                                                     "natural son/daughter // Other"                                     ,
                                                                     "partner/cohabitee // Friend or neighbour"                          ,
                                                                     "civil partner // Friend or neighbour"                              ,
                                                                     "natural brother/sister // Friend or neighbour"                     ,
                                                                     "partner/cohabitee // Multiple care relationships"                  ,
                                                                     "other relative // Parent/parent-in-law"                            ,
                                                                     "parent-in-law // Friend or neighbour"                              ,
                                                                     "parent-in-law // Multiple care relationships"                      ,
                                                                     "natural parent // Other relative"                                  ,
                                                                     "other non-relative // Friend or neighbour"                         ,
                                                                     "natural brother/sister // Parent/parent-in-law"                    ,
                                                                     "natural son/daughter // Client(s) of voluntary organisation"       ,
                                                                     "partner/cohabitee // Other relative"                               ,
                                                                     "grand-parent // Multiple care relationships"                       ,
                                                                     "niece/nephew // Parent/parent-in-law"                              ,
                                                                     "partner/cohabitee // Grandparent"                                  ,
                                                                     "partner/cohabitee // Other"                                        ,
                                                                     "multiple care relationships // Other"                              ,
                                                                     "multiple care relationships // Parent/parent-in-law"               ,
                                                                     "half-brother/sister // Multiple care relationships"                ,
                                                                     "civil partner // Other"                                            ,
                                                                     "other non-relative // Parent/parent-in-law"                        ,
                                                                     "stepson/stepdaughter // Friend or neighbour"                       ,
                                                                     "multiple care relationships // Grandparent"                        ,
                                                                     "natural brother/sister // Multiple care relationships"             ,
                                                                     "partner/cohabitee // Client(s) of voluntary organisation"          ,
                                                                     "multiple care relationships // Client(s) of voluntary organisation",
                                                                     "multiple care relationships // Friend or neighbour"                ,
                                                                     "natural parent // Other"                                           ,
                                                                     "son-in-law/daughter-in-law // Parent/parent-in-law"                ,
                                                                     "husband/wife // Grandparent"                                       ,
                                                                     "stepson/stepdaughter // Multiple care relationships"               ,
                                                                     "grand-parent // Parent/parent-in-law"                              ,
                                                                     "multiple care relationships // Aunt/uncle"                         ,
                                                                     "adoptive parent // Friend or neighbour"                            ,
                                                                     "lodger/boarder/tenant // Friend or neighbour"                      ,
                                                                     "lodger/boarder/tenant // Other"                                    ,
                                                                     "lodger/boarder/tenant // Multiple care relationships"              ,
                                                                     "husband/wife // Client(s) of voluntary organisation"               ,
                                                                     "foster child // Other relative"                                    ,
                                                                     "other relative // Aunt/uncle"                                      ,
                                                                     "other relative // Multiple care relationships"                     ,
                                                                     "adopted son/daughter // Multiple care relationships"               ,
                                                                     "adopted son/daughter // Parent/parent-in-law"                      ,
                                                                     "grand-child // Multiple care relationships"                        ,
                                                                     "natural parent // Client(s) of voluntary organisation"             ,
                                                                     "stepson/stepdaughter // Other"                                     ,
                                                                     "civil partner // Other relative"                                   ,
                                                                     "other non-relative // Grandparent"                                 ,
                                                                     "adopted son/daughter // Other"                                     ,
                                                                     "foster child // Parent/parent-in-law"                              ,
                                                                     "grand-parent // Aunt/uncle"                                        ,
                                                                     "aunt/uncle // Grandparent"                                         ,
                                                                     "grand-parent // Other relative"                                    ,
                                                                     "grand-parent // Friend or neighbour"                               ,
                                                                     "grand-child // Other"                                              ,
                                                                     "stepson/stepdaughter // Parent/parent-in-law"                      ,
                                                                     "other non-relative // Client(s) of voluntary organisation"         ,
                                                                     "other relative // Friend or neighbour"                             ,
                                                                     "grand-child // Grandparent"                                        ,
                                                                     "niece/nephew // Other relative"                                    ,
                                                                     "adoptive parent // Multiple care relationships"                    ,
                                                                     "brother/sister-in-law // Parent/parent-in-law"                     ,
                                                                     "half-brother/sister // Grandparent"                                ,
                                                                     "grand-parent // Other"                                             ,
                                                                     "stepson/stepdaughter // Grandparent"                               ,
                                                                     "landlord/landlady // Parent/parent-in-law"                         ,
                                                                     "natural brother/sister // Aunt/uncle"                              ,
                                                                     "parent-in-law // Other relative"                                   ,
                                                                     "stepson/stepdaughter // Other relative"                            ,
                                                                     "brother/sister-in-law // Friend or neighbour"                      ,
                                                                     "brother/sister-in-law // Multiple care relationships"              ,
                                                                     "lodger/boarder/tenant // Parent/parent-in-law"                     ,
                                                                     "niece/nephew // Multiple care relationships"                       ,
                                                                     "adopted son/daughter // Friend or neighbour"                       ,
                                                                     "grand-child // Friend or neighbour"                                ,
                                                                     "aunt/uncle // Friend or neighbour"                                 ,
                                                                     "civil partner // Parent/parent-in-law"                             ,
                                                                     "brother/sister-in-law // Aunt/uncle"                               ,
                                                                     "grand-child // Aunt/uncle"                                         ,
                                                                     "aunt/uncle // Aunt/uncle"                                          ,
                                                                     "aunt/uncle // Other relative"                                      ,
                                                                     "step-parent // Multiple care relationships"                        ,
                                                                     "step-brother/sister // Aunt/uncle"                                 ,
                                                                     "parent-in-law // Other"                                            ,
                                                                     "natural brother/sister // Other"                                   ,
                                                                     "cousin // Parent/parent-in-law"                                    ,
                                                                     "lodger/boarder/tenant // Other relative"                           ,
                                                                     "natural brother/sister // Client(s) of voluntary organisation"     ,
                                                                     "brother/sister-in-law // Other relative"                           ,
                                                                     "parent-in-law // Aunt/uncle"                                       ,
                                                                     "foster child // Friend or neighbour"                               ,
                                                                     "cousin // Multiple care relationships"                             ,
                                                                     "cousin // Aunt/uncle",
                                                                     "grand-child // Other relative"                                     ,
                                                                     "aunt/uncle // Parent/parent-in-law",
                                                                     "foster child // Multiple care relationships",
                                                                     "foster child // Other"),
                                    other=c( "Other relative"                                                    ,
                                             "grand-child"                                                       ,
                                             "other non-relative"                                                ,
                                             "Friend or neighbour"                                               ,
                                             "Client(s) of voluntary organisation"                               ,
                                             "Other"                                                             ,
                                             "Aunt/uncle"                                                        ,
                                             "cousin"                                                            ,
                                             "other relative"                                                    ,
                                             "other non-relative // Other relative"                              ,
                                             "other non-relative // Other"                                       ,
                                             "niece/nephew"                                                      ,
                                             "lodger/boarder/tenant"                                             ,
                                             "landlord/landlady"                                                 ,
                                             "aunt/uncle"                                                        ,
                                             "other relative // Other relative"                                  ,
                                             "cousin // Other relative"                                          ,
                                             "other relative // Other"                                           ),
                                    parent=c( "Parent/parent-in-law"                                              ,
                                              "natural parent"                                                    ,
                                              "natural parent // Parent/parent-in-law"                            ,
                                              "step-parent"                                                       ,
                                              "parent-in-law // Parent/parent-in-law"                             ,
                                              "parent-in-law"                                                     ,
                                              "adoptive parent"                                                   ,
                                              "adoptive parent // Parent/parent-in-law"                           ,
                                              "step-parent // Parent/parent-in-law"                               ),
                                   partner=c( "partner/cohabitee"                                                 ,
                                              "husband/wife"                                                      ,
                                              "civil partner"                                                     ),
                                   sibling=c( "natural brother/sister"                                            ,
                                              "half-brother/sister"                                               ,
                                              "foster brother/sister"                                             ,
                                              "brother/sister-in-law"                                             ,
                                              "step-brother/sister"                                               ,
                                              "adopted brother/sister"                                            ))) %>%
  mutate(exhh_care_parent=as.numeric(exhh_care_parent),
         cared_for_inhh_parent=as.numeric(cared_for_inhh_parent)) %>% 
  mutate(cares_parent_anywhere=case_when(is_carer_inhh=="Yes"&is_carer_exhh=="Yes"&(cared_for_inhh_parent==1|exhh_care_parent==1) ~ "1",
                                         is_carer_inhh=="Yes"&is_carer_exhh=="Yes"&(cared_for_inhh_parent==0&exhh_care_parent==0) ~ "0",
                                         is_carer_inhh=="Yes"&is_carer_exhh=="No"&(cared_for_inhh_parent==1) ~ "1",
                                         is_carer_inhh=="Yes"&is_carer_exhh=="No"&(cared_for_inhh_parent==0) ~ "0",
                                         is_carer_inhh=="No"&is_carer_exhh=="Yes"&(exhh_care_parent==1) ~ "1",
                                         is_carer_inhh=="No"&is_carer_exhh=="Yes"&(exhh_care_parent==0) ~ "0",
                                         TRUE ~ "NA"))

#Relationship groups

usoc_long <- usoc_long %>%
  mutate(cares_parent_anywhere=as.numeric(cares_parent_anywhere),
         cared_for_inhh_partner=as.numeric(cared_for_inhh_partner),
         cared_for_inhh_child=as.numeric(cared_for_inhh_child)) %>%
  mutate(three_rel_sum=rowSums(across(c("cares_parent_anywhere","cared_for_inhh_partner","cared_for_inhh_child")),na.rm = TRUE)) %>%
  mutate(three_rel_group=case_when(three_rel_sum==1&cares_parent_anywhere==1 ~ "parent",
                                    three_rel_sum==1&cared_for_inhh_child==1 ~ "child",
                                    three_rel_sum==1&cared_for_inhh_partner==1 ~ "partner",
                                    three_rel_sum>1&three_rel_sum<=3 ~ "multiple",
                                    TRUE ~ "NA"))

#Working age group

usoc_long <- usoc_long %>%
  mutate(agedv=as.numeric(agedv)) %>% 
  mutate(over65=ifelse(agedv>=65,1,0),
         retired=ifelse(jbstat=="Retired",1,0)) %>% 
  mutate(over65_or_retired=case_when(over65==0&retired==0 ~ "Under 65 and not retired",
                                     over65==1|retired==1 ~ "Over 65 or retired",
                                     TRUE ~ "NA"))

#Location of caring in long format
usoc_long <- usoc_long %>%
  mutate(caring_location=case_when(is_carer_anywhere!="Yes" ~ "NA",
                                   is_carer_inhh=="Yes"&is_carer_exhh=="Yes" ~ "Both inside and outside household",
                                   is_carer_inhh=="Yes"&(is_carer_exhh!="Yes"|is.na(is_carer_exhh)) ~ "Only inside household",
                                   is_carer_exhh=="Yes"&(is_carer_inhh!="Yes"|is.na(is_carer_inhh)) ~ "Only outside household"))

#New hours of caring variable
usoc_long_carers <- usoc_long_carers %>%
  mutate(hours_cared_small=case_when(hours_cared %in% c("0 - 4 hours per week","5 - 9 hours per week") ~ "0-10 hours per week",
                                     hours_cared %in% c("10 - 19 hours per week") ~ "10-20 hours per week",
                                     hours_cared %in% c("20 - 34 hours per week","35 - 49 hours per week") ~ "20-50 hours per week",
                                     hours_cared %in% c("50 - 99 hours per week","100 or more hours per week/continuous care") ~ "50+ hours per week",
                                     TRUE ~ "NA"),
         hours_cared_even_smaller=case_when(is_carer_anywhere=="Yes"&(hours_cared %in% c("0 - 4 hours per week",
                                                                                         "5 - 9 hours per week",
                                                                                         "10 - 19 hours per week",
                                                                                         "Varies under 20 hours")) ~ "0-20 hours per week",
                                            is_carer_anywhere=="Yes"&(hours_cared %in% c("Varies 20 hours or more",
                                                                                         "20 - 34 hours per week",
                                                                                         "35 - 49 hours per week",
                                                                                         "50 - 99 hours per week",
                                                                                         "100 or more hours per week/continuous care")) ~ "20+ hours per week",
                                            is_carer_anywhere=="No" ~ "Not a carer",
                                            TRUE ~ "NA"))

#######################
###### Save data ######
#######################

s3write_using(usoc_long # What R object we are saving
              , FUN = fwrite # Which R function we are using to save
              , object = paste0("/Understanding Society/","USOC long carers analysis.csv") # Name of the file to save to (include file type)
              , bucket = IHT_bucket) # Bucket name defined above