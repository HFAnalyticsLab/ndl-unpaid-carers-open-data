#######################
###### Libraries ######
#######################

library(readxl)
library(writexl)
library(tidyverse)
library(stringr)
library(tidyr)
library(data.table)
library(janitor)
library(aws.s3)
library(pbapply)
library(snomedizer)

##########################
###### Read in data ######
##########################

#Clean up the global environment

rm(list = ls())

#Directories in S3

IHT_bucket <- "s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp"
R_workbench <- path.expand("~")
localgit <- dirname(rstudioapi::getSourceEditorContext()$path)

####### Read in data

codelists <- read_excel(paste0(R_workbench,"/usoc-carers-ndl/Code lists/","Summary of code lists.xlsx"), sheet = 1)

####### Cleaning

#Remove duplicates

codelists <- codelists %>%
  group_by(Type,Source,Code) %>%
  summarise(.,Name=first(Name))

#Subsets

codelists_snomed <- codelists %>%
  filter(.,Type=="SNOMED")

codelists_read <- codelists %>%
  filter(.,Type=="READ")

#Inclusion matrices

cleanbin <- function(x){
  return(as.numeric(!is.na(x)))
}

#SNOMED

inclusion_snomed <- codelists_snomed %>%
  mutate(.,included=1) %>%
  pivot_wider(
    names_from = Source,
    names_sep = ".",
    values_from = c(included,Name)
  ) %>%
  mutate_at(vars(c("included.Liverpool & Wirral","included.NW London","included.PRIMIS/Nottingham","included.Nuffield","included.HF gap analysis")), cleanbin) %>%
  mutate(.,sumincl=`included.Liverpool & Wirral`+`included.NW London`+`included.PRIMIS/Nottingham`+`included.Nuffield`) %>%
  arrange(.,desc(sumincl))

sno_output_list <- pbmapply(snomedizer::concept_find,conceptIds =inclusion_snomed$Code) %>%
  rbindlist(.) %>%
  select(conceptId,fsn.term) %>%
  rename(.,name.search=fsn.term)

inclusion_snomed <- left_join(inclusion_snomed,sno_output_list,by=c("Code"="conceptId"))

#READ

inclusion_read <- codelists_read %>%
  mutate(.,included=1) %>%
  pivot_wider(
    names_from = Source,
    names_sep = ".",
    values_from = c(included,Name)
  ) %>%
  mutate_at(vars(c("included.Liverpool & Wirral","included.Leeds","included.Wales","included.SCIMP","included.Nuffield","included.HF gap analysis")), cleanbin) %>%
  mutate(.,sumincl=`included.Liverpool & Wirral`+`included.Leeds`+`included.Wales`+`included.SCIMP`+`included.Nuffield`) %>%
  mutate(.,Code_nopunct=str_replace_all(Code, "[^[:alnum:]]", "")) %>%
  arrange(Code_nopunct)

#Search for terms using
#file:///C:/Users/SebastienP/Downloads/Simpler_handling_of_clinical_concepts_in_R_with_cl.pdf

####### Give single name to each code

renaming_read <- data.frame(
  Code=c("918A.",  "918c.",  "918d.",  "918G.",  "918H.",  "918m.",  "918W.",  "918X.",  "918Y.",  "918a.",  "918b.",  "918y.",  "69DC.",  "69DE.", 
"8HkA.",  "8IEP.",  "8IHE.",  "8O7..",  "9180.",  "918A0",  "918A1",  "918A2",  "918t.",  "9d46.",  "9NSS.",  "13HH.",  "13VN.",  "13Wb.", 
"388Q.",  "8BAr.",  "918J.",  "918K.",  "918L.",  "918M.",  "9Ngv.",  "9Ngw.",  "13VP0.", "8HkB.",  "8IAk.",
"^ESCTCA588741","13HH.15","13HH.13","13HH.16","13HH.12","13HH.14","13HH.00","9SW","13HH.18","^ESCTCA808886","13HH.11","91800"),
name_manual=c("Carer",                                                   "Carer of a person with chronic disease",                 
"Carer of a person with mental health problem",            "Is a carer",                                             
"Primary carer",                                           "Carer of a person with a terminal illness",              
"Carer of a person with learning disability",              "Carer of a person with physical disability",             
"Carer of a person with sensory impairment",               "Carer of a person with substance misuse",                
"Carer of a person with alcohol misuse",                   "Carer of person with dementia",                          
"Carer annual health check",                               "Carer health check",                                     
"Referral for general practice carer's assessment",        "Carer annual health check declined",                     
"Carer health check declined",                             "Carer support",                                          
"Carer’s details",                                         "Cares for a friend",                                     
"Cares for a neighbour",                                   "Cares for a relative",                                   
"Carer from Black and minority ethnic group",              "Carer",                                                  
"Carer health check offered",                              "Looks after elderly dependent",                          
"Carer able to cope",                                      "Carer has sole parental responsibility",                 
"Carer strain index score",                                "Carer health check completed",                           
"Carer – home telephone number",                           "Carer – work telephone number",                          
"Carer - mobile telephone number",                         "Carer – email address",                                  
"Carer understands care plan",                             "Carer does not understand care plan",                    
"Receiving carer allowance",                               "Referral for social services carer's assessment",        
"Referral to Princess Royal Trust carers centre declined",
"Carer stress syndrome",
"Looks after chronically sick spouse",
"Looks after chronically sick husband",
"Looks after chronically sick wife",
"Looks after chronically sick father",
"Looks after chronically sick mother",
"Looks after chronically sick relative",
"Assessment of needs offered to carer",
"Looks after physically handicapped dependent",
"Cares for dependent relative at home",
"Cares for mentally handicapped dependent",
"Details of informal carer"))

inclusion_read <- left_join(inclusion_read,renaming_read,by="Code")
rm(renaming_read)
 
# inclusion_read %>%
#   filter(.,is.na(name_manual))

renaming_snomed <- data.frame(
  Code=c("224484003",        "248611000000108",  "407543004",        "413760003",        "413761004",        "413762006",      
         "413763001",        "413764007",        "276047008",        "224485002",        "224486001",        "302767002",       
         "413765008",        "824401000000105",  "224487005",        "413759008",        "728721000000100",  "794111000000107", 
         "962641000000100",  "962701000000105",  "133932002",        "151921000119105",  "266946000",        "276040005",       
         "276041009",        "276042002",        "276043007",        "276044001",        "276045000",        "276046004",       
         "276048003",        "288231000119101",  "407542009",        "135893005",        "1746001000006100", "1786931000006100",
         "70862002",         "710311000000103",  "718312008",        "754731000000108",  "790081000000109",  "790101000000103", 
         "837271000000107",  "962621000000107",  "184140000",        "300979000",        "512321000000109",
         "700225008",
         "700224007",
         "702470005",
         "425578005",
         "400989000",
         "408576002",
         "408402003",
         "408400006",
         "408401005"),
  fsn.term=c("Patient themselves providing care (finding)",                "Carer of a person with a terminal illness",                 
             "Primary caregiver (person)",                                 "Caregiver of a person with chronic disease (finding)",      
             "Caregiver of a person with learning disability (finding)",   "Caregiver of a person with mental health problem (finding)",
             "Caregiver of a person with physical disability (finding)",   "Caregiver of a person with sensory impairment (finding)",   
             "Looks after elderly dependent (finding)",                    "Cares for a friend (finding)",                              
             "Cares for a neighbor (finding)",                             "Cares for a relative (finding)",                            
             "Caregiver of a person with substance misuse (finding)",      "Carer of person with dementia",                             
             "Details of informal caregiver (observable entity)",          "Caregiver of person with alcohol misuse (finding)",         
             "Carer from Black and minority ethnic group",                 "Receiving carer allowance",                                 
             "Carer health check offered",                                 "Carer health check",                                        
             "Caregiver (person)",                                         "Cares for sick or handicapped family member (finding)",     
             "Looks after chronically sick relative (finding)",            "Looks after someone (finding)",                             
             "Cares for mentally handicapped dependent",                   "Looks after chronically sick father (finding)",             
             "Looks after chronically sick husband (finding)",             "Looks after chronically sick mother (finding)",             
             "Looks after chronically sick spouse (finding)",              "Looks after chronically sick wife (finding)",               
             "Looks after physically handicapped dependent (finding)",     "Cares for dependent relative at home (finding)",            
             "Informal caregiver (person)",                                "Carer support",                                             
             "Carer annual health check",                                  "Carer annual health check declined",                        
             "Contact person (person)",                                    "Referral to Princess Royal Trust carers centre declined",   
             "Caregiver health check completed (situation)",               "Carer annual health check",                                 
             "Referral for general practice carer's assessment",           "Referral for social services carer's assessment",           
             "Carer annual health check declined",                         "Carer health check declined",                               
             "Caregiver details (observable entity)",                      "Caregiver stress syndrome (disorder)",                      
             "Assessment of needs offered to carer",
             "Caregiver understands care plan (finding)",
             "Caregiver does not understand care plan (finding)",
             "Caregiver has sole parental responsibility (finding)",
             "Caregiver able to cope (finding)",
             "Carer Strain Index Score (observable entity)",
             "Caregiver email address (observable entity)",
             "Caregiver mobile telephone number (observable entity)",
             "Caregiver home telephone number (observable entity)",
             "Caregiver work telephone number (observable entity)"))

inclusion_snomed <- left_join(inclusion_snomed,renaming_snomed,by="Code")
rm(renaming_snomed)
 
# inclusion_snomed %>%
#   filter(.,is.na(fsn.term))

#Add categories: READ

inclusion_read <- inclusion_read %>%
  mutate(.,category=case_when(name_manual %in% c("Carer","Primary carer","Is a carer") ~ "A. Is a carer",
                              name_manual %in% c("Carer of a person with chronic disease",
                                                 "Carer of a person with mental health problem",
                                                 "Carer of a person with a terminal illness",
                                                 "Carer of a person with learning disability",
                                                 "Carer of a person with physical disability",
                                                 "Carer of a person with sensory impairment",
                                                 "Carer of a person with substance misuse",
                                                 "Carer of a person with alcohol misuse",
                                                 "Carer of person with dementia",
                                                 "Looks after physically handicapped dependent",
                                                 "Cares for mentally handicapped dependent") ~ "B. Cared-for characteristics (medical)",
                              name_manual %in% c("Cares for a friend",
                                                 "Cares for a neighbour",
                                                 "Cares for a relative",
                                                 "Looks after chronically sick spouse",
                                                 "Looks after chronically sick husband",
                                                 "Looks after chronically sick wife",
                                                 "Looks after chronically sick father",
                                                 "Looks after chronically sick mother",
                                                 "Looks after chronically sick relative",
                                                 "Cares for dependent relative at home",
                                                 "Looks after elderly dependent") ~ "C. Cared-for characteristics (relationship)",
                              name_manual %in% c("Assessment of needs offered to carer",
                                                 "Carer health check",
                                                 "Carer annual health check",
                                                 "Carer health check completed",
                                                 "Referral for general practice carer's assessment",
                                                 "Carer health check declined",
                                                 "Carer annual health check declined",
                                                 "Carer support",
                                                 "Carer health check offered",
                                                 "Receiving carer allowance",
                                                 "Referral for social services carer's assessment",
                                                 "Referral to Princess Royal Trust carers centre declined","") ~ "D. Carer support",
                              name_manual %in% c("Details of informal carer",
                                                 "Carer – email address",
                                                 "Carer - mobile telephone number",
                                                 "Carer – work telephone number",
                                                 "Carer – home telephone number",
                                                 "Carer’s details") ~ "E. Carer contact details",
                              name_manual %in% c("Carer stress syndrome",
                                                 "Carer able to cope",
                                                 "Carer strain index score") ~ "F. Carer wellbeing",
                              name_manual %in% c("Carer from Black and minority ethnic group") ~ "G. Carer demographics",
                              name_manual %in% c("Carer has sole parental responsibility") ~ "H. Parental responsbility",
                              name_manual %in% c("Carer understands care plan",
                                                 "Carer does not understand care plan") ~ "I. Care plan",
                              TRUE ~ "NA")) %>%
  select(.,Type,Code,starts_with("included"),name_manual,category)
  
# inclusion_read %>%
#   pull(category) %>%
#   unique(.)

#Add categories: SNOMED

inclusion_snomed <- inclusion_snomed %>%
  mutate(.,category=case_when(fsn.term %in% c("Patient themselves providing care (finding)",
                                                 "Primary caregiver (person)",
                                                 "Caregiver (person)",
                                                 "Informal caregiver (person)",
                                                 "Contact person (person)",
                                                 "Looks after someone (finding)") ~ "A. Is a carer",
                              fsn.term %in% c("Carer of a person with a terminal illness",
                                                 "Caregiver of a person with chronic disease (finding)",
                                                 "Caregiver of a person with learning disability (finding)",
                                                 "Caregiver of a person with mental health problem (finding)",
                                                 "Caregiver of a person with physical disability (finding)",
                                                 "Caregiver of a person with sensory impairment (finding)",
                                                 "Caregiver of a person with substance misuse (finding)",
                                                 "Carer of person with dementia",
                                                 "Caregiver of person with alcohol misuse (finding)",
                                                 "Cares for mentally handicapped dependent",
                                                 "Looks after physically handicapped dependent (finding)") ~ "B. Cared-for characteristics (medical)",
                              fsn.term %in% c("Looks after elderly dependent (finding)",
                                                 "Cares for a friend (finding)",
                                                 "Cares for a neighbor (finding)",
                                                 "Cares for a relative (finding)",
                                                 "Cares for sick or handicapped family member (finding)",
                                                 "Looks after chronically sick relative (finding)",
                                                 "Looks after chronically sick father (finding)",
                                                 "Looks after chronically sick husband (finding)",
                                                 "Looks after chronically sick mother (finding)",
                                                 "Looks after chronically sick spouse (finding)",
                                                 "Looks after chronically sick wife (finding)",
                                                 "Cares for dependent relative at home (finding)") ~ "C. Cared-for characteristics (relationship)",
                              fsn.term %in% c("Assessment of needs offered to carer",
                                                 "Carer health check declined",
                                                 "Referral for social services carer's assessment",
                                                 "Caregiver health check completed (situation)",
                                                 "Referral for general practice carer's assessment",
                                                 "Referral to Princess Royal Trust carers centre declined","",
                                                 "Carer annual health check declined","Carer annual health check",
                                                 "Carer support","Carer health check offered","Carer health check",
                                                 "Receiving carer allowance") ~ "D. Carer support",
                              fsn.term %in% c("Caregiver details (observable entity)",
                                              "Details of informal caregiver (observable entity)",
                                              "Caregiver email address (observable entity)",
                                              "Caregiver mobile telephone number (observable entity)",
                                              "Caregiver home telephone number (observable entity)",
                                              "Caregiver work telephone number (observable entity)") ~ "E. Carer contact details",
                              fsn.term %in% c("Caregiver able to cope (finding)",
                                              "Carer Strain Index Score (observable entity)",
                                              "Caregiver stress syndrome (disorder)") ~ "F. Carer wellbeing",
                              fsn.term %in% c("Carer from Black and minority ethnic group") ~ "G. Carer demographics",
                              fsn.term %in% c("Caregiver has sole parental responsibility (finding)") ~ "H. Parental responsbility",
                              fsn.term %in% c("Caregiver understands care plan (finding)",
                                                 "Caregiver does not understand care plan (finding)") ~ "I. Care plan",
                              TRUE ~ "NA")) %>%
  select(.,Type,Code,starts_with("included"),fsn.term,category) %>%
  rename(.,name_manual=fsn.term)
 
# inclusion_snomed %>%
#   pull(category) %>%
#   unique(.)

# Append both types of codes

inclusion_read_snomed <- plyr::rbind.fill(inclusion_read,inclusion_snomed) %>%
  arrange(.,category,Type,name_manual) %>%
  select(.,category,Type,Code,name_manual,"included.Leeds","included.Liverpool & Wirral","included.Wales","included.NW London") 

# Create pairs

grouping_key <- data.frame(Code=c("13HH.11",
                                  "276041009",
                                  "9Ngw.",
                                  "700224007",
                                  "9Ngv.",
                                  "700225008",
                                  "918m.",
                                  "248611000000108",
                                  "918b.",
                                  "413759008",
                                  "918c.",
                                  "413760003",
                                  "918W.",
                                  "413761004",
                                  "918d.",
                                  "413762006",
                                  "918X.",
                                  "413763001",
                                  "918Y.",
                                  "413764007",
                                  "918a.",
                                  "413765008",
                                  "918y.",
                                  "824401000000105",
                                  "13HH.",
                                  "276047008",
                                  "276048003",
                                  "13HH.18",
                                  "918A0",
                                  "224485002",
                                  "918A1",
                                  "224486001",
                                  "918A2",
                                  "302767002",
                                  "^ESCTCA808886",
                                  "288231000119101",
                                  "13HH.12",
                                  "276042002",
                                  "13HH.13",
                                  "276043007",
                                  "13HH.14",
                                  "276044001",
                                  "13HH.00",
                                  "266946000",
                                  "13HH.15",
                                  "276045000",
                                  "13HH.16",
                                  "276046004",
                                  "918L.",
                                  "408402003",
                                  "918M.",
                                  "408576002",
                                  "918J.",
                                  "408400006",
                                  "918K.",
                                  "408401005",
                                  "9180.",
                                  "184140000",
                                  "918t.",
                                  "728721000000100",
                                  "9SW",
                                  "512321000000109",
                                  "69DC.",
                                  "754731000000108",
                                  "8IEP.",
                                  "1786931000006100",
                                  "69DE.",
                                  "962701000000105",
                                  "8BAr.",
                                  "718312008",
                                  "8IHE.",
                                  "837271000000107",
                                  "9NSS.",
                                  "962641000000100",
                                  "8O7..",
                                  "135893005",
                                  "13VP0.",
                                  "794111000000107",
                                  "8HkA.",
                                  "790081000000109",
                                  "8HkB.",
                                  "790101000000103",
                                  "8IAk.",
                                  "710311000000103",
                                  "13VN.",
                                  "425578005",
                                  "388Q.",
                                  "400989000",
                                  "^ESCTCA588741",
                                  "300979000",
                                  "918A.",
                                  "224484003",
                                  "9d46.",
                                  "70862002",
                                  "918H.",
                                  "407543004",
                                  "13Wb.",
                                  "702470005",
                                  "224487005",
                                  "91800",
                                  "151921000119105",
                                  "962621000000107",
                                  "918G.",
                                  "133932002",
                                  "407542009",
                                  "276040005",
                                  "754731000000108"),
                           Group=c("0",
                                   "0",
                                   "1",
                                   "1",
                                   "2",
                                   "2",
                                   "3",
                                   "3",
                                   "4",
                                   "4",
                                   "5",
                                   "5",
                                   "6",
                                   "6",
                                   "7",
                                   "7",
                                   "8",
                                   "8",
                                   "9",
                                   "9",
                                   "10",
                                   "10",
                                   "11",
                                   "11",
                                   "12",
                                   "12",
                                   "13",
                                   "13",
                                   "14",
                                   "14",
                                   "15",
                                   "15",
                                   "16",
                                   "16",
                                   "17",
                                   "17",
                                   "18",
                                   "18",
                                   "19",
                                   "19",
                                   "20",
                                   "20",
                                   "21",
                                   "21",
                                   "22",
                                   "22",
                                   "23",
                                   "23",
                                   "24",
                                   "24",
                                   "25",
                                   "25",
                                   "26",
                                   "26",
                                   "27",
                                   "27",
                                   "28",
                                   "28",
                                   "101",
                                   "101",
                                   "29",
                                   "29",
                                   "30",
                                   "30",
                                   "31",
                                   "31",
                                   "32",
                                   "32",
                                   "33",
                                   "33",
                                   "34",
                                   "34",
                                   "35",
                                   "35",
                                   "36",
                                   "36",
                                   "37",
                                   "37",
                                   "38",
                                   "38",
                                   "39",
                                   "39",
                                   "40",
                                   "40",
                                   "41",
                                   "41",
                                   "42",
                                   "42",
                                   "43",
                                   "43",
                                   "44",
                                   "44",
                                   "45",
                                   "45",
                                   "46",
                                   "46",
                                   "47",
                                   "47",
                                   "100",
                                   "100",
                                   "B",
                                   "E",
                                   "F",
                                   "G",
                                   "H",
                                   "I",
                                   "K"))

#Merge pairs

inclusion_read_snomed <- left_join(inclusion_read_snomed,grouping_key,by="Code")
rm(grouping_key)

#Reshape

inclusion_matrix_wide <- inclusion_read_snomed %>%
  select(.,category,Group,Type,Code,name_manual,starts_with("included")) %>%
  pivot_wider(names_from = Type,
              names_sep = ".",
              values_from = c(Code, name_manual,"included.Leeds","included.Liverpool & Wirral","included.Wales","included.NW London")) %>%
  select(.,-c("included.Leeds.SNOMED","included.Wales.SNOMED","included.NW London.READ")) %>%
  mutate(.,'Exact match'=ifelse(!is.na(Code.READ)&!is.na(Code.SNOMED),"Yes","No")) %>% 
  select(.,-"Group") %>%
  select(.,category,"Exact match",starts_with("Code"),starts_with("name_manual"),starts_with("included")) %>% 
  arrange(.,category,desc(`Exact match`))

####### Save data

write_xlsx(inclusion_matrix_wide, paste0(R_workbench,"/usoc-carers-ndl/Code lists/","inclusion_matrix_wide.xlsx"))

####### Final check on names / READ: OK / SNOMED: OK
# 
# aurum <- read_excel(paste0(R_workbench,"/usoc-carers-ndl/Code lists/","Aurum-medical.xlsx"), sheet = 1)
# aurum_read <- select(aurum,OriginalReadCode,Term)
# aurum_snomed <- select(aurum,SnomedCTConceptId,Term) %>%
#   mutate(.,SnomedCTConceptId=as.character(SnomedCTConceptId)) %>%
#   group_by(SnomedCTConceptId) %>%
#   summarise(Term=first(Term)) %>% 
#   ungroup()
# 
# living <- read_excel(paste0(R_workbench,"/usoc-carers-ndl/Code lists/","living-arrangements.xlsx"), sheet = 1)
# living <- living %>%
#   filter(stringr::str_detect(Readcode, '13HH')) %>%
#   rename(.,OriginalReadCode=Readcode,Term=Readterm)
# 
# lshtm <- read.delim(paste0(R_workbench,"/usoc-carers-ndl/Code lists/","Clinical_codelist_lshtm.txt"),
#            header = TRUE, sep = ",", dec = ".") %>%
#   select(readcode,readterm) %>%
#   filter(stringr::str_detect(readcode, '13HH')) %>% 
#   rename(.,OriginalReadCode=readcode,Term=readterm)
# 
# aurum_read <- plyr::rbind.fill(aurum_read,living,lshtm) %>%
#   mutate(.,OriginalReadCode=str_replace_all(OriginalReadCode, "[^[:alnum:]]", "")) %>%
#   group_by(OriginalReadCode) %>%
#   summarise(Term=first(Term)) %>% 
#   ungroup()
# 
# read_matches <- inclusion_matrix_wide %>%
#   mutate(.,Code_nopunct=str_replace_all(Code.READ, "[^[:alnum:]]", "")) %>%
#   filter(!is.na(Code_nopunct)) %>% 
#   left_join(.,aurum_read,by=c("Code_nopunct"="OriginalReadCode")) %>%
#   select(.,Code_nopunct,name_manual.READ,Term)
# 
# snomed_matches <- inclusion_matrix_wide %>%
#   left_join(.,aurum_snomed,by=c("Code.SNOMED"="SnomedCTConceptId")) %>%
#   filter(!is.na(Code.SNOMED)) %>% 
#   select(.,Code.SNOMED,name_manual.SNOMED,Term)

# inclusion_snomed_manual %>%
#   janitor::get_dupes(fsn.term) %>%
#   select(.,Code,starts_with("included"),fsn.term)
# 
# inclusion_snomed_manual %>%
#   filter(Code=="70862002") %>% 
#   pull(fsn.term)
