#Auxiliary functions

firstclean <- function(x){
  xnum <- as.numeric(x)
  x_bin <- ifelse(xnum<0,NA,xnum)
  return(x_bin)
}

ismissing <- function(x){
  ismiss <- ifelse(is.na(x)|x<0,1,0)
  return(ismiss)
}

adlclean <- function(x){
  adlvar <- ifelse(x>=2,1,ifelse(x==1,0,NA))
  return(adlvar)
}

binarisefun <- function(x){
  xnum <- as.numeric(x)
  x_bin <- ifelse(xnum!=1|is.na(xnum),0,1)
  return(x_bin)
}

yesnofun <- function(x){
  yesnovar <- ifelse(x==1,"Yes",ifelse(x==0,"No",NA))
  return(yesnovar)
}

carryforwardfun <- function(x){
  xnum <- as.numeric(x)
  xcf <- DescTools::LOCF(xnum)
  return(xcf)
}

#Medical conditions

#Variable names
hcond_without_still <- paste0("hcond",1:22)[c(19,22)]
hcond_with_still <- paste0("hcond",1:22)[-c(18,19,20,22)]
hconds_vars <- paste0("hconds",sprintf("%02d", 1:22))[-c(18,19,20,22)]

#For variables without 'still'
conditions_data_frame_v0 <- usoc_long %>%
  select(pidp,wave_num,hcond_without_still) %>%
  mutate_at(vars(hcond_without_still), firstclean) %>%
  group_by(pidp) %>%
  mutate_at(vars(hcond_without_still), carryforwardfun) %>%
  ungroup()

#For variables with 'still'
conditions_data_frame_v1 <- usoc_long %>%
  select(pidp,wave_num,hcond_with_still,hconds_vars) %>%
  mutate_at(vars(hcond_with_still), firstclean)

#Manual correction using 'still'
conditions_data_frame_v1corr = matrix(NA,nrow = nrow(conditions_data_frame_v1),
                                      ncol = (length(hcond_with_still)+2)) %>% as.data.frame()
conditions_data_frame_v1corr[,1] <- conditions_data_frame_v1$pidp
conditions_data_frame_v1corr[,2] <- conditions_data_frame_v1$wave_num
names(conditions_data_frame_v1corr)[1:2] <- c("pidp","wave_num")

for (k in (1:length(hcond_with_still))){
  #Original binary variable
  hcond_orig <- conditions_data_frame_v1 %>%
    select(hcond_with_still[k])
  #'Still' variable
  hconds_arg <- conditions_data_frame_v1 %>%
    select(hconds_vars[k])
  #Combine into a df
  aux_df <- data.frame(hcond_orig,hconds_arg)
  names(aux_df) <- c("hcond_orig","hconds_arg")
  #New variable using combined information
  hcond_corr <- aux_df %>%
    mutate(.,hcond_corr=case_when(hconds_arg=="2"|hcond_orig=="0" ~ "0",
                                  hcond_orig=="1" ~ "1",
                                  is.na(hcond_orig) ~ "NA",
                                  TRUE ~ "NA")) %>%
    pull(hcond_corr) %>%
    as.numeric()
  #Add into matrix
  conditions_data_frame_v1corr[,(2+k)] <- hcond_corr
  names(conditions_data_frame_v1corr)[(2+k)] <- hcond_with_still[k]
  #Clean up
  rm(hcond_orig,hconds_arg,aux_df,hcond_corr)
}

#Carry forward the manual correction
conditions_data_frame_v1corr <- conditions_data_frame_v1corr %>%
  group_by(pidp) %>%
  mutate_at(vars(hcond_with_still), carryforwardfun) %>%
  ungroup()

#Merge two groups of conditions and add their names

# hcond1 	Asthma 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond2 	Arthritis 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond3 	Congestive heart failure 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond4 	Coronary heart disease 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond5 	Angina 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond6 	Heart attack or myocardial infarction 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond7 	Stroke 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond8 	Emphysema 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond9 	Hyperthyroidism or an over-active thyroid 	indresp 	1, 3, 4, 5, 6, 7, 8, 9
# hconde6 	ever had health condition: heart attack or myocardial infarction 	indresp 	3
# hconde7 	ever had health condition: stroke 	indresp 	3
# hcond10 	Hypothyroidism or an under-active thyroid 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond11 	Chronic bronchitis 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond12 	Any kind of liver condition 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond13 	Cancer or malignancy 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond14 	Diabetes 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond15 	Epilepsy 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond16 	High blood pressure 	indresp 	1, 3, 4, 5, 6, 7, 8, 9, 10, 11
# hcond17 	Clinical depression 	indresp 	1, 3, 4, 5, 6, 7, 8, 9
# hcond18 	Other long standing/chronic condition, please specify 	indresp 	6, 7, 8, 9, 10, 11
# hcond19 	Multiple Sclerosis 	indresp 	7, 8, 9, 10, 11
# hcond21 	COPD (Chronic Obstructive Pulmonary Disease) 	indresp 	10, 11
# hcond22 	An emotional, nervous or psychiatric problem 	indresp 	10, 11

#Clean up and add disease names
conditions_data <- left_join(conditions_data_frame_v0,
                             conditions_data_frame_v1corr,by=c("pidp","wave_num")) %>%
  dplyr::rename(asthma="hcond1",arthritis="hcond2",congestive_heart_fail="hcond3",chd="hcond4",
                angina="hcond5",heart_attack="hcond6",stroke="hcond7",emphysema="hcond8",
                hyper_thyroid="hcond9",hypo_thyroid="hcond10",bronchitis="hcond11",
                liver="hcond12",cancer="hcond13",diabetes="hcond14",epilepsy="hcond15",
                hypertension="hcond16",depression="hcond17",multiple_sclerosis="hcond19",
                copd="hcond21",emotional_disorder="hcond22") %>%
  mutate_at(vars(c("asthma","congestive_heart_fail","chd","heart_attack","stroke","emphysema","cancer","diabetes","hypertension","depression","copd","emotional_disorder")), yesnofun)

rm(conditions_data_frame_v0,conditions_data_frame_v1,conditions_data_frame_v1corr)
rm(hcond_without_still,hcond_with_still,hconds_vars)

#Merge findings back in
usoc_long <- left_join(usoc_long,conditions_data,by=c("pidp","wave_num"))
rm(conditions_data)