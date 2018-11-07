/*******************************************************************************	

Project:  		KePSIE
Created on:  	Oct 30, 2018
Last udpate on: Nov 1, 2018

Written by:  	Thomas Escande (tescande@worldbank.org)

Revision: 		

********************************************************************************			

THIS DO FILE: 	This do-file construct the average demand outcomes and deals with outliers 


CONTENT:		Part 0 - Create globals for directories and files 
				Part 1 - Set all data from 2017 as missing for facilities which started in 2018
				Part 2 - Identifying untrustful Data
						2.1 Set as untrustful months with days of missing data 
				Part 3 - Identifying outliers, set them as missing 
						3.1 Checking whether 0 are likely to be outliers or due to small facility / about to close facility 
						3.2 Outliers with a high max/min ratio 
						3.3 Outliers when there are only two months available
				Part 4 - Estimation of daily data from other sources 
						4.1 Estimation from weekly data sources
						4.2 Estimation from daily data sources
				Part 5: Construction of final variables with inputs
						5.1 Outpatients1
						5.2 Outpatients2
						5.3 Outpatients3
						5.4 Outpatients4
				Part 6: Save dataset

INPUTS:     	* Cleaned data taken from the fees/ outpatient demand cleaned database
				"${KePSIE_DB}/Endline/DataSets/Intermediate/Fees_Outpatient_clean_end.dta"


OUTPUTS:     	
				* Dropbox files: Latest versions created by person in charge (Thomas Escande)
				
					* Database with final outpatient outcomes ready for analysis
					"${KePSIE_DB}/Endline/DataSets/Intermediate/Averages_Outpatient_end.dta"
					
				
				* Local files: Produced when running this code and saved in your local GitHub folder
				
					* Database with final outpatient outcomes ready for analysis
					"${end_dt}/Intermediate/Averages_Outpatient_end.dta"
					

*******************************************************************************/

********************************************************************************
	** Part 0: Create globals for directories and files 
********************************************************************************


clear all
set more off				

	* Input files
	
	global Fees_Outpatient_final		 "C:/Users/`c(username)'/Dropbox/Peer Review 2018 - CDD/Thomas_Data/Fees_Outpatient_clean_end.dta"

	
	* Output files: 
	 
	global Av_Outpatient 			"C:/Users/`c(username)'/Dropbox/Peer Review 2018 - CDD/Thomas_Data/Averages_Outpatient_end.dta"



		use "${Fees_Outpatient_final}", clear


*Defining locals needed for the do file
*Number of days per month depending on how many days the facility is open 
*November
local days_a 		= 30
local days_a_6dw 	= 26 //assuming not working on sundays
local days_a_5dw 	= 22 //assuming not working on saturdays and sundays
local days_a_4dw 	= 18 //assuming not working on Mondays, saturdays and sundays

*December
local days_b 		= 31
local days_b_6dw 	= 26 //assuming not working on sundays
local days_b_5dw 	= 21 //assuming not working on saturdays and sundays
local days_b_4dw 	= 17 //assuming not working on Mondays, saturdays and sundays

*January
local days_c 		= 31
local days_c_6dw 	= 27 //assuming not working on sundays
local days_c_5dw 	= 23 //assuming not working on saturdays and sundays
local days_c_4dw 	= 19 //assuming not working on Mondays, saturdays and sundays

*February
local days_d 		= 28
local days_d_6dw 	= 24 //assuming not working on sundays
local days_d_5dw 	= 20 //assuming not working on saturdays and sundays
local days_d_4dw 	= 16 //assuming not working on Mondays, saturdays and sundays

*March
local days_e 		= 31
local days_e_6dw 	= 27 //assuming not working on sundays
local days_e_5dw 	= 22 //assuming not working on saturdays and sundays
local days_e_4dw 	= 18 //assuming not working on Mondays, saturdays and sundays

*April
local days_f 		= 30
local days_f_6dw 	= 25 //assuming not working on sundays
local days_f_5dw 	= 21 //assuming not working on saturdays and sundays
local days_f_4dw 	= 16 //assuming not working on Mondays, saturdays and sundays

*May
local days_g 		= 31
local days_g_6dw 	= 27 //assuming not working on sundays
local days_g_5dw 	= 23 //assuming not working on saturdays and sundays
local days_g_4dw 	= 19 //assuming not working on Mondays, saturdays and sundays


*Variable useful to know how many observations were put to missing in each case.
foreach month in a b c d e f g {
	gen id_change_`month'=0 if !mi(outpatient_`month')
	}

****************************************************************************************************
*Part 1: Set all data from 2017 as missing for facilities which started in 2018
****************************************************************************************************
tab outpatient_a if startyear == 2018 
tab outpatient_b if startyear == 2018 
*6 facilities have 0 and three facilities (18728, 510120, 588101) have positive numbers. 

foreach month in a b {
	replace id_change_`month'  = 1  if startyear == 2018 & outpatient_`month' == 0	 & !mi(id_change_`month')
	replace outpatient_`month' = .  if startyear == 2018 & outpatient_`month' == 0

	replace id_change_`month'  = 1  if startyear == 2018 & outpatient_`month' >  0 & !mi(outpatient_`month') & !mi(id_change_`month')
	replace outpatient_`month' = .q if startyear == 2018 & outpatient_`month' >  0 & !mi(outpatient_`month')
}


****************************************************************************************************
*Part 2: Identifying untrustful Data
****************************************************************************************************

	*--------------------------------------------------------*
	* 2.1 Set as untrustful months with days of missing data *
	*--------------------------------------------------------*

foreach month in a b c d e f g {
	gen 	  outpatient_complete_`month' 	= outpatient_`month' 
	replace   outpatient_complete_`month' 	= .q   					if !mi(outpatient_miss_`month') & outpatient_miss_`month' > 0 
	replace   id_change_`month'  			= 2 					if !mi(outpatient_miss_`month') & outpatient_miss_`month' > 0  & !mi(id_change_`month')

	label var outpatient_complete_`month'  "$`month':Monthly outpatients without missing data" 
}

*Number of months missing per facilities
	gen month_partial_complete = 0
	gen month_complete = 0

	label var month_partial_complete  	"Number of months with some data"
	label var month_complete 			"Number of months complete"

	foreach month in a b c d e f g {
		replace month_partial_complete 	= month_partial_complete + 1 	if !mi(outpatient_`month') 		
		replace month_complete 			= month_complete + 1 			if !mi(outpatient_complete_`month') 	
			}

*Analysing the changes
	tab month_partial_complete
	tab month_complete

	gen distrust_data = 0

	foreach month in a b c d e f g {
replace distrust_data = distrust_data + 1 if id_change_`month' == 2
	}

	tab distrust_data
	list outpatient_miss_* if distrust_data == 1 
 
	

*10.64 % have no or incomplete data out of which 9.32% have no data at all.
*87.5% have at least one month of complete data. - check again after elimination of outliers


****************************************************************************************************
*Part 3: Identifying outliers, set them as missing 
****************************************************************************************************
*Generate locals to play with tradeoffs / different definitions of outliers
		local max_outlier = 15  


*************Identifying max and min values for later construction
global complete outpatient_complete_a outpatient_complete_b outpatient_complete_c outpatient_complete_d outpatient_complete_e outpatient_complete_f outpatient_complete_g

egen outpatient_complete_max = rowmax($complete)
egen outpatient_complete_min = rowmin($complete)


************Creating new variables without outliers using the complete one*********
foreach month in a b c d e f g {
gen outpatient_noutlier_`month' = outpatient_complete_`month'
label var outpatient_noutlier_`month' "$`month':Monthly outpatients without missing data or outliers"
}


	*--------------------------------------------------------*
	* 3.1 Checking whether 0 are likely to be outliers or due to small facility / about to close facility 
	*--------------------------------------------------------*

foreach month in a b c d e f g {
	*sum outpatient_complete_`month' //There are some 0 which is an issue. Maximums are extremly high
replace outpatient_noutlier_`month' = .o if outpatient_complete_max     == 0 & outpatient_complete_min == 0
replace   id_change_`month'  		= 3  if outpatient_complete_max     == 0 & outpatient_complete_min == 0  & !mi(id_change_`month')
}

*Checking for each observation
*list hfid outpatient_noutlier_a outpatient_noutlier_b outpatient_noutlier_c outpatient_noutlier_d outpatient_noutlier_e outpatient_noutlier_f  outpatient_noutlier_g if outpatient_noutlier_a == 0 | outpatient_noutlier_b == 0 | outpatient_noutlier_c == 0 | outpatient_noutlier_d == 0 | outpatient_noutlier_e == 0 | outpatient_noutlier_f == 0 | outpatient_noutlier_g == 0

foreach month in a b c d e f g {
replace outpatient_noutlier_`month' = .o if outpatient_noutlier_`month' == 0 & outpatient_complete_max > `max_outlier' 
replace   id_change_`month'  		= 3  if outpatient_noutlier_`month' == 0 & outpatient_complete_max > `max_outlier'  & !mi(id_change_`month')
}

*************Identifying new max and min values without 0 outliers
global complete_noutlier outpatient_noutlier_a outpatient_noutlier_b outpatient_noutlier_c outpatient_noutlier_d outpatient_noutlier_e outpatient_noutlier_f outpatient_noutlier_g

egen outpatient_noutlier_max = rowmax($complete_noutlier)
egen outpatient_noutlier_min = rowmin($complete_noutlier)
egen month_complete_noutlier = rownonmiss($complete_noutlier)
egen outpatient_noutlier_sum = rowtotal($complete_noutlier)

label var month_complete_noutlier 			"Number of months complete"
label var outpatient_noutlier_sum  			"Total number of patients recorded"
	


	*--------------------------------------------------------*
	* 3.2 Outliers with a high max/min ratio 
	*--------------------------------------------------------*

*************Identifying potential outliers
*Generate locals to play with tradeoffs / different definitions of outliers
		*For minimum outliers
		local ratio_extrem 		= 10  	//Ratio minmax above which outliers are really extrem
		local ratio_outlier 	= 2   //Ratio minmax above which data are considered as outliers 
		local dist_minmax_min	= 30  	//Distance between min and max above which min data can potentially be an outlier
		local dist_minmax_max	= 40  	//Distance between min and max above which max data can potentially be an outlier
		local limit_min  		= 0.4 	//Ratio min to average without max below which the min is the outlier
		local limit_max 		= 2.25 

*Looking at the data
gen ratio_minmax =  outpatient_noutlier_max / outpatient_noutlier_min if !mi(outpatient_noutlier_min)
list $complete_noutlier 											  if ratio_minmax > `ratio_extrem' & !mi(ratio_minmax)

*Analysis of outliers
histogram ratio_minmax, frequency
histogram ratio_minmax if ratio_minmax < 10 , frequency
histogram ratio_minmax if ratio_minmax < 5 , frequency


gen 	outlier 	= .n  // only applicable if we have at least 3 observation. If only two observations far apart, can discreminate using the other estimation 
replace outlier 	= 0 if month_complete_noutlier > 2
replace outlier 	= 1 if ratio_minmax > `ratio_outlier' & month_complete_noutlier > 2
tab 	outlier

* average without the minimum
gen 	outpatient_noutlier_av_womin 	= ((outpatient_noutlier_sum-outpatient_noutlier_min)/(month_complete_noutlier-1)) 	if month_complete_noutlier >  1
replace outpatient_noutlier_av_womin 	= ((outpatient_noutlier_sum)/(month_complete_noutlier)) 							if month_complete_noutlier == 1

* average without the max
gen 	outpatient_noutlier_av_womax 	= ((outpatient_noutlier_sum-outpatient_noutlier_max)/(month_complete_noutlier-1)) 	if month_complete_noutlier >  1
replace outpatient_noutlier_av_womax 	= ((outpatient_noutlier_sum)/(month_complete_noutlier)) 						 	if month_complete_noutlier == 1

* average for all
gen 	outpatient_noutlier_av_all 		= ((outpatient_noutlier_sum)/(month_complete_noutlier))


* this seems good for the minimums that seem outliers for sure (0.4 *min < averege without the max--> outlier=1... this avoids confusing with then the max is the outlier) 
gen  	dist_maxmin_var 	= outpatient_noutlier_max - outpatient_noutlier_min

gen 	ratio_min_av_womin	= outpatient_noutlier_min / outpatient_noutlier_av_womin
gen 	ratio_max_av_womax	= outpatient_noutlier_max / outpatient_noutlier_av_womax


*Analysis of outliers
hist  	ratio_max_av_womax, frequency
hist  	ratio_max_av_womax if outlier==1, frequency
hist  	ratio_min_av_womin, frequency
hist  	ratio_min_av_womin if outlier==1, frequency


gen 	outpatient_noutlier_test_av1 = 0
replace outpatient_noutlier_test_av1 = 1 if ratio_min_av_womin < `limit_min' & dist_maxmin_var > `dist_minmax_min' 
replace outpatient_noutlier_test_av1 = . if month_complete_noutlier <= 2

	
	* So, let's exclude this first to avoid confusion with the top outliers

foreach month in a b c d e f g {
	replace   id_change_`month'  		= 4  if outpatient_noutlier_`month' == outpatient_noutlier_min & outpatient_noutlier_test_av1 == 1	 & !mi(id_change_`month') 
	replace outpatient_noutlier_`month' = .o if outpatient_noutlier_`month' == outpatient_noutlier_min & outpatient_noutlier_test_av1 == 1 
	}
	

*this seems good for the top outliers (max > 2.25 * averagewithout the max --> outlier=1)
gen 	outpatient_noutlier_test_av2 = 0
replace outpatient_noutlier_test_av2 = 1 if ratio_max_av_womax > `limit_max' & dist_maxmin_var > `dist_minmax_max' 
replace outpatient_noutlier_test_av2 = . if month_complete_noutlier <= 2

	
	* So, let's exclude this first to avoid confusion with the top outliers

foreach month in a b c d e f g {
	replace   id_change_`month'  		= 4  if outpatient_noutlier_`month' == outpatient_noutlier_max & outpatient_noutlier_test_av2 == 1   & !mi(id_change_`month')
	replace outpatient_noutlier_`month' = .o if outpatient_noutlier_`month' == outpatient_noutlier_max & outpatient_noutlier_test_av2 == 1
	}

foreach month in a b c d e f g {
	tab id_change_`month'
}

	*--------------------------------------------------------*
	* 3.3 Outliers when there are only two months available 
	*--------------------------------------------------------*
*Dealing with potential outliers for facilities with two months only (but two months before the current change)
*Only five facilities, deal with it tomorrow
	gen 	outlier2 = 0
	replace outlier2 = 1 if ratio_minmax > `ratio_outlier' & month_complete_noutlier == 2 & !mi(ratio_minmax)

	list hfid ratio_minmax $complete_noutlier outpatient_w  if outlier2 == 1 //all of them have self reported weekly data

*Let's use the weekly data to discriminate between the min and the max
*For this, compare the daily averages from both method
gen 		day_w = outpatient_w / daysopen if outlier2 == 1 

foreach month in a b c d e f g {
	gen 	outpatient_noutlier_daily_`month' = outpatient_noutlier_`month' / `days_`month'' 	  if outlier2 == 1 & (daysopen == 7)  
	replace outpatient_noutlier_daily_`month' = outpatient_noutlier_`month' / `days_`month'_6dw'  if outlier2 == 1 & (daysopen == 6 | mi(daysopen) ) 
	replace outpatient_noutlier_daily_`month' = outpatient_noutlier_`month' / `days_`month'_5dw'  if outlier2 == 1 & (daysopen == 5  )
	replace outpatient_noutlier_daily_`month' = outpatient_noutlier_`month' / `days_`month'_4dw'  if outlier2 == 1 & (daysopen <= 4  )
}

global out_daily_month outpatient_noutlier_daily_a outpatient_noutlier_daily_b outpatient_noutlier_daily_c outpatient_noutlier_daily_d outpatient_noutlier_daily_e outpatient_noutlier_daily_f outpatient_noutlier_daily_g


	egen outpatient_daverage_max 	= rowmax($out_daily_month)
	egen outpatient_daverage_min 	= rowmin($out_daily_month)
	gen  diff_max					= outpatient_daverage_max   - day_w
	gen  diff_min					= -(outpatient_daverage_min - day_w)


*Putting to missing when needed. 	
foreach month in a b c d e f g {
	replace   id_change_`month'  		= 5  if outlier2 == 1 & diff_max < diff_min & outpatient_noutlier_`month' == outpatient_noutlier_min
	replace   id_change_`month'  		= 5  if outlier2 == 1 & diff_max < diff_min & outpatient_noutlier_`month' == outpatient_noutlier_max

	replace outpatient_noutlier_`month' = .o if outlier2 == 1 & diff_max < diff_min & outpatient_noutlier_`month' == outpatient_noutlier_min
	replace outpatient_noutlier_`month' = .o if outlier2 == 1 & diff_max > diff_min	& outpatient_noutlier_`month' == outpatient_noutlier_max
	}

drop day_w outpatient_noutlier_daily_* outpatient_daverage_max outpatient_daverage_min diff_max diff_min //As we will compute them again later. 

	*Computing again the number of complete months
	egen month_complete_noutlier_final = rownonmiss($complete_noutlier)
	tab  month_complete_noutlier_final 

****************************************************************************************************
*Part 4 - Estimation of daily data from other sources 
****************************************************************************************************

	*--------------------------------------------------------*
	* 4.1 Estimation from weekly data sources
	*--------------------------------------------------------*

*Estimate the daily average using the number of patients over the last week divided by the number of days the facility is open
gen 		day_w = outpatient_w / daysopen
label var 	day_w "average daily outpatient from weekly data"

	*--------------------------------------------------------*
	* 4.2 Estimation from daily data sources
	*--------------------------------------------------------*
gen 		day_d = (outpatient_dm1 + outpatient_dm2) / 2
label var 	day_d "average daily outpatient from daily data"


***************************************************************************************************
*Part 5: Construction of final variables with inputs
***************************************************************************************************


gen 		outpatients1_d  = .
label var  	outpatients1_d "Daily outpatient from previous week"

gen 		outpatients2_d  = .
label var  	outpatients2_d "Daily outpatient, average over available months"

gen 		outpatients3_d  = .
label var  	outpatients3_d "Daily outpatient, average over January"

gen 		outpatients4_d  = .
label var  	outpatients4_d "Daily outpatient, average over February"



	*--------------------------------------------------------*
	* 5.1 Outpatients1
	*--------------------------------------------------------*

replace outpatients1_d = day_w
replace outpatients1_d = day_d if mi(day_w) & !mi(day_d)


	*--------------------------------------------------------*
	* 5.2 Outpatients2
	*--------------------------------------------------------*

*Averaging over each month
foreach month in a b c d e f g {
	gen 	outpatient_noutlier_daily_`month' = outpatient_noutlier_`month' / `days_`month'' 	  if daysopen == 7  
	replace outpatient_noutlier_daily_`month' = outpatient_noutlier_`month' / `days_`month'_6dw'  if daysopen == 6 | mi(daysopen)  
	replace outpatient_noutlier_daily_`month' = outpatient_noutlier_`month' / `days_`month'_5dw'  if daysopen == 5  
	replace outpatient_noutlier_daily_`month' = outpatient_noutlier_`month' / `days_`month'_4dw'  if daysopen <= 4  
}

*Inputing whenever possible with data from the weekly or daily
foreach month in a b c d e f g {
	replace outpatient_noutlier_daily_`month' = outpatients1_d if mi(outpatient_noutlier_daily_`month') & !mi(outpatients1_d) 
}


global out_daily_month outpatient_noutlier_daily_a outpatient_noutlier_daily_b outpatient_noutlier_daily_c outpatient_noutlier_daily_d outpatient_noutlier_daily_e outpatient_noutlier_daily_f outpatient_noutlier_daily_g

	egen  	outpatient_noutlier_daymean = rowmean($out_daily_month)
	replace outpatients2_d 				= outpatient_noutlier_daymean


	*--------------------------------------------------------*
	* 5.3 Outpatients3
	*--------------------------------------------------------*

	replace outpatients3_d 				= outpatient_noutlier_daily_c


	*--------------------------------------------------------*
	* 5.4 Outpatients4
	*--------------------------------------------------------*


	replace outpatients4_d 				= outpatient_noutlier_daily_d


*tab final variables
count if mi(outpatients1_d)
count if mi(outpatients2_d)
count if mi(outpatients3_d)
count if mi(outpatients4_d)

*regression to check for correlation
reg outpatients2_d outpatients1_d
reg outpatients2_d outpatients3_d
reg outpatients2_d outpatients4_d

********************************************************************************

	** Part 6: Save dataset

********************************************************************************	

	if c(stata_version)>=14 {
		saveold "${Av_Outpatient}", replace v(13)
	}
	else {
		save "${Av_Outpatient}", replace
	}
	

************************************************End of do file****************************************************
