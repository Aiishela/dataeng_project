# Data Engineering Project Report - Triple Queenery

by BARATOVA Malika, DAI Qinshu, NOUR ELLIL Hafsa

## Abstract



## Datasets Description 

We used two datasets to answer our queries.

The first dataset is named **Olympic Historical Dataset From Olympedia.org** taken from Kaggle, it can be found at https://www.kaggle.com/datasets/josephcheng123456/olympic-historical-dataset-from-olympediaorg.
This dataset contains the information about the Winter and Summer Olympic games starting from the first edition in 1896 to the latest upcoming in 2032.
It is composed of six different csv files :
- Olympic_Athlete_Bio : data about each athlete that competed (e.g. name, country)
- Olympic_Athlete_Event_Results : each row represents the result of one athlete at one event (e.g. position and medal (if won) of athlete A at the 100 meters men event in the 1908 Summer Olympic games)
- Olympic_Games_Medal_Tally : medal tally of countries for each Olympic game, **this file was not used**
- Olympic_Results : each row represents the result of one event (e.g. when and where the 100 meters men event took place and its results), **this file was not used**
- Olympics_Country : list of countries names and their country code
- Olympics_Games : list of all the Olympic games (e.g. edition, year, city, start and end date)

The second dataset is named **Natural Disasters** taken from the site EM-DAT - The international disaster database, found at https://public.emdat.be/.
The dataset contains recorded natural disasters event from 1900 to 2025. Each row represents one natural disaster, the important information kept in the project are : country of origin, date and type of disaster. 

## Queries 

## Pipeline

### Data Ingestion

The natural disaster datataset was downloaded manually from the website https://public.emdat.be and placed in a folder in the project, because you need to be logged in and ask permission to be able to download it.

The Olympic games dataset is obtained by calling the kaggle API via Python.

### Data Wrangling

We chose to not use some of the Olympic games files : 
- the medal tally file could not be used because it is aggregated data. 
- the Olympic result file did not provide us with useful information for our queries.

/// ECRIRE ICI CE QUI A ETE CHANGE DANS LES AUTRES FICHIERS

For the natural disaster dataset, we couldn't use all columns (they weren't useful for our queries anyway), like the death tally and the total amount of money used for the reconstruction.
We considered removing events that happened before a certain date because there is not enough disaster recorded. We decided finally to keep all the rows, because even in the last years we are not sure if all the event happening in the world are correctly recorded in this dataset.
Unfortunately, contrary to the Olympic dataset, we can't be sure that the information in this dataset is 100% correct and collects all the natural disaster events.  


The dates of all the files where normalized to a ```datetime``` object in Python. It eases the comparison of the different events.

## Requirements and how to run

To be able to download the dataset from Kaggle, you have to follow those steps : 
1. Create a folder named ```.kaggle``` in the dataeng_project folder
2. In Kaggle, login to your account then go  Home -> Settings -> Scroll down to the section API -> Create New Token
3. Save the kaggle.json file that you just downloaded inside the folder ```.kaggle```




## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

