# Air Traffic scaling, delay- analysis and prediction using PySpark on Hadoop Cluster

## To start running the code of this project you need to set up a cluster using Hadoop and Spark
### Set up an Apache Hadoop cluster
https://codethief.io/hadoop101/

### Set up an Apache Spark cluster
https://medium.com/ymedialabs-innovation/apache-spark-on-a-multi-node-cluster-b75967c8cb2b

### Install the mpl_toolkits.basemap package which is essential for scaling and simulation,
https://askubuntu.com/questions/555474/problem-importing-a-module-mpl-toolkits-basemap-in-python

### Install other packages
by using the popular "pip install <pachkage_name>" command

## Run an example of Scaling and simulation

### Install the following prerequisted libraries:
1- PySpark
2- datetime
3- time
4- mpl_toolkits.basemap
5- argparse
6- sys
7- json
8- time
9- os
10- shutil
11- matplotlib
12- datetime
13- matplotlib.patches
14- imageio
15- json
16- cv2

### Navigate to the specific folder in project
cd dat500_project/1_scaling_and_simulation

### Run the prepare and preprocessing script on hadoop cluster
python 1_prepare.py 

### Run the scaling step on hadoop cluster
python 2_scale.py --start_datetime="2019-04-10 00:00:00" --end_datetime="2019-04-10 23:59:59" --output_folder="scaled_data_2019_04_10"

### Run the simulation on local machine or hadoop cluster
python 3_simulate.py --input_folder=scaled_data_2019_04_10 --north="42" --south="36" --east="-101" --west="-110" --tag="AIRPORT" --start_datetime="2019-04-10 18:00:00" --end_datetime="2019-04-10 18:30:00" --output_filename="4_Colorado-2019-04-10-from-180000-to-183000-AIRPORT" --keep_snapchat_images="0" --gif_duration=1

The command "spark-submit" can also be used

### The output of the simulation is placed under the folder
4_Colorado-2019-04-10-from-180000-to-183000-AIRPORT


## Run an example of Delay analysis

###Install the following prerequisted libraries:
1- pyspark
2- json
3- datetime

### Navigate to the specific folder in project
cd dat500_project/2_delay_statistics

### Run the analyzing script based on the example initial content in analyze_config.json
python analyze_config.json

### To add a different desired input, study the configurations in analyze_config.json and the dataset csv head and feautres
### You need the vim tool or any other text editor
vim analyze_json.json

### To view the power BI model, install power bi desktop using the link below and then open the file "delay_analysis_power_bi_model.pbix" using power bi desktop
https://powerbi.microsoft.com/en-us/desktop/


## Run an example of Delay Prediction

### Install the following prerequisted libraries:
1- pyspark
2- sys

### Run the script of Decision Tree
python descision_tree.py

### Run the script of Random Forests
python random_forest.py

### Run the script of Logistic Regression
python logistic.py