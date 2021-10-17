### HOW TO BUILD THE DATASETS ###
1 -> run the build_datasets.py ONLY THE FIRST TIME (if they are already created, it will erase everything)
2 -> run update_datasets.py

### ONCE YOU GATHERED ENOUGH DATA ###
3 -> run the "sbt run" command to run the builder.scala code
4 -> enter the /parsed_datasets/0_init/CITY_NAME.json and manually rename all the *.json files to CITY_NAME.json and export them to /parsed_datasets/0_init
5 -> go in /parsed_datasets and run final_parser.py
6 -> datasets will be in /1_final folder

### DATASETS ARE READY TO BE USED IN PANDAS ###
