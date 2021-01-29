""" e6dataflow is a data processing package designed with particular datastream architectures in mind.

This package was designed to process data from an atomic physics experiment at the Univeristy of California, Berkeley.
This experiment is run continuously, producing data from multiple data sources every few seconds. Every data generation
event is referred to as a shot. The experiment runs repeatedly but may step through a list of parameters in a loop.
shot-0 may have one set of parameters, shot-1 may have another set, etc. After all parameters have been stepped through
the list of parameters repeats. The collection of shots with the same set of experimental parameters are referred to as
a point. All of the shots from a given repetition of the parameter list are referred to as a loop. Thus every shot can
be specified by either its shot number or its loop and point number (if the number of points is known).

This data processing package is designed with this data collection pattern in mind. In particular the goal of this
software is to analyze and visualize the data  'in real time' as the data are collected. A secondary purpose of this
package is to save the results of the data processing in a portable, lightweight format. These processed results can
then be subsequently analyzed in a location remote from the raw data. The advantage here is that it would be possible
for a person to analyze the data without requiring access to the large raw data files.

A distinction is drawn between data processing and data analyzing. Data processing encompasses generic ways data might
be processed independent of the particular experiment being run. It gives a high-level view of the experiment
performance. Data analysis, on the other hand, is experiment specific and might vary from one run of the experiment
to the next depending on the particular questions being asked. e6dataflow is meant to be used for data processing. The
lightweight, processed, stored data described above should be used for subsequent run-specific data analysis.

These goals are facillitated by DataModel objects. DataModels contain within them many helper objects called DataTools.
There are the following types of DataTools:
DataStream: Pointer to directories containing raw data. Raw data is stored as .h5 files presently.
DataFields: Abstract pointers to stored data. May point to raw data .h5 files, processed data stored in .h5 files,
or processed data stored within the so-called data_dict within the DataModel. There are ShotDataFields which sort data
by shot and PointDataFields which sort data by point.
Processors: Processors take data from one or more ShotDataFields, apply some transformation to it, and store the results
in one or more new ShotDataFields.
Aggregators: Aggregators combine all the data from all shots within a particular point in some way and store the results
in a PointDataField. For example, a common aggregation would be to average all of the data over the shots within a
particular point.
Reporter: The job of Reporters it to visualize the data stored in the DataFields.

Originally Written: January 2021
Last Updated: January 2021
Author: Justin Gerber (justin.gerber48@gmail.com)
"""