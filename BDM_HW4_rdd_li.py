from pyspark import SparkContext
import datetime
from datetime import datetime
from datetime import timedelta
import csv
import functools
import json
import numpy as np
import sys

def main(sc):
    '''
    Transfer our code from the notebook here, however, remember to replace
    the file paths with the ones provided in the problem description.
    '''
    rddPlaces = sc.textFile('/data/share/bdm/core-places-nyc.csv')
    rddPattern = sc.textFile('/data/share/bdm/weekly-patterns-nyc-2019-2020/*')
    OUTPUT_PREFIX = sys.argv[1]


    
    CAT_CODES = {'445210', '445110', '722410', '452311', '722513', '445120', '446110', '445299', '722515', '311811', '722511', '445230', '446191', '445291', '445220', '452210', '445292'}
    CAT_GROUP = {'452210': 0, '452311': 0, '445120': 1, '722410': 2, '722511': 3, '722513': 4, '446110': 5, '446191': 5,  '722515': 6, '311811': 6, '445210': 7, '445299': 7, '445230': 7, '445291': 7, '445220': 7, '445292': 7,'445110': 8}
    


  #########################
    def filterPOIs(_, lines):
      # TO_BE_COMPLETED
        reader = csv.reader(lines)
        for row in reader:
           if row[9] in CAT_CODES:
               yield (row[0], CAT_GROUP[row[9]]) # (placekey, group_id)

    rddD = rddPlaces.mapPartitionsWithIndex(filterPOIs) \
            .cache()

    ####################
    storeGroup = dict(rddD.collect())
    groupCount = rddD \
        .map(lambda x: (x[1], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[0]) \
        .map(lambda x: x[1]) \
        .collect()



 
    #########################
    #  0: placekey
    # 12: date_range_start
    # 14: raw_visit_counts
    # 16: visits_by_day
    def extractVisits(storeGroup, _, lines):
        #TO_BE_COMPLETED
        if _ == 0:
          next(lines)
        reader = csv.reader(lines)
        for line in reader:
          placekey, date_range_start, raw_visit_counts, visits_by_day = line[0],line[12],line[14],json.loads(line[16])
          if placekey in storeGroup.keys():
              group_id = storeGroup[placekey]
              start_date = datetime.strptime(date_range_start[:10], "%Y-%m-%d")
              dates = []
              for _ in range(7):
                dates.append(str(start_date + timedelta(days=_))[:10])
              for index, date in enumerate(dates):
                  if date[:4] in ['2019', '2020']:
                      delta = (datetime.strptime(date, "%Y-%m-%d") - datetime(2019, 1, 1)).days                 
                      yield (group_id, delta), visits_by_day[index]

    rddG = rddPattern \
            .mapPartitionsWithIndex(functools.partial(extractVisits, storeGroup))



    

    ##########
    def computeStats(groupCount, _, records):
        #TO_BE_COMPLETED
       for row in records:
                group = row[0][0]
                count = groupCount[group]
                compute_list = list(row[1]) + list(np.zeros(count - len(list(row[1]))))
                median = np.median(compute_list)
                stdev = np.std(compute_list)
                low, high = max(0, median - stdev), max(0, median + stdev)
                yield row[0], (median, low, high)
    rddH = rddG.groupByKey() \
        .mapPartitionsWithIndex(functools.partial(computeStats, groupCount))



    ##################
    def to_csv_I(line):       
      date = datetime.strptime("2019-01-01","%Y-%m-%d") + timedelta(days=line[0][1])
      return line[0][0],  ",".join([str(date.year), str("2020" + date.strftime("%Y-%m-%d")[4:]), str(round(line[1][0])), str(round(line[1][1])), str(round(line[1][2]))])

    rddI = rddH.map(to_csv_I)


    #######
    rddJ = rddI.sortBy(lambda x: x[1][:15])
    header = sc.parallelize([(-1, 'year,date,median,low,high')]).coalesce(1)
    rddJ = (header + rddJ).coalesce(10).cache()



    

    filenames = ['big_box_grocers',
                 'convenience_stores',
                 'drinking_places',
                 'full_service_restaurants',
                 'limited_service_restaurants',
                 'pharmacies_and_drug_stores',
                 'snack_and_bakeries',
                 'specialty_food_stores',
                 'supermarkets_except_convenience_stores']

    for i, name in enumerate(filenames):
        rddJ.filter(lambda x: x[0] == i or x[0] == -1).values() \
            .saveAsTextFile(f'{OUTPUT_PREFIX}/{name}')



 
if __name__=='__main__':
    sc = SparkContext()
    main(sc)



