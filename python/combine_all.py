import os
import pandas as pd

# Combine the output of the individual distributions into one

base_path = "final_output/individual_query_results/"
files = os.listdir(base_path)

distributions = []
for file in files:
    csv = pd.read_csv(base_path + file, header=None)

    if csv.iloc[0][0][:2] != "0x":
        raise Exception('Distribution file should not have header')
        
    csv.columns =["Address", file.replace(".csv", "")] 
    distributions.append(csv)

joined = pd.DataFrame(columns=['Address'])

# Merge all individual distributions
for distribution in distributions:
    joined = pd.merge(joined,distribution, how='outer', on='Address')

joined = joined.fillna(0)
joined["Total"] = joined.sum(axis=1)
joined = joined.sort_values('Total', ascending=False)
joined.to_csv("final_output/per_campaign.csv",index=False)

summed = joined[["Address", "Total"]]
summed.to_csv("final_output/summed.csv",index=False)