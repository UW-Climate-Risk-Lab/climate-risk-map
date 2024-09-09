import os

import pipeline

if __name__=="__main__":
    ssps = os.getenv("SSP")

    # Split the string into a list
    if ssps:
        ssp_list = ssps.split(",")
    else:
        raise ValueError("No SSPs provided in ENV variable") # Default to an empty list if the variable is not set


    for ssp in ssp_list:
        pipeline.main(ssp=ssp)


