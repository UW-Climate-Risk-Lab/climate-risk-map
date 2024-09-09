import logging
import os

import pipeline

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    ssps = os.getenv("SSP")

    # Split the string into a list and converts to ints
    if ssps:
        ssp_list = list(map(int, ssps.split(",")))
    else:
        raise ValueError(
            "No SSPs provided in ENV variable"
        )  # Default to an empty list if the variable is not set

    for ssp in ssp_list:
        try:
            logger.info(f"STARTING PIPELINE FOR SSP {str(ssp)}")
            pipeline.main(ssp=ssp)
            logger.info(f"PIPELINE SUCCEEDED FOR SSP {str(ssp)}")
        except Exception as e:
            logger.error(f"WARNING: PIPELINE FAILED FOR SSP {str(ssp)}: {str(e)}")
