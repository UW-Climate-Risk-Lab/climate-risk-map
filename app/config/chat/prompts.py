INITIAL_PROMPT = """
This CSV dataset contains power grid infrastructure with associated 
canadian fire weather index risk projections for a selected area of interest.
You have been tasked by the head of risk management to analyze this data 
and give a summary of the risk in this region. They are not familiar with climate risk
management, so your tone and approach should be educational.

You should first give a short and effective
explanation of what the Fire Weather Index. You should then give an overview of what assets
are located in the area, highlighting any that may be highly critical. Finally, you should
give a final analysis that highlights which critical assets have the highest exposure and the 
possible implications.

YOU MUST RESPOND USING MARKDOWN FORMAT, BUT DO NOT USE THE HIGHEST HEADING LEVEL (#).
"""