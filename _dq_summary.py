import pandas as pd
dq = pd.read_csv(r".\cleaned_data\data_quality_report.csv")
res = (dq.groupby(["severity","issue_type"])
         .size().reset_index(name="count")
         .sort_values(["severity","count"], ascending=[True, False]))
print(res.to_string(index=False))
